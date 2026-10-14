use crate::engine::resolve_redis_addr;
use parking_lot::RwLock;
use rekuiper_connectors::SqlConnectorConfig;
use rekuiper_core::TableManager;
use rekuiper_sql::{Evaluator, Expr, JoinClause, JoinType, RuleState, SelectStmt};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Resolve lookup JOINs for one stream record against table rows.
///
/// Starts from the base record (exposed both under bare field names and
/// `{from}.{field}` qualifiers) and folds each join clause in: the first
/// table row whose `ON` condition holds over the combined map wins and is
/// merged in (bare keys keep stream values via `or_insert`, plus
/// `{target}.{field}` qualifiers). Returns `None` when an `Inner` (or
/// `Right`/`Full`/`Cross` without match) join finds no row and the record
/// must be skipped; `Left` joins fall through un-joined.
/// Derive the point-lookup key for an external table join from an equality
/// `ON` condition between the stream side and the table side (either order).
/// Returns `(table_column, key_value)`; `None` when no usable key exists.
pub(crate) fn join_key_parts(
    join: &JoinClause,
    from: &str,
    from_alias: Option<&str>,
    combined: &HashMap<String, Value>,
) -> Option<(String, String)> {
    fn side(expr: &Expr, from: &str, from_alias: Option<&str>, target: &JoinClause) -> u8 {
        match expr {
            // 0 = stream side, 1 = table side, 2 = unknown.
            Expr::FieldAccess { parent, .. } => match parent.as_ref() {
                Expr::Identifier(name)
                    if name == &target.target
                        || target.alias.as_ref().is_some_and(|a| name == a) =>
                {
                    1
                }
                Expr::Identifier(name) if name == from || from_alias.is_some_and(|a| name == a) => {
                    0
                }
                _ => 2,
            },
            Expr::Identifier(_) => 0,
            _ => 2,
        }
    }
    fn column_name(expr: &Expr) -> Option<String> {
        match expr {
            Expr::FieldAccess { field, .. } => Some(field.clone()),
            Expr::Identifier(name) => Some(name.clone()),
            _ => None,
        }
    }
    fn scalarize(value: Value) -> Option<String> {
        match value {
            Value::Null => None,
            Value::String(s) => Some(s),
            Value::Number(n) => Some(n.to_string()),
            Value::Bool(b) => Some(b.to_string()),
            _ => None,
        }
    }
    let (left, right) = match join.on.as_ref()? {
        Expr::BinaryOp {
            left,
            op: rekuiper_sql::BinaryOperator::Eq,
            right,
        } => (left, right),
        _ => return None,
    };
    let (table_expr, key_expr) = match (
        side(left, from, from_alias, join),
        side(right, from, from_alias, join),
    ) {
        (1, _) => (left, right),
        (_, 1) => (right, left),
        _ => return None,
    };
    Some((
        column_name(table_expr)?,
        scalarize(Evaluator::eval_val(key_expr, combined))?,
    ))
}

/// Derive the point-lookup key value for a Redis table join.
pub(crate) fn extract_lookup_key(
    join: &JoinClause,
    from: &str,
    from_alias: Option<&str>,
    combined: &HashMap<String, Value>,
) -> Option<String> {
    join_key_parts(join, from, from_alias, combined).map(|(_, value)| value)
}

/// Build a single candidate row from a fetched lookup value: objects map to
/// rows directly, scalars bind under `"value"`.
pub(crate) fn lookup_value_to_row(value: Value) -> HashMap<String, Value> {
    match value {
        Value::Object(map) => map.into_iter().collect(),
        scalar => {
            let mut row = HashMap::new();
            row.insert("value".to_string(), scalar);
            row
        }
    }
}

/// Table anchor fallback for SQL configs: explicit `TABLE`, else
/// `DATASOURCE` (the documented stream/table anchor), else the object name.
pub(crate) fn sql_table_fallback(options: &HashMap<String, String>, name: &str) -> String {
    options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("TABLE"))
        .map(|(_, v)| v.clone())
        .filter(|v| !v.trim().is_empty())
        .or_else(|| {
            options
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("DATASOURCE"))
                .map(|(_, v)| v.clone())
                .filter(|v| !v.trim().is_empty())
        })
        .unwrap_or_else(|| name.to_string())
}

/// (`URL`, falling back to `DATASOURCE`, plus `TABLE` or the target name).
pub(crate) fn resolve_sql_lookup(
    table_manager: &TableManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    target: &str,
) -> Option<(String, String)> {
    let def = table_manager.get_table(target)?;
    if let Some(key) = def.options.get("CONF_KEY") {
        if !key.is_empty() {
            let lookup = format!("sql/{}", key);
            if let Some(conf_val) = source_configs.read().get(&lookup).cloned() {
                match serde_json::from_value::<SqlConnectorConfig>(conf_val) {
                    Ok(mut conf) => {
                        if conf.table.trim().is_empty() {
                            conf.table = sql_table_fallback(&def.options, target);
                        }
                        if conf.url.trim().is_empty() {
                            tracing::warn!("sql lookup config '{}' has no url", lookup);
                            return None;
                        }
                        return Some((conf.url, conf.table));
                    }
                    Err(e) => {
                        tracing::warn!("Invalid sql lookup config '{}': {}", lookup, e);
                        return None;
                    }
                }
            }
        }
    }
    let url = def
        .options
        .get("URL")
        .or_else(|| def.options.get("DATASOURCE"))
        .cloned()
        .unwrap_or_default();
    if url.is_empty() {
        return None;
    }
    let table = def
        .options
        .get("TABLE")
        .cloned()
        .unwrap_or_else(|| target.to_string());
    Some((url, table))
}

/// Fetch candidate rows for one join clause: a Redis `GET` point lookup for
/// `TYPE="redis"` tables, a SQL point lookup for `TYPE="sql"` tables,
/// otherwise the locally stored table rows.
pub(crate) async fn lookup_candidates(
    table_manager: &TableManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    from: &str,
    from_alias: Option<&str>,
    join: &JoinClause,
    combined: &HashMap<String, Value>,
) -> Vec<HashMap<String, Value>> {
    let table_type = table_manager
        .get_table(&join.target)
        .and_then(|def| def.options.get("TYPE").cloned())
        .unwrap_or_default();
    if table_type.eq_ignore_ascii_case("redis") {
        let Some(key) = extract_lookup_key(join, from, from_alias, combined) else {
            return Vec::new();
        };
        let conf_key = table_manager
            .get_table(&join.target)
            .and_then(|def| def.options.get("CONF_KEY").cloned())
            .unwrap_or_default();
        let addr = resolve_redis_addr(source_configs, &conf_key);
        return match rekuiper_connectors::redis_lookup_key(&addr, &key).await {
            Ok(Some(value)) => vec![lookup_value_to_row(value)],
            Ok(None) => Vec::new(),
            Err(e) => {
                tracing::warn!("Redis lookup GET {} failed: {}", key, e);
                Vec::new()
            }
        };
    }
    if table_type.eq_ignore_ascii_case("sql") {
        let (url, table, col, val) = match (
            resolve_sql_lookup(table_manager, source_configs, &join.target),
            join_key_parts(join, from, from_alias, combined),
        ) {
            (Some((url, table)), Some((col, val))) => (url, table, col, val),
            _ => return Vec::new(),
        };
        return match rekuiper_connectors::sql_lookup_key(&url, &table, &col, &val).await {
            Ok(Some(value)) => vec![lookup_value_to_row(value)],
            Ok(None) => Vec::new(),
            Err(e) => {
                tracing::warn!("SQL lookup on {} failed: {}", table, e);
                Vec::new()
            }
        };
    }
    table_manager.get_table_rows(&join.target)
}

pub(crate) async fn apply_lookup_joins(
    table_manager: &TableManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    select_stmt: &SelectStmt,
    record: &HashMap<String, Value>,
) -> Option<HashMap<String, Value>> {
    if select_stmt.joins.is_empty() {
        return Some(record.clone());
    }
    // Qualified access (`stream.field`, `table.field`) resolves through nested
    // objects, matching the evaluator's FieldAccess semantics.
    let mut combined = record.clone();
    insert_namespaced(
        &mut combined,
        &select_stmt.from,
        select_stmt.from_alias.as_deref(),
        record,
    );
    for join in &select_stmt.joins {
        if matches!(join.join_type, JoinType::Asof | JoinType::LeftAsof) {
            continue;
        }
        let mut matched: Option<HashMap<String, Value>> = None;
        for row in lookup_candidates(
            table_manager,
            source_configs,
            &select_stmt.from,
            select_stmt.from_alias.as_deref(),
            join,
            &combined,
        )
        .await
        {
            let mut probe = combined.clone();
            for (k, v) in &row {
                probe.entry(k.clone()).or_insert(v.clone());
            }
            insert_namespaced(&mut probe, &join.target, join.alias.as_deref(), &row);
            let cond_ok = match &join.on {
                Some(cond) => Evaluator::eval_bool(cond, &probe),
                // No ON condition: match the first candidate row.
                None => true,
            };
            if cond_ok {
                matched = Some(row);
                break;
            }
        }
        match matched {
            Some(row) => {
                for (k, v) in &row {
                    combined.entry(k.clone()).or_insert(v.clone());
                }
                insert_namespaced(&mut combined, &join.target, join.alias.as_deref(), &row);
            }
            None if join.join_type == JoinType::Left => {}
            None => return None,
        }
    }
    Some(combined)
}

/// Insert a row under its stream/table name plus alias so qualified
/// references (`A.id`, `a.id`) resolve through nested objects.
pub(crate) fn insert_namespaced(
    map: &mut HashMap<String, Value>,
    name: &str,
    alias: Option<&str>,
    row: &HashMap<String, Value>,
) {
    let obj = Value::Object(row.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
    map.insert(name.to_string(), obj.clone());
    if let Some(a) = alias {
        if a != name {
            map.insert(a.to_string(), obj);
        }
    }
}

/// A window-buffered row tagged with the stream that produced it, so
/// multi-stream windows can match rows across sources.
#[derive(Debug, Clone)]
pub(crate) struct TaggedRow {
    pub(crate) source: String,
    pub(crate) data: rekuiper_core::model::SharedMap,
}

/// Maximum distinct join keys retained per ASOF JOIN target stream to guarantee
/// strict O(keys) bounded memory on constrained edge gateways.
#[allow(dead_code)]
pub(crate) const MAX_ASOF_KEYS: usize = 10_000;

/// Latest-value state store for stream-to-stream `ASOF JOIN` and `LEFT ASOF JOIN`.
/// Retains only the most recent right-hand record per join key (bounded by `MAX_ASOF_KEYS`).
#[allow(dead_code)]
#[derive(Debug, Default)]
pub(crate) struct AsofJoinState {
    /// Maps `(target_stream_name, join_key_value)` -> `(right_timestamp_ms, right_row)`.
    latest_by_key: HashMap<(String, String), (i64, HashMap<String, Value>)>,
}

#[allow(dead_code)]
impl AsofJoinState {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Ingest a new event from a joined right-hand stream, updating the latest
    /// snapshot for its join key in O(1) time.
    pub(crate) fn ingest_right_event(
        &mut self,
        select_stmt: &SelectStmt,
        target_stream: &str,
        right_ts: i64,
        right_row: &HashMap<String, Value>,
    ) {
        for join in &select_stmt.joins {
            if join.target != target_stream
                || !matches!(join.join_type, JoinType::Asof | JoinType::LeftAsof)
            {
                continue;
            }
            let mut namespace_probe = right_row.clone();
            insert_namespaced(
                &mut namespace_probe,
                &join.target,
                join.alias.as_deref(),
                right_row,
            );
            if let Some(key_val) = Self::extract_right_key(
                join,
                &select_stmt.from,
                select_stmt.from_alias.as_deref(),
                &namespace_probe,
            ) {
                let map_key = (target_stream.to_string(), key_val);
                if !self.latest_by_key.contains_key(&map_key)
                    && self.latest_by_key.len() >= MAX_ASOF_KEYS
                {
                    // Evict the oldest timestamped entry to strictly bound RAM
                    if let Some(oldest_key) = self
                        .latest_by_key
                        .iter()
                        .min_by_key(|(_, (ts, _))| *ts)
                        .map(|(k, _)| k.clone())
                    {
                        self.latest_by_key.remove(&oldest_key);
                    }
                }
                let entry = self
                    .latest_by_key
                    .entry(map_key)
                    .or_insert_with(|| (right_ts, right_row.clone()));
                if right_ts >= entry.0 {
                    *entry = (right_ts, right_row.clone());
                }
            }
        }
    }

    /// Probe the ASOF state table for a left-hand stream event at `left_ts`.
    /// Matches when `right_ts <= left_ts` and the equality key matches.
    pub(crate) fn probe_left_event(
        &self,
        select_stmt: &SelectStmt,
        left_ts: i64,
        left_row: &HashMap<String, Value>,
    ) -> Option<HashMap<String, Value>> {
        let mut combined = left_row.clone();
        insert_namespaced(
            &mut combined,
            &select_stmt.from,
            select_stmt.from_alias.as_deref(),
            left_row,
        );

        for join in &select_stmt.joins {
            if !matches!(join.join_type, JoinType::Asof | JoinType::LeftAsof) {
                continue;
            }
            let matched_row = join_key_parts(
                join,
                &select_stmt.from,
                select_stmt.from_alias.as_deref(),
                &combined,
            )
            .and_then(|(_, key_val)| {
                self.latest_by_key
                    .get(&(join.target.clone(), key_val))
                    .filter(|(right_ts, _)| *right_ts <= left_ts)
                    .map(|(_, row)| row)
            });

            match matched_row {
                Some(right_row) => {
                    for (k, v) in right_row {
                        combined.entry(k.clone()).or_insert_with(|| v.clone());
                    }
                    insert_namespaced(
                        &mut combined,
                        &join.target,
                        join.alias.as_deref(),
                        right_row,
                    );
                }
                None if join.join_type == JoinType::LeftAsof => {}
                None => return None,
            }
        }
        Some(combined)
    }

    fn extract_right_key(
        join: &JoinClause,
        from: &str,
        from_alias: Option<&str>,
        right_probe: &HashMap<String, Value>,
    ) -> Option<String> {
        let (col, _) = join_key_parts(join, from, from_alias, right_probe).or_else(|| {
            // Fallback: evaluate the right-side column directly from `right_probe`
            let Expr::BinaryOp {
                left,
                op: rekuiper_sql::BinaryOperator::Eq,
                right,
            } = join.on.as_ref()?
            else {
                return None;
            };
            for side_expr in [left.as_ref(), right.as_ref()] {
                if let Expr::FieldAccess { parent, field } = side_expr {
                    if let Expr::Identifier(p) = parent.as_ref() {
                        if p == &join.target || join.alias.as_ref().is_some_and(|a| p == a) {
                            return Some((field.clone(), String::new()));
                        }
                    }
                }
            }
            None
        })?;
        match right_probe.get(&col)? {
            Value::Null => None,
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            Value::Bool(b) => Some(b.to_string()),
            _ => None,
        }
    }
}

/// Upper bound on join fan-out per window trigger: windows are bounded
/// buffers, and an unbounded cross product could exhaust memory.
pub(crate) const MAX_JOIN_FANOUT: usize = 10_000;

/// Evaluate one windowed batch for a rule with JOIN clauses.
///
/// Seeds combined rows from the FROM stream, folds each join (table point
/// lookups take the first row satisfying ON — lookup tables resolve one row
/// per key; stream targets nest-loop over buffered rows with the `ON`
/// condition; CROSS pairs all candidates), then projects: aggregate SELECTs
/// collapse the batch with `eval_aggregate`, plain SELECTs emit one row per
/// match with `eval_select` (which also applies WHERE). LEFT preserves
/// unmatched left rows; RIGHT/FULL additionally preserve unmatched right
/// rows (which never exceeds the fan-out cap), including when the left side
/// is empty. Fan-out per trigger is bounded by `MAX_JOIN_FANOUT`. Returns
/// the output rows (possibly empty); an empty batch yields no output,
/// matching empty-window semantics.
pub(crate) async fn eval_window_join_batch(
    table_manager: &TableManager,
    source_configs: &Arc<RwLock<HashMap<String, Value>>>,
    select_stmt: &SelectStmt,
    batch: &[TaggedRow],
    rule_state: Option<&RuleState>,
) -> Vec<HashMap<String, Value>> {
    if batch.is_empty() {
        return Vec::new();
    }
    let from_rows: Vec<&HashMap<String, Value>> = batch
        .iter()
        .filter(|r| r.source == select_stmt.from)
        .map(|r| &*r.data)
        .collect();
    let mut combined_rows: Vec<HashMap<String, Value>> = Vec::new();
    for row in from_rows {
        let mut combined = row.clone();
        insert_namespaced(
            &mut combined,
            &select_stmt.from,
            select_stmt.from_alias.as_deref(),
            row,
        );
        combined_rows.push(combined);
    }
    for join in &select_stmt.joins {
        let is_table = table_manager.get_table(&join.target).is_some();
        let mut next: Vec<HashMap<String, Value>> = Vec::new();
        if is_table {
            for left in &combined_rows {
                let mut matched: Option<HashMap<String, Value>> = None;
                for row in lookup_candidates(
                    table_manager,
                    source_configs,
                    &select_stmt.from,
                    select_stmt.from_alias.as_deref(),
                    join,
                    left,
                )
                .await
                {
                    // CROSS joins pair every candidate; others take the
                    // first row satisfying ON (or the first row when no ON).
                    if join.join_type != JoinType::Cross {
                        let mut probe = (*left).clone();
                        for (k, v) in &row {
                            probe.entry(k.clone()).or_insert(v.clone());
                        }
                        insert_namespaced(&mut probe, &join.target, join.alias.as_deref(), &row);
                        let cond_ok = match &join.on {
                            Some(cond) => Evaluator::eval_bool(cond, &probe),
                            None => true,
                        };
                        if !cond_ok {
                            continue;
                        }
                        matched = Some(row);
                        break;
                    }
                    let mut merged = (*left).clone();
                    for (k, v) in &row {
                        merged.entry(k.clone()).or_insert(v.clone());
                    }
                    insert_namespaced(&mut merged, &join.target, join.alias.as_deref(), &row);
                    if next.len() < MAX_JOIN_FANOUT {
                        next.push(merged);
                    }
                }
                if join.join_type == JoinType::Cross {
                    continue;
                }
                match matched {
                    Some(row) => {
                        let mut merged = (*left).clone();
                        for (k, v) in &row {
                            merged.entry(k.clone()).or_insert(v.clone());
                        }
                        insert_namespaced(&mut merged, &join.target, join.alias.as_deref(), &row);
                        next.push(merged);
                    }
                    None => match join.join_type {
                        JoinType::Left | JoinType::Full => next.push((*left).clone()),
                        _ => {}
                    },
                }
            }
        } else {
            let right_rows: Vec<&HashMap<String, Value>> = batch
                .iter()
                .filter(|r| r.source == join.target)
                .map(|r| &*r.data)
                .collect();
            // Index of matched right rows (for RIGHT/FULL preservation).
            let mut right_matched = vec![false; right_rows.len()];
            for left in &combined_rows {
                if matches!(join.join_type, JoinType::Asof | JoinType::LeftAsof) {
                    let left_ts = left
                        .get("timestamp")
                        .or_else(|| left.get("ts"))
                        .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)))
                        .unwrap_or(i64::MAX);

                    let mut best: Option<(i64, HashMap<String, Value>)> = None;
                    for right in &right_rows {
                        let right_ts = right
                            .get("timestamp")
                            .or_else(|| right.get("ts"))
                            .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)))
                            .unwrap_or(0);
                        if right_ts > left_ts {
                            continue;
                        }
                        let mut probe = (*left).clone();
                        for (k, v) in right.iter() {
                            probe.entry(k.clone()).or_insert(v.clone());
                        }
                        insert_namespaced(&mut probe, &join.target, join.alias.as_deref(), right);
                        let cond_ok = match &join.on {
                            Some(cond) => Evaluator::eval_bool(cond, &probe),
                            None => true,
                        };
                        if cond_ok && best.as_ref().is_none_or(|(b_ts, _)| right_ts >= *b_ts) {
                            best = Some((right_ts, probe));
                        }
                    }
                    if let Some((_, probe)) = best {
                        if next.len() < MAX_JOIN_FANOUT {
                            next.push(probe);
                        }
                    } else if join.join_type == JoinType::LeftAsof && next.len() < MAX_JOIN_FANOUT {
                        next.push((*left).clone());
                    }
                } else {
                    let mut any = false;
                    for (ri, right) in right_rows.iter().enumerate() {
                        let mut probe = (*left).clone();
                        for (k, v) in right.iter() {
                            probe.entry(k.clone()).or_insert(v.clone());
                        }
                        insert_namespaced(&mut probe, &join.target, join.alias.as_deref(), right);
                        let cond_ok = match &join.on {
                            Some(cond) => Evaluator::eval_bool(cond, &probe),
                            // No ON: cross product of the window.
                            None => true,
                        };
                        if !cond_ok {
                            continue;
                        }
                        any = true;
                        right_matched[ri] = true;
                        if next.len() < MAX_JOIN_FANOUT {
                            next.push(probe);
                        }
                        if next.len() >= MAX_JOIN_FANOUT {
                            break;
                        }
                    }
                    if !any {
                        match join.join_type {
                            JoinType::Left | JoinType::Full => next.push((*left).clone()),
                            _ => {}
                        }
                    }
                }
                if next.len() >= MAX_JOIN_FANOUT {
                    break;
                }
            }
            if matches!(join.join_type, JoinType::Right | JoinType::Full) {
                for (ri, right) in right_rows.iter().enumerate() {
                    if !right_matched[ri] {
                        if next.len() >= MAX_JOIN_FANOUT {
                            break;
                        }
                        let mut preserved = (*right).clone();
                        insert_namespaced(
                            &mut preserved,
                            &join.target,
                            join.alias.as_deref(),
                            right,
                        );
                        next.push(preserved);
                    }
                }
            }
        }
        combined_rows = next;
        // An empty intermediate only ends the pipeline for joins that cannot
        // produce rows without left input. RIGHT/FULL joins still preserve
        // their right side (and later joins fold over it), per SQL semantics.
        if combined_rows.is_empty() && !matches!(join.join_type, JoinType::Right | JoinType::Full) {
            return Vec::new();
        }
    }
    // WHERE over the joined rows, then GROUP BY / HAVING / ORDER BY / LIMIT.
    Evaluator::eval_window_stateful(select_stmt, combined_rows, rule_state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rekuiper_sql::Parser;

    #[test]
    fn test_asof_join_state_basic() {
        let mut state = AsofJoinState::new();
        let stmt = Parser::new("SELECT a.*, b.* FROM a ASOF JOIN b ON a.id = b.id")
            .parse_select()
            .unwrap();

        let mut right1 = HashMap::new();
        right1.insert("id".to_string(), Value::Number(1.into()));
        right1.insert("temp".to_string(), Value::Number(20.into()));
        state.ingest_right_event(&stmt, "b", 100, &right1);

        // Probe before right1 timestamp (left_ts = 50) -> None (ASOF requires right_ts <= left_ts)
        let mut left0 = HashMap::new();
        left0.insert("id".to_string(), Value::Number(1.into()));
        left0.insert("vib".to_string(), Value::Number(100.into()));
        assert!(state.probe_left_event(&stmt, 50, &left0).is_none());

        // Probe after right1 (left_ts = 150) -> matches right1 (temp = 20)
        let matched1 = state.probe_left_event(&stmt, 150, &left0).unwrap();
        assert_eq!(matched1.get("temp"), Some(&Value::Number(20.into())));

        // Ingest next right event at ts 200
        let mut right2 = HashMap::new();
        right2.insert("id".to_string(), Value::Number(1.into()));
        right2.insert("temp".to_string(), Value::Number(25.into()));
        state.ingest_right_event(&stmt, "b", 200, &right2);

        // Probe after right2 (left_ts = 250) -> matches right2 (temp = 25)
        let matched2 = state.probe_left_event(&stmt, 250, &left0).unwrap();
        assert_eq!(matched2.get("temp"), Some(&Value::Number(25.into())));
    }

    #[test]
    fn test_left_asof_join_state() {
        let state = AsofJoinState::new();
        let stmt = Parser::new("SELECT a.*, b.* FROM a LEFT ASOF JOIN b ON a.id = b.id")
            .parse_select()
            .unwrap();

        let mut left0 = HashMap::new();
        left0.insert("id".to_string(), Value::Number(99.into()));
        left0.insert("vib".to_string(), Value::Number(100.into()));
        // Left ASOF preserves left row even if no prior right row exists
        let res = state.probe_left_event(&stmt, 50, &left0);
        assert!(res.is_some());
        assert_eq!(res.unwrap().get("vib"), Some(&Value::Number(100.into())));
    }

    #[tokio::test]
    async fn test_eval_window_join_batch_asof() {
        let table_manager = TableManager::new();
        let source_configs = Arc::new(RwLock::new(HashMap::new()));
        let stmt = Parser::new("SELECT a.id, a.ts, b.val FROM a ASOF JOIN b ON a.id = b.id")
            .parse_select()
            .unwrap();

        let mut b1 = HashMap::new();
        b1.insert("id".to_string(), Value::Number(1.into()));
        b1.insert("val".to_string(), Value::String("first".to_string()));
        b1.insert("ts".to_string(), Value::Number(100.into()));

        let mut b2 = HashMap::new();
        b2.insert("id".to_string(), Value::Number(1.into()));
        b2.insert("val".to_string(), Value::String("second".to_string()));
        b2.insert("ts".to_string(), Value::Number(200.into()));

        let mut a1 = HashMap::new();
        a1.insert("id".to_string(), Value::Number(1.into()));
        a1.insert("ts".to_string(), Value::Number(150.into()));

        let batch = vec![
            TaggedRow {
                source: "b".to_string(),
                data: b1.into(),
            },
            TaggedRow {
                source: "b".to_string(),
                data: b2.into(),
            },
            TaggedRow {
                source: "a".to_string(),
                data: a1.into(),
            },
        ];

        let result =
            eval_window_join_batch(&table_manager, &source_configs, &stmt, &batch, None).await;
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].get("val"),
            Some(&Value::String("first".to_string()))
        );
    }
}
