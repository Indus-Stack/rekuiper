use crate::ast::{BinaryOperator, Expr, SelectStmt, SetOp, UnaryOperator};
use indexmap::IndexMap;
use parking_lot::RwLock;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Trait abstracting read-only row key/value lookups and iteration.
/// Monomorphized over row storage types (`IndexMap`, `HashMap`, `serde_json::Map`, `SharedMap`)
/// to ensure zero heap allocations and full inlining on the evaluation hot path.
pub trait RowLookup {
    type Iter<'a>: Iterator<Item = (&'a String, &'a Value)>
    where
        Self: 'a;

    fn get(&self, key: &str) -> Option<&Value>;
    fn iter(&self) -> Self::Iter<'_>;

    #[inline(always)]
    fn iter_row(&self) -> Self::Iter<'_> {
        self.iter()
    }

    #[inline(always)]
    fn contains_key(&self, key: &str) -> bool {
        self.get(key).is_some()
    }
}

impl RowLookup for HashMap<String, Value> {
    type Iter<'a> = std::collections::hash_map::Iter<'a, String, Value>;

    #[inline(always)]
    fn get(&self, key: &str) -> Option<&Value> {
        HashMap::get(self, key)
    }

    #[inline(always)]
    fn iter(&self) -> Self::Iter<'_> {
        self.iter()
    }
}

impl RowLookup for IndexMap<String, Value> {
    type Iter<'a> = indexmap::map::Iter<'a, String, Value>;

    #[inline(always)]
    fn get(&self, key: &str) -> Option<&Value> {
        IndexMap::get(self, key)
    }

    #[inline(always)]
    fn iter(&self) -> Self::Iter<'_> {
        self.iter()
    }
}

impl RowLookup for serde_json::Map<String, Value> {
    type Iter<'a> = serde_json::map::Iter<'a>;

    #[inline(always)]
    fn get(&self, key: &str) -> Option<&Value> {
        serde_json::Map::get(self, key)
    }

    #[inline(always)]
    fn iter(&self) -> Self::Iter<'_> {
        self.iter()
    }
}

impl RowLookup for rekuiper_core::model::SharedMap {
    type Iter<'a> = indexmap::map::Iter<'a, String, Value>;

    #[inline(always)]
    fn get(&self, key: &str) -> Option<&Value> {
        self.0.get(key)
    }

    #[inline(always)]
    fn iter(&self) -> Self::Iter<'_> {
        self.0.iter()
    }
}

impl<T: RowLookup + ?Sized> RowLookup for &T {
    type Iter<'a>
        = T::Iter<'a>
    where
        Self: 'a;

    #[inline(always)]
    fn get(&self, key: &str) -> Option<&Value> {
        (**self).get(key)
    }

    #[inline(always)]
    fn iter(&self) -> Self::Iter<'_> {
        (**self).iter()
    }
}

impl<T: RowLookup + ?Sized> RowLookup for Arc<T> {
    type Iter<'a>
        = T::Iter<'a>
    where
        Self: 'a;

    #[inline(always)]
    fn get(&self, key: &str) -> Option<&Value> {
        (**self).get(key)
    }

    #[inline(always)]
    fn iter(&self) -> Self::Iter<'_> {
        (**self).iter()
    }
}

pub mod aggregate;
pub mod analytic;
pub mod array;
pub mod datetime;
pub mod json;
pub mod math;
pub mod string;
pub mod transform;
mod window;

pub use datetime::is_valid_timezone;
pub use json::JsonPathStep;
pub use window::{AggKind, IncrementalWindow, SlidingAcc, SlidingIncrementalWindow};

/// Record key holding source metadata (MQTT topic/qos/messageId). Read by
/// `meta()`/`mqtt()`, never projected by `SELECT *`.
pub const META_KEY: &str = "__meta__";

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

/// Per-rule running state for analytic cumulative functions (`acc_*`).
///
/// State keys have the format `{func_name}:{func_call_id}:{partition_key}`,
/// so distinct call sites and `OVER (PARTITION BY ...)` groups evolve
/// independently.
#[derive(Default, Clone)]
pub struct RuleState {
    pub state: Arc<RwLock<HashMap<String, Value>>>,
    row_values: HashMap<String, Value>,
}

impl RuleState {
    /// Maximum number of distinct state keys retained per rule to bound RAM on high-cardinality streams.
    pub const MAX_STATE_KEYS: usize = 10_000;

    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a state entry while bounding total non-metadata keys in `guard` to [`Self::MAX_STATE_KEYS`].
    #[inline]
    pub fn insert_bounded(guard: &mut HashMap<String, Value>, key: String, val: Value) {
        if !guard.contains_key(&key) && guard.len() >= Self::MAX_STATE_KEYS {
            if let Some(evict_key) = guard
                .keys()
                .find(|k| !k.starts_with("__") && !k.starts_with("$$"))
                .cloned()
            {
                guard.remove(&evict_key);
            }
        }
        guard.insert(key, val);
    }
}

thread_local! {
    pub(crate) static ASYNC_DELAY_ACCUMULATOR: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

pub struct Evaluator;

impl Evaluator {
    /// Enables non-blocking delay capture on the current thread so `delay(ms, val)`
    /// records the requested sleep duration instead of blocking the OS worker thread.
    #[inline]
    pub fn begin_async_eval() {
        ASYNC_DELAY_ACCUMULATOR.with(|c| c.set(Some(0)));
    }

    /// Finishes non-blocking delay capture and returns the accumulated delay in milliseconds.
    #[inline]
    pub fn finish_async_eval() -> u64 {
        ASYNC_DELAY_ACCUMULATOR.with(|c| c.replace(None).unwrap_or(0))
    }
    /// Copy of a statement with any trailing `UNION` detached, so a single
    /// branch can be evaluated without recursing into the set operation.
    fn without_set_op(stmt: &SelectStmt) -> SelectStmt {
        let mut single = stmt.clone();
        single.set_op = None;
        single
    }

    /// Merge two single-row UNION branch outputs into one row: key union with
    /// the right branch winning conflicts. `Union` vs `UnionAll` multiplicity
    /// only manifests in multi-row APIs; see `eval_select_stateful_multi`.
    fn merge_union_rows(
        left: Option<IndexMap<String, Value>>,
        right: Option<IndexMap<String, Value>>,
    ) -> Option<IndexMap<String, Value>> {
        match (left, right) {
            (Some(mut l), Some(r)) => {
                for (k, v) in r {
                    l.insert(k, v);
                }
                Some(l)
            }
            (one @ Some(_), None) | (None, one @ Some(_)) => one,
            (None, None) => None,
        }
    }

    /// Format a Value's Go/eKuiper type name for parity error messages (e.g. `string(42)`, `int64(7)`).
    pub fn format_ekuiper_val_type(v: &Value) -> String {
        match v {
            Value::Null => "nil".to_string(),
            Value::Bool(b) => format!("bool({})", b),
            Value::Number(n) => {
                if n.is_i64() || n.is_u64() {
                    format!("int64({})", n)
                } else {
                    format!("float64({})", n)
                }
            }
            Value::String(s) => format!("string({})", s),
            Value::Array(_) => "[]interface {}".to_string(),
            Value::Object(_) => "map[string]interface {}".to_string(),
        }
    }

    pub fn eval_select<R: RowLookup + ?Sized>(
        stmt: &SelectStmt,
        record: &R,
    ) -> Option<IndexMap<String, Value>> {
        if let Some((_, rhs)) = &stmt.set_op {
            // Each branch filters/projects the same record independently.
            let left = Self::eval_select(&Self::without_set_op(stmt), record);
            let right = Self::eval_select(rhs, record);
            return Self::merge_union_rows(left, right);
        }
        if let Some(ref condition) = stmt.where_clause {
            let matches = Self::eval_bool(condition, record);
            if !matches {
                return None;
            }
        }
        let state = RuleState::default();
        Self::eval_select_stateful_fallible(stmt, record, &state)
            .ok()
            .flatten()
    }

    pub fn eval_select_fallible<R: RowLookup + ?Sized>(
        stmt: &SelectStmt,
        record: &R,
    ) -> Result<Option<IndexMap<String, Value>>, String> {
        let state = RuleState::default();
        Self::eval_select_stateful_fallible(stmt, record, &state)
    }

    /// Batch evaluation over a window of records.
    ///
    /// Computes `count/sum/avg/min/max` aggregates in `stmt.fields` over
    /// `records`, retains `group_by` column values (from the first record),
    /// and applies `having` (evaluated against the aggregated row, with
    /// aggregate calls resolved over the batch). Returns `None` when `having`
    /// filters the row out.
    pub fn eval_aggregate<R: RowLookup>(
        stmt: &SelectStmt,
        records: &[R],
    ) -> Option<IndexMap<String, Value>> {
        Self::eval_aggregate_stateful(stmt, records, None, "")
    }

    pub fn eval_aggregate_stateful<R: RowLookup>(
        stmt: &SelectStmt,
        records: &[R],
        state: Option<&RuleState>,
        partition_key: &str,
    ) -> Option<IndexMap<String, Value>> {
        if let Some((_, rhs)) = &stmt.set_op {
            // Each branch aggregates the same batch independently; the two
            // single-row outputs merge (right wins on conflict).
            let left = Self::eval_aggregate_stateful(
                &Self::without_set_op(stmt),
                records,
                state,
                partition_key,
            );
            let right = Self::eval_aggregate_stateful(rhs, records, state, partition_key);
            return Self::merge_union_rows(left, right);
        }
        let first: Option<&R> = records.first();
        let mut output = IndexMap::new();
        let field_names = Self::select_field_names(stmt);

        for (idx, field) in stmt.fields.iter().enumerate() {
            match field {
                Expr::Wildcard => {
                    if let Some(rec) = first {
                        for (k, v) in rec.iter_row() {
                            if k != META_KEY && !k.starts_with("__") {
                                output.insert(k.clone(), v.clone());
                            }
                        }
                    }
                }
                Expr::WildcardModified { except, replace } => {
                    if let Some(rec) = first {
                        for (k, v) in rec.iter_row() {
                            if k != META_KEY && !k.starts_with("__") {
                                if except.iter().any(|e| e == k)
                                    && !replace.iter().any(|(_, c)| c == k)
                                {
                                    continue;
                                }
                                output.insert(k.clone(), v.clone());
                            }
                        }
                    }
                    for (rep_expr, col) in replace {
                        let val = Self::eval_agg_expr_stateful(
                            rep_expr,
                            records,
                            &output,
                            state,
                            partition_key,
                        );
                        output.insert(col.clone(), val);
                    }
                }
                Expr::Identifier(name) => {
                    let key = field_names[idx].clone();
                    let val = first
                        .and_then(|rec| rec.get(name).cloned())
                        .unwrap_or(Value::Null);
                    output.insert(key, val);
                }
                Expr::FieldAccess {
                    parent,
                    field: leaf,
                } if leaf == "*" => {
                    if let Some(rec) = first {
                        let parent_val = Self::eval_val(parent, rec);
                        if let Value::Object(map) = parent_val {
                            for (k, v) in map {
                                if k != META_KEY && !k.starts_with("__") {
                                    output.insert(k, v);
                                }
                            }
                        } else if let Expr::Identifier(p_name) = parent.as_ref() {
                            let has_other_namespace = rec.iter_row().any(|(k, v)| {
                                k != p_name && !k.starts_with("__") && matches!(v, Value::Object(_))
                            });
                            if !has_other_namespace {
                                for (k, v) in rec.iter_row() {
                                    if k != META_KEY && !k.starts_with("__") {
                                        output.insert(k.clone(), v.clone());
                                    }
                                }
                            }
                        }
                    }
                }
                Expr::FieldAccess {
                    parent: _,
                    field: _leaf,
                } => {
                    let key = field_names[idx].clone();
                    let val = match first {
                        Some(rec) => Self::eval_val(field, rec),
                        None => Value::Null,
                    };
                    output.insert(key, val);
                }
                Expr::Call { name, args } if Self::is_aggregate_call(name) => {
                    let val = Self::eval_aggregate_call_stateful(
                        name,
                        args,
                        records,
                        state,
                        partition_key,
                    );
                    let key = field_names[idx].clone();
                    output.insert(key, val);
                }
                _ => {
                    // General expression: may contain nested aggregates
                    // (e.g. `avg(temp) + 1`), so evaluate aggregate-aware.
                    let val =
                        Self::eval_agg_expr_stateful(field, records, &output, state, partition_key);
                    let key = field_names[idx].clone();
                    output.insert(key, val);
                }
            }
        }

        if let Some(having) = &stmt.having {
            let v = Self::eval_agg_expr_stateful(having, records, &output, state, partition_key);
            match v {
                Value::Bool(true) => {}
                _ => return None,
            }
        }

        Some(output)
    }

    /// True for window/batch aggregate function names (`count`, `avg`, …).
    /// Used by the server to decide between per-row projection and batch
    /// aggregation for windowed JOIN outputs.
    pub fn is_aggregate_call(name: &str) -> bool {
        matches!(
            name.to_ascii_lowercase().as_str(),
            "count"
                | "sum"
                | "avg"
                | "min"
                | "max"
                | "collect"
                | "lead"
                | "latest"
                | "median"
                | "stddev"
                | "stddevs"
                | "var"
                | "vars"
                | "percentile"
                | "percentile_cont"
                | "percentile_disc"
                | "last_value"
                | "merge_agg"
                | "row_number"
                | "last_agg_hit_count"
                | "last_agg_hit_time"
        )
    }

    fn eval_agg_expr_stateful<R: RowLookup>(
        expr: &Expr,
        records: &[R],
        output: &IndexMap<String, Value>,
        state: Option<&RuleState>,
        partition_key: &str,
    ) -> Value {
        match expr {
            Expr::Wildcard | Expr::WildcardModified { .. } => Value::Null,
            Expr::Literal(v) => v.clone(),
            Expr::Identifier(name) => {
                if let Some(v) = output.get(name) {
                    return v.clone();
                }
                records
                    .first()
                    .and_then(|rec| rec.get(name).cloned())
                    .unwrap_or(Value::Null)
            }
            Expr::FieldAccess {
                parent: _,
                field: leaf,
            } => {
                if let Some(v) = output.get(leaf) {
                    return v.clone();
                }
                // Also try the full dotted path key.
                let full = expr.to_ekuiper_string();
                if let Some(v) = output.get(&full) {
                    return v.clone();
                }
                match records.first() {
                    Some(rec) => Self::eval_val(expr, rec),
                    None => Value::Null,
                }
            }
            Expr::Index { .. } | Expr::Slice { .. } => match records.first() {
                Some(rec) => Self::eval_val(expr, rec),
                None => Value::Null,
            },
            Expr::Call { name, args } => {
                if Self::is_aggregate_call(name) {
                    return Self::eval_aggregate_call_stateful(
                        name,
                        args,
                        records,
                        state,
                        partition_key,
                    );
                }
                // Contextual system functions resolve against the first batch
                // record when one exists.
                if let Some(first) = records.first() {
                    if let Some(v) = Self::eval_context_call(name, args, first) {
                        return v;
                    }
                }
                let lowered = name.to_ascii_lowercase();
                if let Some(s) = state {
                    if lowered == "had_changed" || lowered == "changed_col" {
                        let vals: Vec<Value> = args
                            .iter()
                            .map(|a| {
                                Self::eval_agg_expr_stateful(
                                    a,
                                    records,
                                    output,
                                    state,
                                    partition_key,
                                )
                            })
                            .collect();
                        let call_id = expr.to_ekuiper_string();
                        return Self::eval_changed(&lowered, &vals, s, &call_id, partition_key);
                    }
                    if lowered == "changed_cols" && args.len() >= 2 {
                        let vals: Vec<Value> = args
                            .iter()
                            .map(|a| {
                                Self::eval_agg_expr_stateful(
                                    a,
                                    records,
                                    output,
                                    state,
                                    partition_key,
                                )
                            })
                            .collect();
                        let prefix = match vals.first() {
                            Some(Value::String(str_val)) => str_val.as_str(),
                            _ => "",
                        };
                        let ignore_null = match vals.get(1) {
                            Some(Value::Bool(b)) => *b,
                            _ => false,
                        };
                        let mut result_map = serde_json::Map::new();
                        for (arg_idx, expr_arg) in args.iter().skip(2).enumerate() {
                            let col_name = Self::column_name(expr_arg, arg_idx);
                            let val = &vals[arg_idx + 2];
                            if ignore_null && val.is_null() {
                                continue;
                            }
                            let call_id = expr.to_ekuiper_string();
                            let state_key =
                                format!("changed_cols:{}:{}:{}", call_id, col_name, partition_key);
                            let prev = s.state.read().get(&state_key).cloned();
                            let changed = match (&prev, val) {
                                (None, _) => true,
                                (Some(p), c) if p.is_null() && c.is_null() => false,
                                (Some(p), c) => !Self::values_equal(p, c),
                            };
                            if changed {
                                s.state.write().insert(state_key, (*val).clone());
                                result_map
                                    .insert(format!("{}{}", prefix, col_name), (*val).clone());
                            }
                        }
                        if result_map.is_empty() {
                            return Value::Null;
                        }
                        return Value::Object(result_map);
                    }
                }
                let vals: Vec<Value> = args
                    .iter()
                    .map(|a| Self::eval_agg_expr_stateful(a, records, output, state, partition_key))
                    .collect();
                Self::eval_call(name, &vals)
            }
            Expr::Over { call, .. } => {
                Self::eval_agg_expr_stateful(call, records, output, state, partition_key)
            }
            Expr::BinaryOp { left, op, right } => {
                let l = Self::eval_agg_expr_stateful(left, records, output, state, partition_key);
                let r = Self::eval_agg_expr_stateful(right, records, output, state, partition_key);
                Self::eval_binary_op(&l, op, &r)
            }
            Expr::UnaryOp { op, expr } => {
                let v = Self::eval_agg_expr_stateful(expr, records, output, state, partition_key);
                Self::eval_unary_op(op, &v)
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let v = Self::eval_agg_expr_stateful(expr, records, output, state, partition_key);
                let l = Self::eval_agg_expr_stateful(low, records, output, state, partition_key);
                let h = Self::eval_agg_expr_stateful(high, records, output, state, partition_key);
                Self::eval_between(&v, &l, &h, *negated)
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let v = Self::eval_agg_expr_stateful(expr, records, output, state, partition_key);
                let mut matched = false;
                for item in list {
                    let iv =
                        Self::eval_agg_expr_stateful(item, records, output, state, partition_key);
                    if Self::values_equal(&v, &iv) {
                        matched = true;
                        break;
                    }
                }
                Value::Bool(if *negated { !matched } else { matched })
            }
            Expr::IsNull { expr, negated } => {
                let v = Self::eval_agg_expr_stateful(expr, records, output, state, partition_key);
                Value::Bool(if *negated { !v.is_null() } else { v.is_null() })
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => Self::eval_case(operand, when_clauses, else_clause, |e| {
                Self::eval_agg_expr_stateful(e, records, output, state, partition_key)
            }),
        }
    }

    /// Shared CASE evaluation over an arbitrary sub-expression evaluator.
    /// Simple CASE compares the operand with each WHEN value via
    /// [`Self::values_equal`]; searched CASE treats each WHEN as a boolean
    /// condition. Falls back to ELSE or `Null` when nothing matches.
    fn eval_case_fallible<E>(
        operand: &Option<Box<Expr>>,
        when_clauses: &[(Expr, Expr)],
        else_clause: &Option<Box<Expr>>,
        mut eval: E,
    ) -> Result<Value, String>
    where
        E: FnMut(&Expr) -> Result<Value, String>,
    {
        if let Some(op) = operand {
            let op_val = eval(op)?;
            for (when_expr, then_expr) in when_clauses {
                let when_val = eval(when_expr)?;
                if Self::values_equal(&op_val, &when_val) {
                    return eval(then_expr);
                }
            }
        } else {
            for (when_cond, then_expr) in when_clauses {
                let cond_val = eval(when_cond)?;
                if matches!(cond_val, Value::Bool(true)) {
                    return eval(then_expr);
                }
            }
        }
        match else_clause {
            Some(e) => eval(e),
            None => Ok(Value::Null),
        }
    }

    fn eval_case<E>(
        operand: &Option<Box<Expr>>,
        when_clauses: &[(Expr, Expr)],
        else_clause: &Option<Box<Expr>>,
        mut eval: E,
    ) -> Value
    where
        E: FnMut(&Expr) -> Value,
    {
        Self::eval_case_fallible(operand, when_clauses, else_clause, |e| Ok(eval(e)))
            .unwrap_or(Value::Null)
    }

    // ---------- stateful analytic evaluation (acc_* + OVER) ----------

    /// Stateful per-record projection for rules using analytic cumulative
    /// functions.
    ///
    /// Unlike [`Self::eval_select`], this deliberately does **not** apply
    /// `where_clause`: the projection (and therefore the analytic state)
    /// advances on every input row, while the caller applies the input-row
    /// filter to decide emission. This mirrors eKuiper analytic semantics,
    /// where e.g. `acc_map_agg` accumulates rows that a later `WHERE` filters
    /// out of the output.
    pub fn eval_select_stateful<R: RowLookup + ?Sized>(
        stmt: &SelectStmt,
        record: &R,
        state: &RuleState,
    ) -> Option<IndexMap<String, Value>> {
        Self::eval_select_stateful_fallible(stmt, record, state)
            .ok()
            .flatten()
    }

    pub fn expr_has_analytic(expr: &Expr) -> bool {
        if Self::is_analytic_expr(expr) {
            return true;
        }
        match expr {
            Expr::Call { args, .. } => args.iter().any(Self::expr_has_analytic),
            Expr::Over {
                call,
                partition_by,
                when,
            } => {
                (if let Expr::Call { args, .. } = call.as_ref() {
                    args.iter().any(Self::expr_has_analytic)
                } else {
                    false
                }) || partition_by.as_deref().is_some_and(Self::expr_has_analytic)
                    || when.as_deref().is_some_and(Self::expr_has_analytic)
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_has_analytic(left) || Self::expr_has_analytic(right)
            }
            Expr::UnaryOp { expr, .. }
            | Expr::IsNull { expr, .. }
            | Expr::FieldAccess { parent: expr, .. } => Self::expr_has_analytic(expr),
            Expr::Index { base, index } => {
                Self::expr_has_analytic(base) || Self::expr_has_analytic(index)
            }
            Expr::Slice { base, lo, hi } => {
                Self::expr_has_analytic(base)
                    || lo.as_deref().is_some_and(Self::expr_has_analytic)
                    || hi.as_deref().is_some_and(Self::expr_has_analytic)
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::expr_has_analytic(expr)
                    || Self::expr_has_analytic(low)
                    || Self::expr_has_analytic(high)
            }
            Expr::InList { expr, list, .. } => {
                Self::expr_has_analytic(expr) || list.iter().any(Self::expr_has_analytic)
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                operand.as_deref().is_some_and(Self::expr_has_analytic)
                    || when_clauses
                        .iter()
                        .any(|(c, v)| Self::expr_has_analytic(c) || Self::expr_has_analytic(v))
                    || else_clause.as_deref().is_some_and(Self::expr_has_analytic)
            }
            Expr::WildcardModified { replace, .. } => {
                replace.iter().any(|(e, _)| Self::expr_has_analytic(e))
            }
            Expr::Wildcard | Expr::Literal(_) | Expr::Identifier(_) => false,
        }
    }

    #[inline]
    pub fn stmt_has_analytic(stmt: &SelectStmt) -> bool {
        stmt.fields.iter().any(Self::expr_has_analytic)
            || stmt
                .where_clause
                .as_ref()
                .is_some_and(Self::expr_has_analytic)
    }

    fn expr_has_delay(expr: &Expr) -> bool {
        match expr {
            Expr::Call { name, args } => {
                name.eq_ignore_ascii_case("delay") || args.iter().any(Self::expr_has_delay)
            }
            Expr::Over { call, .. } => Self::expr_has_delay(call),
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_has_delay(left) || Self::expr_has_delay(right)
            }
            Expr::UnaryOp { expr, .. }
            | Expr::IsNull { expr, .. }
            | Expr::FieldAccess { parent: expr, .. } => Self::expr_has_delay(expr),
            Expr::Index { base, index } => {
                Self::expr_has_delay(base) || Self::expr_has_delay(index)
            }
            Expr::Slice { base, lo, hi } => {
                Self::expr_has_delay(base)
                    || lo.as_deref().is_some_and(Self::expr_has_delay)
                    || hi.as_deref().is_some_and(Self::expr_has_delay)
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::expr_has_delay(expr)
                    || Self::expr_has_delay(low)
                    || Self::expr_has_delay(high)
            }
            Expr::InList { expr, list, .. } => {
                Self::expr_has_delay(expr) || list.iter().any(Self::expr_has_delay)
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                operand.as_deref().is_some_and(Self::expr_has_delay)
                    || when_clauses
                        .iter()
                        .any(|(c, v)| Self::expr_has_delay(c) || Self::expr_has_delay(v))
                    || else_clause.as_deref().is_some_and(Self::expr_has_delay)
            }
            Expr::WildcardModified { replace, .. } => {
                replace.iter().any(|(e, _)| Self::expr_has_delay(e))
            }
            Expr::Wildcard | Expr::Literal(_) | Expr::Identifier(_) => false,
        }
    }

    #[inline]
    pub fn stmt_has_delay(stmt: &SelectStmt) -> bool {
        stmt.fields.iter().any(Self::expr_has_delay)
            || stmt.where_clause.as_ref().is_some_and(Self::expr_has_delay)
    }

    /// Advance analytic functions before WHERE, then project only passing rows.
    /// The per-row cache prevents SELECT and WHERE from advancing the same call twice.
    pub fn eval_select_filtered_stateful_fallible<R: RowLookup + ?Sized>(
        stmt: &SelectStmt,
        record: &R,
        state: &RuleState,
    ) -> Result<Option<IndexMap<String, Value>>, String> {
        let field_names = Self::select_field_names(stmt);
        let has_analytic = Self::stmt_has_analytic(stmt);
        Self::eval_select_stateless_fast(stmt, &field_names, has_analytic, record, state)
    }

    /// Reuses pre-computed `field_names` and `has_analytic` to avoid traversing
    /// the AST or re-computing field alias names on every row.
    #[inline]
    pub fn eval_select_stateless_fast<R: RowLookup + ?Sized>(
        stmt: &SelectStmt,
        field_names: &[String],
        has_analytic: bool,
        record: &R,
        state: &RuleState,
    ) -> Result<Option<IndexMap<String, Value>>, String> {
        let Some(condition) = &stmt.where_clause else {
            return Self::eval_select_stateful_with_names(stmt, field_names, record, state);
        };
        // Fast path: 95%+ of rules have no stateful analytic calls in SELECT/WHERE.
        // Evaluate WHERE first before projecting SELECT, with zero Vec/HashMap allocations.
        if !has_analytic {
            if !Self::eval_bool_stateful_fallible(condition, record, state)? {
                return Ok(None);
            }
            return Self::eval_select_stateful_with_names(stmt, field_names, record, state);
        }

        let mut row_state = RuleState {
            state: state.state.clone(),
            row_values: HashMap::new(),
        };
        for (idx, field) in stmt.fields.iter().enumerate() {
            let mut calls = Vec::new();
            Self::collect_analytic_exprs(field, &mut calls);
            for call in calls {
                let key = format!("{:?}", call);
                if row_state.row_values.contains_key(&key) {
                    continue;
                }
                let value =
                    Self::eval_stateful_expr_fallible(call, record, &row_state).map_err(|err| {
                        let alias = stmt.field_aliases.get(idx).and_then(|a| a.as_deref());
                        let alias = alias.map(|a| format!("alias: {} ", a)).unwrap_or_default();
                        format!(
                            "run Select error: {}expr: {} meet error, err:{}",
                            alias,
                            field.to_ekuiper_string_qualified(&stmt.from),
                            err
                        )
                    })?;
                row_state.row_values.insert(key, value);
            }
        }
        let mut calls = Vec::new();
        Self::collect_analytic_exprs(condition, &mut calls);
        for call in calls {
            let key = format!("{:?}", call);
            if row_state.row_values.contains_key(&key) {
                continue;
            }
            let value = Self::eval_stateful_expr_fallible(call, record, &row_state)
                .map_err(|err| format!("run Where error: {}", err))?;
            row_state.row_values.insert(key, value);
        }
        if !Self::eval_bool_stateful_fallible(condition, record, &row_state)? {
            return Ok(None);
        }
        Self::eval_select_stateful_with_names(stmt, field_names, record, &row_state)
    }

    fn is_analytic_expr(expr: &Expr) -> bool {
        match expr {
            Expr::Call { name, .. } => {
                let name = name.to_ascii_lowercase();
                Self::is_acc_call(&name)
                    || matches!(
                        name.as_str(),
                        "lag"
                            | "had_changed"
                            | "changed_col"
                            | "row_number"
                            | "last_hit_count"
                            | "last_hit_time"
                            | "sequence_match"
                            | "sequence_count"
                    )
            }
            Expr::Over { call, .. } => Self::is_analytic_expr(call),
            _ => false,
        }
    }

    fn collect_analytic_exprs<'a>(expr: &'a Expr, calls: &mut Vec<&'a Expr>) {
        match expr {
            Expr::Call { args, .. } => {
                for arg in args {
                    Self::collect_analytic_exprs(arg, calls);
                }
            }
            Expr::Over {
                call,
                partition_by,
                when,
            } => {
                // The OVER call owns its partition/WHEN update; don't also run it unqualified.
                if let Expr::Call { args, .. } = call.as_ref() {
                    for arg in args {
                        Self::collect_analytic_exprs(arg, calls);
                    }
                }
                for child in [partition_by, when].into_iter().flatten() {
                    Self::collect_analytic_exprs(child, calls);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_analytic_exprs(left, calls);
                Self::collect_analytic_exprs(right, calls);
            }
            Expr::UnaryOp { expr, .. } | Expr::IsNull { expr, .. } => {
                Self::collect_analytic_exprs(expr, calls)
            }
            Expr::FieldAccess { parent, .. } => Self::collect_analytic_exprs(parent, calls),
            Expr::Index { base, index } => {
                Self::collect_analytic_exprs(base, calls);
                Self::collect_analytic_exprs(index, calls);
            }
            Expr::Slice { base, lo, hi } => {
                Self::collect_analytic_exprs(base, calls);
                for child in [lo, hi].into_iter().flatten() {
                    Self::collect_analytic_exprs(child, calls);
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                for child in [expr, low, high] {
                    Self::collect_analytic_exprs(child, calls);
                }
            }
            Expr::InList { expr, list, .. } => {
                Self::collect_analytic_exprs(expr, calls);
                for child in list {
                    Self::collect_analytic_exprs(child, calls);
                }
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                for child in [operand, else_clause].into_iter().flatten() {
                    Self::collect_analytic_exprs(child, calls);
                }
                for (condition, value) in when_clauses {
                    Self::collect_analytic_exprs(condition, calls);
                    Self::collect_analytic_exprs(value, calls);
                }
            }
            Expr::WildcardModified { replace, .. } => {
                for (child, _) in replace {
                    Self::collect_analytic_exprs(child, calls);
                }
            }
            Expr::Wildcard | Expr::Literal(_) | Expr::Identifier(_) => {}
        }
        if Self::is_analytic_expr(expr) {
            calls.push(expr);
        }
    }

    #[inline]
    fn try_eval_numeric_fast<R: RowLookup + ?Sized>(expr: &Expr, record: &R) -> Option<Value> {
        #[inline]
        fn eval_f64<R: RowLookup + ?Sized>(e: &Expr, rec: &R, all_i64: &mut bool) -> Option<f64> {
            match e {
                Expr::Literal(v) => {
                    if !v.is_i64() {
                        *all_i64 = false;
                    }
                    v.as_f64()
                }
                Expr::Identifier(name) => {
                    let v = rec.get(name)?;
                    if !v.is_i64() {
                        *all_i64 = false;
                    }
                    v.as_f64()
                }
                Expr::BinaryOp { left, op, right } => {
                    let l = eval_f64(left, rec, all_i64)?;
                    let r = eval_f64(right, rec, all_i64)?;
                    match op {
                        BinaryOperator::Add => Some(l + r),
                        BinaryOperator::Sub => Some(l - r),
                        BinaryOperator::Mul => Some(l * r),
                        BinaryOperator::Div => {
                            if r == 0.0 {
                                None
                            } else {
                                *all_i64 = false;
                                Some(l / r)
                            }
                        }
                        _ => None,
                    }
                }
                _ => None,
            }
        }

        if !matches!(
            expr,
            Expr::BinaryOp {
                op: BinaryOperator::Add
                    | BinaryOperator::Sub
                    | BinaryOperator::Mul
                    | BinaryOperator::Div,
                ..
            }
        ) {
            return None;
        }
        let mut all_i64 = true;
        let res = eval_f64(expr, record, &mut all_i64)?;
        if !res.is_finite() {
            return None;
        }
        if all_i64 && res >= (i64::MIN as f64) && res <= (i64::MAX as f64) {
            // Fall back on integer expressions so i64 saturating/integer division semantics stay exact
            return None;
        }
        serde_json::Number::from_f64(res).map(Value::Number)
    }

    pub fn eval_select_stateful_fallible<R: RowLookup + ?Sized>(
        stmt: &SelectStmt,
        record: &R,
        state: &RuleState,
    ) -> Result<Option<IndexMap<String, Value>>, String> {
        let field_names = Self::select_field_names(stmt);
        Self::eval_select_stateful_with_names(stmt, &field_names, record, state)
    }

    #[inline]
    pub fn eval_select_stateful_with_names<R: RowLookup + ?Sized>(
        stmt: &SelectStmt,
        field_names: &[String],
        record: &R,
        state: &RuleState,
    ) -> Result<Option<IndexMap<String, Value>>, String> {
        if let Some((_, rhs)) = &stmt.set_op {
            let left =
                Self::eval_select_stateful_fallible(&Self::without_set_op(stmt), record, state)?;
            let right = Self::eval_select_stateful_fallible(rhs, record, state)?;
            return Ok(Self::merge_union_rows(left, right));
        }
        let mut output = IndexMap::with_capacity(stmt.fields.len());
        for (idx, field) in stmt.fields.iter().enumerate() {
            let alias = stmt.field_aliases.get(idx).and_then(|a| a.as_deref());
            match field {
                Expr::Wildcard => {
                    for (k, v) in record.iter() {
                        if k != META_KEY && !k.starts_with("__") {
                            output.insert(k.clone(), v.clone());
                        }
                    }
                }
                Expr::WildcardModified { except, replace } => {
                    for (k, v) in record.iter() {
                        if k != META_KEY && !k.starts_with("__") {
                            if except.iter().any(|e| e == k) && !replace.iter().any(|(_, c)| c == k)
                            {
                                continue;
                            }
                            output.insert(k.clone(), v.clone());
                        }
                    }
                    for (rep_expr, col) in replace {
                        let val = Self::eval_stateful_expr_fallible(rep_expr, record, state)
                            .map_err(|err| {
                                format!(
                                    "run Select error: alias: {} expr: {} meet error, err:{}",
                                    col,
                                    rep_expr.to_ekuiper_string_qualified(&stmt.from),
                                    err
                                )
                            })?;
                        output.insert(col.clone(), val);
                    }
                }
                Expr::Identifier(name) => {
                    let key = if idx < field_names.len() {
                        field_names[idx].clone()
                    } else {
                        Self::select_field_name_at(stmt, idx)
                    };
                    if let Some(val) = record.get(name) {
                        output.insert(key, val.clone());
                    } else if name == "window_start" || name == "window_end" {
                        let internal_key = format!("__{}__", name);
                        if let Some(val) = record.get(&internal_key) {
                            output.insert(key, val.clone());
                        } else {
                            output.insert(key, Value::Null);
                        }
                    } else {
                        output.insert(key, Value::Null);
                    }
                }
                Expr::FieldAccess {
                    parent,
                    field: leaf,
                } if leaf == "*" => {
                    let parent_val = Self::eval_stateful_expr_fallible(parent, record, state)
                        .map_err(|err| {
                            let a = alias.unwrap_or("*");
                            format!(
                                "run Select error: alias: {} expr: {} meet error, err:{}",
                                a,
                                field.to_ekuiper_string_qualified(&stmt.from),
                                err
                            )
                        })?;
                    if let Value::Object(map) = parent_val {
                        for (k, v) in map {
                            if k != META_KEY && !k.starts_with("__") {
                                output.insert(k, v);
                            }
                        }
                    } else if let Expr::Identifier(p_name) = parent.as_ref() {
                        let has_other_namespace = record.iter().any(|(k, v)| {
                            k != p_name && !k.starts_with("__") && matches!(v, Value::Object(_))
                        });
                        if !has_other_namespace {
                            for (k, v) in record.iter() {
                                if k != META_KEY && !k.starts_with("__") {
                                    output.insert(k.clone(), v.clone());
                                }
                            }
                        }
                    }
                }
                Expr::FieldAccess {
                    parent: _,
                    field: leaf,
                } => {
                    let val =
                        Self::eval_stateful_expr_fallible(field, record, state).map_err(|err| {
                            let a = alias.unwrap_or(leaf);
                            format!(
                                "run Select error: alias: {} expr: {} meet error, err:{}",
                                a,
                                field.to_ekuiper_string_qualified(&stmt.from),
                                err
                            )
                        })?;
                    let key = if idx < field_names.len() {
                        field_names[idx].clone()
                    } else {
                        Self::select_field_name_at(stmt, idx)
                    };
                    output.insert(key, val);
                }
                Expr::Call { name, args } if name.eq_ignore_ascii_case("extract") => {
                    if let Some(arg) = args.first() {
                        let val = Self::eval_stateful_expr_fallible(arg, record, state).map_err(
                            |err| {
                                let a = alias.unwrap_or("extract");
                                format!(
                                    "run Select error: alias: {} expr: {} meet error, err:{}",
                                    a,
                                    field.to_ekuiper_string_qualified(&stmt.from),
                                    err
                                )
                            },
                        )?;
                        if let Value::Object(map) = val {
                            for (k, v) in map {
                                output.insert(k, v);
                            }
                        }
                    }
                }
                Expr::Call { name, args }
                    if name.eq_ignore_ascii_case("changed_cols") && args.len() >= 2 =>
                {
                    let prefix = match Self::eval_stateful_expr(&args[0], record, state) {
                        Value::String(s) => s,
                        _ => String::new(),
                    };
                    let ignore_null = match Self::eval_stateful_expr(&args[1], record, state) {
                        Value::Bool(b) => b,
                        _ => false,
                    };
                    for (arg_idx, expr_arg) in args[2..].iter().enumerate() {
                        if matches!(expr_arg, Expr::Wildcard) {
                            for (k, v) in record.iter() {
                                if k == META_KEY {
                                    continue;
                                }
                                if ignore_null && v.is_null() {
                                    continue;
                                }
                                let state_key = format!("changed_cols:{}:{}:{}", idx, k, "");
                                let prev = state.state.read().get(&state_key).cloned();
                                let changed = match (&prev, v) {
                                    (None, _) => true,
                                    (Some(p), c) if p.is_null() && c.is_null() => false,
                                    (Some(p), c) => !Self::values_equal(p, c),
                                };
                                if changed {
                                    state.state.write().insert(state_key, v.clone());
                                    output.insert(format!("{}{}", prefix, k), v.clone());
                                }
                            }
                        } else {
                            let col_name = Self::column_name(expr_arg, arg_idx);
                            let val = Self::eval_stateful_expr(expr_arg, record, state);
                            if ignore_null && val.is_null() {
                                continue;
                            }
                            let state_key = format!("changed_cols:{}:{}:{}", idx, col_name, "");
                            let prev = state.state.read().get(&state_key).cloned();
                            let changed = match (&prev, &val) {
                                (None, _) => true,
                                (Some(p), c) if p.is_null() && c.is_null() => false,
                                (Some(p), c) => !Self::values_equal(p, c),
                            };
                            if changed {
                                state.state.write().insert(state_key, val.clone());
                                output.insert(format!("{}{}", prefix, col_name), val);
                            }
                        }
                    }
                }
                _ => {
                    let val = if let Some(fast_num) = Self::try_eval_numeric_fast(field, record) {
                        fast_num
                    } else {
                        Self::eval_stateful_expr_fallible(field, record, state).map_err(|err| {
                            let expr_str = field.to_ekuiper_string_qualified(&stmt.from);
                            if let Some(a) = alias {
                                format!(
                                    "run Select error: alias: {} expr: {} meet error, err:{}",
                                    a, expr_str, err
                                )
                            } else {
                                format!(
                                    "run Select error: expr: {} meet error, err:{}",
                                    expr_str, err
                                )
                            }
                        })?
                    };
                    let key = if idx < field_names.len() {
                        field_names[idx].clone()
                    } else {
                        Self::select_field_name_at(stmt, idx)
                    };
                    output.insert(key, val);
                }
            }
        }
        Ok(Some(output))
    }

    /// Multi-row stateful projection supporting `unnest(array_expr)`.
    ///
    /// Evaluates fields with [`Self::eval_select_stateful`]; when a top-level
    /// field is an `unnest(array)` call, the base row is cloned once per
    /// array item: object items merge their sub-keys into the row (the
    /// placeholder entry is dropped), while scalar items bind under the
    /// field alias (or generated column name). A non-array argument passes
    /// the base row through unchanged; without any `unnest` field the result
    /// is a single-element vector.
    pub fn eval_select_stateful_multi<R: RowLookup + ?Sized>(
        stmt: &SelectStmt,
        record: &R,
        state: &RuleState,
    ) -> Vec<IndexMap<String, Value>> {
        let mut rows = Self::eval_own_rows_multi(stmt, record, state);
        if let Some((op, rhs)) = &stmt.set_op {
            let right = Self::eval_select_stateful_multi(rhs, record, state);
            if *op == SetOp::Union {
                for row in right {
                    if !rows.contains(&row) {
                        rows.push(row);
                    }
                }
            } else {
                rows.extend(right);
            }
        }
        rows
    }

    /// Single-statement portion of [`Self::eval_select_stateful_multi`]:
    /// projection (+ unnest expansion) without following `set_op`.
    fn eval_own_rows_multi<R: RowLookup + ?Sized>(
        stmt: &SelectStmt,
        record: &R,
        state: &RuleState,
    ) -> Vec<IndexMap<String, Value>> {
        // NOTE: set_op-free base on purpose — the caller combines branches.
        let single = Self::without_set_op(stmt);
        let Some(base) = Self::eval_select_stateful(&single, record, state) else {
            return Vec::new();
        };
        let unnest_pos = stmt.fields.iter().position(
            |f| matches!(f, Expr::Call { name, .. } if name.eq_ignore_ascii_case("unnest") || name.eq_ignore_ascii_case("extract")),
        );
        let Some(idx) = unnest_pos else {
            return vec![base];
        };
        let Expr::Call { args, .. } = &stmt.fields[idx] else {
            return vec![base];
        };
        let items = match args.first() {
            Some(arg) => Self::eval_stateful_expr(arg, record, state),
            None => Value::Null,
        };
        let Value::Array(items) = items else {
            return vec![base];
        };
        let placeholder = Self::select_field_name_at(stmt, idx);
        let mut rows = Vec::with_capacity(items.len());
        for item in items {
            let mut row = base.clone();
            row.shift_remove(&placeholder);
            match item {
                Value::Object(map) => {
                    for (k, v) in map {
                        row.insert(k, v);
                    }
                }
                scalar => {
                    row.insert(placeholder.clone(), scalar);
                }
            }
            rows.push(row);
        }
        rows
    }

    /// Evaluate an expression with analytic (`acc_*`) calls resolved against
    /// `state`. All other expressions behave exactly like [`Self::eval_val`].
    fn eval_stateful_expr<R: RowLookup + ?Sized>(
        expr: &Expr,
        record: &R,
        state: &RuleState,
    ) -> Value {
        Self::eval_stateful_expr_fallible(expr, record, state).unwrap_or(Value::Null)
    }

    pub fn eval_stateful_expr_fallible<R: RowLookup + ?Sized>(
        expr: &Expr,
        record: &R,
        state: &RuleState,
    ) -> Result<Value, String> {
        if !state.row_values.is_empty() && Self::is_analytic_expr(expr) {
            if let Some(value) = state.row_values.get(&format!("{:?}", expr)) {
                return Ok(value.clone());
            }
        }
        match expr {
            Expr::Wildcard | Expr::WildcardModified { .. } => Ok(Value::Null),
            Expr::Literal(val) => Ok(val.clone()),
            Expr::Identifier(name) => {
                if let Some(val) = record.get(name) {
                    Ok(val.clone())
                } else if name == "window_start" || name == "window_end" {
                    Ok(record
                        .get(&format!("__{}__", name))
                        .cloned()
                        .unwrap_or(Value::Null))
                } else {
                    Ok(Value::Null)
                }
            }
            Expr::FieldAccess { parent, field } => {
                let parent_val = Self::eval_stateful_expr_fallible(parent, record, state)?;
                if let Value::Object(map) = &parent_val {
                    if let Some(v) = map.get(field) {
                        return Ok(v.clone());
                    }
                }
                if let Expr::Identifier(p_name) = parent.as_ref() {
                    if let Some(v) = record.get(&format!("{}.{}", p_name, field)) {
                        return Ok(v.clone());
                    }
                    let owned_by_other = record.iter().any(|(k, v)| {
                        k != p_name && matches!(v, Value::Object(m) if m.contains_key(field))
                    });
                    if !owned_by_other {
                        if let Some(v) = record.get(field) {
                            return Ok(v.clone());
                        }
                    }
                }
                Ok(Value::Null)
            }
            Expr::Index { base, index } => {
                let b = Self::eval_stateful_expr_fallible(base, record, state)?;
                let i = Self::eval_stateful_expr_fallible(index, record, state)?;
                Ok(Self::index_value(&b, &i))
            }
            Expr::Slice { base, lo, hi } => {
                let b = Self::eval_stateful_expr_fallible(base, record, state)?;
                let l = match lo {
                    Some(e) => Some(Self::eval_stateful_expr_fallible(e, record, state)?),
                    None => None,
                };
                let h = match hi {
                    Some(e) => Some(Self::eval_stateful_expr_fallible(e, record, state)?),
                    None => None,
                };
                Ok(Self::slice_value(&b, l.as_ref(), h.as_ref()))
            }
            Expr::BinaryOp { left, op, right } => {
                let l = Self::eval_stateful_expr_fallible(left, record, state)?;
                let r = Self::eval_stateful_expr_fallible(right, record, state)?;
                Self::eval_binary_op_fallible(&l, op, &r)
            }
            Expr::UnaryOp { op, expr } => {
                let v = Self::eval_stateful_expr_fallible(expr, record, state)?;
                Ok(Self::eval_unary_op(op, &v))
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let v = Self::eval_stateful_expr_fallible(expr, record, state)?;
                let l = Self::eval_stateful_expr_fallible(low, record, state)?;
                let h = Self::eval_stateful_expr_fallible(high, record, state)?;
                Self::eval_between_fallible(&v, &l, &h, *negated)
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let v = Self::eval_stateful_expr_fallible(expr, record, state)?;
                let mut matched = false;
                for item in list {
                    let item_val = Self::eval_stateful_expr_fallible(item, record, state)?;
                    if Self::values_equal(&v, &item_val) {
                        matched = true;
                        break;
                    }
                }
                if *negated {
                    Ok(Value::Bool(!matched))
                } else {
                    Ok(Value::Bool(matched))
                }
            }
            Expr::IsNull { expr, negated } => {
                let v = Self::eval_stateful_expr_fallible(expr, record, state)?;
                let is_null = v.is_null();
                if *negated {
                    Ok(Value::Bool(!is_null))
                } else {
                    Ok(Value::Bool(is_null))
                }
            }
            Expr::Call { .. } => {
                Self::eval_stateful_call_fallible(expr, record, state, None, false)
            }
            Expr::Over {
                call,
                partition_by,
                when,
            } => {
                let partition_key = match partition_by {
                    Some(p) => {
                        Self::value_to_key(&Self::eval_stateful_expr_fallible(p, record, state)?)
                    }
                    None => String::new(),
                };
                let skip_update = if let Some(cond) = when {
                    let cond_val = Self::eval_stateful_expr_fallible(cond, record, state)?;
                    !cond_val.as_bool().unwrap_or(false)
                } else {
                    false
                };
                Self::eval_stateful_call_fallible(
                    call,
                    record,
                    state,
                    Some(&partition_key),
                    skip_update,
                )
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => Self::eval_case_fallible(operand, when_clauses, else_clause, |e| {
                Self::eval_stateful_expr_fallible(e, record, state)
            }),
        }
    }

    fn eval_stateful_call_fallible<R: RowLookup + ?Sized>(
        expr: &Expr,
        record: &R,
        state: &RuleState,
        partition_key: Option<&str>,
        skip_update: bool,
    ) -> Result<Value, String> {
        let (name, args) = match expr {
            Expr::Call { name, args } => (name, args),
            _ => return Self::eval_stateful_expr_fallible(expr, record, state),
        };
        let lowered = name.to_ascii_lowercase();
        if Self::is_acc_call(&lowered) {
            let vals: Vec<Value> = args
                .iter()
                .map(|a| Self::eval_stateful_expr_fallible(a, record, state))
                .collect::<Result<_, _>>()?;
            let call_id = expr.to_ekuiper_string();
            return Ok(Self::eval_acc_call(
                &lowered,
                &vals,
                state,
                &call_id,
                partition_key.unwrap_or(""),
                skip_update,
            ));
        }
        if lowered == "lag" {
            let vals: Vec<Value> = args
                .iter()
                .map(|a| Self::eval_stateful_expr_fallible(a, record, state))
                .collect::<Result<_, _>>()?;
            let call_id = expr.to_ekuiper_string();
            return Ok(Self::eval_lag(
                &vals,
                state,
                &call_id,
                partition_key.unwrap_or(""),
                skip_update,
            ));
        }
        if lowered == "sequence_match" || lowered == "sequence_count" {
            let vals: Vec<Value> = args
                .iter()
                .map(|a| Self::eval_stateful_expr_fallible(a, record, state))
                .collect::<Result<_, _>>()?;
            let call_id = expr.to_ekuiper_string();
            let event_ts = Self::resolve_event_time(record).as_i64().unwrap_or(0);
            return Ok(Self::eval_sequence_cep(
                &lowered,
                &vals,
                event_ts,
                state,
                &call_id,
                partition_key.unwrap_or(""),
                skip_update,
            ));
        }
        if lowered == "had_changed" || lowered == "changed_col" {
            let vals: Vec<Value> = args
                .iter()
                .map(|a| Self::eval_stateful_expr_fallible(a, record, state))
                .collect::<Result<_, _>>()?;
            let call_id = expr.to_ekuiper_string();
            return Ok(Self::eval_changed(
                &lowered,
                &vals,
                state,
                &call_id,
                partition_key.unwrap_or(""),
            ));
        }
        if lowered == "changed_cols" && args.len() >= 2 {
            let vals: Vec<Value> = args
                .iter()
                .map(|a| Self::eval_stateful_expr_fallible(a, record, state))
                .collect::<Result<_, _>>()?;
            let prefix = match vals.first() {
                Some(Value::String(s)) => s.as_str(),
                _ => "",
            };
            let ignore_null = match vals.get(1) {
                Some(Value::Bool(b)) => *b,
                _ => false,
            };
            let mut result_map = serde_json::Map::new();
            for (arg_idx, expr_arg) in args.iter().skip(2).enumerate() {
                let col_name = Self::column_name(expr_arg, arg_idx);
                let val = &vals[arg_idx + 2];
                if ignore_null && val.is_null() {
                    continue;
                }
                let call_id = expr.to_ekuiper_string();
                let state_key = format!(
                    "changed_cols:{}:{}:{}",
                    call_id,
                    col_name,
                    partition_key.unwrap_or("")
                );
                let prev = state.state.read().get(&state_key).cloned();
                let changed = match (&prev, val) {
                    (None, _) => true,
                    (Some(p), c) if p.is_null() && c.is_null() => false,
                    (Some(p), c) => !Self::values_equal(p, c),
                };
                if changed {
                    state.state.write().insert(state_key, (*val).clone());
                    result_map.insert(format!("{}{}", prefix, col_name), (*val).clone());
                }
            }
            if result_map.is_empty() {
                return Ok(Value::Null);
            }
            return Ok(Value::Object(result_map));
        }
        if lowered == "row_number" {
            if !args.is_empty() {
                return Ok(Value::Null);
            }
            let call_id = expr.to_ekuiper_string();
            let state_key = format!("row_number:{}:{}", call_id, partition_key.unwrap_or(""));
            let mut guard = state.state.write();
            let current = guard.get(&state_key).and_then(|v| v.as_i64()).unwrap_or(0);
            if skip_update {
                return Ok(Value::from(current));
            }
            let next = current.saturating_add(1);
            guard.insert(state_key, Value::from(next));
            return Ok(Value::from(next));
        }
        if lowered == "last_hit_count" {
            let call_id = expr.to_ekuiper_string();
            let state_key = format!(
                "$$last_hit_count:{}:{}",
                call_id,
                partition_key.unwrap_or("")
            );
            let mut guard = state.state.write();
            let current = guard.get(&state_key).and_then(|v| v.as_i64()).unwrap_or(0);
            guard.insert(state_key, Value::from(current.saturating_add(1)));
            return Ok(Value::from(current));
        }
        if lowered == "last_hit_time" {
            let call_id = expr.to_ekuiper_string();
            let state_key = format!(
                "$$last_hit_time:{}:{}",
                call_id,
                partition_key.unwrap_or("")
            );
            let event_time = Self::resolve_event_time(record).as_i64().unwrap_or(0);
            let mut guard = state.state.write();
            let prev = guard.get(&state_key).and_then(|v| v.as_i64()).unwrap_or(0);
            guard.insert(state_key, Value::from(event_time));
            return Ok(Value::from(prev));
        }
        let lowered = name.to_ascii_lowercase();
        if lowered == "rule_id" {
            if let Some(Value::String(s)) = record.get("__rule_id__") {
                return Ok(Value::String(s.clone()));
            }
            if let Some(Value::String(s)) = state.state.read().get("__rule_id__") {
                return Ok(Value::String(s.clone()));
            }
            return Ok(Value::String(String::new()));
        }
        if lowered == "rule_start" {
            if let Some(Value::Number(n)) = record.get("__rule_start__") {
                return Ok(Value::Number(n.clone()));
            }
            if let Some(Value::Number(n)) = state.state.read().get("__rule_start__") {
                return Ok(Value::Number(n.clone()));
            }
            return Ok(Value::from(chrono::Utc::now().timestamp_millis()));
        }
        // Contextual system functions resolve against the record itself.
        if let Some(v) = Self::eval_context_call(name, args, record) {
            return Ok(v);
        }
        if matches!(
            name.to_ascii_lowercase().as_str(),
            "count" | "sum" | "avg" | "min" | "max"
        ) {
            let row_map: HashMap<String, Value> =
                record.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            return Ok(Self::eval_aggregate_call(
                name,
                args,
                std::slice::from_ref(&row_map),
            ));
        }
        let vals: Vec<Value> = args
            .iter()
            .map(|a| Self::eval_stateful_expr_fallible(a, record, state))
            .collect::<Result<_, _>>()?;
        Self::eval_call_fallible(name, &vals)
    }

    fn is_acc_call(lowered_name: &str) -> bool {
        matches!(
            lowered_name,
            "acc_map_agg"
                | "acc_max"
                | "acc_max_by"
                | "acc_min"
                | "acc_min_by"
                | "acc_count"
                | "acc_sum"
                | "acc_avg"
                | "acc_collect"
                | "acc_distinct_collect"
                | "distinct_acc"
        )
    }

    /// Deterministic, type-tagged rendering of a partition value for state keys.
    fn value_to_key(v: &Value) -> String {
        match v {
            Value::Null => "null:".to_string(),
            Value::Bool(b) => format!("bool:{}", b),
            Value::Number(n) => format!("num:{}", n),
            Value::String(s) => format!("str:{}", s),
            Value::Array(_) | Value::Object(_) => {
                format!("json:{}", serde_json::to_string(v).unwrap_or_default())
            }
        }
    }

    fn eval_acc_call(
        lowered_name: &str,
        args: &[Value],
        state: &RuleState,
        call_id: &str,
        partition_key: &str,
        skip_update: bool,
    ) -> Value {
        let state_key = format!("{}:{}:{}", lowered_name, call_id, partition_key);
        if skip_update {
            if lowered_name == "acc_count" {
                return state
                    .state
                    .read()
                    .get(&state_key)
                    .cloned()
                    .unwrap_or(Value::from(0));
            }
            if lowered_name == "acc_avg" {
                return Self::acc_avg_current(state, &state_key);
            }
            return state
                .state
                .read()
                .get(&state_key)
                .cloned()
                .unwrap_or(Value::Null);
        }
        match lowered_name {
            "acc_map_agg" => Self::acc_map_agg(state, &state_key, args),
            "acc_max" => Self::acc_extreme(state, &state_key, args, true),
            "acc_max_by" => Self::acc_extreme_by(state, &state_key, args, true),
            "acc_min" => Self::acc_extreme(state, &state_key, args, false),
            "acc_min_by" => Self::acc_extreme_by(state, &state_key, args, false),
            "acc_count" => Self::acc_count(state, &state_key, args),
            "acc_sum" => Self::acc_sum(state, &state_key, args),
            "acc_avg" => Self::acc_avg(state, &state_key, args),
            "acc_collect" => Self::acc_collect(state, &state_key, args),
            "acc_distinct_collect" | "distinct_acc" => {
                Self::acc_distinct_collect(state, &state_key, args)
            }
            _ => Value::Null,
        }
    }

    /// Zero-Vec helper to resolve the output column name for a single SELECT field at `idx`.
    #[inline]
    pub fn select_field_name_at(stmt: &SelectStmt, idx: usize) -> String {
        if let Some(Some(alias)) = stmt.field_aliases.get(idx) {
            return alias.clone();
        }
        match &stmt.fields[idx] {
            Expr::Wildcard | Expr::WildcardModified { .. } => "*".to_string(),
            Expr::Identifier(name) => name.clone(),
            Expr::FieldAccess { field: leaf, .. } => leaf.clone(),
            Expr::Call { name, .. } => name.clone(),
            _ => {
                let unaliased_idx = stmt.fields[..idx]
                    .iter()
                    .enumerate()
                    .filter(|&(i, f)| {
                        stmt.field_aliases.get(i).and_then(|a| a.as_ref()).is_none()
                            && !matches!(
                                f,
                                Expr::Wildcard
                                    | Expr::WildcardModified { .. }
                                    | Expr::Identifier(_)
                                    | Expr::FieldAccess { .. }
                                    | Expr::Call { .. }
                            )
                    })
                    .count();
                format!("kuiper_field_{}", unaliased_idx)
            }
        }
    }

    pub fn select_field_names(stmt: &SelectStmt) -> Vec<String> {
        (0..stmt.fields.len())
            .map(|i| Self::select_field_name_at(stmt, i))
            .collect()
    }

    pub fn column_name(expr: &Expr, idx: usize) -> String {
        match expr {
            Expr::Wildcard | Expr::WildcardModified { .. } => "*".to_string(),
            Expr::Identifier(name) => name.clone(),
            Expr::FieldAccess { field: leaf, .. } => leaf.clone(),
            Expr::Call { name, .. } => name.clone(),
            _ => format!("kuiper_field_{}", idx),
        }
    }

    /// Static result-type inference for schema introspection (`GET
    /// /rules/:id/schema`). Dynamic inputs (identifiers, untyped calls)
    /// infer to `"any"`.
    pub fn infer_expr_type(expr: &Expr) -> &'static str {
        match expr {
            Expr::Wildcard
            | Expr::WildcardModified { .. }
            | Expr::Identifier(_)
            | Expr::FieldAccess { .. } => "any",
            Expr::Index { .. } => "any",
            Expr::Slice { .. } => "array",
            Expr::Literal(val) => match val {
                Value::Bool(_) => "boolean",
                Value::Number(n) => {
                    if n.is_i64() || n.is_u64() {
                        "bigint"
                    } else {
                        "float"
                    }
                }
                Value::String(_) => "string",
                Value::Array(_) => "array",
                Value::Object(_) => "struct",
                Value::Null => "any",
            },
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::Eq
                | BinaryOperator::Neq
                | BinaryOperator::Lt
                | BinaryOperator::Lte
                | BinaryOperator::Gt
                | BinaryOperator::Gte
                | BinaryOperator::And
                | BinaryOperator::Or
                | BinaryOperator::Like => "boolean",
                BinaryOperator::Div => "float",
                BinaryOperator::BitAnd | BinaryOperator::BitOr | BinaryOperator::BitXor => "bigint",
                BinaryOperator::Add
                | BinaryOperator::Sub
                | BinaryOperator::Mul
                | BinaryOperator::Mod => {
                    if Self::infer_expr_type(left) == "bigint"
                        && Self::infer_expr_type(right) == "bigint"
                    {
                        "bigint"
                    } else {
                        "float"
                    }
                }
            },
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => "boolean",
                UnaryOperator::Neg => {
                    if Self::infer_expr_type(expr) == "bigint" {
                        "bigint"
                    } else {
                        "float"
                    }
                }
            },
            Expr::Between { .. } | Expr::InList { .. } | Expr::IsNull { .. } => "boolean",
            Expr::Call { name, .. } => match name.to_ascii_lowercase().as_str() {
                "avg" | "stddev" | "stddevs" | "var" | "vars" | "percentile"
                | "percentile_cont" | "sin" | "cos" | "tan" | "asin" | "acos" | "atan"
                | "atan2" | "cosh" | "sinh" | "tanh" | "cot" | "radians" | "degrees" | "exp"
                | "ln" | "log" | "log2" | "log10" | "sqrt" | "pi" | "rand" => "float",
                "count" | "length" | "cardinality" | "array_cardinality" | "array_length"
                | "array_position" | "row_number" | "acc_count" | "sequence_count" | "year"
                | "month" | "day" | "day_of_week" | "day_of_month" | "day_of_year" | "hour"
                | "minute" | "second" | "microsecond" | "tstamp" | "to_seconds" | "from_days"
                | "bitand" | "bitor" | "bitxor" | "bitnot" => "bigint",
                "isnan" | "isnumeric" | "isnull" | "had_changed" | "changed_col"
                | "sequence_match" | "regexp_matches" | "startswith" | "endswith"
                | "json_path_exists" | "array_contains" | "array_contains_any" => "boolean",
                "concat" | "lower" | "upper" | "trim" | "ltrim" | "rtrim" | "lpad" | "rpad"
                | "replace" | "reverse" | "substr" | "substring" | "regexp_replace"
                | "regexp_substring" | "regexp_substr" | "split_value" | "chr" | "hex2dec"
                | "dec2hex" | "encode" | "base64_encode" | "decode" | "base64_decode" | "uuid"
                | "newuuid" | "format_date" | "day_name" | "month_name" | "to_json" | "tojson"
                | "rule_id" => "string",
                "split"
                | "array_create"
                | "array_slice"
                | "array_concat"
                | "deduplicate"
                | "array_remove"
                | "array_distinct"
                | "array_intersect"
                | "array_union"
                | "array_except"
                | "array_flatten"
                | "array_sort"
                | "repeat"
                | "sequence"
                | "obj_to_kvpair_array"
                | "object_to_kvpair_array"
                | "collect"
                | "keys"
                | "values"
                | "items"
                | "acc_collect" => "array",
                "object_construct"
                | "object_concat"
                | "object"
                | "zip"
                | "erase"
                | "object_erase"
                | "object_pick"
                | "kvpair_array_to_obj"
                | "json_map"
                | "merge_agg"
                | "acc_map_agg" => "struct",
                _ => "any",
            },
            Expr::Case {
                when_clauses,
                else_clause,
                ..
            } => {
                for (_, then_expr) in when_clauses {
                    let inferred = Self::infer_expr_type(then_expr);
                    if inferred != "any" {
                        return inferred;
                    }
                }
                else_clause
                    .as_ref()
                    .map(|e| Self::infer_expr_type(e))
                    .unwrap_or("any")
            }
            Expr::Over { call, .. } => Self::infer_expr_type(call),
        }
    }

    /// Output column name to inferred type for every SELECT field, honoring
    /// explicit `AS` aliases.
    pub fn infer_select_schema(stmt: &SelectStmt) -> serde_json::Map<String, Value> {
        let mut schema = serde_json::Map::new();
        let field_names = Self::select_field_names(stmt);
        for (idx, field) in stmt.fields.iter().enumerate() {
            let field_name = field_names[idx].clone();
            schema.insert(
                field_name,
                Value::String(Self::infer_expr_type(field).to_string()),
            );
        }
        schema
    }

    pub fn eval_bool_fallible<R: RowLookup + ?Sized>(
        expr: &Expr,
        record: &R,
    ) -> Result<bool, String> {
        Self::eval_bool_stateful_fallible(expr, record, &RuleState::new())
    }

    #[inline]
    pub fn eval_bool_stateful_fallible<R: RowLookup + ?Sized>(
        expr: &Expr,
        record: &R,
        state: &RuleState,
    ) -> Result<bool, String> {
        // Fast path: direct borrowed comparison for `col <op> literal` or `literal <op> col`
        // (eliminates 2 Value clones and 2 recursive AST calls per WHERE evaluation).
        if let Expr::BinaryOp { left, op, right } = expr {
            match (left.as_ref(), right.as_ref()) {
                (Expr::Identifier(name), Expr::Literal(lit)) => {
                    if let Some(l_val) = record.get(name) {
                        if let (Some(lf), Some(rf)) = (l_val.as_f64(), lit.as_f64()) {
                            return match op {
                                BinaryOperator::Gt => Ok(lf > rf),
                                BinaryOperator::Gte => Ok(lf >= rf),
                                BinaryOperator::Lt => Ok(lf < rf),
                                BinaryOperator::Lte => Ok(lf <= rf),
                                BinaryOperator::Eq => Ok((lf - rf).abs() < f64::EPSILON),
                                BinaryOperator::Neq => Ok((lf - rf).abs() >= f64::EPSILON),
                                _ => match Self::eval_binary_op_fallible(l_val, op, lit) {
                                    Ok(Value::Bool(b)) => Ok(b),
                                    Ok(Value::Null) => Ok(false),
                                    Ok(other) => Err(format!(
                                        "run Where error: invalid condition that returns non-bool value {}",
                                        Self::format_ekuiper_val_type(&other)
                                    )),
                                    Err(e) => Err(format!("run Where error: {}", e)),
                                },
                            };
                        }
                        return match Self::eval_binary_op_fallible(l_val, op, lit) {
                            Ok(Value::Bool(b)) => Ok(b),
                            Ok(Value::Null) => Ok(false),
                            Ok(other) => Err(format!(
                                "run Where error: invalid condition that returns non-bool value {}",
                                Self::format_ekuiper_val_type(&other)
                            )),
                            Err(e) => Err(format!("run Where error: {}", e)),
                        };
                    }
                }
                (Expr::Literal(lit), Expr::Identifier(name)) => {
                    if let Some(r_val) = record.get(name) {
                        if let (Some(lf), Some(rf)) = (lit.as_f64(), r_val.as_f64()) {
                            return match op {
                                BinaryOperator::Gt => Ok(lf > rf),
                                BinaryOperator::Gte => Ok(lf >= rf),
                                BinaryOperator::Lt => Ok(lf < rf),
                                BinaryOperator::Lte => Ok(lf <= rf),
                                BinaryOperator::Eq => Ok((lf - rf).abs() < f64::EPSILON),
                                BinaryOperator::Neq => Ok((lf - rf).abs() >= f64::EPSILON),
                                _ => match Self::eval_binary_op_fallible(lit, op, r_val) {
                                    Ok(Value::Bool(b)) => Ok(b),
                                    Ok(Value::Null) => Ok(false),
                                    Ok(other) => Err(format!(
                                        "run Where error: invalid condition that returns non-bool value {}",
                                        Self::format_ekuiper_val_type(&other)
                                    )),
                                    Err(e) => Err(format!("run Where error: {}", e)),
                                },
                            };
                        }
                        return match Self::eval_binary_op_fallible(lit, op, r_val) {
                            Ok(Value::Bool(b)) => Ok(b),
                            Ok(Value::Null) => Ok(false),
                            Ok(other) => Err(format!(
                                "run Where error: invalid condition that returns non-bool value {}",
                                Self::format_ekuiper_val_type(&other)
                            )),
                            Err(e) => Err(format!("run Where error: {}", e)),
                        };
                    }
                }
                _ => {}
            }
        }

        match Self::eval_stateful_expr_fallible(expr, record, state) {
            Ok(Value::Bool(b)) => Ok(b),
            Ok(Value::Null) => Ok(false),
            Ok(other) => Err(format!(
                "run Where error: invalid condition that returns non-bool value {}",
                Self::format_ekuiper_val_type(&other)
            )),
            Err(e) => Err(format!("run Where error: {}", e)),
        }
    }

    pub fn eval_bool<R: RowLookup + ?Sized>(expr: &Expr, record: &R) -> bool {
        Self::eval_bool_fallible(expr, record).unwrap_or(false)
    }

    pub fn eval_bool_stateful<R: RowLookup + ?Sized>(
        expr: &Expr,
        record: &R,
        state: &RuleState,
    ) -> bool {
        Self::eval_bool_stateful_fallible(expr, record, state).unwrap_or(false)
    }

    pub fn eval_val_fallible<R: RowLookup + ?Sized>(
        expr: &Expr,
        record: &R,
    ) -> Result<Value, String> {
        Self::eval_stateful_expr_fallible(expr, record, &RuleState::new())
    }

    pub fn eval_val<R: RowLookup + ?Sized>(expr: &Expr, record: &R) -> Value {
        Self::eval_val_fallible(expr, record).unwrap_or(Value::Null)
    }

    fn eval_unary_op(op: &UnaryOperator, val: &Value) -> Value {
        match op {
            UnaryOperator::Not => match val {
                Value::Bool(b) => Value::Bool(!b),
                Value::Null => Value::Null,
                _ => Value::Null,
            },
            UnaryOperator::Neg => {
                if val.is_null() {
                    return Value::Null;
                }
                if let Some(i) = val.as_i64() {
                    // checked_neg to avoid overflow on i64::MIN
                    if let Some(n) = i.checked_neg() {
                        return Value::from(n);
                    } else {
                        // i64::MIN negated overflows; fall back to float
                        return serde_json::json!(-(i as f64));
                    }
                }
                if let Some(u) = val.as_u64() {
                    // u64 that didn't fit in i64 path (large); negate to i64 if fits else float
                    if u <= i64::MAX as u64 {
                        return Value::from(-(u as i64));
                    } else {
                        return serde_json::json!(-(u as f64));
                    }
                }
                if let Some(f) = val.as_f64() {
                    return serde_json::json!(-f);
                }
                Value::Null
            }
        }
    }

    fn eval_binary_op_fallible(
        left: &Value,
        op: &BinaryOperator,
        right: &Value,
    ) -> Result<Value, String> {
        let op_token = match op {
            BinaryOperator::Add => "+",
            BinaryOperator::Sub => "-",
            BinaryOperator::Mul => "*",
            BinaryOperator::Div => "/",
            BinaryOperator::Mod => "%",
            BinaryOperator::Eq => "=",
            BinaryOperator::Neq => "!=",
            BinaryOperator::Lt => "<",
            BinaryOperator::Lte => "<=",
            BinaryOperator::Gt => ">",
            BinaryOperator::Gte => ">=",
            BinaryOperator::And => "AND",
            BinaryOperator::Or => "OR",
            BinaryOperator::BitAnd => "&",
            BinaryOperator::BitOr => "|",
            BinaryOperator::BitXor => "^",
            BinaryOperator::Like => "LIKE",
        };

        if left.is_null() || right.is_null() {
            return match op {
                BinaryOperator::And
                | BinaryOperator::Or
                | BinaryOperator::BitAnd
                | BinaryOperator::BitOr
                | BinaryOperator::BitXor
                | BinaryOperator::Eq
                | BinaryOperator::Neq
                | BinaryOperator::Gt
                | BinaryOperator::Gte
                | BinaryOperator::Lt
                | BinaryOperator::Lte => Ok(Value::Bool(false)),
                _ => Ok(Value::Null),
            };
        }

        match op {
            BinaryOperator::And => {
                if let (Some(lb), Some(rb)) = (left.as_bool(), right.as_bool()) {
                    Ok(Value::Bool(lb && rb))
                } else {
                    Err(format!(
                        "invalid operation {} AND {}",
                        Self::format_ekuiper_val_type(left),
                        Self::format_ekuiper_val_type(right)
                    ))
                }
            }
            BinaryOperator::Or => {
                if let (Some(lb), Some(rb)) = (left.as_bool(), right.as_bool()) {
                    Ok(Value::Bool(lb || rb))
                } else {
                    Err(format!(
                        "invalid operation {} OR {}",
                        Self::format_ekuiper_val_type(left),
                        Self::format_ekuiper_val_type(right)
                    ))
                }
            }
            BinaryOperator::Eq => {
                if left.is_number() && right.is_number() {
                    Ok(Value::Bool(Self::values_equal(left, right)))
                } else if (left.is_string() && right.is_string())
                    || (left.is_boolean() && right.is_boolean())
                {
                    Ok(Value::Bool(left == right))
                } else {
                    Err(format!(
                        "invalid operation {} = {}",
                        Self::format_ekuiper_val_type(left),
                        Self::format_ekuiper_val_type(right)
                    ))
                }
            }
            BinaryOperator::Neq => {
                if left.is_number() && right.is_number() {
                    Ok(Value::Bool(!Self::values_equal(left, right)))
                } else if (left.is_string() && right.is_string())
                    || (left.is_boolean() && right.is_boolean())
                {
                    Ok(Value::Bool(left != right))
                } else {
                    Err(format!(
                        "invalid operation {} != {}",
                        Self::format_ekuiper_val_type(left),
                        Self::format_ekuiper_val_type(right)
                    ))
                }
            }
            BinaryOperator::Lt | BinaryOperator::Lte | BinaryOperator::Gt | BinaryOperator::Gte => {
                let ordering = if left.is_number() && right.is_number() {
                    left.as_f64()
                        .and_then(|lf| right.as_f64().and_then(|rf| lf.partial_cmp(&rf)))
                } else if let (Some(ls), Some(rs)) = (left.as_str(), right.as_str()) {
                    Some(ls.cmp(rs))
                } else if let (Some(lb), Some(rb)) = (left.as_bool(), right.as_bool()) {
                    Some(lb.cmp(&rb))
                } else {
                    return Err(format!(
                        "invalid operation {} {} {}",
                        Self::format_ekuiper_val_type(left),
                        op_token,
                        Self::format_ekuiper_val_type(right)
                    ));
                };

                let matched = matches!(
                    (op, ordering),
                    (BinaryOperator::Lt, Some(std::cmp::Ordering::Less))
                        | (
                            BinaryOperator::Lte,
                            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal),
                        )
                        | (BinaryOperator::Gt, Some(std::cmp::Ordering::Greater))
                        | (
                            BinaryOperator::Gte,
                            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal),
                        )
                );
                Ok(Value::Bool(matched))
            }
            BinaryOperator::Add => {
                if left.is_number() && right.is_number() {
                    if left.is_i64() && right.is_i64() {
                        Ok(Value::from(
                            left.as_i64()
                                .unwrap()
                                .saturating_add(right.as_i64().unwrap()),
                        ))
                    } else {
                        let lf = left.as_f64().unwrap();
                        let rf = right.as_f64().unwrap();
                        Ok(serde_json::json!(lf + rf))
                    }
                } else {
                    Err(format!(
                        "invalid operation {} + {}",
                        Self::format_ekuiper_val_type(left),
                        Self::format_ekuiper_val_type(right)
                    ))
                }
            }
            BinaryOperator::Sub => {
                if left.is_number() && right.is_number() {
                    if left.is_i64() && right.is_i64() {
                        Ok(Value::from(
                            left.as_i64()
                                .unwrap()
                                .saturating_sub(right.as_i64().unwrap()),
                        ))
                    } else {
                        let lf = left.as_f64().unwrap();
                        let rf = right.as_f64().unwrap();
                        Ok(serde_json::json!(lf - rf))
                    }
                } else {
                    Err(format!(
                        "invalid operation {} - {}",
                        Self::format_ekuiper_val_type(left),
                        Self::format_ekuiper_val_type(right)
                    ))
                }
            }
            BinaryOperator::Mul => {
                if left.is_number() && right.is_number() {
                    if left.is_i64() && right.is_i64() {
                        Ok(Value::from(
                            left.as_i64()
                                .unwrap()
                                .saturating_mul(right.as_i64().unwrap()),
                        ))
                    } else {
                        let lf = left.as_f64().unwrap();
                        let rf = right.as_f64().unwrap();
                        Ok(serde_json::json!(lf * rf))
                    }
                } else {
                    Err(format!(
                        "invalid operation {} * {}",
                        Self::format_ekuiper_val_type(left),
                        Self::format_ekuiper_val_type(right)
                    ))
                }
            }
            BinaryOperator::Div => {
                if left.is_number() && right.is_number() {
                    let rf = right.as_f64().unwrap();
                    if rf == 0.0 {
                        return Err("divided by zero".to_string());
                    }
                    if left.is_i64() && right.is_i64() {
                        let li = left.as_i64().unwrap();
                        let ri = right.as_i64().unwrap();
                        if ri == 0 {
                            return Err("divided by zero".to_string());
                        }
                        Ok(Value::from(li / ri))
                    } else {
                        let lf = left.as_f64().unwrap();
                        Ok(serde_json::json!(lf / rf))
                    }
                } else {
                    Err(format!(
                        "invalid operation {} / {}",
                        Self::format_ekuiper_val_type(left),
                        Self::format_ekuiper_val_type(right)
                    ))
                }
            }
            BinaryOperator::Mod => {
                if left.is_number() && right.is_number() {
                    let rf = right.as_f64().unwrap();
                    if rf == 0.0 {
                        return Err("divided by zero".to_string());
                    }
                    if left.is_i64() && right.is_i64() {
                        let li = left.as_i64().unwrap();
                        let ri = right.as_i64().unwrap();
                        if ri == 0 {
                            return Err("divided by zero".to_string());
                        }
                        Ok(Value::from(li % ri))
                    } else {
                        let lf = left.as_f64().unwrap();
                        Ok(serde_json::json!(lf % rf))
                    }
                } else {
                    Err(format!(
                        "invalid operation {} % {}",
                        Self::format_ekuiper_val_type(left),
                        Self::format_ekuiper_val_type(right)
                    ))
                }
            }
            BinaryOperator::Like => Ok(Self::eval_like(left, right)),
            BinaryOperator::BitAnd | BinaryOperator::BitOr | BinaryOperator::BitXor => {
                Ok(Self::eval_bitwise(left, right, op))
            }
        }
    }

    fn eval_binary_op(left: &Value, op: &BinaryOperator, right: &Value) -> Value {
        Self::eval_binary_op_fallible(left, op, right).unwrap_or(Value::Null)
    }

    /// Numeric-aware equality: 1 == 1.0 is true; otherwise strict Value equality.
    pub(crate) fn values_equal(a: &Value, b: &Value) -> bool {
        if a.is_null() || b.is_null() {
            // In SQL, NULL = NULL is UNKNOWN (not true). For WHERE filtering,
            // UNKNOWN is treated as false. But for direct Eq evaluation, follow
            // eKuiper-ish simple semantics: NULL == NULL -> false? Or true?
            // Most engines: NULL = NULL is NULL (false in WHERE).
            // Return false when either is NULL to keep WHERE correct.
            // However, for IN list with NULL? NULL IN (...) should be false.
            // So return false if either NULL.
            return false;
        }
        if a.is_number() && b.is_number() {
            // Compare as f64; handles int/float mixing.
            // For large ints beyond 2^53, f64 loses precision, so also try exact i64/u64 compare first.
            if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
                return ai == bi;
            }
            if let (Some(au), Some(bu)) = (a.as_u64(), b.as_u64()) {
                return au == bu;
            }
            // Mixed int/uint/float: compare as f64
            if let (Some(af), Some(bf)) = (a.as_f64(), b.as_f64()) {
                return af == bf;
            }
            return false;
        }
        a == b
    }

    pub(crate) fn compare_values(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
        if a.is_number() && b.is_number() {
            let af = a.as_f64()?;
            let bf = b.as_f64()?;
            return af.partial_cmp(&bf);
        }
        if let (Some(as_), Some(bs)) = (a.as_str(), b.as_str()) {
            return Some(as_.cmp(bs));
        }
        // Booleans? false < true
        if let (Some(ab), Some(bb)) = (a.as_bool(), b.as_bool()) {
            return Some(ab.cmp(&bb));
        }
        None
    }

    fn eval_between_fallible(
        val: &Value,
        low: &Value,
        high: &Value,
        negated: bool,
    ) -> Result<Value, String> {
        if val.is_null() || low.is_null() || high.is_null() {
            return Ok(Value::Bool(false));
        }
        let ord1 = match Self::compare_values(val, low) {
            Some(o) => o,
            None => {
                return Err(format!(
                    "between operator cannot compare {} and {}",
                    Self::format_ekuiper_val_type(val),
                    Self::format_ekuiper_val_type(low)
                ));
            }
        };
        let ord2 = match Self::compare_values(val, high) {
            Some(o) => o,
            None => {
                return Err(format!(
                    "between operator cannot compare {} and {}",
                    Self::format_ekuiper_val_type(val),
                    Self::format_ekuiper_val_type(high)
                ));
            }
        };
        let in_range = (ord1 == std::cmp::Ordering::Greater || ord1 == std::cmp::Ordering::Equal)
            && (ord2 == std::cmp::Ordering::Less || ord2 == std::cmp::Ordering::Equal);
        if negated {
            Ok(Value::Bool(!in_range))
        } else {
            Ok(Value::Bool(in_range))
        }
    }

    fn eval_between(val: &Value, low: &Value, high: &Value, negated: bool) -> Value {
        Self::eval_between_fallible(val, low, high, negated).unwrap_or(Value::Bool(false))
    }

    fn eval_like(left: &Value, right: &Value) -> Value {
        let (Some(s), Some(p)) = (left.as_str(), right.as_str()) else {
            return Value::Bool(false);
        };
        Value::Bool(Self::like_match(s, p))
    }

    /// Truly zero-allocation SQL LIKE matcher:
    /// - Fast path: byte-slice matching when both strings are ASCII.
    /// - UTF-8 path: stack-only `std::str::Chars` cursor backtracking (0 heap allocations).
    fn like_match(s: &str, pattern: &str) -> bool {
        if s.is_ascii() && pattern.is_ascii() {
            let (s_bytes, p_bytes) = (s.as_bytes(), pattern.as_bytes());
            let (mut si, mut pi) = (0usize, 0usize);
            let (mut star_pi, mut star_si) = (None::<usize>, 0usize);

            while si < s_bytes.len() {
                if pi < p_bytes.len() {
                    match p_bytes[pi] {
                        b'\\' => {
                            pi += 1;
                            if pi < p_bytes.len() && p_bytes[pi] == s_bytes[si] {
                                si += 1;
                                pi += 1;
                                continue;
                            }
                        }
                        b'_' => {
                            si += 1;
                            pi += 1;
                            continue;
                        }
                        b'%' => {
                            star_pi = Some(pi);
                            star_si = si;
                            pi += 1;
                            continue;
                        }
                        b if b == s_bytes[si] => {
                            si += 1;
                            pi += 1;
                            continue;
                        }
                        _ => {}
                    }
                }
                if let Some(sp) = star_pi {
                    pi = sp + 1;
                    star_si += 1;
                    si = star_si;
                } else {
                    return false;
                }
            }
            while pi < p_bytes.len() && p_bytes[pi] == b'%' {
                pi += 1;
            }
            return pi == p_bytes.len();
        }

        // UTF-8 fallback using stack-allocated Chars iterators (zero heap allocation)
        let mut s_it = s.chars();
        let mut p_it = pattern.chars();
        let mut star_p: Option<std::str::Chars<'_>> = None;
        let mut star_s: Option<std::str::Chars<'_>> = None;

        loop {
            let mut s_peek = s_it.clone();
            let Some(sc) = s_peek.next() else {
                break;
            };

            let mut p_peek = p_it.clone();
            if let Some(pc) = p_peek.next() {
                match pc {
                    '\\' => {
                        let matched = match p_peek.next() {
                            Some(escaped) => escaped == sc,
                            None => '\\' == sc,
                        };
                        if matched {
                            s_it = s_peek;
                            p_it = p_peek;
                            continue;
                        }
                    }
                    '_' => {
                        s_it = s_peek;
                        p_it = p_peek;
                        continue;
                    }
                    '%' => {
                        p_it = p_peek;
                        star_p = Some(p_it.clone());
                        star_s = Some(s_it.clone());
                        continue;
                    }
                    c if c == sc => {
                        s_it = s_peek;
                        p_it = p_peek;
                        continue;
                    }
                    _ => {}
                }
            }

            if let (Some(ref sp), Some(ref mut ss)) = (&star_p, &mut star_s) {
                if ss.next().is_some() {
                    s_it = ss.clone();
                    p_it = sp.clone();
                    continue;
                }
            }
            return false;
        }

        while p_it.clone().next() == Some('%') {
            p_it.next();
        }
        p_it.next().is_none()
    }

    pub(crate) fn eval_arith(left: &Value, right: &Value, op: ArithOp) -> Value {
        if left.is_null() || right.is_null() {
            return Value::Null;
        }
        // Fast path: both i64 -> preserve integer (except Div which returns float)
        if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
            // Ensure both were truly i64 (not float truncated). as_i64 returns Some only for
            // integer JSON numbers, so safe.
            match op {
                ArithOp::Add => {
                    return match l.checked_add(r) {
                        Some(v) => Value::from(v),
                        None => Value::Null,
                    }
                }
                ArithOp::Sub => {
                    return match l.checked_sub(r) {
                        Some(v) => Value::from(v),
                        None => Value::Null,
                    }
                }
                ArithOp::Mul => {
                    return match l.checked_mul(r) {
                        Some(v) => Value::from(v),
                        None => Value::Null,
                    }
                }
                ArithOp::Mod => {
                    if r == 0 {
                        return Value::Null;
                    }
                    return Value::from(l % r);
                }
                ArithOp::Div => {
                    if r == 0 {
                        return Value::Null;
                    }
                    // SQL division returns float
                    let res = (l as f64) / (r as f64);
                    return serde_json::json!(res);
                }
            }
        }
        // General numeric path: convert both to f64
        if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
            let res = match op {
                ArithOp::Add => l + r,
                ArithOp::Sub => l - r,
                ArithOp::Mul => l * r,
                ArithOp::Div => {
                    if r == 0.0 {
                        return Value::Null;
                    }
                    l / r
                }
                ArithOp::Mod => {
                    if r == 0.0 {
                        return Value::Null;
                    }
                    l % r
                }
            };
            return serde_json::json!(res);
        }
        // Non-numeric operands
        Value::Null
    }

    fn eval_bitwise(left: &Value, right: &Value, op: &BinaryOperator) -> Value {
        if left.is_null() || right.is_null() {
            return Value::Null;
        }
        if let (Some(lb), Some(rb)) = (left.as_bool(), right.as_bool()) {
            return match op {
                BinaryOperator::BitAnd => Value::Bool(lb && rb),
                BinaryOperator::BitOr => Value::Bool(lb || rb),
                BinaryOperator::BitXor => Value::Bool(lb != rb),
                _ => Value::Null,
            };
        }
        if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
            return match op {
                BinaryOperator::BitAnd => Value::from(l & r),
                BinaryOperator::BitOr => Value::from(l | r),
                BinaryOperator::BitXor => Value::from(l ^ r),
                _ => Value::Null,
            };
        }
        if let (Some(l), Some(r)) = (left.as_u64(), right.as_u64()) {
            return match op {
                BinaryOperator::BitAnd => Value::from(l & r),
                BinaryOperator::BitOr => Value::from(l | r),
                BinaryOperator::BitXor => Value::from(l ^ r),
                _ => Value::Null,
            };
        }
        Value::Null
    }

    fn eval_call_fallible(name: &str, args: &[Value]) -> Result<Value, String> {
        match name.to_ascii_lowercase().as_str() {
            "abs" => Self::func_abs_fallible(args),
            "ln" => Self::func_ln_fallible(args),
            "sqrt" => Self::func_sqrt_fallible(args),
            "cot" => Self::func_cot_fallible(args),
            "mod" => Self::func_mod_fallible(args),
            "cast" => Self::func_cast_fallible(args),
            "split_value" => Self::func_split_value_fallible(args),
            _ => Ok(Self::eval_call(name, args)),
        }
    }

    pub(crate) fn eval_call(name: &str, args: &[Value]) -> Value {
        match name.to_ascii_lowercase().as_str() {
            // ---- Math ----
            "abs" => Self::func_abs(args),
            "ceil" | "ceiling" => Self::func_ceil(args),
            "floor" => Self::func_floor(args),
            "round" => Self::func_round(args),
            "sqrt" => Self::func_sqrt(args),
            "power" | "pow" => Self::func_power(args),
            // ---- Math & trig ----
            "sin" => Self::func_sin(args),
            "cos" => Self::func_cos(args),
            "tan" => Self::func_tan(args),
            "asin" => Self::func_asin(args),
            "acos" => Self::func_acos(args),
            "atan" => Self::func_atan(args),
            "atan2" => Self::func_atan2(args),
            "exp" => Self::func_exp(args),
            "ln" => Self::func_ln(args),
            "log" => Self::func_log(args),
            "log2" => Self::func_log2(args),
            "log10" => Self::func_log10(args),
            "sign" => Self::func_sign(args),
            "mod" => Self::func_mod(args),
            "cosh" => Self::func_cosh(args),
            "sinh" => Self::func_sinh(args),
            "tanh" => Self::func_tanh(args),
            "cot" => Self::func_cot(args),
            "radians" => Self::func_radians(args),
            "degrees" => Self::func_degrees(args),
            "bitand" => Self::func_bitand(args),
            "bitor" => Self::func_bitor(args),
            "bitxor" => Self::func_bitxor(args),
            "bitnot" => Self::func_bitnot(args),
            "pi" => Self::func_pi(args),
            "rand" => Self::func_rand(args),
            "conv" => Self::func_conv(args),
            // ---- String ----
            "concat" => Self::func_concat(args),
            "lower" => Self::func_lower(args),
            "upper" => Self::func_upper(args),
            "length" => Self::func_length(args),
            "trim" => Self::func_trim(args),
            "ltrim" => Self::func_ltrim(args),
            "rtrim" => Self::func_rtrim(args),
            "lpad" => Self::func_lpad(args),
            "rpad" => Self::func_rpad(args),
            "replace" => Self::func_replace(args),
            "split" => Self::func_split(args),
            "reverse" => Self::func_reverse(args),
            "substr" | "substring" => Self::func_substr(args),
            "startswith" => Self::func_startswith(args),
            "endswith" => Self::func_endswith(args),
            "indexof" => Self::func_indexof(args),
            "format" => Self::func_format(args),
            // ---- Vector similarity & math ----
            "cosine_similarity" => Self::func_cosine_similarity(args),
            "vector_l2" | "euclidean_distance" => Self::func_vector_l2(args),
            "vector_dot" | "vector_dot_product" => Self::func_vector_dot(args),
            "vector_match" => Self::func_vector_match(args),
            // ---- WebAssembly (WASM) plugin functions ----
            "wasm" | "wasm_run" => Self::func_wasm_run(args),
            // ---- Array & object ----
            "array_contains" => Self::func_array_contains(args),
            "array_join" => Self::func_array_join(args),
            "keys" => Self::func_keys(args),
            "values" => Self::func_values(args),
            "object" => Self::func_object(args),
            "zip" => Self::func_zip(args),
            "items" => Self::func_items(args),
            // ---- Conversion & utility ----
            "cast" => Self::func_cast(args),
            "coalesce" => Self::func_coalesce(args),
            "delay" => Self::func_delay(args),
            "compress" => Self::func_compress(args),
            "decompress" => Self::func_decompress(args),
            "extract" => Self::func_extract(args),
            "unnest" => Self::func_unnest_scalar(args),
            "changed_cols" => Self::func_changed_cols_scalar(args),
            // ---- Validation & utility ----
            "isnan" => Self::func_isnan(args),
            "isnumeric" => Self::func_isnumeric(args),
            "nvl" => Self::func_coalesce(args),
            // ---- Object construction ----
            "object_construct" => Self::func_object_construct(args),
            "object_concat" => Self::func_object_concat(args),
            "erase" | "object_erase" => Self::func_erase(args),
            "object_pick" => Self::func_object_pick(args),
            "obj_to_kvpair_array" | "object_to_kvpair_array" => {
                Self::func_obj_to_kvpair_array(args)
            }
            "to_json" | "tojson" => Self::func_to_json(args),
            "parse_json" | "parsejson" | "json_parse" => Self::func_parse_json(args),
            // ---- DateTime ----
            "now" => Self::func_now(args),
            "current_timestamp" | "local_timestamp" => Self::func_now(args),
            "current_date" | "cur_date" => Self::func_current_date(args),
            "current_time" | "cur_time" | "local_time" => Self::func_current_time(args),
            "format_date" | "format_time" => Self::func_format_date(args),
            "from_unix_time" => Self::func_from_unix_time(args),
            "date_parse" => Self::func_date_parse(args),
            "date_add" => Self::func_date_add(args),
            "date_diff" => Self::func_date_diff(args),
            "date_calc" => Self::func_date_calc(args),
            "convert_tz" => Self::func_convert_tz(args),
            "year" => Self::func_year(args),
            "month" => Self::func_month(args),
            "day" => Self::func_day(args),
            "day_of_week" => Self::func_day_of_week(args),
            "day_of_month" => Self::func_day(args),
            "day_of_year" => Self::func_day_of_year(args),
            "day_name" => Self::func_day_name(args),
            "month_name" => Self::func_month_name(args),
            "microsecond" => Self::func_microsecond(args),
            "last_day" => Self::func_last_day(args),
            "to_seconds" => Self::func_to_seconds(args),
            "from_days" => Self::func_from_days(args),
            "hour" => Self::func_hour(args),
            "minute" => Self::func_minute(args),
            "second" => Self::func_second(args),
            // ---- JSON path ----
            "json_path_query" => Self::func_json_path_query(args),
            "json_path_query_first" => Self::func_json_path_query_first(args),
            "json_path_exists" => Self::func_json_path_exists(args),
            "json_map" => Self::func_json_map(args),
            // ---- Crypto & encoding ----
            "md5" => Self::func_md5(args),
            "sha256" => Self::func_sha256(args),
            "sha512" => Self::func_sha512(args),
            "sha1" => Self::func_sha1(args),
            "sha384" => Self::func_sha384(args),
            "crc32" => Self::func_crc32(args),
            "regexp_matches" => Self::func_regexp_matches(args),
            "regexp_replace" => Self::func_regexp_replace(args),
            "regexp_substring" | "regexp_substr" => Self::func_regexp_substring(args),
            "split_value" => Self::func_split_value(args),
            "numbytes" => Self::func_numbytes(args),
            "chr" => Self::func_chr(args),
            "trunc" => Self::func_trunc(args),
            "hex2dec" => Self::func_hex2dec(args),
            "dec2hex" => Self::func_dec2hex(args),
            "encode" => Self::func_encode(args),
            "base64_encode" => Self::func_base64_encode(args),
            "decode" => Self::func_decode(args),
            "base64_decode" => Self::func_base64_decode(args),
            // ---- Extended array ----
            "array_create" => Self::func_array_create(args),
            "array_position" => Self::func_array_position(args),
            "array_positions" => Self::func_array_positions(args),
            "array_last_position" => Self::func_array_last_position(args),
            "array_shuffle" => Self::func_array_shuffle(args),
            "array_map" => Self::func_array_map(args),
            "array_length" => Self::func_array_length(args),
            "array_slice" => Self::func_array_slice(args),
            "array_concat" => Self::func_array_concat(args),
            "deduplicate" => Self::func_deduplicate(args),
            "cardinality" | "array_cardinality" => Self::func_cardinality(args),
            "element_at" => Self::func_element_at(args),
            "array_contains_any" => Self::func_array_contains_any(args),
            "array_remove" => Self::func_array_remove(args),
            "array_distinct" => Self::func_array_distinct(args),
            "array_intersect" => Self::func_array_intersect(args),
            "array_union" => Self::func_array_union(args),
            "array_except" => Self::func_array_except(args),
            "array_max" => Self::func_array_max(args),
            "array_min" => Self::func_array_min(args),
            "array_avg" => Self::func_array_avg(args),
            "array_flatten" => Self::func_array_flatten(args),
            "array_sort" => Self::func_array_sort(args),
            "repeat" => Self::func_repeat(args),
            "sequence" => Self::func_sequence(args),
            "kvpair_array_to_obj" => Self::func_kvpair_array_to_obj(args),
            // ---- Analytic scalar fallbacks (batch/stateful paths below) ----
            "collect" => Self::func_collect_scalar(args),
            "lead" => Self::func_lead_scalar(args),
            "lag" => Self::func_lag_scalar(args),
            "latest" => Self::func_latest_scalar(args),
            "had_changed" => Self::func_had_changed_scalar(args),
            "changed_col" => Self::func_changed_col_scalar(args),
            "row_number" => Self::func_row_number_scalar(args),
            "sequence_match" => Value::Bool(false),
            "sequence_count" => Value::from(0),
            // ---- System & metadata ----
            "isnull" => Value::Bool(match args.first() {
                Some(v) => v.is_null(),
                None => true,
            }),
            "tstamp" => serde_json::json!(chrono::Utc::now().timestamp_millis()),
            "uuid" | "newuuid" => Self::func_uuid(args),
            // Contextual functions without record context: static defaults.
            // (With a record in scope, `eval_context_call` resolves them.)
            "window_start" | "window_end" => Value::Null,
            "rule_id" => Value::String(String::new()),
            "rule_start" => serde_json::json!(chrono::Utc::now().timestamp_millis()),
            "last_hit_count" | "last_hit_time" => Value::from(0),
            "last_agg_hit_count" | "last_agg_hit_time" => Value::from(0),
            "get_keyed_state" => Self::func_get_keyed_state(args),
            "meta" | "mqtt" => Value::Null,
            "event_time" => serde_json::json!(chrono::Utc::now().timestamp_millis()),
            // ---- Registered UDFs (anything not built in) ----
            _ => Self::call_global_udf(name, args),
        }
    }

    /// RFC 4122 UUID v4 as a hyphenated lowercase string.
    fn func_uuid(_args: &[Value]) -> Value {
        Value::String(uuid::Uuid::new_v4().to_string())
    }

    fn func_get_keyed_state(args: &[Value]) -> Value {
        if args.len() != 3 {
            return Value::Null;
        }
        let Some(key) = args[0].as_str() else {
            return args.get(2).cloned().unwrap_or(Value::Null);
        };
        let Some(data_type) = args[1].as_str() else {
            return args.get(2).cloned().unwrap_or(Value::Null);
        };
        let default_val = &args[2];

        match rekuiper_core::get_keyed_state(key) {
            Some(val_str) => match data_type.to_ascii_lowercase().as_str() {
                "bigint" => val_str
                    .parse::<i64>()
                    .map(Value::from)
                    .unwrap_or_else(|_| default_val.clone()),
                "float" => val_str
                    .parse::<f64>()
                    .map(|f| serde_json::json!(f))
                    .unwrap_or_else(|_| default_val.clone()),
                "boolean" => val_str
                    .parse::<bool>()
                    .map(Value::from)
                    .unwrap_or_else(|_| default_val.clone()),
                "string" => Value::String(val_str),
                "datetime" => val_str
                    .parse::<i64>()
                    .map(Value::from)
                    .unwrap_or_else(|_| default_val.clone()),
                _ => default_val.clone(),
            },
            None => default_val.clone(),
        }
    }

    /// Resolve a contextual system function against the current record.
    /// Returns `None` for non-contextual names so callers fall through to
    /// normal argument evaluation.
    fn eval_context_call<R: RowLookup + ?Sized>(
        name: &str,
        args: &[Expr],
        record: &R,
    ) -> Option<Value> {
        match name.to_ascii_lowercase().as_str() {
            "meta" => Some(Self::resolve_meta(
                args.first(),
                record,
                &["__meta__", "meta"],
            )),
            "mqtt" => Some(Self::resolve_meta(
                args.first(),
                record,
                &["__mqtt__", "mqtt", META_KEY],
            )),
            "event_time" => Some(Self::resolve_event_time(record)),
            "rule_id" => Some(Self::resolve_rule_id(record)),
            "rule_start" => Some(Self::resolve_rule_start(record)),
            "window_start" => Some(Self::resolve_window_bound(
                record,
                &["window_start", "__window_start__"],
            )),
            "window_end" => Some(Self::resolve_window_bound(
                record,
                &["window_end", "__window_end__"],
            )),
            _ => None,
        }
    }

    /// Look a metadata key up in the first present meta object, falling back
    /// to a top-level record field. No argument returns the whole object.
    fn resolve_meta<R: RowLookup + ?Sized>(
        arg: Option<&Expr>,
        record: &R,
        object_keys: &[&str],
    ) -> Value {
        let meta_obj = object_keys
            .iter()
            .find_map(|k| record.get(k))
            .and_then(|v| v.as_object());
        let Some(arg) = arg else {
            return meta_obj
                .map(|m| Value::Object(m.clone()))
                .unwrap_or(Value::Null);
        };
        // Unquoted identifiers name the key directly; anything else is
        // evaluated first and must yield a string.
        let key = match arg {
            Expr::Wildcard => {
                return meta_obj
                    .map(|m| Value::Object(m.clone()))
                    .unwrap_or(Value::Null);
            }
            Expr::Identifier(name) => Some(name.clone()),
            other => match Self::eval_val(other, record) {
                Value::String(s) => Some(s),
                _ => None,
            },
        };
        let Some(key) = key else {
            return Value::Null;
        };
        if let Some(m) = meta_obj {
            // eKuiper metadata keys are case-insensitive (`messageid`).
            if let Some(value) = m.get(&key).or_else(|| {
                m.iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(&key))
                    .map(|(_, v)| v)
            }) {
                return value.clone();
            }
        }
        record.get(&key).cloned().unwrap_or(Value::Null)
    }

    fn resolve_event_time<R: RowLookup + ?Sized>(record: &R) -> Value {
        for key in ["timestamp", "event_time", "__timestamp__"] {
            if let Some(v) = record.get(key) {
                if !v.is_null() {
                    return v.clone();
                }
            }
        }
        serde_json::json!(chrono::Utc::now().timestamp_millis())
    }

    fn resolve_rule_id<R: RowLookup + ?Sized>(record: &R) -> Value {
        match record.get("__rule_id__") {
            Some(Value::String(s)) => Value::String(s.clone()),
            _ => Value::String(String::new()),
        }
    }

    fn resolve_rule_start<R: RowLookup + ?Sized>(record: &R) -> Value {
        match record.get("__rule_start__") {
            Some(Value::Number(n)) => Value::Number(n.clone()),
            _ => Value::from(chrono::Utc::now().timestamp_millis()),
        }
    }

    fn resolve_window_bound<R: RowLookup + ?Sized>(record: &R, keys: &[&str]) -> Value {
        keys.iter()
            .find_map(|k| record.get(k))
            .cloned()
            .unwrap_or(Value::Null)
    }

    /// Fall back to a user-registered UDF for names outside the built-in
    /// match list; `Value::Null` when nothing is registered under the name.
    fn call_global_udf(name: &str, args: &[Value]) -> Value {
        rekuiper_core::plugin::get_global_udf_registry()
            .call_udf(name, args)
            .unwrap_or(Value::Null)
    }

    // ---------- helpers for function args ----------

    /// Convert a JSON value to f64 if numeric or numeric-string.
    pub(crate) fn to_f64(v: &Value) -> Option<f64> {
        if let Some(f) = v.as_f64() {
            return Some(f);
        }
        if let Some(s) = v.as_str() {
            let t = s.trim();
            if let Ok(f) = t.parse::<f64>() {
                return Some(f);
            }
        }
        None
    }

    pub(crate) fn to_i64_arg(v: &Value) -> Option<i64> {
        if let Some(i) = v.as_i64() {
            return Some(i);
        }
        if let Some(u) = v.as_u64() {
            if u <= i64::MAX as u64 {
                return Some(u as i64);
            } else {
                return None;
            }
        }
        if let Some(f) = v.as_f64() {
            // Truncate toward zero like eKuiper cast
            if f.is_finite() {
                return Some(f.trunc() as i64);
            }
            return None;
        }
        if let Some(s) = v.as_str() {
            let t = s.trim();
            if let Ok(i) = t.parse::<i64>() {
                return Some(i);
            }
            if let Ok(f) = t.parse::<f64>() {
                if f.is_finite() {
                    return Some(f.trunc() as i64);
                }
            }
        }
        if let Some(b) = v.as_bool() {
            return Some(if b { 1 } else { 0 });
        }
        None
    }

    /// eKuiper `ToStringAlways`: nil -> "", otherwise minimal string form.
    pub(crate) fn to_string_always(v: &Value) -> String {
        match v {
            Value::Null => String::new(),
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Array(_) | Value::Object(_) => serde_json::to_string(v).unwrap_or_default(),
        }
    }

    /// Normalize a bracket bound to an integer offset. Numbers truncate
    /// toward zero; numeric strings parse; anything else is invalid.
    pub(crate) fn as_index_i64(v: &Value) -> Option<i64> {
        match v {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Some(i)
                } else if let Some(u) = n.as_u64() {
                    i64::try_from(u).ok()
                } else {
                    n.as_f64()
                        .filter(|f| f.is_finite())
                        .map(|f| f.trunc() as i64)
                }
            }
            Value::String(s) => s.trim().parse::<i64>().ok(),
            _ => None,
        }
    }

    /// Postfix index evaluation (0-based; negative counts back from the
    /// end; string keys look up object fields). Out-of-range and
    /// type mismatches yield Null.
    pub(crate) fn index_value(base: &Value, index: &Value) -> Value {
        match base {
            Value::Array(arr) => {
                let Some(i) = Self::as_index_i64(index) else {
                    return Value::Null;
                };
                let len = arr.len() as i64;
                let pos = if i >= 0 { i } else { len + i };
                if pos < 0 || pos >= len {
                    return Value::Null;
                }
                arr[pos as usize].clone()
            }
            Value::Object(map) => match index {
                Value::String(k) => map.get(k).cloned().unwrap_or(Value::Null),
                _ => Value::Null,
            },
            _ => Value::Null,
        }
    }

    /// Postfix slice evaluation: `base[lo:hi)` with end-exclusive `hi`,
    /// negatives from the end, omitted bounds meaning array start/end.
    /// Bounds clamp into range; an empty/inverted range yields `[]`.
    pub(crate) fn slice_value(base: &Value, lo: Option<&Value>, hi: Option<&Value>) -> Value {
        let Value::Array(arr) = base else {
            return Value::Null;
        };
        let len = arr.len() as i64;
        let norm = |v: Option<&Value>, default: i64| -> Option<i64> {
            match v {
                None => Some(default),
                Some(x) => {
                    let i = Self::as_index_i64(x)?;
                    Some(if i >= 0 { i } else { len + i })
                }
            }
        };
        let (Some(mut l), Some(mut h)) = (norm(lo, 0), norm(hi, len)) else {
            return Value::Null;
        };
        l = l.clamp(0, len);
        h = h.clamp(0, len);
        if l >= h {
            return Value::Array(Vec::new());
        }
        Value::Array(arr[l as usize..h as usize].to_vec())
    }
}
