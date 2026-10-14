//! Window trigger evaluation with eKuiper semantics.
//!
//! A window trigger turns the rows collected for one window into output rows:
//! `WHERE` filters the input rows, `GROUP BY` partitions them (one output row
//! per group, groups in first-seen order), `HAVING` filters groups, and
//! `ORDER BY` / `LIMIT` shape the result. A statement without aggregates or
//! `GROUP BY` projects every surviving row.
//!
//! [`IncrementalWindow`] computes the same result for the common IIoT shape
//! (group columns plus `count/sum/avg/min/max`) without retaining rows: memory
//! is O(groups) instead of O(rows in the window).

use super::{Evaluator, RuleState};
use crate::ast::{Expr, SelectStmt, SortOrder};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::fmt::Write as _;

static NULL: Value = Value::Null;

type Row = HashMap<String, Value>;

impl Evaluator {
    /// True when `expr` calls an aggregate function anywhere inside it.
    pub fn contains_aggregate(expr: &Expr) -> bool {
        match expr {
            Expr::Call { name, args } => {
                Self::is_aggregate_call(name) || args.iter().any(Self::contains_aggregate)
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::contains_aggregate(left) || Self::contains_aggregate(right)
            }
            Expr::UnaryOp { expr, .. } => Self::contains_aggregate(expr),
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::contains_aggregate(expr)
                    || Self::contains_aggregate(low)
                    || Self::contains_aggregate(high)
            }
            Expr::InList { expr, list, .. } => {
                Self::contains_aggregate(expr) || list.iter().any(Self::contains_aggregate)
            }
            Expr::IsNull { expr, .. } => Self::contains_aggregate(expr),
            Expr::FieldAccess { parent, .. } => Self::contains_aggregate(parent),
            Expr::Index { base, index } => {
                Self::contains_aggregate(base) || Self::contains_aggregate(index)
            }
            Expr::Slice { base, lo, hi } => {
                Self::contains_aggregate(base)
                    || lo.as_ref().is_some_and(|e| Self::contains_aggregate(e))
                    || hi.as_ref().is_some_and(|e| Self::contains_aggregate(e))
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                operand
                    .as_ref()
                    .is_some_and(|e| Self::contains_aggregate(e))
                    || when_clauses
                        .iter()
                        .any(|(w, t)| Self::contains_aggregate(w) || Self::contains_aggregate(t))
                    || else_clause
                        .as_ref()
                        .is_some_and(|e| Self::contains_aggregate(e))
            }
            Expr::Over {
                call,
                partition_by,
                when,
            } => {
                Self::contains_aggregate(call)
                    || partition_by
                        .as_ref()
                        .is_some_and(|e| Self::contains_aggregate(e))
                    || when.as_ref().is_some_and(|e| Self::contains_aggregate(e))
            }
            Expr::Wildcard
            | Expr::WildcardModified { .. }
            | Expr::Identifier(_)
            | Expr::Literal(_) => false,
        }
    }

    /// True when a window trigger yields grouped rows (aggregates or
    /// `GROUP BY`) rather than one projected row per input row.
    pub fn is_grouped_window(stmt: &SelectStmt) -> bool {
        !stmt.group_by.is_empty()
            || stmt.fields.iter().any(Self::contains_aggregate)
            || stmt.having.is_some()
    }

    /// Input-row `WHERE` test (rows pass when there is no `WHERE`).
    pub fn passes_where(stmt: &SelectStmt, row: &Row) -> bool {
        match &stmt.where_clause {
            Some(cond) => Self::eval_bool(cond, row),
            None => true,
        }
    }

    /// Evaluate one window trigger over its rows (applies `WHERE`).
    pub fn eval_window(stmt: &SelectStmt, rows: Vec<Row>) -> Vec<Row> {
        Self::eval_window_stateful(stmt, rows, None)
    }

    /// Stateful variant of [`Self::eval_window`] with comparison state context.
    pub fn eval_window_stateful(
        stmt: &SelectStmt,
        rows: Vec<Row>,
        state: Option<&RuleState>,
    ) -> Vec<Row> {
        let rows = if stmt.where_clause.is_some() {
            rows.into_iter()
                .filter(|r| Self::passes_where(stmt, r))
                .collect()
        } else {
            rows
        };
        Self::eval_window_filtered_stateful(stmt, rows, state)
    }

    /// [`Self::eval_window`] for rows that already passed `WHERE`.
    pub fn eval_window_filtered(stmt: &SelectStmt, rows: Vec<Row>) -> Vec<Row> {
        Self::eval_window_filtered_stateful(stmt, rows, None)
    }

    /// Stateful variant of [`Self::eval_window_filtered`] for rows that already passed `WHERE`.
    pub fn eval_window_filtered_stateful(
        stmt: &SelectStmt,
        rows: Vec<Row>,
        state: Option<&RuleState>,
    ) -> Vec<Row> {
        if rows.is_empty() {
            return Vec::new();
        }
        let mut out: Vec<Row> = if !Self::is_grouped_window(stmt) {
            let projection = SelectStmt {
                where_clause: None,
                ..stmt.clone()
            };
            if let Some(s) = state {
                rows.iter()
                    .filter_map(|r| Self::eval_select_stateful(&projection, r, s))
                    .collect()
            } else {
                rows.iter()
                    .filter_map(|r| Self::eval_select(&projection, r))
                    .collect()
            }
        } else if stmt.group_by.is_empty() {
            Self::eval_aggregate_stateful(stmt, &rows, state, "")
                .into_iter()
                .collect()
        } else {
            Self::partition_keyed_groups(&stmt.group_by, rows)
                .iter()
                .filter_map(|(pkey, group)| Self::eval_aggregate_stateful(stmt, group, state, pkey))
                .collect()
        };
        Self::apply_order_limit(stmt, &mut out);
        out
    }

    /// Split rows into `GROUP BY` partitions with their group keys.
    fn partition_keyed_groups(group_by: &[Expr], rows: Vec<Row>) -> Vec<(String, Vec<Row>)> {
        let mut index: HashMap<String, usize> = HashMap::new();
        let mut groups: Vec<(String, Vec<Row>)> = Vec::new();
        let mut key = String::new();
        for row in rows {
            key.clear();
            for expr in group_by {
                write_group_key(&mut key, &Self::group_value(expr, &row));
            }
            match index.get(key.as_str()) {
                Some(&i) => groups[i].1.push(row),
                None => {
                    index.insert(key.clone(), groups.len());
                    groups.push((key.clone(), vec![row]));
                }
            }
        }
        groups
    }

    /// Value of a grouping or accumulator expression, borrowing plain columns.
    fn group_value<'a>(expr: &Expr, row: &'a Row) -> Cow<'a, Value> {
        match expr {
            Expr::Identifier(name) => Cow::Borrowed(row.get(name).unwrap_or(&NULL)),
            other => Cow::Owned(Self::eval_val(other, row)),
        }
    }

    /// Stable `ORDER BY` over output rows, then `LIMIT`.
    pub(crate) fn apply_order_limit(stmt: &SelectStmt, rows: &mut Vec<Row>) {
        if !stmt.order_by.is_empty() && rows.len() > 1 {
            rows.sort_by(|a, b| {
                for (idx, item) in stmt.order_by.iter().enumerate() {
                    let va = Self::order_value(&item.expr, a, idx);
                    let vb = Self::order_value(&item.expr, b, idx);
                    let ord = Self::compare_values(&va, &vb).unwrap_or(std::cmp::Ordering::Equal);
                    let ord = match item.order {
                        SortOrder::Asc => ord,
                        SortOrder::Desc => ord.reverse(),
                    };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
        if let Some(limit) = stmt.limit {
            rows.truncate(limit);
        }
    }

    /// Sort key of an output row: the expression over the row, or for an
    /// aggregate call the projected column it produced.
    fn order_value(expr: &Expr, row: &Row, idx: usize) -> Value {
        let v = Self::eval_val(expr, row);
        if v.is_null() && matches!(expr, Expr::Call { .. }) {
            if let Some(found) = row.get(&Self::column_name(expr, idx)) {
                return found.clone();
            }
        }
        v
    }
}

/// Appends an unambiguous encoding of one grouping value to `key`.
fn write_group_key(key: &mut String, value: &Value) {
    match value {
        // Length-prefixed so strings never collide with other types or with
        // the next key component.
        Value::String(s) => {
            let _ = write!(key, "s{}:", s.len());
            key.push_str(s);
        }
        other => {
            let _ = write!(key, "v{}", other);
        }
    }
    key.push('\u{1f}');
}

/// Compact ~2 KB DDSketch for O(1) bounded-memory streaming percentiles and medians.
/// Uses logarithmic buckets with gamma = 1.02 (~1% relative accuracy across all scales).
#[derive(Debug, Clone)]
pub struct CompactDdSketch {
    counts: Vec<u64>,
    offset: i32,
    zero_count: u64,
    total_count: u64,
    min_val: f64,
    max_val: f64,
}

impl Default for CompactDdSketch {
    fn default() -> Self {
        Self::new()
    }
}

impl CompactDdSketch {
    const LN_GAMMA: f64 = 0.01980262729617973; // ln(1.02)
    const MIN_TRACKABLE: f64 = 1e-9;

    pub fn new() -> Self {
        Self {
            counts: vec![0; 512],
            offset: -128,
            zero_count: 0,
            total_count: 0,
            min_val: f64::INFINITY,
            max_val: f64::NEG_INFINITY,
        }
    }

    #[inline]
    pub fn insert(&mut self, val: f64) {
        if !val.is_finite() {
            return;
        }
        self.total_count += 1;
        if val < self.min_val {
            self.min_val = val;
        }
        if val > self.max_val {
            self.max_val = val;
        }
        if val <= Self::MIN_TRACKABLE {
            self.zero_count += 1;
            return;
        }
        let idx = (val.ln() / Self::LN_GAMMA).ceil() as i32;
        let slot = (idx - self.offset).clamp(0, (self.counts.len() as i32) - 1) as usize;
        self.counts[slot] += 1;
    }

    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.total_count == 0 {
            return None;
        }
        if self.total_count == 1 || (self.max_val - self.min_val).abs() <= f64::EPSILON {
            return Some(self.min_val);
        }
        let q = q.clamp(0.0, 1.0);
        if q == 0.0 {
            return Some(self.min_val);
        }
        if q == 1.0 {
            return Some(self.max_val);
        }
        let target_rank = (q * (self.total_count as f64)).ceil() as u64;
        if target_rank <= self.zero_count {
            return Some(self.min_val.max(0.0));
        }
        let mut cumulative = self.zero_count;
        for (i, &c) in self.counts.iter().enumerate() {
            cumulative += c;
            if cumulative >= target_rank && c > 0 {
                let bucket_idx = (i as i32) + self.offset;
                let approx = ((bucket_idx as f64 - 0.5) * Self::LN_GAMMA).exp();
                return Some(approx.clamp(self.min_val, self.max_val));
            }
        }
        Some(self.max_val)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggKind {
    CountAll,
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Stddev,
    Stddevs,
    Var,
    Vars,
    Median,
    Percentile(f64),
}

#[derive(Debug, Clone)]
enum Plan {
    /// Plain column: first value in the group (eKuiper non-aggregate field).
    First { name: String, expr: Expr },
    Agg {
        name: String,
        kind: AggKind,
        arg: Option<Expr>,
    },
}

#[derive(Debug, Clone)]
enum Acc {
    Count(i64),
    Sum {
        int: i64,
        float: f64,
        int_ok: bool,
        any: bool,
    },
    Avg {
        sum: f64,
        n: u64,
    },
    Extreme(Option<Value>),
    Welford {
        kind: AggKind,
        count: u64,
        mean: f64,
        m2: f64,
    },
    Sketch {
        q: f64,
        sketch: Box<CompactDdSketch>,
    },
}

impl Acc {
    fn new(kind: AggKind) -> Self {
        match kind {
            AggKind::CountAll | AggKind::Count => Acc::Count(0),
            AggKind::Sum => Acc::Sum {
                int: 0,
                float: 0.0,
                int_ok: true,
                any: false,
            },
            AggKind::Avg => Acc::Avg { sum: 0.0, n: 0 },
            AggKind::Min | AggKind::Max => Acc::Extreme(None),
            AggKind::Stddev | AggKind::Stddevs | AggKind::Var | AggKind::Vars => Acc::Welford {
                kind,
                count: 0,
                mean: 0.0,
                m2: 0.0,
            },
            AggKind::Median => Acc::Sketch {
                q: 0.5,
                sketch: Box::new(CompactDdSketch::new()),
            },
            AggKind::Percentile(p) => Acc::Sketch {
                q: p,
                sketch: Box::new(CompactDdSketch::new()),
            },
        }
    }

    /// Mirrors `agg_count/sum/avg/min/max/stddev/percentile` exactly, one row at a time.
    fn update(&mut self, kind: AggKind, value: Option<&Value>) {
        match (self, kind) {
            (Acc::Count(n), AggKind::CountAll) => *n += 1,
            (Acc::Count(n), _) => {
                if value.is_some_and(|v| !v.is_null()) {
                    *n += 1;
                }
            }
            (
                Acc::Sum {
                    int,
                    float,
                    int_ok,
                    any,
                },
                _,
            ) => {
                let Some(v) = value.filter(|v| v.is_number()) else {
                    return;
                };
                *any = true;
                *float += v.as_f64().unwrap_or(0.0);
                if *int_ok {
                    match v.as_i64().and_then(|i| int.checked_add(i)) {
                        Some(next) => *int = next,
                        None => *int_ok = false,
                    }
                }
            }
            (Acc::Avg { sum, n }, _) => {
                if let Some(v) = value.filter(|v| v.is_number()) {
                    *sum += v.as_f64().unwrap_or(0.0);
                    *n += 1;
                }
            }
            (Acc::Extreme(best), kind) => {
                let Some(v) = value.filter(|v| v.is_number()) else {
                    return;
                };
                let wanted = match kind {
                    AggKind::Min => std::cmp::Ordering::Less,
                    _ => std::cmp::Ordering::Greater,
                };
                match best {
                    None => *best = Some(v.clone()),
                    Some(current) => {
                        if Evaluator::compare_values(v, current) == Some(wanted) {
                            *best = Some(v.clone());
                        }
                    }
                }
            }
            (
                Acc::Welford {
                    count, mean, m2, ..
                },
                _,
            ) => {
                if let Some(x) = value.and_then(|v| v.as_f64()) {
                    *count += 1;
                    let delta = x - *mean;
                    *mean += delta / (*count as f64);
                    let delta2 = x - *mean;
                    *m2 += delta * delta2;
                }
            }
            (Acc::Sketch { sketch, .. }, _) => {
                if let Some(x) = value.and_then(|v| v.as_f64()) {
                    sketch.insert(x);
                }
            }
        }
    }

    fn finish(self) -> Value {
        match self {
            Acc::Count(n) => Value::from(n),
            Acc::Sum {
                int,
                float,
                int_ok,
                any,
            } => {
                if !any {
                    Value::Null
                } else if int_ok {
                    Value::from(int)
                } else {
                    serde_json::json!(float)
                }
            }
            Acc::Avg { sum, n } => {
                if n == 0 {
                    Value::Null
                } else {
                    serde_json::json!(sum / (n as f64))
                }
            }
            Acc::Extreme(best) => best.unwrap_or(Value::Null),
            Acc::Welford {
                kind,
                count,
                mean: _,
                m2,
            } => match kind {
                AggKind::Stddev => {
                    if count == 0 {
                        Value::Null
                    } else {
                        serde_json::json!((m2 / count as f64).sqrt())
                    }
                }
                AggKind::Stddevs => {
                    if count < 2 {
                        Value::Null
                    } else {
                        serde_json::json!((m2 / (count - 1) as f64).sqrt())
                    }
                }
                AggKind::Var => {
                    if count == 0 {
                        Value::Null
                    } else {
                        serde_json::json!(m2 / count as f64)
                    }
                }
                AggKind::Vars => {
                    if count < 2 {
                        Value::Null
                    } else {
                        serde_json::json!(m2 / (count - 1) as f64)
                    }
                }
                _ => Value::Null,
            },
            Acc::Sketch { q, sketch } => match sketch.quantile(q) {
                Some(val) => serde_json::json!(val),
                None => Value::Null,
            },
        }
    }
}

struct Group {
    group_values: Vec<Value>,
    firsts: Vec<Value>,
    accs: Vec<Acc>,
}

/// Row-free window state for statements whose projection is only group
/// columns, plain columns and `count/sum/avg/min/max` of simple arguments.
/// Produces exactly what [`Evaluator::eval_window`] produces for the same rows.
pub struct IncrementalWindow {
    where_clause: Option<Expr>,
    group_by: Vec<Expr>,
    group_names: Vec<String>,
    plans: Vec<Plan>,
    order_stmt: SelectStmt,
    index: HashMap<String, usize>,
    groups: Vec<Group>,
    key: String,
}

impl IncrementalWindow {
    /// Builds the incremental plan, or `None` when the statement needs rows
    /// (joins, `HAVING`, `UNION`, other aggregates or expressions).
    pub fn try_new(stmt: &SelectStmt) -> Option<Self> {
        if stmt.set_op.is_some()
            || !stmt.joins.is_empty()
            || stmt.having.is_some()
            || !Evaluator::is_grouped_window(stmt)
            || stmt.group_by.iter().any(Evaluator::contains_aggregate)
        {
            return None;
        }
        let mut plans = Vec::with_capacity(stmt.fields.len());
        let field_names = Evaluator::select_field_names(stmt);
        for (idx, field) in stmt.fields.iter().enumerate() {
            let plan = match field {
                Expr::Identifier(_) => Plan::First {
                    name: field_names[idx].clone(),
                    expr: field.clone(),
                },
                Expr::FieldAccess { parent, field: _ } => {
                    if Evaluator::contains_aggregate(parent) {
                        return None;
                    }
                    Plan::First {
                        name: field_names[idx].clone(),
                        expr: field.clone(),
                    }
                }
                Expr::Call { name, args } => {
                    let lower = name.to_ascii_lowercase();
                    if args.is_empty() || Evaluator::contains_aggregate(&args[0]) {
                        return None;
                    }
                    let wildcard = matches!(args[0], Expr::Wildcard);
                    let kind = match (lower.as_str(), wildcard, args.len()) {
                        ("count", true, 1) => AggKind::CountAll,
                        ("count", false, 1) => AggKind::Count,
                        ("sum", false, 1) => AggKind::Sum,
                        ("avg", false, 1) => AggKind::Avg,
                        ("min", false, 1) => AggKind::Min,
                        ("max", false, 1) => AggKind::Max,
                        ("stddev", false, 1) => AggKind::Stddev,
                        ("stddevs", false, 1) => AggKind::Stddevs,
                        ("var", false, 1) => AggKind::Var,
                        ("vars", false, 1) => AggKind::Vars,
                        ("median", false, 1) => AggKind::Median,
                        ("percentile" | "percentile_cont" | "percentile_disc", false, 2) => {
                            let p = match &args[1] {
                                Expr::Literal(Value::Number(num)) => num.as_f64()?,
                                _ => return None,
                            };
                            if !(0.0..=1.0).contains(&p) {
                                return None;
                            }
                            AggKind::Percentile(p)
                        }
                        _ => return None,
                    };
                    Plan::Agg {
                        name: field_names[idx].clone(),
                        kind,
                        arg: (!wildcard).then(|| args[0].clone()),
                    }
                }
                _ => return None,
            };
            plans.push(plan);
        }
        let group_names = stmt
            .group_by
            .iter()
            .enumerate()
            .map(|(idx, g)| match g {
                Expr::Identifier(name) => name.clone(),
                Expr::FieldAccess { field: leaf, .. } => leaf.clone(),
                _ => Evaluator::column_name(g, idx),
            })
            .collect();
        Some(Self {
            where_clause: stmt.where_clause.clone(),
            group_by: stmt.group_by.clone(),
            group_names,
            plans,
            order_stmt: stmt.clone(),
            index: HashMap::new(),
            groups: Vec::new(),
            key: String::new(),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    /// Number of open groups.
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Accumulates one input row. Returns `false` when `WHERE` rejected it.
    pub fn push(&mut self, row: &Row) -> bool {
        if let Some(cond) = &self.where_clause {
            if !Evaluator::eval_bool(cond, row) {
                return false;
            }
        }
        self.key.clear();
        for expr in &self.group_by {
            write_group_key(&mut self.key, &Evaluator::group_value(expr, row));
        }
        let gi = match self.index.get(self.key.as_str()) {
            Some(&i) => i,
            None => {
                let group = Group {
                    group_values: self
                        .group_by
                        .iter()
                        .map(|g| Evaluator::group_value(g, row).into_owned())
                        .collect(),
                    firsts: self
                        .plans
                        .iter()
                        .map(|p| match p {
                            Plan::First { expr, .. } => {
                                Evaluator::group_value(expr, row).into_owned()
                            }
                            Plan::Agg { .. } => Value::Null,
                        })
                        .collect(),
                    accs: self
                        .plans
                        .iter()
                        .map(|p| match p {
                            Plan::Agg { kind, .. } => Acc::new(*kind),
                            Plan::First { .. } => Acc::Count(0),
                        })
                        .collect(),
                };
                self.index.insert(self.key.clone(), self.groups.len());
                self.groups.push(group);
                self.groups.len() - 1
            }
        };
        let group = &mut self.groups[gi];
        for (plan, acc) in self.plans.iter().zip(group.accs.iter_mut()) {
            if let Plan::Agg { kind, arg, .. } = plan {
                match arg {
                    None => acc.update(*kind, None),
                    Some(expr) => {
                        let v = Evaluator::group_value(expr, row);
                        acc.update(*kind, Some(v.as_ref()));
                    }
                }
            }
        }
        true
    }

    /// Closes the window: output rows for every group, state reset.
    pub fn take(&mut self) -> Vec<Row> {
        self.index.clear();
        let groups = std::mem::take(&mut self.groups);
        let mut out = Vec::with_capacity(groups.len());
        for group in groups {
            let mut row: Row = HashMap::with_capacity(self.group_names.len() + self.plans.len());
            for (name, value) in self.group_names.iter().zip(group.group_values) {
                row.insert(name.clone(), value);
            }
            for ((plan, first), acc) in self.plans.iter().zip(group.firsts).zip(group.accs) {
                match plan {
                    Plan::First { name, .. } => {
                        row.entry(name.clone()).or_insert(first);
                    }
                    Plan::Agg { name, .. } => {
                        row.insert(name.clone(), acc.finish());
                    }
                }
            }
            out.push(row);
        }
        Evaluator::apply_order_limit(&self.order_stmt, &mut out);
        out
    }
}

/// Invertible O(1) accumulator for sliding and hopping time windows.
/// - `COUNT`, `SUM`, `AVG` update in O(1) on both insertion and expiration.
/// - `MIN` and `MAX` use a monotonic `VecDeque<(i64, f64, Value)>` for O(1) amortized
///   sliding extrema without scanning the window buffer.
#[derive(Debug, Clone)]
pub enum SlidingAcc {
    Count {
        n: i64,
        entries: VecDeque<(i64, bool)>,
    },
    Sum {
        int_sum: i64,
        float_sum: f64,
        float_count: usize,
        valid_count: usize,
        entries: VecDeque<(i64, Option<i64>, Option<f64>)>,
    },
    Avg {
        sum: f64,
        n: u64,
        entries: VecDeque<(i64, Option<f64>)>,
    },
    Extreme {
        is_max: bool,
        /// Monotonic queue of `(timestamp_ms, numeric_value, original_json_value)`
        mono_deque: VecDeque<(i64, f64, Value)>,
    },
}

impl SlidingAcc {
    pub fn new(kind: AggKind) -> Self {
        match kind {
            AggKind::CountAll | AggKind::Count => SlidingAcc::Count {
                n: 0,
                entries: VecDeque::new(),
            },
            AggKind::Sum => SlidingAcc::Sum {
                int_sum: 0,
                float_sum: 0.0,
                float_count: 0,
                valid_count: 0,
                entries: VecDeque::new(),
            },
            AggKind::Avg => SlidingAcc::Avg {
                sum: 0.0,
                n: 0,
                entries: VecDeque::new(),
            },
            AggKind::Min => SlidingAcc::Extreme {
                is_max: false,
                mono_deque: VecDeque::new(),
            },
            AggKind::Max => SlidingAcc::Extreme {
                is_max: true,
                mono_deque: VecDeque::new(),
            },
            _ => SlidingAcc::Count {
                n: 0,
                entries: VecDeque::new(),
            },
        }
    }

    pub fn push(&mut self, kind: AggKind, ts: i64, value: Option<&Value>) {
        match (self, kind) {
            (SlidingAcc::Count { n, entries }, AggKind::CountAll) => {
                *n += 1;
                entries.push_back((ts, true));
            }
            (SlidingAcc::Count { n, entries }, _) => {
                let valid = value.is_some_and(|v| !v.is_null());
                if valid {
                    *n += 1;
                }
                entries.push_back((ts, valid));
            }
            (
                SlidingAcc::Sum {
                    int_sum,
                    float_sum,
                    float_count,
                    valid_count,
                    entries,
                },
                _,
            ) => {
                if let Some(v) = value.filter(|v| v.is_number()) {
                    let f = v.as_f64().unwrap_or(0.0);
                    *float_sum += f;
                    *valid_count += 1;
                    if let Some(i) = v.as_i64() {
                        *int_sum = int_sum.saturating_add(i);
                        entries.push_back((ts, Some(i), Some(f)));
                    } else {
                        *float_count += 1;
                        entries.push_back((ts, None, Some(f)));
                    }
                } else {
                    entries.push_back((ts, None, None));
                }
            }
            (SlidingAcc::Avg { sum, n, entries }, _) => {
                if let Some(v) = value.filter(|v| v.is_number()) {
                    let f = v.as_f64().unwrap_or(0.0);
                    *sum += f;
                    *n += 1;
                    entries.push_back((ts, Some(f)));
                } else {
                    entries.push_back((ts, None));
                }
            }
            (SlidingAcc::Extreme { is_max, mono_deque }, _) => {
                if let Some(v) = value.filter(|v| v.is_number()) {
                    let Some(f) = v.as_f64() else {
                        return;
                    };
                    // Maintain monotonic order: pop back elements dominated by `f`
                    while let Some(&(_, back_f, _)) = mono_deque.back() {
                        let dominated = if *is_max { f >= back_f } else { f <= back_f };
                        if dominated {
                            mono_deque.pop_back();
                        } else {
                            break;
                        }
                    }
                    mono_deque.push_back((ts, f, v.clone()));
                }
            }
        }
    }

    /// Evict all entries strictly older than `cutoff_ts` in O(1) amortized time.
    pub fn evict_before(&mut self, cutoff_ts: i64) {
        match self {
            SlidingAcc::Count { n, entries } => {
                while let Some(&(ts, valid)) = entries.front() {
                    if ts < cutoff_ts {
                        if valid {
                            *n -= 1;
                        }
                        entries.pop_front();
                    } else {
                        break;
                    }
                }
            }
            SlidingAcc::Sum {
                int_sum,
                float_sum,
                float_count,
                valid_count,
                entries,
            } => {
                while let Some(&(ts, opt_i, opt_f)) = entries.front() {
                    if ts < cutoff_ts {
                        if let Some(f) = opt_f {
                            *float_sum -= f;
                            *valid_count = valid_count.saturating_sub(1);
                            if let Some(i) = opt_i {
                                *int_sum = int_sum.saturating_sub(i);
                            } else {
                                *float_count = float_count.saturating_sub(1);
                            }
                        }
                        entries.pop_front();
                    } else {
                        break;
                    }
                }
                if *valid_count == 0 {
                    *int_sum = 0;
                    *float_sum = 0.0;
                    *float_count = 0;
                }
            }
            SlidingAcc::Avg { sum, n, entries } => {
                while let Some(&(ts, opt_f)) = entries.front() {
                    if ts < cutoff_ts {
                        if let Some(f) = opt_f {
                            *sum -= f;
                            *n = n.saturating_sub(1);
                        }
                        entries.pop_front();
                    } else {
                        break;
                    }
                }
                if *n == 0 {
                    *sum = 0.0;
                }
            }
            SlidingAcc::Extreme { mono_deque, .. } => {
                while let Some(&(ts, _, _)) = mono_deque.front() {
                    if ts < cutoff_ts {
                        mono_deque.pop_front();
                    } else {
                        break;
                    }
                }
            }
        }
    }

    pub fn current_value(&self) -> Value {
        match self {
            SlidingAcc::Count { n, .. } => Value::from(*n),
            SlidingAcc::Sum {
                int_sum,
                float_sum,
                float_count,
                valid_count,
                ..
            } => {
                if *valid_count == 0 {
                    Value::Null
                } else if *float_count == 0 {
                    Value::from(*int_sum)
                } else {
                    serde_json::json!(*float_sum)
                }
            }
            SlidingAcc::Avg { sum, n, .. } => {
                if *n == 0 {
                    Value::Null
                } else {
                    serde_json::json!(*sum / (*n as f64))
                }
            }
            SlidingAcc::Extreme { mono_deque, .. } => mono_deque
                .front()
                .map(|(_, _, val)| val.clone())
                .unwrap_or(Value::Null),
        }
    }
}

#[derive(Debug, Clone)]
struct SlidingGroup {
    group_values: Vec<Value>,
    firsts: Vec<Value>,
    accs: Vec<SlidingAcc>,
}

/// Row-free O(groups) incremental accumulator for sliding and hopping time windows.
#[derive(Debug, Clone)]
pub struct SlidingIncrementalWindow {
    where_clause: Option<Expr>,
    group_by: Vec<Expr>,
    group_names: Vec<String>,
    plans: Vec<Plan>,
    order_stmt: SelectStmt,
    index: HashMap<String, usize>,
    groups: Vec<SlidingGroup>,
    key: String,
}

impl SlidingIncrementalWindow {
    pub fn try_new(stmt: &SelectStmt) -> Option<Self> {
        let base = IncrementalWindow::try_new(stmt)?;
        if base.plans.iter().any(|p| {
            matches!(
                p,
                Plan::Agg {
                    kind: AggKind::Stddev
                        | AggKind::Stddevs
                        | AggKind::Var
                        | AggKind::Vars
                        | AggKind::Median
                        | AggKind::Percentile(_),
                    ..
                }
            )
        }) {
            return None;
        }
        Some(Self {
            where_clause: base.where_clause,
            group_by: base.group_by,
            group_names: base.group_names,
            plans: base.plans,
            order_stmt: base.order_stmt,
            index: HashMap::new(),
            groups: Vec::new(),
            key: String::new(),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
            || self.groups.iter().all(|g| {
                g.accs.iter().all(|a| match a {
                    SlidingAcc::Count { n, .. } => *n == 0,
                    SlidingAcc::Sum { valid_count, .. } => *valid_count == 0,
                    SlidingAcc::Avg { n, .. } => *n == 0,
                    SlidingAcc::Extreme { mono_deque, .. } => mono_deque.is_empty(),
                })
            })
    }

    pub fn push(&mut self, ts: i64, row: &Row) -> bool {
        if let Some(cond) = &self.where_clause {
            if !Evaluator::eval_bool(cond, row) {
                return false;
            }
        }
        self.key.clear();
        for expr in &self.group_by {
            write_group_key(&mut self.key, &Evaluator::group_value(expr, row));
        }
        let gi = match self.index.get(self.key.as_str()) {
            Some(&i) => i,
            None => {
                let group = SlidingGroup {
                    group_values: self
                        .group_by
                        .iter()
                        .map(|g| Evaluator::group_value(g, row).into_owned())
                        .collect(),
                    firsts: self
                        .plans
                        .iter()
                        .map(|p| match p {
                            Plan::First { expr, .. } => {
                                Evaluator::group_value(expr, row).into_owned()
                            }
                            Plan::Agg { .. } => Value::Null,
                        })
                        .collect(),
                    accs: self
                        .plans
                        .iter()
                        .map(|p| match p {
                            Plan::Agg { kind, .. } => SlidingAcc::new(*kind),
                            Plan::First { .. } => SlidingAcc::new(AggKind::Count),
                        })
                        .collect(),
                };
                self.index.insert(self.key.clone(), self.groups.len());
                self.groups.push(group);
                self.groups.len() - 1
            }
        };
        let group = &mut self.groups[gi];
        for (plan, acc) in self.plans.iter().zip(group.accs.iter_mut()) {
            if let Plan::Agg { kind, arg, .. } = plan {
                match arg {
                    None => acc.push(*kind, ts, None),
                    Some(expr) => {
                        let v = Evaluator::group_value(expr, row);
                        acc.push(*kind, ts, Some(v.as_ref()));
                    }
                }
            }
        }
        true
    }

    pub fn evict_before(&mut self, cutoff_ts: i64) {
        for group in &mut self.groups {
            for acc in &mut group.accs {
                acc.evict_before(cutoff_ts);
            }
        }
    }

    pub fn snapshot(&self) -> Vec<Row> {
        let mut out = Vec::with_capacity(self.groups.len());
        for group in &self.groups {
            let has_active = group.accs.iter().any(|a| match a {
                SlidingAcc::Count { n, .. } => *n > 0,
                SlidingAcc::Sum { valid_count, .. } => *valid_count > 0,
                SlidingAcc::Avg { n, .. } => *n > 0,
                SlidingAcc::Extreme { mono_deque, .. } => !mono_deque.is_empty(),
            });
            if !has_active && !group.accs.is_empty() {
                continue;
            }
            let mut row: Row = HashMap::with_capacity(self.group_names.len() + self.plans.len());
            for (name, value) in self.group_names.iter().zip(&group.group_values) {
                row.insert(name.clone(), value.clone());
            }
            for ((plan, first), acc) in self.plans.iter().zip(&group.firsts).zip(&group.accs) {
                match plan {
                    Plan::First { name, .. } => {
                        row.entry(name.clone()).or_insert_with(|| first.clone());
                    }
                    Plan::Agg { name, .. } => {
                        row.insert(name.clone(), acc.current_value());
                    }
                }
            }
            out.push(row);
        }
        Evaluator::apply_order_limit(&self.order_stmt, &mut out);
        out
    }
}
