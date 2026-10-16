use super::{ArithOp, Evaluator, RuleState, META_KEY};
use crate::ast::Expr;
use serde_json::Value;
use std::collections::HashMap;

const MAX_ACC_ITEMS: usize = 10_000;

impl Evaluator {
    pub(crate) fn eval_aggregate_call(
        name: &str,
        args: &[Expr],
        records: &[HashMap<String, Value>],
    ) -> Value {
        Self::eval_aggregate_call_stateful(name, args, records, None, "")
    }

    pub(crate) fn eval_aggregate_call_stateful(
        name: &str,
        args: &[Expr],
        records: &[HashMap<String, Value>],
        state: Option<&RuleState>,
        _partition_key: &str,
    ) -> Value {
        match name.to_ascii_lowercase().as_str() {
            "count" => Self::agg_count(args, records),
            "sum" => Self::agg_sum(args, records),
            "avg" => Self::agg_avg(args, records),
            "min" => Self::agg_min(args, records),
            "max" => Self::agg_max(args, records),
            "collect" => Self::agg_collect(args, records),
            "lead" => Self::agg_lead(args, records),
            "latest" => Self::agg_latest(args, records),
            "median" => Self::agg_median(args, records),
            "stddev" => Self::agg_stddev(args, records),
            "stddevs" => Self::agg_stddevs(args, records),
            "var" => Self::agg_var(args, records),
            "vars" => Self::agg_vars(args, records),
            "percentile" | "percentile_cont" => Self::agg_percentile(args, records),
            "percentile_disc" => Self::agg_percentile_disc(args, records),
            "last_value" => Self::agg_last_value(args, records),
            "merge_agg" => Self::agg_merge_agg(args, records),
            "row_number" => Self::agg_row_number(args, records),
            "last_agg_hit_count" => Self::agg_last_agg_hit_count_stateful(records, state),
            "last_agg_hit_time" => Self::agg_last_agg_hit_time_stateful(records, state),
            _ => Value::Null,
        }
    }

    /// `acc_map_agg(key, val)`: maintains an array of `{key, value}` objects
    /// in state, updating the value in place when the key already exists.
    pub(crate) fn acc_map_agg(state: &RuleState, state_key: &str, args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let key_str = Self::to_string_always(&args[0]);
        let mut entries: Vec<(String, Value)> = Vec::new();
        {
            let guard = state.state.read();
            if let Some(Value::Array(arr)) = guard.get(state_key) {
                for item in arr {
                    if let Value::Object(map) = item {
                        if let (Some(Value::String(k)), Some(v)) =
                            (map.get("key"), map.get("value"))
                        {
                            entries.push((k.clone(), v.clone()));
                        }
                    }
                }
            }
        }
        match entries.iter_mut().find(|(k, _)| *k == key_str) {
            Some((_, v)) => *v = args[1].clone(),
            None => {
                if entries.len() >= MAX_ACC_ITEMS {
                    entries.remove(0);
                }
                entries.push((key_str, args[1].clone()));
            }
        }
        let out = Value::Array(
            entries
                .iter()
                .map(|(k, v)| {
                    let mut map = serde_json::Map::with_capacity(2);
                    map.insert("key".to_string(), Value::String(k.clone()));
                    map.insert("value".to_string(), v.clone());
                    Value::Object(map)
                })
                .collect(),
        );
        let mut guard = state.state.write();
        RuleState::insert_bounded(&mut guard, state_key.to_string(), out.clone());
        out
    }

    /// Shared running-extremum logic for `acc_max`/`acc_min` (single value)
    /// using [`Self::compare_values`] so numbers and strings both work.
    /// Null or incomparable inputs leave the state untouched.
    pub(crate) fn acc_extreme(
        state: &RuleState,
        state_key: &str,
        args: &[Value],
        take_greater: bool,
    ) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let val = &args[0];
        if val.is_null() {
            return state
                .state
                .read()
                .get(state_key)
                .cloned()
                .unwrap_or(Value::Null);
        }
        let mut guard = state.state.write();
        match guard.get(state_key).cloned() {
            Some(current) => match Self::compare_values(val, &current) {
                Some(ord)
                    if (take_greater && ord == std::cmp::Ordering::Greater)
                        || (!take_greater && ord == std::cmp::Ordering::Less) =>
                {
                    RuleState::insert_bounded(&mut guard, state_key.to_string(), val.clone());
                    val.clone()
                }
                _ => current,
            },
            None => {
                RuleState::insert_bounded(&mut guard, state_key.to_string(), val.clone());
                val.clone()
            }
        }
    }

    /// Shared logic for `acc_max_by(val, compare)` / `acc_min_by`: the stored
    /// `(compare, value)` pair is replaced when the new `compare` wins
    /// (greater-or-equal for max, less-or-equal for min); the running `value`
    /// is returned.
    pub(crate) fn acc_extreme_by(
        state: &RuleState,
        state_key: &str,
        args: &[Value],
        take_greater: bool,
    ) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (val, compare) = (&args[0], &args[1]);
        let stored: Option<(Value, Value)> = state.state.read().get(state_key).and_then(|v| {
            if let Value::Object(map) = v {
                match (map.get("compare"), map.get("value")) {
                    (Some(c), Some(val)) => Some((c.clone(), val.clone())),
                    _ => None,
                }
            } else {
                None
            }
        });
        if val.is_null() || compare.is_null() {
            return stored.map(|(_, v)| v).unwrap_or(Value::Null);
        }
        let should_store = match &stored {
            None => true,
            Some((stored_compare, _)) => match Self::compare_values(compare, stored_compare) {
                Some(ord) => {
                    ord == std::cmp::Ordering::Equal
                        || (take_greater && ord == std::cmp::Ordering::Greater)
                        || (!take_greater && ord == std::cmp::Ordering::Less)
                }
                None => false,
            },
        };
        if should_store {
            let mut map = serde_json::Map::with_capacity(2);
            map.insert("compare".to_string(), compare.clone());
            map.insert("value".to_string(), val.clone());
            let mut guard = state.state.write();
            RuleState::insert_bounded(&mut guard, state_key.to_string(), Value::Object(map));
            val.clone()
        } else {
            stored.map(|(_, v)| v).unwrap_or(Value::Null)
        }
    }

    /// `acc_count(val)`: running counter, skipping Null inputs.
    pub(crate) fn acc_count(state: &RuleState, state_key: &str, args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let mut guard = state.state.write();
        if args[0].is_null() {
            return guard.get(state_key).cloned().unwrap_or(Value::from(0));
        }
        let next = guard
            .get(state_key)
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
            .saturating_add(1);
        RuleState::insert_bounded(&mut guard, state_key.to_string(), Value::from(next));
        Value::from(next)
    }

    /// `acc_sum(val)`: running sum over numeric inputs (int-preserving via
    /// [`Self::eval_arith`]); non-numeric inputs leave state untouched.
    pub(crate) fn acc_sum(state: &RuleState, state_key: &str, args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let val = &args[0];
        if val.is_null() || !val.is_number() {
            return state
                .state
                .read()
                .get(state_key)
                .cloned()
                .unwrap_or(Value::Null);
        }
        let mut guard = state.state.write();
        let next = match guard.get(state_key).cloned() {
            Some(cur) => Self::eval_arith(&cur, val, ArithOp::Add),
            None => val.clone(),
        };
        RuleState::insert_bounded(&mut guard, state_key.to_string(), next.clone());
        next
    }

    /// `acc_avg(val)`: maintains `(sum, count)` in state, returns `sum/count`
    /// as a float; non-numeric inputs leave state untouched.
    pub(crate) fn acc_avg(state: &RuleState, state_key: &str, args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let val = &args[0];
        if val.is_null() || !val.is_number() {
            return Self::acc_avg_current(state, state_key);
        }
        let mut guard = state.state.write();
        let (sum, count) = match guard.get(state_key) {
            Some(Value::Object(map)) => {
                let sum = map.get("sum").cloned().unwrap_or(serde_json::json!(0));
                let count = map.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
                (sum, count)
            }
            _ => (serde_json::json!(0), 0),
        };
        let sum = Self::eval_arith(&sum, val, ArithOp::Add);
        let count = count.saturating_add(1);
        let avg = match (sum.as_f64(), count) {
            (Some(s), c) if c > 0 => serde_json::json!(s / (c as f64)),
            _ => Value::Null,
        };
        let mut map = serde_json::Map::with_capacity(2);
        map.insert("sum".to_string(), sum);
        map.insert("count".to_string(), Value::from(count));
        RuleState::insert_bounded(&mut guard, state_key.to_string(), Value::Object(map));
        avg
    }

    /// Current average without updating state.
    pub(crate) fn acc_avg_current(state: &RuleState, state_key: &str) -> Value {
        let guard = state.state.read();
        match guard.get(state_key) {
            Some(Value::Object(map)) => {
                let sum = map.get("sum").and_then(|v| v.as_f64());
                let count = map.get("count").and_then(|v| v.as_i64());
                match (sum, count) {
                    (Some(s), Some(c)) if c > 0 => serde_json::json!(s / (c as f64)),
                    _ => Value::Null,
                }
            }
            _ => Value::Null,
        }
    }

    /// `acc_collect(val)`: accumulates non-null expression values into a running array.
    pub(crate) fn acc_collect(state: &RuleState, state_key: &str, args: &[Value]) -> Value {
        if args.is_empty() {
            return Value::Null;
        }
        let mut list: Vec<Value> = {
            let guard = state.state.read();
            if let Some(Value::Array(arr)) = guard.get(state_key) {
                arr.clone()
            } else {
                Vec::new()
            }
        };
        if !args[0].is_null() {
            if list.len() >= MAX_ACC_ITEMS {
                list.remove(0);
            }
            list.push(args[0].clone());
            let mut guard = state.state.write();
            RuleState::insert_bounded(
                &mut guard,
                state_key.to_string(),
                Value::Array(list.clone()),
            );
        }
        Value::Array(list)
    }

    /// `acc_distinct_collect(val)` / `distinct_acc(val)`: accumulates unique non-null
    /// expression values into a running array, preserving insertion order.
    pub(crate) fn acc_distinct_collect(
        state: &RuleState,
        state_key: &str,
        args: &[Value],
    ) -> Value {
        if args.is_empty() {
            return Value::Null;
        }
        let mut list: Vec<Value> = {
            let guard = state.state.read();
            if let Some(Value::Array(arr)) = guard.get(state_key) {
                arr.clone()
            } else {
                Vec::new()
            }
        };
        if !args[0].is_null()
            && !list
                .iter()
                .any(|existing| Self::values_equal(existing, &args[0]))
        {
            if list.len() >= MAX_ACC_ITEMS {
                list.remove(0);
            }
            list.push(args[0].clone());
            let mut guard = state.state.write();
            RuleState::insert_bounded(
                &mut guard,
                state_key.to_string(),
                Value::Array(list.clone()),
            );
        }
        Value::Array(list)
    }

    pub(crate) fn agg_last_agg_hit_count_stateful(
        records: &[HashMap<String, Value>],
        state: Option<&RuleState>,
    ) -> Value {
        if let Some(s) = state {
            let mut guard = s.state.write();
            let key = "$$last_agg_hit_count";
            let cur = guard.get(key).and_then(|v| v.as_i64()).unwrap_or(0);
            RuleState::insert_bounded(&mut guard, key.to_string(), Value::from(cur + 1));
            Value::from(cur)
        } else {
            Self::agg_last_agg_hit_count(records)
        }
    }

    pub(crate) fn agg_last_agg_hit_time_stateful(
        records: &[HashMap<String, Value>],
        state: Option<&RuleState>,
    ) -> Value {
        if let Some(s) = state {
            let mut guard = s.state.write();
            let key = "$$last_agg_hit_time";
            let prev = guard.get(key).and_then(|v| v.as_i64()).unwrap_or(0);
            let window_end = records
                .first()
                .and_then(|r| r.get("__window_end__").or_else(|| r.get("window_end")))
                .and_then(|v| v.as_i64())
                .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
            RuleState::insert_bounded(&mut guard, key.to_string(), Value::from(window_end));
            Value::from(prev)
        } else {
            Self::agg_last_agg_hit_time(records)
        }
    }

    pub(crate) fn agg_last_agg_hit_count(_records: &[HashMap<String, Value>]) -> Value {
        static COUNT: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
        let cur = COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Value::from(cur)
    }

    pub(crate) fn agg_last_agg_hit_time(records: &[HashMap<String, Value>]) -> Value {
        static LAST_TIME: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
        let prev = LAST_TIME.load(std::sync::atomic::Ordering::Relaxed);
        let window_end = records
            .first()
            .and_then(|r| r.get("__window_end__").or_else(|| r.get("window_end")))
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
        LAST_TIME.store(window_end, std::sync::atomic::Ordering::Relaxed);
        Value::from(prev)
    }

    pub(crate) fn agg_count(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        if args.is_empty() {
            return Value::Null;
        }
        // count(*) counts all rows.
        if args.len() == 1 && matches!(args[0], Expr::Wildcard) {
            return Value::from(records.len() as i64);
        }
        let mut n: i64 = 0;
        for rec in records {
            let v = Self::eval_val(&args[0], rec);
            if !v.is_null() {
                n += 1;
            }
        }
        Value::from(n)
    }

    /// Collect numeric arg values across the batch, skipping nulls/non-numerics.
    pub(crate) fn agg_numeric_values(arg: &Expr, records: &[HashMap<String, Value>]) -> Vec<Value> {
        let mut out = Vec::new();
        for rec in records {
            let v = Self::eval_val(arg, rec);
            if v.is_null() {
                continue;
            }
            if v.is_number() {
                out.push(v);
            }
        }
        out
    }

    pub(crate) fn agg_sum(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if matches!(args[0], Expr::Wildcard) {
            return Value::Null;
        }
        let vals = Self::agg_numeric_values(&args[0], records);
        if vals.is_empty() {
            return Value::Null;
        }
        if vals.iter().all(|v| v.as_i64().is_some()) {
            let mut acc: i64 = 0;
            for v in &vals {
                let i = v.as_i64().unwrap();
                match acc.checked_add(i) {
                    Some(n) => acc = n,
                    None => {
                        // Overflow: fall back to float sum.
                        let f: f64 = vals.iter().filter_map(|x| x.as_f64()).sum();
                        return serde_json::json!(f);
                    }
                }
            }
            return Value::from(acc);
        }
        let sum: f64 = vals.iter().filter_map(|v| v.as_f64()).sum();
        serde_json::json!(sum)
    }

    pub(crate) fn agg_avg(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if matches!(args[0], Expr::Wildcard) {
            return Value::Null;
        }
        let vals = Self::agg_numeric_values(&args[0], records);
        if vals.is_empty() {
            return Value::Null;
        }
        let sum: f64 = vals.iter().filter_map(|v| v.as_f64()).sum();
        serde_json::json!(sum / (vals.len() as f64))
    }

    pub(crate) fn agg_min(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if matches!(args[0], Expr::Wildcard) {
            return Value::Null;
        }
        let vals: Vec<Value> = records
            .iter()
            .map(|rec| Self::eval_val(&args[0], rec))
            .filter(|v| !v.is_null())
            .collect();
        if vals.is_empty() {
            return Value::Null;
        }
        let has_numbers = vals.iter().any(|v| v.is_number());
        let filtered: Vec<&Value> = if has_numbers {
            vals.iter().filter(|v| v.is_number()).collect()
        } else {
            vals.iter().collect()
        };
        if filtered.is_empty() {
            return Value::Null;
        }
        let mut best = filtered[0];
        for v in &filtered[1..] {
            if let Some(ord) = Self::compare_values(v, best) {
                if ord == std::cmp::Ordering::Less {
                    best = v;
                }
            }
        }
        best.clone()
    }

    pub(crate) fn agg_max(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if matches!(args[0], Expr::Wildcard) {
            return Value::Null;
        }
        let vals: Vec<Value> = records
            .iter()
            .map(|rec| Self::eval_val(&args[0], rec))
            .filter(|v| !v.is_null())
            .collect();
        if vals.is_empty() {
            return Value::Null;
        }
        let has_numbers = vals.iter().any(|v| v.is_number());
        let filtered: Vec<&Value> = if has_numbers {
            vals.iter().filter(|v| v.is_number()).collect()
        } else {
            vals.iter().collect()
        };
        if filtered.is_empty() {
            return Value::Null;
        }
        let mut best = filtered[0];
        for v in &filtered[1..] {
            if let Some(ord) = Self::compare_values(v, best) {
                if ord == std::cmp::Ordering::Greater {
                    best = v;
                }
            }
        }
        best.clone()
    }

    /// `collect(col)`: all non-null values of `col` across the window batch,
    /// in row order.
    pub(crate) fn agg_collect(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if matches!(args[0], Expr::Wildcard) {
            return Value::Array(
                records
                    .iter()
                    .map(|rec| {
                        let mut map = serde_json::Map::new();
                        for (k, v) in rec {
                            if k != META_KEY && !k.starts_with("__") {
                                map.insert(k.clone(), v.clone());
                            }
                        }
                        Value::Object(map)
                    })
                    .collect(),
            );
        }
        Value::Array(
            records
                .iter()
                .map(|rec| Self::eval_val(&args[0], rec))
                .filter(|v| !v.is_null())
                .collect(),
        )
    }

    /// `latest(col, [default])`: the most recent (last) non-null value of `col` in the
    /// batch, or `default` (or `Null`) when there is none.
    pub(crate) fn agg_latest(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        if args.is_empty() || args.len() > 2 {
            return Value::Null;
        }
        if matches!(args[0], Expr::Wildcard) {
            return Value::Null;
        }
        let default_val = if args.len() == 2 {
            records
                .first()
                .map(|rec| Self::eval_val(&args[1], rec))
                .unwrap_or(Value::Null)
        } else {
            Value::Null
        };
        records
            .iter()
            .rev()
            .map(|rec| Self::eval_val(&args[0], rec))
            .find(|v| !v.is_null())
            .unwrap_or(default_val)
    }

    /// `lead(col, [offset], [default], [ignoreNull])`: forward lookup within the window
    /// batch. The single output row represents the whole window, so `offset`
    /// (default 1) counts forward from the first row (0-based); out-of-range
    /// offsets yield `default` (default `Null`). `ignoreNull` (default `true`)
    /// skips null records while counting forward.
    pub(crate) fn agg_lead(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        if args.is_empty() || args.len() > 4 {
            return Value::Null;
        }
        if matches!(args[0], Expr::Wildcard) {
            return Value::Null;
        }
        let first = records.first();
        let offset: i64 = if args.len() >= 2 {
            first
                .and_then(|rec| Self::to_i64_arg(&Self::eval_val(&args[1], rec)))
                .unwrap_or(1)
        } else {
            1
        };
        let default: Value = if args.len() >= 3 {
            first
                .map(|rec| Self::eval_val(&args[2], rec))
                .unwrap_or(Value::Null)
        } else {
            Value::Null
        };
        if offset < 0 {
            return default;
        }
        let ignore_null: bool = if args.len() >= 4 {
            first
                .and_then(|rec| match Self::eval_val(&args[3], rec) {
                    Value::Bool(b) => Some(b),
                    _ => None,
                })
                .unwrap_or(true)
        } else {
            true
        };

        if ignore_null {
            let mut count = 0i64;
            for rec in records {
                let v = Self::eval_val(&args[0], rec);
                if !v.is_null() {
                    if count == offset {
                        return v;
                    }
                    count += 1;
                }
            }
            default
        } else {
            records
                .get(offset as usize)
                .map(|rec| Self::eval_val(&args[0], rec))
                .unwrap_or(default)
        }
    }

    /// Sorted numeric column values across the window batch, skipping
    /// nulls and non-numerics. Shared by the statistical aggregates.
    pub(crate) fn agg_sorted_numbers(arg: &Expr, records: &[HashMap<String, Value>]) -> Vec<f64> {
        let mut vals: Vec<f64> = Self::agg_numeric_values(arg, records)
            .iter()
            .filter_map(|v| v.as_f64())
            .collect();
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        vals
    }

    pub(crate) fn agg_median(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if matches!(args[0], Expr::Wildcard) {
            return Value::Null;
        }
        let mut raw: Vec<Value> = Vec::new();
        for rec in records {
            let v = Self::eval_val(&args[0], rec);
            if v.is_null() || !v.is_number() {
                continue;
            }
            raw.push(v);
        }
        if raw.is_empty() {
            return Value::Null;
        }
        raw.sort_by(|a, b| Self::compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal));
        let mid = raw.len() / 2;
        if raw.len() % 2 == 1 {
            raw[mid].clone()
        } else {
            let (Some(lo), Some(hi)) = (raw[mid - 1].as_f64(), raw[mid].as_f64()) else {
                return Value::Null;
            };
            serde_json::json!((lo + hi) / 2.0)
        }
    }

    /// Single-pass Welford's online algorithm for sum-of-squared-deviations;
    /// returns `(ssd, count)` with zero heap allocations and zero sorting.
    pub(crate) fn agg_ssd(
        args: &[Expr],
        records: &[HashMap<String, Value>],
    ) -> Option<(f64, usize)> {
        if args.len() != 1 || matches!(args[0], Expr::Wildcard) {
            return None;
        }
        let mut count = 0usize;
        let mut mean = 0.0f64;
        let mut m2 = 0.0f64;

        for rec in records {
            let v = Self::eval_val(&args[0], rec);
            if let Some(x) = v.as_f64() {
                count += 1;
                let delta = x - mean;
                mean += delta / (count as f64);
                let delta2 = x - mean;
                m2 += delta * delta2;
            }
        }
        if count == 0 {
            None
        } else {
            Some((m2, count))
        }
    }

    pub(crate) fn agg_stddev(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        match Self::agg_ssd(args, records) {
            None => Value::Null,
            Some((_, 0)) => Value::Null,
            Some((ssd, n)) => serde_json::json!((ssd / n as f64).sqrt()),
        }
    }

    pub(crate) fn agg_stddevs(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        match Self::agg_ssd(args, records) {
            // Sample statistics need at least 2 points (0 dof otherwise).
            None => Value::Null,
            Some((_, n)) if n < 2 => Value::Null,
            Some((ssd, n)) => serde_json::json!((ssd / (n - 1) as f64).sqrt()),
        }
    }

    pub(crate) fn agg_var(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        match Self::agg_ssd(args, records) {
            None => Value::Null,
            Some((_, 0)) => Value::Null,
            Some((ssd, n)) => serde_json::json!(ssd / n as f64),
        }
    }

    pub(crate) fn agg_vars(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        match Self::agg_ssd(args, records) {
            None => Value::Null,
            Some((_, n)) if n < 2 => Value::Null,
            Some((ssd, n)) => serde_json::json!(ssd / (n - 1) as f64),
        }
    }

    /// Evaluate the percentile fraction `p` against the first batch record
    /// (constants in practice); `None` when missing, non-numeric or outside
    /// `[0, 1]`.
    fn agg_percentile_p(args: &[Expr], records: &[HashMap<String, Value>]) -> Option<f64> {
        if args.len() != 2 {
            return None;
        }
        if matches!(args[0], Expr::Wildcard) {
            return None;
        }
        let first = records.first()?;
        let p = Self::to_f64(&Self::eval_val(&args[1], first))?;
        if !(0.0..=1.0).contains(&p) {
            return None;
        }
        Some(p)
    }

    pub(crate) fn agg_percentile(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        let Some(p) = Self::agg_percentile_p(args, records) else {
            return Value::Null;
        };
        let vals = Self::agg_sorted_numbers(&args[0], records);
        let n = vals.len();
        if n == 0 {
            return Value::Null;
        }
        if n == 1 {
            return serde_json::json!(vals[0]);
        }
        let rank = p * (n - 1) as f64;
        let i = rank.floor() as usize;
        if i + 1 >= n {
            return serde_json::json!(vals[n - 1]);
        }
        let frac = rank - i as f64;
        serde_json::json!(vals[i] + frac * (vals[i + 1] - vals[i]))
    }

    pub(crate) fn agg_percentile_disc(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        let Some(p) = Self::agg_percentile_p(args, records) else {
            return Value::Null;
        };
        let mut raw: Vec<Value> = Vec::new();
        for rec in records {
            let v = Self::eval_val(&args[0], rec);
            if v.is_null() || !v.is_number() {
                continue;
            }
            raw.push(v);
        }
        if raw.is_empty() {
            return Value::Null;
        }
        raw.sort_by(|a, b| Self::compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal));
        let n = raw.len();
        let index = if p == 0.0 {
            0
        } else {
            ((p * n as f64).ceil() as usize)
                .saturating_sub(1)
                .min(n - 1)
        };
        raw[index].clone()
    }

    pub(crate) fn agg_last_value(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        if args.is_empty() || args.len() > 2 {
            return Value::Null;
        }
        let ignore_nulls = if args.len() == 2 {
            let Some(first) = records.first() else {
                return Value::Null;
            };
            Self::eval_val(&args[1], first).as_bool().unwrap_or(false)
        } else {
            false
        };
        if matches!(args[0], Expr::Wildcard) {
            if !ignore_nulls {
                return records
                    .last()
                    .map(|rec| {
                        Value::Object(rec.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                    })
                    .unwrap_or(Value::Null);
            }
            return records
                .iter()
                .rev()
                .find(|rec| rec.values().any(|v| !v.is_null()))
                .map(|rec| Value::Object(rec.iter().map(|(k, v)| (k.clone(), v.clone())).collect()))
                .unwrap_or(Value::Null);
        }
        if !ignore_nulls {
            return records
                .last()
                .map(|rec| Self::eval_val(&args[0], rec))
                .unwrap_or(Value::Null);
        }
        records
            .iter()
            .rev()
            .map(|rec| Self::eval_val(&args[0], rec))
            .find(|v| !v.is_null())
            .unwrap_or(Value::Null)
    }

    pub(crate) fn agg_merge_agg(args: &[Expr], records: &[HashMap<String, Value>]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let mut merged = serde_json::Map::new();
        if matches!(args[0], Expr::Wildcard) {
            for rec in records {
                for (k, v) in rec {
                    merged.insert(k.clone(), v.clone());
                }
            }
            return Value::Object(merged);
        }
        for rec in records {
            if let Value::Object(map) = Self::eval_val(&args[0], rec) {
                for (k, v) in map {
                    merged.insert(k, v);
                }
            }
        }
        Value::Object(merged)
    }

    pub(crate) fn agg_row_number(args: &[Expr], _records: &[HashMap<String, Value>]) -> Value {
        if !args.is_empty() {
            return Value::Null;
        }
        Value::from(1)
    }
}
