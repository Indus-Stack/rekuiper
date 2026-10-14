use super::{Evaluator, RuleState};
use serde_json::Value;

impl Evaluator {
    // ---------- analytic / window navigation ----------

    /// `lag(val, [offset], [default])`: returns the value seen `offset`
    /// rows ago within the same partition (default 1), or `default`
    /// (default `Null`) when fewer rows have been seen. The current value is
    /// appended to history after the lookup. State key:
    /// `lag:{func_call_id}:{partition_key}`.
    pub(crate) fn eval_lag(
        args: &[Value],
        state: &RuleState,
        call_id: &str,
        partition_key: &str,
        skip_update: bool,
    ) -> Value {
        if args.is_empty() || args.len() > 4 {
            return Value::Null;
        }
        let offset = args.get(1).and_then(Self::to_i64_arg).unwrap_or(1);
        if offset <= 0 {
            return args[0].clone();
        }
        let default = args.get(2).cloned().unwrap_or(Value::Null);
        let ignore_null = args.get(3).and_then(|v| v.as_bool()).unwrap_or(false);
        let state_key = format!("lag:{}:{}", call_id, partition_key);
        let mut guard = state.state.write();
        let mut history: Vec<Value> = match guard.get(&state_key) {
            Some(Value::Array(arr)) => arr.clone(),
            _ => Vec::new(),
        };
        let out = if (history.len() as i64) >= offset {
            history[history.len() - offset as usize].clone()
        } else {
            default
        };
        if !skip_update && (!ignore_null || !args[0].is_null()) {
            history.push(args[0].clone());
            guard.insert(state_key, Value::Array(history));
        }
        out
    }

    /// Stateful change detection shared by `had_changed` / `changed_col`:
    /// compares against the stored previous value (both-null counts as
    /// unchanged), then stores the current value.
    pub(crate) fn eval_changed(
        lowered_name: &str,
        args: &[Value],
        state: &RuleState,
        call_id: &str,
        partition_key: &str,
    ) -> Value {
        if args.is_empty() {
            return Value::Null;
        }
        let (ignore_null, cols) = if args.len() >= 2 && args[0].is_boolean() {
            (args[0].as_bool().unwrap_or(false), &args[1..])
        } else {
            (false, args)
        };

        let mut any_changed = false;
        let mut first_val = Value::Null;

        for (idx, current) in cols.iter().enumerate() {
            let state_key = format!("{}:{}:{}:{}", lowered_name, call_id, idx, partition_key);
            if current.is_null() {
                if ignore_null {
                    continue;
                }
                continue;
            }
            let previous = state.state.read().get(&state_key).cloned();
            let changed = match &previous {
                None => true,
                Some(p) => !Self::values_equal(p, current),
            };
            if changed {
                state.state.write().insert(state_key, current.clone());
                any_changed = true;
                if first_val.is_null() {
                    first_val = current.clone();
                }
            }
        }

        if lowered_name == "had_changed" {
            Value::Bool(any_changed)
        } else if any_changed {
            first_val
        } else {
            Value::Null
        }
    }

    // ---------- analytic scalar fallbacks (single-record context) ----------

    pub(crate) fn func_collect_scalar(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Array(Vec::new());
        }
        Value::Array(vec![args[0].clone()])
    }

    pub(crate) fn func_lead_scalar(args: &[Value]) -> Value {
        if args.is_empty() || args.len() > 4 {
            return Value::Null;
        }
        // A single row has no future rows: resolve to the default.
        args.get(2).cloned().unwrap_or(Value::Null)
    }

    pub(crate) fn func_lag_scalar(args: &[Value]) -> Value {
        if args.is_empty() || args.len() > 4 {
            return Value::Null;
        }
        // A single row without prior history: resolve to the default (arg 2).
        args.get(2).cloned().unwrap_or(Value::Null)
    }

    pub(crate) fn func_latest_scalar(args: &[Value]) -> Value {
        if args.is_empty() || args.len() > 2 {
            return Value::Null;
        }
        let default_val = args.get(1).cloned().unwrap_or(Value::Null);
        if args[0].is_null() {
            return default_val;
        }
        args[0].clone()
    }

    pub(crate) fn func_row_number_scalar(args: &[Value]) -> Value {
        if !args.is_empty() {
            return Value::Null;
        }
        Value::from(1)
    }

    pub(crate) fn func_had_changed_scalar(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        // Without history every value counts as changed.
        Value::Bool(true)
    }

    pub(crate) fn func_changed_col_scalar(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        args[0].clone()
    }

    /// Maximum partition keys tracked per CEP state machine to guarantee O(keys) bounded RAM.
    const MAX_CEP_PARTITIONS: usize = 10_000;

    /// Deterministic Finite Automaton (DFA) for sequential Complex Event Processing:
    /// `sequence_match(max_span_ms, cond1, cond2, ...)` -> `Value::Bool(true)` when all
    /// conditions are satisfied in strict sequential order within `max_span_ms`.
    /// `sequence_count(max_span_ms, cond1, cond2, ...)` -> `Value::Number(completed_sequences)`.
    pub(crate) fn eval_sequence_cep(
        lowered_name: &str,
        args: &[Value],
        event_ts: i64,
        state: &RuleState,
        call_id: &str,
        partition_key: &str,
        skip_update: bool,
    ) -> Value {
        let is_count = lowered_name == "sequence_count";
        if args.len() < 3 {
            return if is_count {
                Value::from(0)
            } else {
                Value::Bool(false)
            };
        }
        let max_span_ms = Self::to_i64_arg(&args[0]).unwrap_or(0);
        let steps = &args[1..];
        let num_steps = steps.len();
        let state_key = format!("cep:{}:{}:{}", lowered_name, call_id, partition_key);

        let mut guard = state.state.write();
        // Read current (stage, start_ts, completed_count) from state
        let (mut stage, mut start_ts, mut completed) = match guard.get(&state_key) {
            Some(Value::Array(arr)) if arr.len() == 3 => (
                arr[0].as_u64().unwrap_or(0) as usize,
                arr[1].as_i64().unwrap_or(event_ts),
                arr[2].as_i64().unwrap_or(0),
            ),
            _ => (0usize, event_ts, 0i64),
        };

        if skip_update {
            return if is_count {
                Value::from(completed)
            } else {
                Value::Bool(false)
            };
        }

        // 1. Check timeout window expiration on in-progress sequence
        if stage > 0 && max_span_ms > 0 && event_ts.saturating_sub(start_ts) > max_span_ms {
            stage = 0;
        }

        // 2. Advance DFA transitions across current event conditions
        let step_bool =
            |idx: usize| -> bool { steps.get(idx).and_then(|v| v.as_bool()).unwrap_or(false) };

        if stage == 0 {
            if step_bool(0) {
                stage = 1;
                start_ts = event_ts;
            }
        } else if stage < num_steps {
            if step_bool(stage) {
                stage += 1;
            } else if step_bool(0) {
                // Restart sequence from step 1 with fresh start timestamp
                stage = 1;
                start_ts = event_ts;
            }
        }

        // 3. Check if terminal accepting state was reached
        let matched_now = if stage >= num_steps {
            completed = completed.saturating_add(1);
            stage = 0;
            true
        } else {
            false
        };

        // Bound state map size to prevent unbounded growth on high-cardinality keys
        if !guard.contains_key(&state_key) && guard.len() >= Self::MAX_CEP_PARTITIONS {
            if let Some(evict_key) = guard.keys().find(|k| k.starts_with("cep:")).cloned() {
                guard.remove(&evict_key);
            }
        }

        guard.insert(
            state_key,
            Value::Array(vec![
                Value::from(stage as i64),
                Value::from(start_ts),
                Value::from(completed),
            ]),
        );

        if is_count {
            Value::from(completed)
        } else {
            Value::Bool(matched_now)
        }
    }
}
