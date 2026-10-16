use super::Evaluator;
use serde_json::Value;

impl Evaluator {
    // ---------- array & object functions ----------

    pub(crate) fn func_array_contains(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Bool(false);
        };
        Value::Bool(arr.iter().any(|item| Self::values_equal(item, &args[1])))
    }

    pub(crate) fn func_array_join(args: &[Value]) -> Value {
        if args.is_empty() || args.len() > 3 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        if arr.is_empty() {
            return Value::Null;
        }
        let sep = if args.len() >= 2 {
            if args[1].is_null() {
                return Value::Null;
            }
            Self::to_string_always(&args[1])
        } else {
            ",".to_string()
        };
        let null_replacement = if args.len() == 3 {
            Some(Self::to_string_always(&args[2]))
        } else {
            None
        };
        let mut items = Vec::new();
        for item in arr {
            if item.is_null() {
                if let Some(ref nr) = null_replacement {
                    items.push(nr.clone());
                }
            } else {
                items.push(Self::to_string_always(item));
            }
        }
        Value::String(items.join(&sep))
    }

    pub(crate) fn func_keys(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(obj) = args[0].as_object() else {
            return Value::Null;
        };
        Value::Array(obj.keys().map(|k| Value::String(k.clone())).collect())
    }

    pub(crate) fn func_values(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(obj) = args[0].as_object() else {
            return Value::Null;
        };
        Value::Array(obj.values().cloned().collect())
    }

    pub(crate) fn func_object(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(keys), Some(vals)) = (args[0].as_array(), args[1].as_array()) else {
            return Value::Null;
        };
        if keys.len() != vals.len() {
            return Value::Null;
        }
        let mut map = serde_json::Map::with_capacity(keys.len());
        for (k, v) in keys.iter().zip(vals.iter()) {
            let key_str = match k {
                Value::String(s) => s.clone(),
                other => Self::to_string_always(other),
            };
            map.insert(key_str, v.clone());
        }
        Value::Object(map)
    }

    pub(crate) fn func_zip(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(entries) = args[0].as_array() else {
            return Value::Null;
        };
        let mut map = serde_json::Map::with_capacity(entries.len());
        for entry in entries {
            let Some(pair) = entry.as_array() else {
                return Value::Null;
            };
            if pair.len() != 2 {
                return Value::Null;
            }
            let key_str = match &pair[0] {
                Value::String(s) => s.clone(),
                other => Self::to_string_always(other),
            };
            map.insert(key_str, pair[1].clone());
        }
        Value::Object(map)
    }

    pub(crate) fn func_items(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(obj) = args[0].as_object() else {
            return Value::Null;
        };
        let items: Vec<Value> = obj
            .iter()
            .map(|(k, v)| Value::Array(vec![Value::String(k.clone()), v.clone()]))
            .collect();
        Value::Array(items)
    }

    /// Builds an object from alternating key/value arguments:
    /// `object_construct(k1, v1, k2, v2, ...)`. Keys are stringified via
    /// [`Self::to_string_always`]; pairs with `Null` values are omitted
    /// (eKuiper parity); an odd argument count yields `Null`.
    pub(crate) fn func_object_construct(args: &[Value]) -> Value {
        if args.len() & 1 != 0 {
            return Value::Null;
        }
        let mut map = serde_json::Map::with_capacity(args.len() / 2);
        let mut it = args.iter();
        while let (Some(k), Some(v)) = (it.next(), it.next()) {
            if v.is_null() {
                continue;
            }
            map.insert(Self::to_string_always(k), v.clone());
        }
        Value::Object(map)
    }

    /// Merges two or more objects left to right; later keys win.
    pub(crate) fn func_object_concat(args: &[Value]) -> Value {
        if args.len() < 2 {
            return Value::Null;
        }
        let mut map = serde_json::Map::new();
        for arg in args {
            let Some(obj) = arg.as_object() else {
                return Value::Null;
            };
            for (k, v) in obj {
                map.insert(k.clone(), v.clone());
            }
        }
        Value::Object(map)
    }

    /// Collects key names to erase/pick: plain values stringify, arrays
    /// contribute each element as a string.
    fn key_names(args: &[Value]) -> Vec<String> {
        let mut keys = Vec::new();
        for arg in args {
            if let Some(arr) = arg.as_array() {
                keys.extend(arr.iter().map(Self::to_string_always));
            } else {
                keys.push(Self::to_string_always(arg));
            }
        }
        keys
    }

    pub(crate) fn func_erase(args: &[Value]) -> Value {
        if args.len() < 2 {
            return Value::Null;
        }
        let Some(obj) = args[0].as_object() else {
            return Value::Null;
        };
        let mut map = obj.clone();
        for key in Self::key_names(&args[1..]) {
            map.remove(&key);
        }
        Value::Object(map)
    }

    pub(crate) fn func_object_pick(args: &[Value]) -> Value {
        if args.len() < 2 {
            return Value::Null;
        }
        let Some(obj) = args[0].as_object() else {
            return Value::Null;
        };
        let mut map = serde_json::Map::new();
        for key in Self::key_names(&args[1..]) {
            if let Some(v) = obj.get(&key) {
                map.insert(key, v.clone());
            }
        }
        Value::Object(map)
    }

    /// Inverse of [`Self::func_kvpair_array_to_obj`]: object entries become
    /// `{"key": k, "value": v}` elements.
    pub(crate) fn func_obj_to_kvpair_array(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(obj) = args[0].as_object() else {
            return Value::Null;
        };
        Value::Array(
            obj.iter()
                .map(|(k, v)| {
                    let mut entry = serde_json::Map::with_capacity(2);
                    entry.insert("key".to_string(), Value::String(k.clone()));
                    entry.insert("value".to_string(), v.clone());
                    Value::Object(entry)
                })
                .collect(),
        )
    }

    pub(crate) fn func_to_json(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        match serde_json::to_string(&args[0]) {
            Ok(s) => Value::String(s),
            Err(_) => Value::Null,
        }
    }

    pub(crate) fn func_parse_json(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        match &args[0] {
            Value::Null => Value::Null,
            Value::String(s) => serde_json::from_str::<Value>(s).unwrap_or(Value::Null),
            structured => structured.clone(),
        }
    }

    // ---------- extended array functions ----------

    pub(crate) fn func_array_create(args: &[Value]) -> Value {
        Value::Array(args.to_vec())
    }

    pub(crate) fn func_array_position(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::from(-1);
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        match arr
            .iter()
            .position(|item| Self::values_equal(item, &args[1]))
        {
            // 0-based index, -1 when absent or nil (matching eKuiper specification).
            Some(i) => Value::from(i as i64),
            None => Value::from(-1),
        }
    }

    pub(crate) fn func_array_last_position(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::from(-1);
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        match arr
            .iter()
            .rposition(|item| Self::values_equal(item, &args[1]))
        {
            // 0-based index, -1 when absent or nil (matching eKuiper specification).
            Some(i) => Value::from(i as i64),
            None => Value::from(-1),
        }
    }

    pub(crate) fn func_array_positions(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Array(Vec::new());
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        let target = &args[1];
        let mut positions = Vec::new();
        for (i, item) in arr.iter().enumerate() {
            if Self::values_equal(item, target) {
                positions.push(Value::from(i as i64));
            }
        }
        Value::Array(positions)
    }

    pub(crate) fn func_array_shuffle(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        let mut out = arr.clone();
        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();
        out.shuffle(&mut rng);
        Value::Array(out)
    }

    pub(crate) fn func_array_map(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let Some(func_name) = args[0].as_str() else {
            return Value::Null;
        };
        let Some(arr) = args[1].as_array() else {
            return Value::Null;
        };
        let mapped: Vec<Value> = arr
            .iter()
            .map(|item| Self::eval_call(func_name, std::slice::from_ref(item)))
            .collect();
        Value::Array(mapped)
    }

    pub(crate) fn func_array_length(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        match args[0].as_array() {
            Some(arr) => Value::from(arr.len() as i64),
            None => Value::Null,
        }
    }

    pub(crate) fn func_array_slice(args: &[Value]) -> Value {
        if args.len() != 3 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        let (Some(mut start), Some(mut end)) =
            (Self::to_i64_arg(&args[1]), Self::to_i64_arg(&args[2]))
        else {
            return Value::Null;
        };
        // 1-based inclusive bounds, clamped into range.
        let len = arr.len() as i64;
        if start < 1 {
            start = 1;
        }
        if end > len {
            end = len;
        }
        if start > end || start > len || end < 1 {
            return Value::Array(Vec::new());
        }
        Value::Array(arr[(start - 1) as usize..end as usize].to_vec())
    }

    pub(crate) fn func_array_concat(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(a), Some(b)) = (args[0].as_array(), args[1].as_array()) else {
            return Value::Null;
        };
        Value::Array(a.iter().chain(b.iter()).cloned().collect())
    }

    pub(crate) fn func_deduplicate(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        let mut out: Vec<Value> = Vec::with_capacity(arr.len());
        for item in arr {
            if !out.iter().any(|seen| Self::values_equal(seen, item)) {
                out.push(item.clone());
            }
        }
        Value::Array(out)
    }

    pub(crate) fn func_cardinality(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        match &args[0] {
            Value::Array(arr) => Value::from(arr.len() as i64),
            Value::Null => Value::from(0),
            _ => Value::Null,
        }
    }

    pub(crate) fn func_element_at(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        if let Some(obj) = args[0].as_object() {
            let key = match &args[1] {
                Value::String(s) => s.as_str(),
                _ => return Value::Null,
            };
            return obj.get(key).cloned().unwrap_or(Value::Null);
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        let Some(index) = Self::to_i64_arg(&args[1]) else {
            return Value::Null;
        };
        // 0-based indexing for positive indices; negatives count back from the end (-1 is last).
        let len = arr.len() as i64;
        let pos = if index >= 0 { index } else { len + index };
        if pos < 0 || pos >= len {
            return Value::Null;
        }
        arr[pos as usize].clone()
    }

    pub(crate) fn func_array_contains_any(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(haystack), Some(needles)) = (args[0].as_array(), args[1].as_array()) else {
            return Value::Bool(false);
        };
        Value::Bool(
            needles
                .iter()
                .any(|n| haystack.iter().any(|h| Self::values_equal(h, n))),
        )
    }

    pub(crate) fn func_array_remove(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        Value::Array(
            arr.iter()
                .filter(|item| !Self::values_equal(item, &args[1]))
                .cloned()
                .collect(),
        )
    }

    pub(crate) fn func_array_distinct(args: &[Value]) -> Value {
        Self::func_deduplicate(args)
    }

    pub(crate) fn func_array_intersect(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(a), Some(b)) = (args[0].as_array(), args[1].as_array()) else {
            return Value::Null;
        };
        let mut out = Vec::new();
        for item in a {
            if b.iter().any(|other| Self::values_equal(item, other))
                && !out.iter().any(|seen| Self::values_equal(seen, item))
            {
                out.push(item.clone());
            }
        }
        Value::Array(out)
    }

    pub(crate) fn func_array_union(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(a), Some(b)) = (args[0].as_array(), args[1].as_array()) else {
            return Value::Null;
        };
        let mut out: Vec<Value> = Vec::with_capacity(a.len() + b.len());
        for item in a.iter().chain(b.iter()) {
            if !out.iter().any(|seen| Self::values_equal(seen, item)) {
                out.push(item.clone());
            }
        }
        Value::Array(out)
    }

    pub(crate) fn func_array_except(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(a), Some(b)) = (args[0].as_array(), args[1].as_array()) else {
            return Value::Null;
        };
        Value::Array(
            a.iter()
                .filter(|item| !b.iter().any(|other| Self::values_equal(item, other)))
                .cloned()
                .collect(),
        )
    }

    /// Shared numeric scan for `array_max`/`array_min`: extreme value among
    /// numeric elements, preserving the original JSON representation.
    fn array_extreme<F>(arr: &[Value], better: F) -> Value
    where
        F: Fn(std::cmp::Ordering) -> bool,
    {
        let mut best: Option<&Value> = None;
        for item in arr {
            if !item.is_number() {
                continue;
            }
            best = Some(match best {
                None => item,
                Some(current) => match Self::compare_values(item, current) {
                    Some(ord) if better(ord) => item,
                    _ => current,
                },
            });
        }
        best.cloned().unwrap_or(Value::Null)
    }

    pub(crate) fn func_array_max(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        Self::array_extreme(arr, |ord| ord == std::cmp::Ordering::Greater)
    }

    pub(crate) fn func_array_min(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        Self::array_extreme(arr, |ord| ord == std::cmp::Ordering::Less)
    }

    pub(crate) fn func_array_avg(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        let mut sum = 0.0;
        let mut count = 0u64;
        for item in arr {
            if let Some(f) = item.as_f64() {
                sum += f;
                count += 1;
            }
        }
        if count == 0 {
            return Value::Null;
        }
        serde_json::json!(sum / count as f64)
    }

    pub(crate) fn func_array_flatten(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        let mut out = Vec::new();
        for item in arr {
            match item {
                Value::Array(inner) => out.extend(inner.iter().cloned()),
                other => out.push(other.clone()),
            }
        }
        Value::Array(out)
    }

    pub(crate) fn func_array_sort(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        let mut out = arr.clone();
        out.sort_by(|a, b| {
            Self::compare_values(a, b)
                .unwrap_or_else(|| Self::to_string_always(a).cmp(&Self::to_string_always(b)))
        });
        Value::Array(out)
    }

    pub(crate) fn func_repeat(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let Some(n) = Self::to_i64_arg(&args[1]) else {
            return Value::Null;
        };
        if n < 0 {
            return Value::Null;
        }
        Value::Array(vec![args[0].clone(); n as usize])
    }

    pub(crate) fn func_sequence(args: &[Value]) -> Value {
        if args.len() != 2 && args.len() != 3 {
            return Value::Null;
        }
        let (Some(start), Some(stop)) = (Self::to_i64_arg(&args[0]), Self::to_i64_arg(&args[1]))
        else {
            return Value::Null;
        };
        let step = if args.len() == 3 {
            let Some(step) = Self::to_i64_arg(&args[2]) else {
                return Value::Null;
            };
            if step == 0 {
                return Value::Null;
            }
            step
        } else if start <= stop {
            1
        } else {
            -1
        };
        // An explicit step fighting the direction would loop forever.
        if (step > 0 && start > stop) || (step < 0 && start < stop) {
            return Value::Null;
        }
        let mut out = Vec::new();
        let mut current = start;
        loop {
            out.push(Value::from(current));
            if current == stop {
                break;
            }
            match current.checked_add(step) {
                Some(next) => current = next,
                None => break,
            }
        }
        Value::Array(out)
    }

    pub(crate) fn func_kvpair_array_to_obj(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(arr) = args[0].as_array() else {
            return Value::Null;
        };
        let mut map = serde_json::Map::new();
        for item in arr {
            let Some(obj) = item.as_object() else {
                continue;
            };
            let Some(key) = obj
                .get("key")
                .or_else(|| obj.get("Key"))
                .or_else(|| obj.get("k"))
                .map(Self::to_string_always)
            else {
                continue;
            };
            let value = obj
                .get("value")
                .or_else(|| obj.get("Value"))
                .or_else(|| obj.get("v"))
                .cloned()
                .unwrap_or(Value::Null);
            map.insert(key, value);
        }
        Value::Object(map)
    }
}
