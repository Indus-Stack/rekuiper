use super::Evaluator;
use serde_json::Value;

impl Evaluator {
    // ---------- math functions ----------

    pub(crate) fn func_abs_fallible(args: &[Value]) -> Result<Value, String> {
        if args.len() != 1 {
            return Ok(Value::Null);
        }
        let v = &args[0];
        if v.is_null() {
            return Ok(Value::Null);
        }
        if let Some(i) = v.as_i64() {
            if let Some(n) = i.checked_abs() {
                return Ok(Value::from(n));
            }
            return Ok(serde_json::json!((i as f64).abs()));
        }
        if let Some(u) = v.as_u64() {
            return Ok(Value::from(u));
        }
        if let Some(f) = v.as_f64() {
            return Ok(serde_json::json!(f.abs()));
        }
        Err("call func abs error: only float64 & int type are supported".to_string())
    }

    pub(crate) fn func_abs(args: &[Value]) -> Value {
        Self::func_abs_fallible(args).unwrap_or(Value::Null)
    }

    pub(crate) fn func_ceil(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        match Self::to_f64(&args[0]) {
            Some(f) => serde_json::json!(f.ceil()),
            None => Value::Null,
        }
    }

    pub(crate) fn func_floor(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        match Self::to_f64(&args[0]) {
            Some(f) => serde_json::json!(f.floor()),
            None => Value::Null,
        }
    }

    pub(crate) fn func_round(args: &[Value]) -> Value {
        if args.is_empty() || args.len() > 2 {
            return Value::Null;
        }
        if args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        let v = match Self::to_f64(&args[0]) {
            Some(f) => f,
            None => return Value::Null,
        };
        let precision: i32 = if args.len() == 2 {
            match Self::to_i64_arg(&args[1]) {
                Some(p) => p as i32,
                None => return Value::Null,
            }
        } else {
            0
        };
        let factor = 10f64.powi(precision);
        if !factor.is_finite() {
            return Value::Null;
        }
        let scaled = v * factor;
        if !scaled.is_finite() {
            return Value::Null;
        }
        serde_json::json!(scaled.round() / factor)
    }

    pub(crate) fn func_sqrt_fallible(args: &[Value]) -> Result<Value, String> {
        if args.len() != 1 {
            return Ok(Value::Null);
        }
        let v = &args[0];
        if v.is_null() {
            return Ok(Value::Null);
        }
        let f = match Self::to_f64(v) {
            Some(f) => f,
            None => {
                return Err(format!(
                    "call func sqrt error: cannot convert {} to float64",
                    Self::format_ekuiper_val_type(v)
                ));
            }
        };
        if f < 0.0 {
            let formatted = if let Some(i) = v.as_i64() {
                i.to_string()
            } else {
                f.to_string()
            };
            return Err(format!(
                "call func sqrt error: The argument must be a positive number but got {}",
                formatted
            ));
        }
        let r = f.sqrt();
        if r.is_nan() {
            return Ok(Value::Null);
        }
        Ok(serde_json::json!(r))
    }

    pub(crate) fn func_sqrt(args: &[Value]) -> Value {
        Self::func_sqrt_fallible(args).unwrap_or(Value::Null)
    }

    pub(crate) fn func_power(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        if args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        // Integer fast path: exact results for non-negative exponents.
        if let (Some(base), Some(exp)) = (args[0].as_i64(), args[1].as_i64()) {
            if exp >= 0 {
                return match u32::try_from(exp).ok().and_then(|e| base.checked_pow(e)) {
                    Some(n) => Value::from(n),
                    None => Value::Null,
                };
            }
        }
        let (Some(x), Some(y)) = (Self::to_f64(&args[0]), Self::to_f64(&args[1])) else {
            return Value::Null;
        };
        let r = x.powf(y);
        if r.is_nan() || r.is_infinite() {
            return Value::Null;
        }
        serde_json::json!(r)
    }

    // ---------- extended math & trig functions ----------

    /// Single-arg trig-style helper: numeric input via [`Self::to_f64`],
    /// `Null` for null/non-numeric input or NaN/infinite results.
    pub(crate) fn func_float1(args: &[Value], f: fn(f64) -> f64) -> Value {
        if args.len() != 1 || args[0].is_null() {
            return Value::Null;
        }
        match Self::to_f64(&args[0]) {
            Some(v) => {
                let r = f(v);
                if r.is_nan() || r.is_infinite() {
                    return Value::Null;
                }
                serde_json::json!(r)
            }
            None => Value::Null,
        }
    }

    pub(crate) fn func_sin(args: &[Value]) -> Value {
        Self::func_float1(args, f64::sin)
    }

    pub(crate) fn func_cos(args: &[Value]) -> Value {
        Self::func_float1(args, f64::cos)
    }

    pub(crate) fn func_tan(args: &[Value]) -> Value {
        Self::func_float1(args, f64::tan)
    }

    pub(crate) fn func_asin(args: &[Value]) -> Value {
        Self::func_float1(args, f64::asin)
    }

    pub(crate) fn func_acos(args: &[Value]) -> Value {
        Self::func_float1(args, f64::acos)
    }

    pub(crate) fn func_atan(args: &[Value]) -> Value {
        Self::func_float1(args, f64::atan)
    }

    pub(crate) fn func_atan2(args: &[Value]) -> Value {
        if args.len() != 2 || args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        match (Self::to_f64(&args[0]), Self::to_f64(&args[1])) {
            (Some(y), Some(x)) => {
                let r = y.atan2(x);
                if r.is_nan() || r.is_infinite() {
                    return Value::Null;
                }
                serde_json::json!(r)
            }
            _ => Value::Null,
        }
    }

    pub(crate) fn func_exp(args: &[Value]) -> Value {
        Self::func_float1(args, f64::exp)
    }

    pub(crate) fn func_ln_fallible(args: &[Value]) -> Result<Value, String> {
        if args.len() != 1 {
            return Ok(Value::Null);
        }
        let v = &args[0];
        if v.is_null() {
            return Ok(Value::Null);
        }
        let f = match Self::to_f64(v) {
            Some(f) => f,
            None => {
                return Err(format!(
                    "call func ln error: cannot convert {} to float64",
                    Self::format_ekuiper_val_type(v)
                ));
            }
        };
        if f <= 0.0 {
            let formatted = if let Some(i) = v.as_i64() {
                i.to_string()
            } else {
                f.to_string()
            };
            return Err(format!(
                "call func ln error: The argument must be a strictly positive number but got {}",
                formatted
            ));
        }
        let r = f.ln();
        if r.is_nan() || r.is_infinite() {
            return Ok(Value::Null);
        }
        Ok(serde_json::json!(r))
    }

    pub(crate) fn func_ln(args: &[Value]) -> Value {
        Self::func_ln_fallible(args).unwrap_or(Value::Null)
    }

    pub(crate) fn func_log10(args: &[Value]) -> Value {
        if args.len() != 1 || args[0].is_null() {
            return Value::Null;
        }
        match Self::to_f64(&args[0]) {
            Some(v) if v > 0.0 => {
                let r = v.log10();
                if r.is_nan() || r.is_infinite() {
                    return Value::Null;
                }
                serde_json::json!(r)
            }
            _ => Value::Null,
        }
    }

    pub(crate) fn func_log(args: &[Value]) -> Value {
        // eKuiper accepts `log(x)` (base-10 log, distinct from `ln`) OR `log(b, x)`
        // (logarithm of `x` to base `b`).
        match args.len() {
            1 => Self::func_log10(args),
            2 => {
                if args.iter().any(|v| v.is_null()) {
                    return Value::Null;
                }
                let (Some(base), Some(val)) = (Self::to_f64(&args[0]), Self::to_f64(&args[1]))
                else {
                    return Value::Null;
                };
                if base <= 0.0 || (base - 1.0).abs() < f64::EPSILON || val <= 0.0 {
                    return Value::Null;
                }
                let raw = if (base - 10.0).abs() < f64::EPSILON {
                    val.log10()
                } else if (base - 2.0).abs() < f64::EPSILON {
                    val.log2()
                } else {
                    val.log(base)
                };
                if raw.is_nan() || raw.is_infinite() {
                    return Value::Null;
                }
                let rounded = raw.round();
                let r = if (raw - rounded).abs() < 1e-12 {
                    rounded
                } else {
                    raw
                };
                serde_json::json!(r)
            }
            _ => Value::Null,
        }
    }

    pub(crate) fn func_log2(args: &[Value]) -> Value {
        if args.len() != 1 || args[0].is_null() {
            return Value::Null;
        }
        match Self::to_f64(&args[0]) {
            Some(v) if v > 0.0 => {
                let r = v.log2();
                if r.is_nan() || r.is_infinite() {
                    return Value::Null;
                }
                serde_json::json!(r)
            }
            _ => Value::Null,
        }
    }

    pub(crate) fn func_cosh(args: &[Value]) -> Value {
        Self::func_float1(args, f64::cosh)
    }

    pub(crate) fn func_sinh(args: &[Value]) -> Value {
        Self::func_float1(args, f64::sinh)
    }

    pub(crate) fn func_tanh(args: &[Value]) -> Value {
        Self::func_float1(args, f64::tanh)
    }

    pub(crate) fn func_cot_fallible(args: &[Value]) -> Result<Value, String> {
        if args.len() != 1 {
            return Ok(Value::Null);
        }
        let v = &args[0];
        if v.is_null() {
            return Ok(Value::Null);
        }
        let f = match Self::to_f64(v) {
            Some(f) => f,
            None => {
                return Err(format!(
                    "call func cot error: cannot convert {} to float64",
                    Self::format_ekuiper_val_type(v)
                ));
            }
        };
        let tan = f.tan();
        if tan == 0.0 {
            return Err("call func cot error: divided by zero".to_string());
        }
        let r = 1.0 / tan;
        if r.is_nan() || r.is_infinite() {
            return Ok(Value::Null);
        }
        Ok(serde_json::json!(r))
    }

    pub(crate) fn func_cot(args: &[Value]) -> Value {
        Self::func_cot_fallible(args).unwrap_or(Value::Null)
    }

    pub(crate) fn func_radians(args: &[Value]) -> Value {
        if args.len() != 1 || args[0].is_null() {
            return Value::Null;
        }
        match Self::to_f64(&args[0]) {
            Some(v) => serde_json::json!(v.to_radians()),
            None => Value::Null,
        }
    }

    pub(crate) fn func_degrees(args: &[Value]) -> Value {
        if args.len() != 1 || args[0].is_null() {
            return Value::Null;
        }
        match Self::to_f64(&args[0]) {
            Some(v) => serde_json::json!(v.to_degrees()),
            None => Value::Null,
        }
    }

    pub(crate) fn func_bitand(args: &[Value]) -> Value {
        if args.len() != 2 || args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        match (Self::to_i64_arg(&args[0]), Self::to_i64_arg(&args[1])) {
            (Some(a), Some(b)) => Value::from(a & b),
            _ => Value::Null,
        }
    }

    pub(crate) fn func_bitor(args: &[Value]) -> Value {
        if args.len() != 2 || args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        match (Self::to_i64_arg(&args[0]), Self::to_i64_arg(&args[1])) {
            (Some(a), Some(b)) => Value::from(a | b),
            _ => Value::Null,
        }
    }

    pub(crate) fn func_bitxor(args: &[Value]) -> Value {
        if args.len() != 2 || args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        match (Self::to_i64_arg(&args[0]), Self::to_i64_arg(&args[1])) {
            (Some(a), Some(b)) => Value::from(a ^ b),
            _ => Value::Null,
        }
    }

    pub(crate) fn func_bitnot(args: &[Value]) -> Value {
        if args.len() != 1 || args[0].is_null() {
            return Value::Null;
        }
        match Self::to_i64_arg(&args[0]) {
            Some(a) => Value::from(!a),
            _ => Value::Null,
        }
    }

    pub(crate) fn func_pi(_args: &[Value]) -> Value {
        serde_json::json!(std::f64::consts::PI)
    }

    /// `rand()`: random float in `[0.0, 1.0)`.
    pub(crate) fn func_rand(_args: &[Value]) -> Value {
        use rand::Rng;
        serde_json::json!(rand::thread_rng().gen::<f64>())
    }

    /// `conv(num, from_base, to_base)`: radix conversion (2-36) rendering a
    /// lowercase string. `num` may be an integer or its string form.
    pub(crate) fn func_conv(args: &[Value]) -> Value {
        if args.len() != 3 {
            return Value::Null;
        }
        if args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        let (Some(from_base), Some(to_base)) =
            (Self::to_i64_arg(&args[1]), Self::to_i64_arg(&args[2]))
        else {
            return Value::Null;
        };
        if !(2..=36).contains(&from_base) || !(2..=36).contains(&to_base) {
            return Value::Null;
        }
        let digits = match &args[0] {
            Value::String(s) => s.trim().to_string(),
            Value::Number(_) => match Self::to_i64_arg(&args[0]) {
                Some(n) => n.to_string(),
                None => return Value::Null,
            },
            _ => return Value::Null,
        };
        let value = match Self::parse_radix(&digits, from_base as u32) {
            Some(n) => n,
            None => return Value::Null,
        };
        Value::String(Self::format_radix(value, to_base as u32))
    }

    fn parse_radix(text: &str, base: u32) -> Option<i64> {
        let text = text.trim();
        let (negative, digits) = match text.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, text.strip_prefix('+').unwrap_or(text)),
        };
        if digits.is_empty() {
            return None;
        }
        let mut acc: i64 = 0;
        for c in digits.chars() {
            let d = c.to_digit(base)? as i64;
            acc = acc.checked_mul(base as i64)?.checked_add(d)?;
        }
        Some(if negative { acc.checked_neg()? } else { acc })
    }

    fn format_radix(value: i64, base: u32) -> String {
        if value == 0 {
            return "0".to_string();
        }
        let negative = value < 0;
        // Work in unsigned space so i64::MIN converts losslessly.
        let mut n = if negative {
            (value as u64).wrapping_neg()
        } else {
            value as u64
        };
        let mut out = Vec::new();
        while n > 0 {
            let d = (n % base as u64) as u32;
            out.push(char::from_digit(d, base).unwrap_or('?'));
            n /= base as u64;
        }
        if negative {
            out.push('-');
        }
        out.iter().rev().collect()
    }

    pub(crate) fn func_sign(args: &[Value]) -> Value {
        if args.len() != 1 || args[0].is_null() {
            return Value::Null;
        }
        if let Some(i) = args[0].as_i64() {
            return Value::from(i.signum());
        }
        if let Some(u) = args[0].as_u64() {
            // u64 values are non-negative; zero maps to 0.
            return Value::from(if u == 0 { 0 } else { 1 });
        }
        match Self::to_f64(&args[0]) {
            Some(f) if f > 0.0 => Value::from(1),
            Some(f) if f < 0.0 => Value::from(-1),
            Some(_) => Value::from(0),
            None => Value::Null,
        }
    }

    pub(crate) fn func_mod_fallible(args: &[Value]) -> Result<Value, String> {
        if args.len() != 2 {
            return Ok(Value::Null);
        }
        if args[0].is_null() || args[1].is_null() {
            return Ok(Value::Null);
        }
        let f1 = match Self::to_f64(&args[0]) {
            Some(f) => f,
            None => {
                return Err(format!(
                    "call func mod error: cannot convert {} to float64",
                    Self::format_ekuiper_val_type(&args[0])
                ));
            }
        };
        let f2 = match Self::to_f64(&args[1]) {
            Some(f) => f,
            None => {
                return Err(format!(
                    "call func mod error: cannot convert {} to float64",
                    Self::format_ekuiper_val_type(&args[1])
                ));
            }
        };
        if f2 == 0.0 {
            return Err("call func mod error: divided by zero".to_string());
        }
        if args[0].is_i64() && args[1].is_i64() {
            let i1 = args[0].as_i64().unwrap();
            let i2 = args[1].as_i64().unwrap();
            if i2 == 0 {
                return Err("call func mod error: divided by zero".to_string());
            }
            return Ok(Value::from(i1 % i2));
        }
        Ok(serde_json::json!(f1 % f2))
    }

    pub(crate) fn func_mod(args: &[Value]) -> Value {
        Self::func_mod_fallible(args).unwrap_or(Value::Null)
    }

    // ---------- vector similarity & distance functions ----------

    fn extract_f64_vec(val: &Value) -> Option<Vec<f64>> {
        match val {
            Value::Array(arr) => {
                let mut res = Vec::with_capacity(arr.len());
                for item in arr {
                    if let Some(f) = item.as_f64() {
                        res.push(f);
                    } else if let Some(i) = item.as_i64() {
                        res.push(i as f64);
                    } else {
                        let s = item.as_str()?;
                        let f = s.trim().parse::<f64>().ok()?;
                        res.push(f);
                    }
                }
                Some(res)
            }
            Value::String(s) => {
                if let Ok(Value::Array(arr)) = serde_json::from_str::<Value>(s.trim()) {
                    Self::extract_f64_vec(&Value::Array(arr))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub(crate) fn func_cosine_similarity(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(v1), Some(v2)) = (
            Self::extract_f64_vec(&args[0]),
            Self::extract_f64_vec(&args[1]),
        ) else {
            return Value::Null;
        };
        if v1.is_empty() || v1.len() != v2.len() {
            return Value::Null;
        }

        let mut dot = 0.0f64;
        let mut norm1 = 0.0f64;
        let mut norm2 = 0.0f64;

        for (a, b) in v1.iter().zip(v2.iter()) {
            dot += a * b;
            norm1 += a * a;
            norm2 += b * b;
        }

        if norm1 <= 0.0 || norm2 <= 0.0 {
            return serde_json::json!(0.0);
        }

        let sim = (dot / (norm1.sqrt() * norm2.sqrt())).clamp(-1.0, 1.0);
        serde_json::json!(sim)
    }

    pub(crate) fn func_vector_l2(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(v1), Some(v2)) = (
            Self::extract_f64_vec(&args[0]),
            Self::extract_f64_vec(&args[1]),
        ) else {
            return Value::Null;
        };
        if v1.is_empty() || v1.len() != v2.len() {
            return Value::Null;
        }

        let sum_sq: f64 = v1
            .iter()
            .zip(v2.iter())
            .map(|(a, b)| (a - b) * (a - b))
            .sum();
        serde_json::json!(sum_sq.sqrt())
    }

    pub(crate) fn func_vector_dot(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(v1), Some(v2)) = (
            Self::extract_f64_vec(&args[0]),
            Self::extract_f64_vec(&args[1]),
        ) else {
            return Value::Null;
        };
        if v1.is_empty() || v1.len() != v2.len() {
            return Value::Null;
        }

        let dot: f64 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();
        serde_json::json!(dot)
    }

    pub(crate) fn func_vector_match(args: &[Value]) -> Value {
        if args.len() < 2 || args.len() > 3 {
            return Value::Null;
        }
        let Some(query_vec) = Self::extract_f64_vec(&args[0]) else {
            return Value::Null;
        };
        let Some(candidates) = args[1].as_array() else {
            return Value::Null;
        };
        let top_k = if args.len() == 3 {
            args[2].as_u64().unwrap_or(5) as usize
        } else {
            5
        };

        let mut scored: Vec<(f64, Value)> = Vec::new();

        for candidate in candidates {
            let cand_vec_opt = if let Some(obj) = candidate.as_object() {
                obj.get("embedding")
                    .or_else(|| obj.get("vector"))
                    .and_then(Self::extract_f64_vec)
            } else {
                Self::extract_f64_vec(candidate)
            };

            if let Some(cand_vec) = cand_vec_opt {
                if cand_vec.len() == query_vec.len() {
                    let mut dot = 0.0f64;
                    let mut norm1 = 0.0f64;
                    let mut norm2 = 0.0f64;
                    for (a, b) in query_vec.iter().zip(cand_vec.iter()) {
                        dot += a * b;
                        norm1 += a * a;
                        norm2 += b * b;
                    }
                    let sim = if norm1 > 0.0 && norm2 > 0.0 {
                        (dot / (norm1.sqrt() * norm2.sqrt())).clamp(-1.0, 1.0)
                    } else {
                        0.0
                    };
                    let mut item_obj = serde_json::Map::new();
                    item_obj.insert("similarity".to_string(), serde_json::json!(sim));
                    item_obj.insert("item".to_string(), candidate.clone());
                    scored.push((sim, Value::Object(item_obj)));
                }
            }
        }

        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        let results: Vec<Value> = scored.into_iter().take(top_k).map(|(_, v)| v).collect();
        Value::Array(results)
    }
}
