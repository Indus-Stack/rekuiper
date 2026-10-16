use super::Evaluator;
use base64::Engine as _;
use md5::Digest as _;
use serde_json::Value;

impl Evaluator {
    // ---------- conversion & utility ----------

    pub(crate) fn func_cast_fallible(args: &[Value]) -> Result<Value, String> {
        if args.len() != 2 {
            return Ok(Value::Null);
        }
        if args.iter().any(|v| v.is_null()) {
            return Ok(Value::Null);
        }
        let Some(target) = args[1].as_str() else {
            return Ok(Value::Null);
        };
        match target.trim().to_ascii_lowercase().as_str() {
            "bigint" | "int" => Self::cast_to_bigint_fallible(&args[0]),
            "float" | "double" => Self::cast_to_float_fallible(&args[0]),
            "string" => Ok(Self::cast_to_string(&args[0])),
            "boolean" | "bool" => Ok(Self::cast_to_bool(&args[0])),
            "datetime" | "timestamp" => {
                if let Some(ms) = Self::to_epoch_millis(&args[0]) {
                    if let Some(dt) = Self::datetime_from_millis(ms) {
                        let formatted = if dt.timestamp_subsec_millis() == 0 {
                            dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
                        } else {
                            dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
                        };
                        Ok(Value::String(formatted))
                    } else {
                        Ok(Value::Null)
                    }
                } else {
                    Ok(Value::Null)
                }
            }
            "bytea" => {
                let s = Self::to_string_always(&args[0]);
                if s.is_empty() {
                    Ok(Value::String(String::new()))
                } else {
                    Ok(Value::String(
                        base64::engine::general_purpose::STANDARD.encode(s.as_bytes()),
                    ))
                }
            }
            _ => Ok(Value::Null),
        }
    }

    pub(crate) fn func_cast(args: &[Value]) -> Value {
        Self::func_cast_fallible(args).unwrap_or(Value::Null)
    }

    fn cast_to_bigint_fallible(v: &Value) -> Result<Value, String> {
        if v.is_null() {
            return Ok(Value::Null);
        }
        if let Some(i) = v.as_i64() {
            return Ok(Value::from(i));
        }
        if let Some(u) = v.as_u64() {
            if u <= i64::MAX as u64 {
                return Ok(Value::from(u as i64));
            }
            return Ok(Value::Null);
        }
        if let Some(f) = v.as_f64() {
            if f.is_finite() {
                return Ok(Value::from(f.trunc() as i64));
            }
            return Ok(Value::Null);
        }
        if let Some(b) = v.as_bool() {
            return Ok(Value::from(if b { 1 } else { 0 }));
        }
        if let Some(s) = v.as_str() {
            let t = s.trim();
            if let Ok(i) = t.parse::<i64>() {
                return Ok(Value::from(i));
            }
        }
        Err(format!(
            "call func cast error: not supported type conversion, got error cannot convert {} to int",
            Self::format_ekuiper_val_type(v)
        ))
    }

    fn cast_to_float_fallible(v: &Value) -> Result<Value, String> {
        if v.is_null() {
            return Ok(Value::Null);
        }
        if let Some(f) = v.as_f64() {
            return Ok(serde_json::json!(f));
        }
        if let Some(b) = v.as_bool() {
            return Ok(serde_json::json!(if b { 1.0 } else { 0.0 }));
        }
        if let Some(s) = v.as_str() {
            if let Ok(f) = s.trim().parse::<f64>() {
                return Ok(serde_json::json!(f));
            }
        }
        Err(format!(
            "call func cast error: not supported type conversion, got error cannot convert {} to float64",
            Self::format_ekuiper_val_type(v)
        ))
    }

    fn cast_to_string(v: &Value) -> Value {
        if v.is_null() {
            return Value::Null;
        }
        match v {
            Value::String(s) => Value::String(s.clone()),
            Value::Number(n) => Value::String(n.to_string()),
            Value::Bool(b) => Value::String(b.to_string()),
            Value::Array(_) | Value::Object(_) => match serde_json::to_string(v) {
                Ok(s) => Value::String(s),
                Err(_) => Value::Null,
            },
            Value::Null => Value::Null,
        }
    }

    fn cast_to_bool(v: &Value) -> Value {
        if v.is_null() {
            return Value::Null;
        }
        if let Some(b) = v.as_bool() {
            return Value::Bool(b);
        }
        if v.is_number() {
            if let Some(f) = v.as_f64() {
                return Value::Bool(f != 0.0);
            }
            return Value::Null;
        }
        if let Some(s) = v.as_str() {
            // Mirror Go strconv.ParseBool: 1,t,T,TRUE,true,True,0,f,F,FALSE,false,False
            match s.trim().to_ascii_lowercase().as_str() {
                "1" | "t" | "true" => return Value::Bool(true),
                "0" | "f" | "false" => return Value::Bool(false),
                _ => return Value::Null,
            }
        }
        Value::Null
    }

    pub(crate) fn func_coalesce(args: &[Value]) -> Value {
        if args.is_empty() {
            return Value::Null;
        }
        for a in args {
            if !a.is_null() {
                return a.clone();
            }
        }
        Value::Null
    }

    // ---------- validation functions ----------

    pub(crate) fn func_isnan(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        Value::Bool(matches!(args[0].as_f64(), Some(f) if f.is_nan()))
    }

    pub(crate) fn func_isnumeric(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        Value::Bool(Self::to_f64(&args[0]).is_some())
    }

    // ---------- WebAssembly plugin invocation ----------

    pub(crate) fn func_wasm_run(args: &[Value]) -> Value {
        if args.len() < 2 {
            return Value::Null;
        }
        let Some(module_name) = args[0].as_str() else {
            return Value::Null;
        };
        let Some(func_name) = args[1].as_str() else {
            return Value::Null;
        };
        let func_args = &args[2..];
        rekuiper_core::get_global_wasm_registry()
            .call_module_func(module_name, func_name, func_args)
            .unwrap_or(Value::Null)
    }

    // ---------- crypto & encoding functions ----------

    fn hash_input(value: &Value) -> Option<Vec<u8>> {
        if value.is_null() {
            return None;
        }
        Some(Self::to_string_always(value).into_bytes())
    }

    pub(crate) fn func_md5(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        match Self::hash_input(&args[0]) {
            Some(bytes) => Value::String(format!("{:x}", md5::Md5::digest(bytes))),
            None => Value::Null,
        }
    }

    pub(crate) fn func_sha256(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        match Self::hash_input(&args[0]) {
            Some(bytes) => Value::String(format!("{:x}", sha2::Sha256::digest(bytes))),
            None => Value::Null,
        }
    }

    pub(crate) fn func_sha512(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        match Self::hash_input(&args[0]) {
            Some(bytes) => Value::String(format!("{:x}", sha2::Sha512::digest(bytes))),
            None => Value::Null,
        }
    }

    pub(crate) fn func_sha1(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        match Self::hash_input(&args[0]) {
            Some(bytes) => Value::String(format!("{:x}", sha1::Sha1::digest(bytes))),
            None => Value::Null,
        }
    }

    pub(crate) fn func_sha384(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        match Self::hash_input(&args[0]) {
            Some(bytes) => Value::String(format!("{:x}", sha2::Sha384::digest(bytes))),
            None => Value::Null,
        }
    }

    pub(crate) fn func_crc32(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        let s = Self::to_string_always(&args[0]);
        if s.is_empty() {
            return Value::String("0".to_string());
        }
        let hash = crc32fast::hash(s.as_bytes());
        Value::String(format!("{:x}", hash))
    }

    pub(crate) fn func_regexp_matches(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        if args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        let (Some(text), Some(pattern)) = (args[0].as_str(), args[1].as_str()) else {
            return Value::Null;
        };
        match regex::Regex::new(pattern) {
            Ok(re) => Value::Bool(re.is_match(text)),
            Err(_) => Value::Null,
        }
    }

    pub(crate) fn func_regexp_replace(args: &[Value]) -> Value {
        if args.len() != 3 {
            return Value::Null;
        }
        if args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        let (Some(text), Some(pattern), Some(repl)) =
            (args[0].as_str(), args[1].as_str(), args[2].as_str())
        else {
            return Value::Null;
        };
        match regex::Regex::new(pattern) {
            Ok(re) => Value::String(re.replace_all(text, repl).into_owned()),
            Err(_) => Value::Null,
        }
    }

    pub(crate) fn func_regexp_substring(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        if args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        let (Some(text), Some(pattern)) = (args[0].as_str(), args[1].as_str()) else {
            return Value::Null;
        };
        let Ok(re) = regex::Regex::new(pattern) else {
            return Value::Null;
        };
        let Some(caps) = re.captures(text) else {
            return Value::Null;
        };
        match caps.get(1).or_else(|| caps.get(0)) {
            Some(m) => Value::String(m.as_str().to_string()),
            None => Value::Null,
        }
    }

    pub(crate) fn func_split_value_fallible(args: &[Value]) -> Result<Value, String> {
        if args.len() != 3 {
            return Ok(Value::Null);
        }
        if args.iter().any(|v| v.is_null()) {
            return Ok(Value::Null);
        }
        let text = Self::to_string_always(&args[0]);
        let sep = Self::to_string_always(&args[1]);
        let Some(index) = Self::to_i64_arg(&args[2]) else {
            return Ok(Value::Null);
        };
        let parts: Vec<&str> = text.split(&sep).collect();
        let len = parts.len() as i64;
        if index > len - 1 || index < -len {
            return Err(format!(
                "call func split_value error: {} out of index array (size = {})",
                index, len
            ));
        }
        let actual_idx = if index >= 0 {
            index as usize
        } else {
            (len + index) as usize
        };
        Ok(Value::String(parts[actual_idx].to_string()))
    }

    /// 0-based split: index counts from the leading (possibly empty) segment.
    pub(crate) fn func_split_value(args: &[Value]) -> Value {
        Self::func_split_value_fallible(args).unwrap_or(Value::Null)
    }

    pub(crate) fn func_numbytes(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        match &args[0] {
            Value::String(s) => Value::from(s.len() as i64),
            _ => Value::Null,
        }
    }

    pub(crate) fn func_chr(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        let Some(code) = Self::to_i64_arg(&args[0]) else {
            return Value::Null;
        };
        if !(0..=u32::MAX as i64).contains(&code) {
            return Value::Null;
        }
        match char::from_u32(code as u32) {
            Some(c) => Value::String(c.to_string()),
            None => Value::Null,
        }
    }

    /// Truncate toward zero at `decimals` places (clamped to [0, 34]).
    /// Whole results come back as integers.
    pub(crate) fn func_trunc(args: &[Value]) -> Value {
        if args.is_empty() || args.len() > 2 {
            return Value::Null;
        }
        if args.iter().any(|v| v.is_null()) {
            return Value::Null;
        }
        let Some(num) = Self::to_f64(&args[0]) else {
            return Value::Null;
        };
        if !num.is_finite() {
            return Value::Null;
        }
        let decimals = if args.len() == 2 {
            let Some(d) = Self::to_i64_arg(&args[1]) else {
                return Value::Null;
            };
            d.clamp(0, 34)
        } else {
            0
        };
        let factor = 10f64.powi(decimals as i32);
        let truncated = (num * factor).trunc() / factor;
        // Whole results as integers when exactly representable.
        if decimals == 0
            && (-9_007_199_254_740_992.0..=9_007_199_254_740_992.0).contains(&truncated)
        {
            return Value::from(truncated as i64);
        }
        match serde_json::Number::from_f64(truncated) {
            Some(n) => Value::Number(n),
            None => Value::Null,
        }
    }

    pub(crate) fn func_hex2dec(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        let Some(s) = args[0].as_str() else {
            return Value::Null;
        };
        let trimmed = s.trim();
        let hex = trimmed
            .strip_prefix("0x")
            .or_else(|| trimmed.strip_prefix("0X"))
            .unwrap_or(trimmed);
        match i64::from_str_radix(hex, 16) {
            Ok(n) => Value::from(n),
            Err(_) => Value::Null,
        }
    }

    pub(crate) fn func_dec2hex(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        let Some(n) = Self::to_i64_arg(&args[0]) else {
            return Value::Null;
        };
        if n >= 0 {
            Value::String(format!("0x{:x}", n))
        } else {
            Value::String(format!("-0x{:x}", n.unsigned_abs()))
        }
    }

    pub(crate) fn func_encode(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let Some(method) = args[1].as_str() else {
            return Value::Null;
        };
        if args[0].is_null() {
            return Value::Null;
        }
        match method.trim().to_ascii_lowercase().as_str() {
            "base64" => Value::String(
                base64::engine::general_purpose::STANDARD.encode(Self::to_string_always(&args[0])),
            ),
            _ => Value::Null,
        }
    }

    pub(crate) fn func_base64_encode(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        Value::String(
            base64::engine::general_purpose::STANDARD.encode(Self::to_string_always(&args[0])),
        )
    }

    pub(crate) fn func_decode(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(text), Some(method)) = (args[0].as_str(), args[1].as_str()) else {
            return Value::Null;
        };
        match method.trim().to_ascii_lowercase().as_str() {
            "base64" => match base64::engine::general_purpose::STANDARD.decode(text.trim()) {
                Ok(bytes) => match String::from_utf8(bytes) {
                    Ok(s) => Value::String(s),
                    Err(_) => Value::Null,
                },
                Err(_) => Value::Null,
            },
            _ => Value::Null,
        }
    }

    pub(crate) fn func_base64_decode(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(text) = args[0].as_str() else {
            return Value::Null;
        };
        match base64::engine::general_purpose::STANDARD.decode(text.trim()) {
            Ok(bytes) => match String::from_utf8(bytes) {
                Ok(s) => Value::String(s),
                Err(_) => Value::Null,
            },
            Err(_) => Value::Null,
        }
    }

    pub(crate) fn func_compress(args: &[Value]) -> Value {
        use std::io::Write;
        if args.len() != 2 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        let Some(method) = args[1].as_str() else {
            return Value::Null;
        };
        let input_bytes = match &args[0] {
            Value::String(s) => s.as_bytes().to_vec(),
            other => Self::to_string_always(other).into_bytes(),
        };
        let compressed = match method.trim().to_ascii_lowercase().as_str() {
            "zlib" => {
                let mut encoder =
                    flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
                if encoder.write_all(&input_bytes).is_err() {
                    return Value::Null;
                }
                encoder.finish().ok()
            }
            "gzip" => {
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                if encoder.write_all(&input_bytes).is_err() {
                    return Value::Null;
                }
                encoder.finish().ok()
            }
            "flate" | "deflate" => {
                let mut encoder =
                    flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
                if encoder.write_all(&input_bytes).is_err() {
                    return Value::Null;
                }
                encoder.finish().ok()
            }
            "zstd" => zstd::encode_all(&input_bytes[..], 0).ok(),
            _ => return Value::Null,
        };
        match compressed {
            Some(bytes) => Value::String(base64::engine::general_purpose::STANDARD.encode(bytes)),
            None => Value::Null,
        }
    }

    pub(crate) fn func_decompress(args: &[Value]) -> Value {
        use std::io::Read;
        if args.len() != 2 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        let Some(method) = args[1].as_str() else {
            return Value::Null;
        };
        let compressed_bytes = match &args[0] {
            Value::String(s) => base64::engine::general_purpose::STANDARD
                .decode(s.trim())
                .unwrap_or_else(|_| s.as_bytes().to_vec()),
            other => Self::to_string_always(other).into_bytes(),
        };
        let decompressed = match method.trim().to_ascii_lowercase().as_str() {
            "zlib" => {
                let mut decoder = flate2::read::ZlibDecoder::new(&compressed_bytes[..]);
                let mut out = Vec::new();
                if decoder.read_to_end(&mut out).is_err() {
                    return Value::Null;
                }
                Some(out)
            }
            "gzip" => {
                let mut decoder = flate2::read::GzDecoder::new(&compressed_bytes[..]);
                let mut out = Vec::new();
                if decoder.read_to_end(&mut out).is_err() {
                    return Value::Null;
                }
                Some(out)
            }
            "flate" | "deflate" => {
                let mut decoder = flate2::read::DeflateDecoder::new(&compressed_bytes[..]);
                let mut out = Vec::new();
                if decoder.read_to_end(&mut out).is_err() {
                    return Value::Null;
                }
                Some(out)
            }
            "zstd" => zstd::decode_all(&compressed_bytes[..]).ok(),
            _ => return Value::Null,
        };
        match decompressed {
            Some(bytes) => match String::from_utf8(bytes) {
                Ok(s) => Value::String(s),
                Err(e) => {
                    Value::String(base64::engine::general_purpose::STANDARD.encode(e.into_bytes()))
                }
            },
            None => Value::Null,
        }
    }

    pub(crate) fn func_delay(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        if let Some(ms) = args[0].as_i64() {
            if ms > 0 {
                let sleep_ms = ms.min(10_000) as u64;
                let captured = super::ASYNC_DELAY_ACCUMULATOR.with(|c| {
                    if let Some(acc) = c.get() {
                        c.set(Some(acc.saturating_add(sleep_ms)));
                        true
                    } else {
                        false
                    }
                });
                if !captured {
                    std::thread::sleep(std::time::Duration::from_millis(sleep_ms));
                }
            }
        }
        args[1].clone()
    }

    pub(crate) fn func_extract(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        args[0].clone()
    }

    pub(crate) fn func_unnest_scalar(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        args[0].clone()
    }

    pub(crate) fn func_changed_cols_scalar(args: &[Value]) -> Value {
        if args.len() < 3 {
            return Value::Null;
        }
        args[2].clone()
    }
}
