use rekuiper_core::KvStore;
use serde_json::Value;

/// Checks if a JSON key represents a sensitive credential field.
pub fn is_sensitive_key(key: &str) -> bool {
    let lower = key.to_lowercase();
    lower.contains("password")
        || lower.contains("token")
        || lower.contains("secret")
        || lower.contains("private_key")
        || lower.contains("privatekey")
        || lower.contains("api_key")
        || lower.contains("apikey")
        || lower.contains("client_secret")
        || lower.contains("access_key")
}

/// Recursively masks sensitive fields with "******" in place.
pub fn mask_secrets(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (k, v) in map.iter_mut() {
                if is_sensitive_key(k) {
                    if let Value::String(s) = v {
                        if !s.is_empty() {
                            *v = Value::String("******".to_string());
                        }
                    }
                } else {
                    mask_secrets(v);
                }
            }
        }
        Value::Array(arr) => {
            for v in arr.iter_mut() {
                mask_secrets(v);
            }
        }
        _ => {}
    }
}

/// When updating an entity, if the incoming payload contains the masked placeholder "******",
/// preserve the existing real secret from the stored entity.
pub fn preserve_masked_secrets(incoming: &mut Value, existing: &Value) {
    match (incoming, existing) {
        (Value::Object(in_map), Value::Object(ex_map)) => {
            for (k, in_val) in in_map.iter_mut() {
                if is_sensitive_key(k) {
                    if let Value::String(s) = in_val {
                        if s == "******" {
                            if let Some(real_val) = ex_map.get(k) {
                                *in_val = real_val.clone();
                            }
                        }
                    }
                } else if let Some(ex_sub) = ex_map.get(k) {
                    preserve_masked_secrets(in_val, ex_sub);
                }
            }
        }
        (Value::Array(in_arr), Value::Array(ex_arr)) => {
            for (idx, in_item) in in_arr.iter_mut().enumerate() {
                if let Some(ex_item) = ex_arr.get(idx) {
                    preserve_masked_secrets(in_item, ex_item);
                }
            }
        }
        _ => {}
    }
}

/// Resolves `${ENV:VAR}` and `${SECRET:key}` expressions in string values.
pub fn interpolate_string(input: &str, kv: Option<&dyn KvStore>) -> String {
    let re_env = regex::Regex::new(r"\$\{ENV:([a-zA-Z0-9_]+)\}").unwrap();
    let re_sec = regex::Regex::new(r"\$\{SECRET:([a-zA-Z0-9_\-.]+)\}").unwrap();

    let mut result = input.to_string();

    // 1. ${ENV:VAR}
    result = re_env
        .replace_all(&result, |caps: &regex::Captures| {
            let var_name = &caps[1];
            std::env::var(var_name).unwrap_or_default()
        })
        .to_string();

    // 2. ${SECRET:key}
    if let Some(kv_store) = kv {
        result = re_sec
            .replace_all(&result, |caps: &regex::Captures| {
                let sec_key = &caps[1];
                futures::executor::block_on(async {
                    if let Ok(Some(val)) = kv_store.get("secrets", sec_key).await {
                        val
                    } else if let Ok(Some(val)) = kv_store.get("connections", sec_key).await {
                        val
                    } else {
                        String::new()
                    }
                })
            })
            .to_string();
    }

    result
}

/// Recursively traverses a JSON value and interpolates environment variables and secrets.
pub fn interpolate_secrets(value: &mut Value, kv: Option<&dyn KvStore>) {
    match value {
        Value::String(s) => {
            if s.contains("${ENV:") || s.contains("${SECRET:") {
                *s = interpolate_string(s, kv);
            }
        }
        Value::Object(map) => {
            for v in map.values_mut() {
                interpolate_secrets(v, kv);
            }
        }
        Value::Array(arr) => {
            for v in arr.iter_mut() {
                interpolate_secrets(v, kv);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_secret_masking_and_preservation() {
        let original = json!({
            "name": "mqtt_prod",
            "server": "tcp://broker:1883",
            "password": "super_secret_password_123",
            "token": "bearer-abc-xyz",
            "nested": {
                "private_key": "MIIEvgIBADANBgkqhkiG9w0BAQEFAASC",
                "normal": "value"
            }
        });

        // 1. Mask secrets
        let mut masked = original.clone();
        mask_secrets(&mut masked);

        assert_eq!(masked["password"], "******");
        assert_eq!(masked["token"], "******");
        assert_eq!(masked["nested"]["private_key"], "******");
        assert_eq!(masked["nested"]["normal"], "value");
        assert_eq!(masked["server"], "tcp://broker:1883");

        // 2. Incoming update sends masked placeholder back
        let mut update_payload = json!({
            "name": "mqtt_prod",
            "server": "tcp://broker-new:1883",
            "password": "******", // unchanged
            "token": "new-token-456", // changed
            "nested": {
                "private_key": "******", // unchanged
                "normal": "updated_value"
            }
        });

        preserve_masked_secrets(&mut update_payload, &original);

        assert_eq!(update_payload["password"], "super_secret_password_123");
        assert_eq!(update_payload["token"], "new-token-456");
        assert_eq!(
            update_payload["nested"]["private_key"],
            "MIIEvgIBADANBgkqhkiG9w0BAQEFAASC"
        );
        assert_eq!(update_payload["nested"]["normal"], "updated_value");
        assert_eq!(update_payload["server"], "tcp://broker-new:1883");
    }

    #[test]
    fn test_env_interpolation() {
        std::env::set_var("TEST_DB_PASS", "my_secret_env_pass_999");
        let mut config = json!({
            "url": "http://127.0.0.1:8080",
            "password": "${ENV:TEST_DB_PASS}",
            "auth": "Bearer ${ENV:TEST_DB_PASS}"
        });

        interpolate_secrets(&mut config, None);

        assert_eq!(config["password"], "my_secret_env_pass_999");
        assert_eq!(config["auth"], "Bearer my_secret_env_pass_999");
    }
}
