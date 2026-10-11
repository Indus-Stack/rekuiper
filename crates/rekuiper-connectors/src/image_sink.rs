use crate::data_template::apply_data_template_value;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use uuid::Uuid;

/// Image sink configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageSinkConfig {
    /// Target directory path for saved images (supports data template e.g. `"/tmp/images/{{.device}}"`).
    pub path: String,
    /// Image format: `"jpeg"`, `"jpg"`, or `"png"`.
    #[serde(default = "default_format")]
    pub format: String,
    /// Field name containing the raw image binary bytes or base64 string.
    #[serde(default)]
    pub image_field: Option<String>,
    /// Optional file name template (e.g. `"{{.deviceId}}_{{.ts}}.png"`). Defaults to `"<uuid>.<format>"`.
    #[serde(default)]
    pub file_name: Option<String>,
    /// Maximum retention time in hours (default 72).
    #[serde(default = "default_max_age")]
    pub max_age: u64,
    /// Maximum number of stored image files (default 1000).
    #[serde(default = "default_max_count")]
    pub max_count: usize,
}

fn default_format() -> String {
    "jpeg".to_string()
}
fn default_max_age() -> u64 {
    72
}
fn default_max_count() -> usize {
    1000
}

impl ImageSinkConfig {
    /// Extracts image byte payload from the record.
    pub fn extract_bytes(&self, record: &HashMap<String, Value>) -> Result<Vec<u8>, String> {
        let field_val = if let Some(ref field) = self.image_field {
            record.get(field).ok_or_else(|| format!("image field '{}' not found in record", field))?
        } else {
            // Auto-detect common field names
            let candidates = ["image", "image_bytes", "data", "payload"];
            let mut found = None;
            for c in candidates {
                if let Some(v) = record.get(c) {
                    found = Some(v);
                    break;
                }
            }
            match found {
                Some(v) => v,
                None => {
                    // If no candidate, try serialization
                    return Err("no image binary field found in record".to_string());
                }
            }
        };

        match field_val {
            Value::String(s) => {
                // Try base64 decoding first
                if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(s.trim()) {
                    Ok(bytes)
                } else {
                    Ok(s.as_bytes().to_vec())
                }
            }
            Value::Array(arr) => {
                let bytes: Result<Vec<u8>, String> = arr
                    .iter()
                    .map(|v| match v {
                        Value::Number(n) => n
                            .as_u64()
                            .map(|u| u as u8)
                            .ok_or_else(|| "non-byte number in array".to_string()),
                        _ => Err("non-numeric byte in array".to_string()),
                    })
                    .collect();
                bytes
            }
            other => Err(format!("unsupported image value type: {:?}", other)),
        }
    }

    /// Resolves target directory path and file name for the record.
    pub fn resolve_file_path(&self, record: &HashMap<String, Value>) -> (PathBuf, PathBuf) {
        let record_val = Value::Object(record.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
        let dir_str = if self.path.contains("{{") {
            apply_data_template_value(&self.path, &record_val)
        } else {
            self.path.clone()
        };
        let dir_path = PathBuf::from(dir_str);

        let fmt_lower = self.format.to_lowercase();
        let ext = match fmt_lower.as_str() {
            "png" => "png",
            "jpg" | "jpeg" => "jpeg",
            _ => fmt_lower.as_str(),
        };

        let file_str = if let Some(ref fname_tmpl) = self.file_name {
            if fname_tmpl.contains("{{") {
                apply_data_template_value(fname_tmpl, &record_val)
            } else {
                fname_tmpl.clone()
            }
        } else {
            format!("{}.{}", Uuid::new_v4(), ext)
        };

        let file_path = dir_path.join(file_str);
        (dir_path, file_path)
    }

    /// Writes image bytes to disk and applies pruning policies.
    pub async fn save_image(&self, record: &HashMap<String, Value>) -> Result<PathBuf, String> {
        let bytes = self.extract_bytes(record)?;
        let (dir_path, file_path) = self.resolve_file_path(record);

        tokio::fs::create_dir_all(&dir_path)
            .await
            .map_err(|e| format!("failed to create image directory {:?}: {}", dir_path, e))?;

        tokio::fs::write(&file_path, &bytes)
            .await
            .map_err(|e| format!("failed to write image {:?}: {}", file_path, e))?;

        // Asynchronously apply retention policies
        let max_age_secs = self.max_age * 3600;
        let max_count = self.max_count;
        let dir_clone = dir_path.clone();
        tokio::spawn(async move {
            let _ = prune_directory(&dir_clone, max_age_secs, max_count).await;
        });

        Ok(file_path)
    }
}

async fn prune_directory(dir: &Path, max_age_secs: u64, max_count: usize) -> std::io::Result<()> {
    let mut entries = tokio::fs::read_dir(dir).await?;
    let mut files = Vec::new();
    let now = SystemTime::now();

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.is_file() {
            if let Ok(meta) = entry.metadata().await {
                let mtime = meta.modified().unwrap_or(now);
                files.push((path, mtime));
            }
        }
    }

    // 1. Prune expired by age
    let max_age_dur = Duration::from_secs(max_age_secs);
    let mut remaining = Vec::new();
    for (path, mtime) in files {
        if let Ok(age) = now.duration_since(mtime) {
            if age > max_age_dur {
                let _ = tokio::fs::remove_file(&path).await;
                continue;
            }
        }
        remaining.push((path, mtime));
    }

    // 2. Prune excess by count (oldest first)
    if remaining.len() > max_count {
        remaining.sort_by_key(|(_, mtime)| *mtime);
        let to_remove = remaining.len() - max_count;
        for (path, _) in remaining.into_iter().take(to_remove) {
            let _ = tokio::fs::remove_file(path).await;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_image_extract_and_resolve() {
        let mut record = HashMap::new();
        record.insert("deviceId".to_string(), json!("cam01"));
        record.insert("image".to_string(), json!("aGVsbG8gd29ybGQ=")); // "hello world" in base64

        let cfg = ImageSinkConfig {
            path: "/tmp/camera/{{.deviceId}}".to_string(),
            format: "png".to_string(),
            image_field: None,
            file_name: Some("frame_{{.deviceId}}.png".to_string()),
            max_age: 24,
            max_count: 50,
        };

        let bytes = cfg.extract_bytes(&record).unwrap();
        assert_eq!(bytes, b"hello world");

        let (dir, file) = cfg.resolve_file_path(&record);
        assert_eq!(dir, PathBuf::from("/tmp/camera/cam01"));
        assert_eq!(file, PathBuf::from("/tmp/camera/cam01/frame_cam01.png"));
    }
}
