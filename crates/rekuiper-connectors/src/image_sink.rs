use crate::data_template::apply_data_template_value;
use base64::Engine;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};
use uuid::Uuid;

/// Image sink specific errors separating input validation errors from disk IO errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImageSinkError {
    Validation(String),
    Io(String),
}

impl std::fmt::Display for ImageSinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(msg) => write!(f, "image sink validation error: {}", msg),
            Self::Io(msg) => write!(f, "image sink io error: {}", msg),
        }
    }
}

impl std::error::Error for ImageSinkError {}

#[derive(Debug, Clone)]
struct PruneDirectoryState {
    format: String,
    max_age_secs: u64,
    max_count: usize,
    last_write: std::time::Instant,
}

static ACTIVE_PRUNE_DIRS: OnceLock<Mutex<HashMap<PathBuf, PruneDirectoryState>>> = OnceLock::new();

fn ensure_prune_timer(dir: PathBuf, format: String, max_age_secs: u64, max_count: usize) {
    let registry = ACTIVE_PRUNE_DIRS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut dirs = registry.lock();
    if let Some(state) = dirs.get_mut(&dir) {
        state.format = format;
        state.max_age_secs = max_age_secs;
        state.max_count = max_count;
        state.last_write = std::time::Instant::now();
        return;
    }

    dirs.insert(
        dir.clone(),
        PruneDirectoryState {
            format,
            max_age_secs,
            max_count,
            last_write: std::time::Instant::now(),
        },
    );

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(15));
        interval.tick().await;
        loop {
            interval.tick().await;

            let settings = if let Some(reg) = ACTIVE_PRUNE_DIRS.get() {
                reg.lock().get(&dir).cloned()
            } else {
                None
            };

            let Some(state) = settings else {
                break;
            };

            if !dir.exists() {
                if let Some(reg) = ACTIVE_PRUNE_DIRS.get() {
                    reg.lock().remove(&dir);
                }
                break;
            }

            let _ = prune_directory(&dir, &state.format, state.max_age_secs, state.max_count).await;

            // Auto-terminate timer when idle: if no writes occurred for idle timeout (e.g. 5 minutes or max_age_secs),
            // de-register task so deleted rules and device templates do not leak persistent tasks.
            let idle_threshold = Duration::from_secs(state.max_age_secs.max(300));
            if state.last_write.elapsed() >= idle_threshold {
                if let Some(reg) = ACTIVE_PRUNE_DIRS.get() {
                    let mut dirs = reg.lock();
                    if let Some(curr) = dirs.get(&dir) {
                        if curr.last_write.elapsed() >= idle_threshold {
                            dirs.remove(&dir);
                            break;
                        }
                    }
                }
            }
        }
    });
}

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
    pub fn extract_bytes(
        &self,
        record: &HashMap<String, Value>,
    ) -> Result<Vec<u8>, ImageSinkError> {
        let field_val = if let Some(ref field) = self.image_field {
            record.get(field).ok_or_else(|| {
                ImageSinkError::Validation(format!("image field '{}' not found in record", field))
            })?
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
                    return Err(ImageSinkError::Validation(
                        "no image binary field found in record".to_string(),
                    ));
                }
            }
        };

        match field_val {
            Value::String(s) => {
                let trimmed = s.trim();
                base64::engine::general_purpose::STANDARD
                    .decode(trimmed)
                    .map_err(|e| {
                        ImageSinkError::Validation(format!(
                            "image string is not valid base64: {}",
                            e
                        ))
                    })
            }
            Value::Array(arr) => {
                let mut bytes = Vec::with_capacity(arr.len());
                for v in arr {
                    match v {
                        Value::Number(n) => {
                            let u = n.as_u64().ok_or_else(|| {
                                ImageSinkError::Validation("non-integer byte in array".to_string())
                            })?;
                            if u > 255 {
                                return Err(ImageSinkError::Validation(format!(
                                    "byte value {} exceeds 255",
                                    u
                                )));
                            }
                            bytes.push(u as u8);
                        }
                        _ => {
                            return Err(ImageSinkError::Validation(
                                "non-numeric byte in array".to_string(),
                            ))
                        }
                    }
                }
                Ok(bytes)
            }
            other => Err(ImageSinkError::Validation(format!(
                "unsupported image value type: {:?}",
                other
            ))),
        }
    }

    /// Resolves target directory path and file name for the record, validating against path traversal.
    pub fn resolve_file_path(
        &self,
        record: &HashMap<String, Value>,
    ) -> Result<(PathBuf, PathBuf), ImageSinkError> {
        let record_val =
            Value::Object(record.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
        let dir_str = if self.path.contains("{{") {
            apply_data_template_value(&self.path, &record_val)
        } else {
            self.path.clone()
        };
        let dir_trimmed = dir_str.trim();
        if dir_trimmed.is_empty() {
            return Err(ImageSinkError::Validation(
                "target directory path resolved to empty".to_string(),
            ));
        }
        let dir_path = PathBuf::from(dir_trimmed);
        if dir_trimmed.contains("..")
            || dir_path
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(ImageSinkError::Validation(format!(
                "invalid image directory path '{}': directory traversal '..' is prohibited",
                dir_trimmed
            )));
        }

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
        let file_trimmed = file_str.trim();
        if file_trimmed.is_empty() {
            return Err(ImageSinkError::Validation(
                "image file name resolved to empty".to_string(),
            ));
        }
        if file_trimmed.contains('/')
            || file_trimmed.contains('\\')
            || file_trimmed.contains(':')
            || file_trimmed.contains("..")
        {
            return Err(ImageSinkError::Validation(format!(
                "invalid image file name '{}': path separators, colons, and '..' are prohibited",
                file_trimmed
            )));
        }

        let file_path_obj = Path::new(file_trimmed);
        let components: Vec<_> = file_path_obj.components().collect();
        if components.len() != 1 || !matches!(components[0], std::path::Component::Normal(_)) {
            return Err(ImageSinkError::Validation(format!(
                "invalid image file name '{}': must be a single normal file component",
                file_trimmed
            )));
        }

        let stem = file_path_obj
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or(file_trimmed);
        if is_windows_reserved_device_name(stem) {
            return Err(ImageSinkError::Validation(format!(
                "invalid image file name '{}': Windows reserved device names are prohibited",
                file_trimmed
            )));
        }

        let file_path = dir_path.join(file_trimmed);
        Ok((dir_path, file_path))
    }

    /// Writes image bytes to disk and applies retention pruning policies via per-directory timers.
    pub async fn save_image(
        &self,
        record: &HashMap<String, Value>,
    ) -> Result<PathBuf, ImageSinkError> {
        let bytes = self.extract_bytes(record)?;
        let (dir_path, file_path) = self.resolve_file_path(record)?;

        tokio::fs::create_dir_all(&dir_path).await.map_err(|e| {
            ImageSinkError::Io(format!(
                "failed to create image directory {:?}: {}",
                dir_path, e
            ))
        })?;

        tokio::fs::write(&file_path, &bytes).await.map_err(|e| {
            ImageSinkError::Io(format!("failed to write image {:?}: {}", file_path, e))
        })?;

        if self.max_age > 0 || self.max_count > 0 {
            ensure_prune_timer(
                dir_path.clone(),
                self.format.clone(),
                self.max_age * 3600,
                self.max_count,
            );
        }

        Ok(file_path)
    }
}

fn is_windows_reserved_device_name(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    matches!(
        upper.as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
}

pub(crate) async fn prune_directory(
    dir: &Path,
    format: &str,
    max_age_secs: u64,
    max_count: usize,
) -> std::io::Result<()> {
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(e) => e,
        Err(_) => return Ok(()),
    };
    let mut files = Vec::new();
    let now = SystemTime::now();

    let fmt_lower = format.to_lowercase();
    let allowed_exts: &[&str] = match fmt_lower.as_str() {
        "png" => &["png"],
        "jpg" | "jpeg" => &["jpg", "jpeg"],
        other => &[other],
    };

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.is_file() {
            let matches_ext = path
                .extension()
                .and_then(|e| e.to_str())
                .is_some_and(|e| allowed_exts.contains(&e.to_lowercase().as_str()));
            if !matches_ext {
                continue;
            }
            if let Ok(meta) = entry.metadata().await {
                let mtime = meta.modified().unwrap_or(now);
                files.push((path, mtime));
            }
        }
    }

    // 1. Prune expired by age only when max_age_secs > 0 (0 means unlimited retention)
    let mut remaining = Vec::new();
    if max_age_secs > 0 {
        let max_age_dur = Duration::from_secs(max_age_secs);
        for (path, mtime) in files {
            if let Ok(age) = now.duration_since(mtime) {
                if age > max_age_dur {
                    let _ = tokio::fs::remove_file(&path).await;
                    continue;
                }
            }
            remaining.push((path, mtime));
        }
    } else {
        remaining = files;
    }

    // 2. Prune excess by count (oldest first) only when max_count > 0 (0 means unlimited count)
    if max_count > 0 && remaining.len() > max_count {
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

        let (dir, file) = cfg.resolve_file_path(&record).unwrap();
        assert_eq!(dir, PathBuf::from("/tmp/camera/cam01"));
        assert_eq!(file, PathBuf::from("/tmp/camera/cam01/frame_cam01.png"));
    }

    #[test]
    fn test_image_path_traversal_rejected() {
        let mut r1 = HashMap::new();
        r1.insert("file".to_string(), json!("../../etc/shadow"));
        let cfg1 = ImageSinkConfig {
            path: "/tmp/images".to_string(),
            format: "png".to_string(),
            image_field: None,
            file_name: Some("{{.file}}".to_string()),
            max_age: 0,
            max_count: 0,
        };
        assert!(cfg1.resolve_file_path(&r1).is_err());

        let mut r2 = HashMap::new();
        r2.insert("dir".to_string(), json!("../escaped"));
        let cfg2 = ImageSinkConfig {
            path: "/tmp/camera/{{.dir}}".to_string(),
            format: "png".to_string(),
            image_field: None,
            file_name: None,
            max_age: 0,
            max_count: 0,
        };
        assert!(cfg2.resolve_file_path(&r2).is_err());
    }

    #[test]
    fn test_image_strict_byte_validation() {
        let cfg = ImageSinkConfig {
            path: "/tmp".to_string(),
            format: "jpeg".to_string(),
            image_field: None,
            file_name: None,
            max_age: 0,
            max_count: 0,
        };

        // 1. Non-base64 string rejected
        let mut r_invalid_str = HashMap::new();
        r_invalid_str.insert("image".to_string(), json!("not_valid_base64_!@#$"));
        assert!(cfg.extract_bytes(&r_invalid_str).is_err());

        // 2. Out of range byte array (> 255) rejected
        let mut r_invalid_arr = HashMap::new();
        r_invalid_arr.insert("image".to_string(), json!([10, 20, 300]));
        assert!(cfg.extract_bytes(&r_invalid_arr).is_err());

        // 3. Valid byte array accepted
        let mut r_valid_arr = HashMap::new();
        r_valid_arr.insert("image".to_string(), json!([65, 66, 67]));
        assert_eq!(cfg.extract_bytes(&r_valid_arr).unwrap(), vec![65, 66, 67]);
    }

    #[tokio::test]
    async fn test_image_sink_save_image_and_disk_write() {
        let temp_dir = std::env::temp_dir().join(format!("rekuiper_img_test_{}", Uuid::new_v4()));
        let cfg = ImageSinkConfig {
            path: temp_dir.to_str().unwrap().to_string(),
            format: "png".to_string(),
            image_field: None,
            file_name: Some("test_snapshot.png".to_string()),
            max_age: 0,
            max_count: 0,
        };

        let mut record = HashMap::new();
        record.insert("image".to_string(), json!("aGVsbG8gd29ybGQ=")); // "hello world"

        let saved_path = cfg
            .save_image(&record)
            .await
            .expect("save_image should succeed");
        assert!(saved_path.exists(), "File should be created on disk");

        let written_bytes = tokio::fs::read(&saved_path)
            .await
            .expect("read written image");
        assert_eq!(written_bytes, b"hello world");

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_image_sink_scoped_pruning() {
        let temp_dir = std::env::temp_dir().join(format!("rekuiper_prune_test_{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        // Write an unrelated file in the directory
        let unrelated_file = temp_dir.join("important_config.conf");
        tokio::fs::write(&unrelated_file, b"secret_data")
            .await
            .unwrap();

        // Create 5 image files with sequential timestamps
        let mut file_paths = Vec::new();
        for i in 1..=5 {
            let p = temp_dir.join(format!("frame_{}.jpg", i));
            tokio::fs::write(&p, format!("data_{}", i).as_bytes())
                .await
                .unwrap();
            file_paths.push(p);
            tokio::time::sleep(tokio::time::Duration::from_millis(15)).await;
        }

        // Prune down to max_count = 3 (oldest 2 images should be deleted, unrelated file untouched)
        prune_directory(&temp_dir, "jpg", 0, 3).await.unwrap();

        // Unrelated file must still exist!
        assert!(unrelated_file.exists(), "Non-image file must not be pruned");

        // frame_1 and frame_2 should have been pruned
        assert!(!file_paths[0].exists(), "frame_1 should be deleted");
        assert!(!file_paths[1].exists(), "frame_2 should be deleted");

        // frame_3, frame_4, and frame_5 should remain
        assert!(file_paths[2].exists(), "frame_3 should remain");
        assert!(file_paths[3].exists(), "frame_4 should remain");
        assert!(file_paths[4].exists(), "frame_5 should remain");

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_image_sink_pruning_max_age() {
        let temp_dir = std::env::temp_dir().join(format!("rekuiper_age_test_{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        let old_file = temp_dir.join("old.png");
        tokio::fs::write(&old_file, b"old_data").await.unwrap();

        // Wait 1.1s so file is > 1s old
        tokio::time::sleep(tokio::time::Duration::from_millis(1100)).await;

        let new_file = temp_dir.join("new.png");
        tokio::fs::write(&new_file, b"new_data").await.unwrap();

        // Prune with max_age_secs = 1
        prune_directory(&temp_dir, "png", 1, 0).await.unwrap();

        assert!(!old_file.exists(), "Old file should be pruned");
        assert!(new_file.exists(), "New file should be kept");

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[test]
    fn test_image_windows_file_names_and_colons_rejected() {
        let hostile_cases = vec![
            "C:x.png",
            "D:foo.jpg",
            "file.png:stream",
            "aux.png",
            "AUX.PNG",
            "con.jpg",
            "prn.png",
            "nul.jpeg",
            "com1.png",
            "lpt1.png",
            "/absolute.png",
            "\\absolute.png",
            "../escape.png",
        ];

        for hostile in hostile_cases {
            let mut record = HashMap::new();
            record.insert("file".to_string(), json!(hostile));
            let custom_cfg = ImageSinkConfig {
                path: "/tmp/images".to_string(),
                format: "png".to_string(),
                image_field: None,
                file_name: Some("{{.file}}".to_string()),
                max_age: 0,
                max_count: 0,
            };
            assert!(
                custom_cfg.resolve_file_path(&record).is_err(),
                "Hostile filename '{}' should be rejected",
                hostile
            );
        }

        // Valid file name passes
        let mut valid_rec = HashMap::new();
        valid_rec.insert("file".to_string(), json!("device_01_frame.png"));
        let valid_cfg = ImageSinkConfig {
            path: "/tmp/images".to_string(),
            format: "png".to_string(),
            image_field: None,
            file_name: Some("{{.file}}".to_string()),
            max_age: 0,
            max_count: 0,
        };
        assert!(valid_cfg.resolve_file_path(&valid_rec).is_ok());
    }

    #[tokio::test]
    async fn test_dynamic_prune_timer_settings_update() {
        let test_dir = std::env::temp_dir().join(format!("rekuiper_dyn_prune_{}", Uuid::new_v4()));

        // Initial settings
        ensure_prune_timer(test_dir.clone(), "png".to_string(), 7200, 100);
        {
            let reg = ACTIVE_PRUNE_DIRS.get().unwrap();
            let dirs = reg.lock();
            let state = dirs.get(&test_dir).expect("entry must exist");
            assert_eq!(state.format, "png");
            assert_eq!(state.max_age_secs, 7200);
            assert_eq!(state.max_count, 100);
        }

        // Update settings dynamically
        ensure_prune_timer(test_dir.clone(), "jpeg".to_string(), 3600, 50);
        {
            let reg = ACTIVE_PRUNE_DIRS.get().unwrap();
            let dirs = reg.lock();
            let state = dirs.get(&test_dir).expect("entry must exist");
            assert_eq!(state.format, "jpeg");
            assert_eq!(state.max_age_secs, 3600);
            assert_eq!(state.max_count, 50);
        }
    }
}
