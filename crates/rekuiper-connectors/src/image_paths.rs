use crate::data_template::apply_data_template_value;
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;

/// Errors returned when validating an image sink root and relative components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImagePathError {
    EmptyRoot,
    UnanchoredTemplate(String),
    TraversalDetected(String),
    DriveOrUncEscape(String),
    InvalidComponent(String),
}

impl std::fmt::Display for ImagePathError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyRoot => write!(f, "trusted image root path must not be empty"),
            Self::UnanchoredTemplate(p) => {
                write!(f, "unanchored template in image path '{p}' is prohibited")
            }
            Self::TraversalDetected(p) => {
                write!(f, "directory traversal '..' in '{p}' is prohibited")
            }
            Self::DriveOrUncEscape(p) => {
                write!(f, "drive or UNC path escape in '{p}' is prohibited")
            }
            Self::InvalidComponent(p) => {
                write!(f, "invalid image path component '{p}'")
            }
        }
    }
}

impl std::error::Error for ImagePathError {}

/// Resolved image target separated into a trusted root, validated relative
/// subdirectories, and a strict basename, produced without opening any files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedImagePath {
    pub trusted_root: PathBuf,
    pub relative_dirs: Vec<String>,
    pub file_name: String,
}

impl ValidatedImagePath {
    pub fn dir_path(&self) -> PathBuf {
        let mut p = self.trusted_root.clone();
        for seg in &self.relative_dirs {
            p.push(seg);
        }
        p
    }

    pub fn full_path(&self) -> PathBuf {
        self.dir_path().join(&self.file_name)
    }
}

fn has_drive_or_unc_prefix(raw: &str) -> bool {
    let bytes = raw.as_bytes();
    if raw.starts_with("\\\\") || raw.starts_with("//") {
        return true;
    }
    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

fn validate_static_root(root: &str) -> Result<PathBuf, ImagePathError> {
    let trimmed = root.trim();
    if trimmed.is_empty() {
        return Err(ImagePathError::EmptyRoot);
    }
    if trimmed.contains('\0') {
        return Err(ImagePathError::InvalidComponent(trimmed.to_owned()));
    }
    if trimmed.split(['/', '\\']).any(|seg| seg == "..") {
        return Err(ImagePathError::TraversalDetected(trimmed.to_owned()));
    }
    Ok(PathBuf::from(trimmed))
}

/// Validates a row-rendered relative subpath (e.g. `device_1/cam_a`) and returns
/// normalized relative components without touching the filesystem.
pub fn validate_relative_components(rendered_subpath: &str) -> Result<Vec<String>, ImagePathError> {
    let trimmed = rendered_subpath.trim();
    if trimmed.is_empty() {
        return Err(ImagePathError::InvalidComponent(
            "empty relative path".to_owned(),
        ));
    }
    if trimmed.contains('\0') {
        return Err(ImagePathError::InvalidComponent(trimmed.to_owned()));
    }
    if trimmed.starts_with('/') || trimmed.starts_with('\\') || has_drive_or_unc_prefix(trimmed) {
        return Err(ImagePathError::DriveOrUncEscape(trimmed.to_owned()));
    }

    let mut out = Vec::new();
    for seg in trimmed.split(['/', '\\']) {
        if seg.is_empty() || seg == "." {
            return Err(ImagePathError::InvalidComponent(trimmed.to_owned()));
        }
        if seg == ".." {
            return Err(ImagePathError::TraversalDetected(trimmed.to_owned()));
        }
        if seg.contains(':') {
            return Err(ImagePathError::DriveOrUncEscape(seg.to_owned()));
        }
        out.push(seg.to_owned());
    }
    Ok(out)
}

/// Validates a strict single-component image basename (no separators, drive colons, `.` or `..`).
pub fn validate_strict_basename(file_name: &str) -> Result<String, ImagePathError> {
    let trimmed = file_name.trim();
    if trimmed.is_empty()
        || trimmed == "."
        || trimmed == ".."
        || trimmed.contains('\0')
        || trimmed.contains('/')
        || trimmed.contains('\\')
        || trimmed.contains("..")
    {
        return Err(ImagePathError::InvalidComponent(trimmed.to_owned()));
    }
    if trimmed.contains(':') {
        return Err(ImagePathError::DriveOrUncEscape(trimmed.to_owned()));
    }
    Ok(trimmed.to_owned())
}

/// Splits `path_template` into a trusted static root and optional templated suffix,
/// renders the suffix against `record`, and validates all relative components and basename.
pub fn resolve_image_path(
    path_template: &str,
    file_name_rendered: &str,
    record: &HashMap<String, Value>,
) -> Result<ValidatedImagePath, ImagePathError> {
    let file_name = validate_strict_basename(file_name_rendered)?;

    let Some(tmpl_idx) = path_template.find("{{") else {
        let trusted_root = validate_static_root(path_template)?;
        return Ok(ValidatedImagePath {
            trusted_root,
            relative_dirs: Vec::new(),
            file_name,
        });
    };

    let prefix = &path_template[..tmpl_idx];
    let sep_idx = prefix.rfind(['/', '\\']);
    let Some(sep) = sep_idx else {
        return Err(ImagePathError::UnanchoredTemplate(path_template.to_owned()));
    };

    let root_raw = &path_template[..=sep];
    let suffix_tmpl = &path_template[sep + 1..];
    let trusted_root = validate_static_root(root_raw)?;

    let record_val = Value::Object(record.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
    let rendered_suffix = apply_data_template_value(suffix_tmpl, &record_val);
    let relative_dirs = validate_relative_components(&rendered_suffix)?;

    Ok(ValidatedImagePath {
        trusted_root,
        relative_dirs,
        file_name,
    })
}
