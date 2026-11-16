use rekuiper_connectors::image_paths::resolve_image_path;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn resolve_image_path_allows_trailing_slash_on_templated_dir() {
    let mut record = HashMap::new();
    record.insert("dir".to_owned(), json!("cam_01"));
    let validated = resolve_image_path("/var/lib/images/{{.dir}}/", "frame.jpg", &record).unwrap();
    assert_eq!(validated.relative_dirs, vec!["cam_01"]);
    assert_eq!(validated.file_name, "frame.jpg");
}

#[cfg(not(windows))]
#[test]
fn resolve_image_path_allows_colon_in_linux_rendered_segment() {
    let mut record = HashMap::new();
    record.insert("ts".to_owned(), json!("2026-11-15T12:00:00"));
    let validated = resolve_image_path("/var/lib/images/{{.ts}}", "frame.jpg", &record).unwrap();
    assert_eq!(validated.relative_dirs, vec!["2026-11-15T12:00:00"]);
}
