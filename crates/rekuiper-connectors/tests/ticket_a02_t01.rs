use rekuiper_connectors::image_paths::{
    resolve_image_path, validate_relative_components, validate_strict_basename, ImagePathError,
};
use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;

#[test]
fn a02_t01_table_driven_windows_and_posix_image_path_validation() {
    // 1. Valid legacy static paths remain valid (POSIX & Windows roots).
    let empty_rec = HashMap::new();
    for static_root in [
        "/var/lib/rekuiper/images",
        "C:\\rekuiper\\images",
        "./local_imgs",
    ] {
        let res = resolve_image_path(static_root, "frame_01.png", &empty_rec).unwrap();
        assert_eq!(res.trusted_root, PathBuf::from(static_root));
        assert!(res.relative_dirs.is_empty());
        assert_eq!(res.file_name, "frame_01.png");
    }

    // 2. Valid anchored template with relative components resolves without opening files.
    let rec = HashMap::from([
        ("site".to_owned(), json!("plant_a")),
        ("cam".to_owned(), json!("cam_02")),
    ]);
    let anchored = resolve_image_path("/srv/images/{{.site}}/{{.cam}}", "snap.jpeg", &rec).unwrap();
    assert_eq!(anchored.trusted_root, PathBuf::from("/srv/images/"));
    assert_eq!(anchored.relative_dirs, vec!["plant_a", "cam_02"]);
    assert_eq!(anchored.file_name, "snap.jpeg");

    // 3. Invalid cases fail before directory creation.
    let invalid_cases = [
        ("{{.site}}/sub", "a.png", json!("etc")), // unanchored template
        ("/srv/images/{{.site}}", "a.png", json!("../etc")), // parent traversal
        ("/srv/images/{{.site}}", "a.png", json!("/etc/passwd")), // rooted escape
        ("/srv/images/{{.site}}", "a.png", json!("C:\\Windows")), // Windows drive escape
        ("/srv/images/{{.site}}", "a.png", json!("\\\\srv\\sh")), // UNC escape
        ("/srv/images/{{.site}}", "a.png", json!("")), // empty component
    ];
    for (tmpl, fname, site_val) in invalid_cases {
        let mut r = HashMap::new();
        r.insert("site".to_owned(), site_val);
        let err =
            resolve_image_path(tmpl, fname, &r).expect_err(&format!("expected failure for {tmpl}"));
        assert!(matches!(
            err,
            ImagePathError::UnanchoredTemplate(_)
                | ImagePathError::TraversalDetected(_)
                | ImagePathError::DriveOrUncEscape(_)
                | ImagePathError::InvalidComponent(_)
        ));
    }

    // 4. Relative component & strict basename helpers reject escapes.
    assert!(validate_relative_components("a/b").is_ok());
    assert!(validate_relative_components("a/../b").is_err());
    assert!(validate_relative_components("D:secret").is_err());
    assert!(validate_strict_basename("ok_1.png").is_ok());
    assert!(validate_strict_basename("sub/bad.png").is_err());
    assert!(validate_strict_basename("C:bad.png").is_err());
    assert!(validate_strict_basename("..").is_err());
}
