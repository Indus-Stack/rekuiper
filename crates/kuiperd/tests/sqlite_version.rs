use std::process::Command;

#[test]
fn both_daemon_names_report_the_patched_sqlite_version() {
    for binary in [
        env!("CARGO_BIN_EXE_kuiperd"),
        env!("CARGO_BIN_EXE_rekuiperd"),
    ] {
        let output = Command::new(binary)
            .arg("--sqlite-version")
            .output()
            .expect("run packaged daemon");
        assert!(
            output.status.success(),
            "{binary}: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let version = String::from_utf8(output.stdout).expect("SQLite version is UTF-8");
        let components = version
            .trim()
            .split('.')
            .map(str::parse::<u32>)
            .collect::<Result<Vec<_>, _>>()
            .expect("SQLite reports a numeric version");
        assert!(
            components.len() == 3 && components.as_slice() >= &[3, 54, 0],
            "{binary} reports SQLite {version}"
        );
    }
}
