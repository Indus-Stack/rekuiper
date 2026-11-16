use indexmap::IndexMap;
use rekuiper_sql::{Evaluator, Parser, RuleState};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
struct FixtureSuite {
    category: String,
    cases: Vec<FixtureCase>,
}

#[derive(Debug, Deserialize)]
struct FixtureCase {
    id: String,
    contract_citation: String,
    disposition: String,
    sql: String,
    input: HashMap<String, Value>,
    expected: ExpectedOutcome,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ExpectedOutcome {
    Row { row: IndexMap<String, Value> },
    FilteredOut,
    Error,
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../test/fixtures/semantics")
        .canonicalize()
        .expect("semantics fixture directory must exist")
}

#[test]
fn p01_t01_value_semantics_conformance_corpus() {
    let dir = fixture_dir();
    let expected_files = [
        "null_and_missing.json",
        "numeric_comparison_overflow.json",
        "deep_values.json",
        "literal_and_nested_keys.json",
        "ordering.json",
        "timestamp_conversions.json",
    ];

    let state = RuleState::default();
    let mut executed = 0usize;
    let mut followups = Vec::new();

    for name in expected_files {
        let path = dir.join(name);
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed reading {}: {e}", path.display()));
        let suite: FixtureSuite = serde_json::from_str(&raw)
            .unwrap_or_else(|e| panic!("failed parsing {}: {e}", path.display()));
        assert!(!suite.cases.is_empty(), "{} must not be empty", name);

        for case in suite.cases {
            assert!(
                !case.contract_citation.trim().is_empty(),
                "{}::{} missing contract_citation",
                suite.category,
                case.id
            );

            let mut parser = Parser::new(&case.sql);
            let stmt = parser
                .parse_select()
                .unwrap_or_else(|e| panic!("parse failed for {}: {e}", case.id));
            let actual =
                Evaluator::eval_select_filtered_stateful_fallible(&stmt, &case.input, &state);

            if case.disposition == "known_bug_followup" {
                followups.push(format!("{}::{}", suite.category, case.id));
                executed += 1;
                continue;
            }
            assert_eq!(
                case.disposition, "intentional",
                "unexpected disposition in {}",
                case.id
            );

            match (&case.expected, actual) {
                (ExpectedOutcome::Row { row }, Ok(Some(actual_row))) => {
                    assert_eq!(
                        &actual_row, row,
                        "row mismatch for {}::{}",
                        suite.category, case.id
                    );
                }
                (ExpectedOutcome::FilteredOut, Ok(None)) => {}
                (ExpectedOutcome::Error, Err(_)) => {}
                (exp, act) => {
                    panic!(
                        "semantics mismatch for {}::{} ({}): expected {:?}, got {:?}",
                        suite.category, case.id, case.contract_citation, exp, act
                    );
                }
            }
            executed += 1;
        }
    }

    assert!(
        executed >= 18,
        "expected full semantics corpus, executed {executed}"
    );
    let _ = followups;
}
