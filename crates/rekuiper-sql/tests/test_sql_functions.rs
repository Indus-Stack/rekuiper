use rekuiper_sql::{Evaluator, Parser, RuleState};
use serde_json::{json, Value};
use std::collections::HashMap;

type Record = HashMap<String, Value>;

fn empty() -> Record {
    HashMap::new()
}

fn rec(pairs: &[(&str, Value)]) -> Record {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

/// Evaluate a single-projection `SELECT <expr> AS v ...` and return `v`.
fn eval_one(sql: &str, record: &Record) -> Value {
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_select().expect("Should parse");
    assert_eq!(stmt.fields.len(), 1);
    match Evaluator::eval_select(&stmt, record) {
        Some(mut row) => row.shift_remove("v").unwrap_or(Value::Null),
        None => Value::Null,
    }
}

/// Evaluate a single-projection aggregate query over a batch.
fn eval_agg_one(sql: &str, records: &[Record]) -> Value {
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_select().expect("Should parse");
    assert_eq!(stmt.fields.len(), 1);
    Evaluator::eval_aggregate(&stmt, records)
        .expect("Should aggregate")
        .shift_remove("v")
        .unwrap()
}

// Fixed point in time: 2024-01-01T00:00:00Z (a Monday, UTC).
const T0: i64 = 1_704_067_200_000;

// ---------------------------------------------------------------------------
// DateTime functions
// ---------------------------------------------------------------------------

#[test]
fn test_datetime_now() {
    let v = eval_one("SELECT now() AS v FROM demo", &empty());
    let s = v.as_str().expect("now() returns a string");
    assert_eq!(
        s.len(),
        19,
        "now() format should be YYYY-MM-DD HH:mm:ss: {}",
        s
    );
    // Arity is strict.
    assert_eq!(
        eval_one("SELECT now(1) AS v FROM demo", &empty()),
        Value::Null
    );
}

#[test]
fn test_datetime_format_date() {
    assert_eq!(
        eval_one(
            "SELECT format_date(1704067200000, '%Y-%m-%d %H:%M:%S') AS v FROM demo",
            &empty()
        ),
        json!("2024-01-01 00:00:00")
    );
    assert_eq!(
        eval_one(
            "SELECT format_date(1704067200000, '%Y-%m-%dT%H:%M:%SZ') AS v FROM demo",
            &empty()
        ),
        json!("2024-01-01T00:00:00Z")
    );
    // RFC3339 input is accepted as well.
    assert_eq!(
        eval_one(
            "SELECT format_date('2024-01-01T00:00:00Z', '%Y-%m-%d') AS v FROM demo",
            &empty()
        ),
        json!("2024-01-01")
    );
    // Garbage in -> Null.
    assert_eq!(
        eval_one(
            "SELECT format_date('not-a-date', '%Y') AS v FROM demo",
            &empty()
        ),
        Value::Null
    );
    assert_eq!(
        eval_one(
            "SELECT format_date(1704067200000, 42) AS v FROM demo",
            &empty()
        ),
        Value::Null
    );
}

#[test]
fn test_datetime_date_parse() {
    assert_eq!(
        eval_one(
            "SELECT date_parse('2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S') AS v FROM demo",
            &empty()
        ),
        json!(T0)
    );
    // Date-only format resolves to midnight UTC.
    assert_eq!(
        eval_one(
            "SELECT date_parse('2024-01-01', '%Y-%m-%d') AS v FROM demo",
            &empty()
        ),
        json!(T0)
    );
    // Round-trips with format_date (2024-06-15T12:30:00Z).
    assert_eq!(
        eval_one(
            "SELECT date_parse('2024-06-15T12:30:00Z', '%Y-%m-%dT%H:%M:%SZ') AS v FROM demo",
            &empty()
        ),
        json!(1_718_454_600_000i64)
    );
    assert_eq!(
        eval_one("SELECT date_parse('nope', '%Y') AS v FROM demo", &empty()),
        Value::Null
    );
}

#[test]
fn test_datetime_date_add() {
    assert_eq!(
        eval_one(
            "SELECT date_add('hh', 2, 1704067200000) AS v FROM demo",
            &empty()
        ),
        json!(1_704_074_400_000i64)
    );
    assert_eq!(
        eval_one(
            "SELECT date_add('dd', 1, 1704067200000) AS v FROM demo",
            &empty()
        ),
        json!(1_704_153_600_000i64)
    );
    assert_eq!(
        eval_one(
            "SELECT date_add('mi', 90, 1704067200000) AS v FROM demo",
            &empty()
        ),
        json!(1_704_072_600_000i64)
    );
    assert_eq!(
        eval_one(
            "SELECT date_add('ss', 30, 1704067200000) AS v FROM demo",
            &empty()
        ),
        json!(1_704_067_230_000i64)
    );
    assert_eq!(
        eval_one(
            "SELECT date_add('ms', 500, 1704067200000) AS v FROM demo",
            &empty()
        ),
        json!(1_704_067_200_500i64)
    );
    // Aliases and negatives work.
    assert_eq!(
        eval_one(
            "SELECT date_add('day', -1, 1704067200000) AS v FROM demo",
            &empty()
        ),
        json!(1_703_980_800_000i64)
    );
    assert_eq!(
        eval_one(
            "SELECT date_add('minute', 1, 1704067200000) AS v FROM demo",
            &empty()
        ),
        json!(1_704_067_260_000i64)
    );
    // Unknown unit -> Null.
    assert_eq!(
        eval_one(
            "SELECT date_add('xx', 1, 1704067200000) AS v FROM demo",
            &empty()
        ),
        Value::Null
    );
}

#[test]
fn test_datetime_date_diff() {
    assert_eq!(
        eval_one(
            "SELECT date_diff('hh', 1704067200000, 1704074400000) AS v FROM demo",
            &empty()
        ),
        json!(2)
    );
    assert_eq!(
        eval_one(
            "SELECT date_diff('dd', 1704067200000, 1704153600000) AS v FROM demo",
            &empty()
        ),
        json!(1)
    );
    assert_eq!(
        eval_one(
            "SELECT date_diff('mi', 1704067200000, 1704067260000) AS v FROM demo",
            &empty()
        ),
        json!(1)
    );
    assert_eq!(
        eval_one(
            "SELECT date_diff('ss', 1704067200000, 1704067200500) AS v FROM demo",
            &empty()
        ),
        json!(0)
    );
    assert_eq!(
        eval_one(
            "SELECT date_diff('ms', 1704067200000, 1704067200500) AS v FROM demo",
            &empty()
        ),
        json!(500)
    );
    // Reversed order goes negative.
    assert_eq!(
        eval_one(
            "SELECT date_diff('hh', 1704074400000, 1704067200000) AS v FROM demo",
            &empty()
        ),
        json!(-2)
    );
}

#[test]
fn test_datetime_components() {
    // 2024-01-01T00:00:00Z plus 36h01m02s = 2024-01-02T12:01:02Z.
    let ts = T0 + 36 * 3_600_000 + 60_000 + 2_000;
    let r = rec(&[("ts", json!(ts))]);
    assert_eq!(eval_one("SELECT year(ts) AS v FROM demo", &r), json!(2024));
    assert_eq!(eval_one("SELECT month(ts) AS v FROM demo", &r), json!(1));
    assert_eq!(eval_one("SELECT day(ts) AS v FROM demo", &r), json!(2));
    assert_eq!(eval_one("SELECT hour(ts) AS v FROM demo", &r), json!(12));
    assert_eq!(eval_one("SELECT minute(ts) AS v FROM demo", &r), json!(1));
    assert_eq!(eval_one("SELECT second(ts) AS v FROM demo", &r), json!(2));
    // Missing column -> Null.
    assert_eq!(
        eval_one("SELECT year(nope) AS v FROM demo", &empty()),
        Value::Null
    );
}

// ---------------------------------------------------------------------------
// JSON path functions
// ---------------------------------------------------------------------------

fn doc_record() -> Record {
    rec(&[
        ("doc", json!({"a": {"b": 1, "c": [10, 20]}})),
        ("arr", json!([{"k": "x"}, {"k": "y"}])),
        ("nums", json!([7, 8])),
    ])
}

#[test]
fn test_json_path_query() {
    let r = doc_record();
    // Dot notation.
    assert_eq!(
        eval_one("SELECT json_path_query(doc, 'a.b') AS v FROM demo", &r),
        json!(1)
    );
    assert_eq!(
        eval_one("SELECT json_path_query(doc, 'a.c[1]') AS v FROM demo", &r),
        json!(20)
    );
    // JSON pointer.
    assert_eq!(
        eval_one("SELECT json_path_query(doc, '/a/b') AS v FROM demo", &r),
        json!(1)
    );
    assert_eq!(
        eval_one("SELECT json_path_query(arr, '/0/k') AS v FROM demo", &r),
        json!("x")
    );
    // Whole subtrees come back intact.
    assert_eq!(
        eval_one("SELECT json_path_query(doc, 'a.c') AS v FROM demo", &r),
        json!([10, 20])
    );
    // Missing paths -> Null.
    assert_eq!(
        eval_one("SELECT json_path_query(doc, 'a.zzz') AS v FROM demo", &r),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT json_path_query(doc, '/a/9') AS v FROM demo", &r),
        Value::Null
    );
}

#[test]
fn test_json_path_query_first() {
    let r = doc_record();
    // Arrays collapse to their first element; scalars pass through.
    assert_eq!(
        eval_one(
            "SELECT json_path_query_first(doc, 'a.c') AS v FROM demo",
            &r
        ),
        json!(10)
    );
    assert_eq!(
        eval_one("SELECT json_path_query_first(nums, '') AS v FROM demo", &r),
        json!(7)
    );
    assert_eq!(
        eval_one(
            "SELECT json_path_query_first(doc, 'a.b') AS v FROM demo",
            &r
        ),
        json!(1)
    );
    assert_eq!(
        eval_one(
            "SELECT json_path_query_first(doc, 'missing') AS v FROM demo",
            &r
        ),
        Value::Null
    );
}

#[test]
fn test_json_path_exists() {
    let r = doc_record();
    assert_eq!(
        eval_one("SELECT json_path_exists(doc, 'a.b') AS v FROM demo", &r),
        json!(true)
    );
    assert_eq!(
        eval_one("SELECT json_path_exists(doc, 'a.nope') AS v FROM demo", &r),
        json!(false)
    );
    assert_eq!(
        eval_one("SELECT json_path_exists(arr, '/1/k') AS v FROM demo", &r),
        json!(true)
    );
}

// ---------------------------------------------------------------------------
// Postfix index/slice expressions + JSONPath wildcards (0.424 parity)
// ---------------------------------------------------------------------------

fn nest_record() -> Record {
    rec(&[
        ("id", json!(7)),
        ("children", json!(["Sara", "Alex", "Jack"])),
        (
            "followers",
            json!({"Group1": [{"first": "John", "last": "Shavor", "age": 22}]}),
        ),
    ])
}

#[test]
fn test_postfix_index_documented() {
    let r = nest_record();
    // Zero-based; -1 is the last element.
    assert_eq!(
        eval_one("SELECT children[0] AS v FROM demo", &r),
        json!("Sara")
    );
    assert_eq!(
        eval_one("SELECT children[1] AS v FROM demo", &r),
        json!("Alex")
    );
    assert_eq!(
        eval_one("SELECT children[-1] AS v FROM demo", &r),
        json!("Jack")
    );
    assert_eq!(
        eval_one("SELECT children[-2] AS v FROM demo", &r),
        json!("Alex")
    );
    // Out of range and non-arrays yield Null, never an error.
    assert_eq!(
        eval_one("SELECT children[9] AS v FROM demo", &r),
        Value::Null
    );
    assert_eq!(eval_one("SELECT id[0] AS v FROM demo", &r), Value::Null);
    // String keys look up object fields.
    assert_eq!(
        eval_one("SELECT followers['Group1'] AS v FROM demo", &r),
        json!([{"first": "John", "last": "Shavor", "age": 22}])
    );
    // Chained navigation after an index.
    assert_eq!(
        eval_one("SELECT followers['Group1'][0]->last AS v FROM demo", &r),
        json!("Shavor")
    );
}

#[test]
fn test_postfix_slice_documented() {
    let r = nest_record();
    // End-exclusive; negatives count from the end.
    assert_eq!(
        eval_one("SELECT children[0:1] AS v FROM demo", &r),
        json!(["Sara"])
    );
    assert_eq!(
        eval_one("SELECT children[1:-1] AS v FROM demo", &r),
        json!(["Alex"])
    );
    assert_eq!(
        eval_one("SELECT children[0:-1] AS v FROM demo", &r),
        json!(["Sara", "Alex"])
    );
    assert_eq!(
        eval_one("SELECT children[0:2] AS v FROM demo", &r),
        json!(["Sara", "Alex"])
    );
    // Omitted bounds mean array start/end.
    assert_eq!(
        eval_one("SELECT children[:] AS v FROM demo", &r),
        json!(["Sara", "Alex", "Jack"])
    );
    assert_eq!(
        eval_one("SELECT children[:2] AS v FROM demo", &r),
        json!(["Sara", "Alex"])
    );
    assert_eq!(
        eval_one("SELECT children[1:] AS v FROM demo", &r),
        json!(["Alex", "Jack"])
    );
    // Inverted ranges yield an empty array, not an error.
    assert_eq!(
        eval_one("SELECT children[2:1] AS v FROM demo", &r),
        json!([])
    );
    assert_eq!(eval_one("SELECT id[0:1] AS v FROM demo", &r), Value::Null);
}

#[test]
fn test_json_path_wildcard_documented() {
    let r = nest_record();
    // Documented example shape over a single-member group.
    assert_eq!(
        eval_one(
            "SELECT json_path_query(followers, '$.Group1[*].last') AS v FROM demo",
            &r
        ),
        json!(["Shavor"])
    );
    assert_eq!(
        eval_one(
            "SELECT json_path_query_first(followers, '$.Group1[*].last') AS v FROM demo",
            &r
        ),
        json!("Shavor")
    );
    assert_eq!(
        eval_one(
            "SELECT json_path_exists(followers, '$.Group1[*].last') AS v FROM demo",
            &r
        ),
        json!(true)
    );
    assert_eq!(
        eval_one(
            "SELECT json_path_exists(followers, '$.Group2[*].last') AS v FROM demo",
            &r
        ),
        json!(false)
    );
    // Scalar `$`-rooted paths still unwrap to scalars.
    assert_eq!(
        eval_one("SELECT json_path_query(id, '$') AS v FROM demo", &r),
        json!(7)
    );
}

#[test]
fn test_json_map() {
    assert_eq!(
        eval_one(
            "SELECT json_map('k1', 1, 'k2', 'v') AS v FROM demo",
            &empty()
        ),
        json!({"k1": 1, "k2": "v"})
    );
    assert_eq!(
        eval_one("SELECT json_map() AS v FROM demo", &empty()),
        json!({})
    );
    // Non-string keys are stringified; odd arity -> Null.
    assert_eq!(
        eval_one("SELECT json_map(1, 'a') AS v FROM demo", &empty()),
        json!({"1": "a"})
    );
    assert_eq!(
        eval_one("SELECT json_map('k1') AS v FROM demo", &empty()),
        Value::Null
    );
}

// ---------------------------------------------------------------------------
// Crypto & encoding functions
// ---------------------------------------------------------------------------

#[test]
fn test_crypto_hashes() {
    assert_eq!(
        eval_one("SELECT md5('hello') AS v FROM demo", &empty()),
        json!("5d41402abc4b2a76b9719d911017c592")
    );
    assert_eq!(
        eval_one("SELECT sha256('hello') AS v FROM demo", &empty()),
        json!("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
    );
    assert_eq!(
        eval_one("SELECT sha512('hello') AS v FROM demo", &empty()),
        json!("9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043")
    );
    // Empty string hashes to the well-known empty digests.
    assert_eq!(
        eval_one("SELECT md5('') AS v FROM demo", &empty()),
        json!("d41d8cd98f00b204e9800998ecf8427e")
    );
    // Null input -> Null.
    assert_eq!(
        eval_one("SELECT sha256(nothing_here) AS v FROM demo", &empty()),
        Value::Null
    );
}

#[test]
fn test_base64_encode_decode() {
    assert_eq!(
        eval_one("SELECT encode('hello', 'base64') AS v FROM demo", &empty()),
        json!("aGVsbG8=")
    );
    assert_eq!(
        eval_one("SELECT base64_encode('hello') AS v FROM demo", &empty()),
        json!("aGVsbG8=")
    );
    assert_eq!(
        eval_one(
            "SELECT decode('aGVsbG8=', 'base64') AS v FROM demo",
            &empty()
        ),
        json!("hello")
    );
    assert_eq!(
        eval_one("SELECT base64_decode('aGVsbG8=') AS v FROM demo", &empty()),
        json!("hello")
    );
    // Round-trip through both spellings.
    assert_eq!(
        eval_one(
            "SELECT base64_decode(base64_encode('rekuiper')) AS v FROM demo",
            &empty()
        ),
        json!("rekuiper")
    );
    // Unknown methods and garbage -> Null.
    assert_eq!(
        eval_one("SELECT encode('hello', 'hex') AS v FROM demo", &empty()),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT base64_decode('!!!') AS v FROM demo", &empty()),
        Value::Null
    );
}

// ---------------------------------------------------------------------------
// Extended array functions
// ---------------------------------------------------------------------------

fn arr_record() -> Record {
    rec(&[
        ("arr", json!([10, 20, 30])),
        ("dups", json!([1, 2, 2, 3, 1])),
        ("words", json!(["a", "b"])),
    ])
}

#[test]
fn test_array_create_position_length() {
    assert_eq!(
        eval_one("SELECT array_create(1, 'a', true) AS v FROM demo", &empty()),
        json!([1, "a", true])
    );
    assert_eq!(
        eval_one("SELECT array_create() AS v FROM demo", &empty()),
        json!([])
    );
    let r = arr_record();
    assert_eq!(
        eval_one("SELECT array_position(arr, 20) AS v FROM demo", &r),
        json!(1)
    );
    assert_eq!(
        eval_one("SELECT array_position(arr, 10) AS v FROM demo", &r),
        json!(0)
    );
    assert_eq!(
        eval_one("SELECT array_position(arr, 99) AS v FROM demo", &r),
        json!(-1)
    );
    assert_eq!(
        eval_one("SELECT array_length(arr) AS v FROM demo", &r),
        json!(3)
    );
    assert_eq!(
        eval_one("SELECT array_length(words) AS v FROM demo", &r),
        json!(2)
    );
    // Non-arrays -> Null.
    assert_eq!(
        eval_one("SELECT array_length(5) AS v FROM demo", &empty()),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT array_position(5, 5) AS v FROM demo", &empty()),
        Value::Null
    );
}

#[test]
fn test_array_slice_concat_deduplicate() {
    let r = arr_record();
    assert_eq!(
        eval_one("SELECT array_slice(arr, 2, 3) AS v FROM demo", &r),
        json!([20, 30])
    );
    assert_eq!(
        eval_one("SELECT array_slice(arr, 1, 1) AS v FROM demo", &r),
        json!([10])
    );
    // Bounds clamp; inverted ranges are empty.
    assert_eq!(
        eval_one("SELECT array_slice(arr, 2, 99) AS v FROM demo", &r),
        json!([20, 30])
    );
    assert_eq!(
        eval_one("SELECT array_slice(arr, 3, 1) AS v FROM demo", &r),
        json!([])
    );
    assert_eq!(
        eval_one("SELECT array_concat(arr, words) AS v FROM demo", &r),
        json!([10, 20, 30, "a", "b"])
    );
    assert_eq!(
        eval_one("SELECT deduplicate(dups) AS v FROM demo", &r),
        json!([1, 2, 3])
    );
    assert_eq!(
        eval_one("SELECT deduplicate(words) AS v FROM demo", &r),
        json!(["a", "b"])
    );
}

// ---------------------------------------------------------------------------
// Aggregate & analytic functions
// ---------------------------------------------------------------------------

fn temp_batch() -> Vec<Record> {
    vec![
        rec(&[("temp", json!(10))]),
        rec(&[("temp", json!(20))]),
        rec(&[("temp", json!(30))]),
    ]
}

#[test]
fn test_aggregate_collect_latest_lead() {
    let rows = temp_batch();
    assert_eq!(
        eval_agg_one("SELECT collect(temp) AS v FROM demo", &rows),
        json!([10, 20, 30])
    );
    assert_eq!(
        eval_agg_one("SELECT latest(temp) AS v FROM demo", &rows),
        json!(30)
    );
    // lead() reads forward from the window start (0-based offset).
    assert_eq!(
        eval_agg_one("SELECT lead(temp) AS v FROM demo", &rows),
        json!(20)
    );
    assert_eq!(
        eval_agg_one("SELECT lead(temp, 2) AS v FROM demo", &rows),
        json!(30)
    );
    assert_eq!(
        eval_agg_one("SELECT lead(temp, 0) AS v FROM demo", &rows),
        json!(10)
    );
    assert_eq!(
        eval_agg_one("SELECT lead(temp, 5, -1) AS v FROM demo", &rows),
        json!(-1)
    );
    assert_eq!(
        eval_agg_one("SELECT lead(temp, 5) AS v FROM demo", &rows),
        Value::Null
    );

    // Nulls are skipped by collect, latest sees past them.
    let gappy = vec![
        rec(&[("temp", json!(10))]),
        rec(&[("temp", Value::Null)]),
        rec(&[("temp", json!(30))]),
    ];
    assert_eq!(
        eval_agg_one("SELECT collect(temp) AS v FROM demo", &gappy),
        json!([10, 30])
    );
    assert_eq!(
        eval_agg_one("SELECT latest(temp) AS v FROM demo", &gappy),
        json!(30)
    );
    let trailing_null = vec![rec(&[("temp", json!(10))]), rec(&[("temp", Value::Null)])];
    assert_eq!(
        eval_agg_one("SELECT latest(temp) AS v FROM demo", &trailing_null),
        json!(10)
    );
    // Degenerate batches.
    let empty: Vec<Record> = vec![];
    assert_eq!(
        eval_agg_one("SELECT collect(temp) AS v FROM demo", &empty),
        json!([])
    );
    assert_eq!(
        eval_agg_one("SELECT latest(temp) AS v FROM demo", &empty),
        Value::Null
    );
}

#[test]
fn test_stateful_had_changed_and_changed_col() {
    let mut parser = Parser::new("SELECT had_changed(temp) AS ch FROM demo");
    let stmt = parser.parse_select().expect("Should parse");
    let state = RuleState::default();
    let row = |t: Value| {
        Evaluator::eval_select_stateful(&stmt, &rec(&[("temp", t)]), &state)
            .expect("row always projects")
            .shift_remove("ch")
            .unwrap()
    };
    assert_eq!(row(json!(10)), json!(true));
    assert_eq!(row(json!(10)), json!(false));
    assert_eq!(row(json!(20)), json!(true));

    let mut parser = Parser::new("SELECT changed_col(temp) AS c FROM demo");
    let stmt = parser.parse_select().expect("Should parse");
    let state = RuleState::default();
    let row = |t: Value| {
        Evaluator::eval_select_stateful(&stmt, &rec(&[("temp", t)]), &state)
            .expect("row always projects")
            .shift_remove("c")
            .unwrap()
    };
    assert_eq!(row(json!(10)), json!(10));
    assert_eq!(row(json!(10)), Value::Null);
    assert_eq!(row(json!(20)), json!(20));

    // Scalar (stateless) fallbacks stay total.
    assert_eq!(
        eval_one("SELECT had_changed(5) AS v FROM demo", &empty()),
        json!(true)
    );
    assert_eq!(
        eval_one("SELECT changed_col(5) AS v FROM demo", &empty()),
        json!(5)
    );
    assert_eq!(
        eval_one("SELECT latest(7) AS v FROM demo", &empty()),
        json!(7)
    );
    assert_eq!(
        eval_one("SELECT collect(7) AS v FROM demo", &empty()),
        json!([7])
    );
    assert_eq!(
        eval_one("SELECT lead(7) AS v FROM demo", &empty()),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT lead(7, 0, 99) AS v FROM demo", &empty()),
        json!(99)
    );
}

// ---------------------------------------------------------------------------
// System & metadata functions
// ---------------------------------------------------------------------------

fn assert_uuid_v4(s: &str) {
    assert_eq!(s.len(), 36, "uuid length: {}", s);
    for (i, c) in s.chars().enumerate() {
        match i {
            8 | 13 | 18 | 23 => assert_eq!(c, '-', "hyphen at {}", i),
            14 => assert_eq!(c, '4', "version nibble"),
            19 => assert!(matches!(c, '8' | '9' | 'a' | 'b'), "variant nibble: {}", c),
            _ => assert!(c.is_ascii_hexdigit(), "hex at {}: {}", i, c),
        }
    }
}

#[test]
fn test_system_and_meta_functions() {
    // isnull across literal kinds.
    let mut parser =
        Parser::new("SELECT isnull(null) AS a, isnull(1) AS b, isnull('abc') AS c FROM demo");
    let stmt = parser.parse_select().expect("Should parse");
    let out = Evaluator::eval_select(&stmt, &empty()).expect("Should project");
    assert_eq!(out.get("a"), Some(&json!(true)));
    assert_eq!(out.get("b"), Some(&json!(false)));
    assert_eq!(out.get("c"), Some(&json!(false)));

    // uuid()/newuuid(): hyphenated v4, unique per call.
    for func in ["uuid", "newuuid"] {
        let first = eval_one(&format!("SELECT {}() AS v FROM demo", func), &empty());
        let second = eval_one(&format!("SELECT {}() AS v FROM demo", func), &empty());
        let (a, b) = (
            first.as_str().expect("uuid returns a string"),
            second.as_str().expect("uuid returns a string"),
        );
        assert_uuid_v4(a);
        assert_uuid_v4(b);
        assert_ne!(a, b, "uuids must differ");
    }

    // tstamp(): epoch millis near now.
    let before = chrono::Utc::now().timestamp_millis();
    let v = eval_one("SELECT tstamp() AS v FROM demo", &empty());
    let after = chrono::Utc::now().timestamp_millis();
    let t = v.as_i64().expect("tstamp() returns an integer");
    assert!(
        t > 0 && t >= before && t <= after,
        "tstamp out of range: {}",
        t
    );

    // meta()/mqtt() extraction from envelope objects.
    let r = rec(&[
        (
            "meta",
            json!({"topic": "factory/temp", "device": "sensor1"}),
        ),
        ("mqtt", json!({"topic": "m/t"})),
    ]);
    // Unquoted identifier and quoted string keys both work.
    assert_eq!(
        eval_one("SELECT meta(topic) AS v FROM demo", &r),
        json!("factory/temp")
    );
    assert_eq!(
        eval_one("SELECT meta('device') AS v FROM demo", &r),
        json!("sensor1")
    );
    assert_eq!(
        eval_one("SELECT mqtt(topic) AS v FROM demo", &r),
        json!("m/t")
    );
    // No-arg form returns the whole envelope object.
    assert_eq!(
        eval_one("SELECT meta() AS v FROM demo", &r),
        json!({"topic": "factory/temp", "device": "sensor1"})
    );
    // Missing keys and envelopes resolve to Null.
    assert_eq!(
        eval_one("SELECT meta('missing') AS v FROM demo", &r),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT meta() AS v FROM demo", &empty()),
        Value::Null
    );
    // Without an envelope, plain top-level fields are the fallback.
    let r = rec(&[("topic", json!("plain"))]);
    assert_eq!(
        eval_one("SELECT meta(topic) AS v FROM demo", &r),
        json!("plain")
    );

    // event_time(): timestamp keys win, otherwise now().
    assert_eq!(
        eval_one(
            "SELECT event_time() AS v FROM demo",
            &rec(&[("timestamp", json!(1700000000000i64))])
        ),
        json!(1700000000000i64)
    );
    assert_eq!(
        eval_one(
            "SELECT event_time() AS v FROM demo",
            &rec(&[("event_time", json!(123))])
        ),
        json!(123)
    );
    let v = eval_one("SELECT event_time() AS v FROM demo", &empty());
    assert!(v.as_i64().unwrap_or(0) > 0);

    // rule_id(): envelope value or empty string.
    assert_eq!(
        eval_one(
            "SELECT rule_id() AS v FROM demo",
            &rec(&[("__rule_id__", json!("r1"))])
        ),
        json!("r1")
    );
    assert_eq!(
        eval_one("SELECT rule_id() AS v FROM demo", &empty()),
        json!("")
    );

    // window bounds resolve from plain or dunder keys.
    let r = rec(&[("window_start", json!(1000)), ("window_end", json!(2000))]);
    let mut parser = Parser::new("SELECT window_start() AS s, window_end() AS e FROM demo");
    let stmt = parser.parse_select().expect("Should parse");
    let out = Evaluator::eval_select(&stmt, &r).expect("Should project");
    assert_eq!(out.get("s"), Some(&json!(1000)));
    assert_eq!(out.get("e"), Some(&json!(2000)));
    assert_eq!(
        eval_one("SELECT window_start() AS v FROM demo", &empty()),
        Value::Null
    );

    // Contextual functions also resolve on the stateful path.
    let mut parser = Parser::new("SELECT meta(topic) AS v FROM demo");
    let stmt = parser.parse_select().expect("Should parse");
    let state = rekuiper_sql::RuleState::default();
    let r = rec(&[("meta", json!({"topic": "factory/temp"}))]);
    let out = Evaluator::eval_select_stateful(&stmt, &r, &state).expect("Should project");
    assert_eq!(out.get("v"), Some(&json!("factory/temp")));
}

// ---------------------------------------------------------------------------
// Math & bitwise parity
// ---------------------------------------------------------------------------

fn assert_approx(actual: &Value, expected: f64, eps: f64) {
    let v = actual.as_f64().expect("expected numeric value");
    assert!(
        (v - expected).abs() <= eps,
        "expected ~{}, got {}",
        expected,
        v
    );
}

#[test]
fn test_math_and_bitwise_parity() {
    let empty = empty();

    // Bitwise integer ops.
    assert_eq!(
        eval_one("SELECT bitand(12, 10) AS v FROM demo", &empty),
        json!(8)
    );
    assert_eq!(
        eval_one("SELECT bitor(12, 10) AS v FROM demo", &empty),
        json!(14)
    );
    assert_eq!(
        eval_one("SELECT bitxor(12, 10) AS v FROM demo", &empty),
        json!(6)
    );
    assert_eq!(
        eval_one("SELECT bitnot(0) AS v FROM demo", &empty),
        json!(-1)
    );
    assert_eq!(
        eval_one("SELECT bitnot(5) AS v FROM demo", &empty),
        json!(-6)
    );
    // Null / non-integer inputs propagate Null.
    assert_eq!(
        eval_one("SELECT bitand(null, 1) AS v FROM demo", &empty),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT bitor(1, null) AS v FROM demo", &empty),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT bitxor('abc', 1) AS v FROM demo", &empty),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT bitnot(null) AS v FROM demo", &empty),
        Value::Null
    );

    // Pi & random.
    assert_approx(
        &eval_one("SELECT pi() AS v FROM demo", &empty),
        std::f64::consts::PI,
        1e-12,
    );
    let r = eval_one("SELECT rand() AS v FROM demo", &empty);
    let f = r.as_f64().expect("rand() returns a float");
    assert!((0.0..1.0).contains(&f), "rand() out of range: {}", f);

    // Logarithms.
    assert_approx(
        &eval_one("SELECT log2(8) AS v FROM demo", &empty),
        3.0,
        1e-12,
    );
    assert_approx(
        &eval_one("SELECT log10(100) AS v FROM demo", &empty),
        2.0,
        1e-12,
    );
    assert_approx(
        &eval_one("SELECT log(100) AS v FROM demo", &empty),
        2.0,
        1e-6,
    );
    assert_approx(
        &eval_one("SELECT log(2, 8) AS v FROM demo", &empty),
        3.0,
        1e-9,
    );
    assert_eq!(
        eval_one("SELECT log(0) AS v FROM demo", &empty),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT log(-1) AS v FROM demo", &empty),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT log2(0) AS v FROM demo", &empty),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT log(null) AS v FROM demo", &empty),
        Value::Null
    );

    // Integer-preserving powers plus float fallback.
    assert_eq!(
        eval_one("SELECT power(2, 10) AS v FROM demo", &empty),
        json!(1024)
    );
    assert_eq!(
        eval_one("SELECT pow(3, 2) AS v FROM demo", &empty),
        json!(9)
    );
    assert_eq!(
        eval_one("SELECT power(2, -1) AS v FROM demo", &empty),
        json!(0.5)
    );
    assert_approx(
        &eval_one("SELECT power(9, 0.5) AS v FROM demo", &empty),
        3.0,
        1e-12,
    );
    assert_eq!(
        eval_one("SELECT power(10, 1000) AS v FROM demo", &empty),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT power(null, 2) AS v FROM demo", &empty),
        Value::Null
    );

    // Hyperbolic, cotangent, degree conversion.
    assert_eq!(
        eval_one("SELECT cosh(0) AS v FROM demo", &empty),
        json!(1.0)
    );
    assert_eq!(
        eval_one("SELECT sinh(0) AS v FROM demo", &empty),
        json!(0.0)
    );
    assert_eq!(
        eval_one("SELECT tanh(0) AS v FROM demo", &empty),
        json!(0.0)
    );
    assert_approx(
        &eval_one("SELECT cot(1) AS v FROM demo", &empty),
        0.6420926159,
        1e-9,
    );
    assert_eq!(
        eval_one("SELECT cot(0) AS v FROM demo", &empty),
        Value::Null
    );
    assert_approx(
        &eval_one("SELECT radians(180) AS v FROM demo", &empty),
        std::f64::consts::PI,
        1e-12,
    );
    assert_approx(
        &eval_one("SELECT degrees(pi()) AS v FROM demo", &empty),
        180.0,
        1e-9,
    );

    // Base conversion.
    assert_eq!(
        eval_one("SELECT conv('a', 16, 2) AS v FROM demo", &empty),
        json!("1010")
    );
    assert_eq!(
        eval_one("SELECT conv(15, 10, 16) AS v FROM demo", &empty),
        json!("f")
    );
    assert_eq!(
        eval_one("SELECT conv('1111', 2, 10) AS v FROM demo", &empty),
        json!("15")
    );
    assert_eq!(
        eval_one("SELECT conv('z', 36, 10) AS v FROM demo", &empty),
        json!("35")
    );
    assert_eq!(
        eval_one("SELECT conv('-10', 10, 16) AS v FROM demo", &empty),
        json!("-a")
    );
    assert_eq!(
        eval_one("SELECT conv('x', 16, 2) AS v FROM demo", &empty),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT conv('a', 1, 2) AS v FROM demo", &empty),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT conv('a', 16, 37) AS v FROM demo", &empty),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT conv(null, 16, 2) AS v FROM demo", &empty),
        Value::Null
    );
}

// ---------------------------------------------------------------------------
// Array manipulation functions
// ---------------------------------------------------------------------------

#[test]
fn test_array_functions_parity() {
    let empty = empty();

    // Cardinality.
    assert_eq!(
        eval_one("SELECT cardinality([1, 2, 3]) AS v FROM demo", &empty),
        json!(3)
    );
    assert_eq!(
        eval_one("SELECT array_cardinality([]) AS v FROM demo", &empty),
        json!(0)
    );
    assert_eq!(
        eval_one("SELECT cardinality(null) AS v FROM demo", &empty),
        json!(0)
    );

    // Element access: 0-based, negative from the end.
    assert_eq!(
        eval_one("SELECT element_at([10, 20, 30], 0) AS v FROM demo", &empty),
        json!(10)
    );
    assert_eq!(
        eval_one("SELECT element_at([10, 20, 30], 1) AS v FROM demo", &empty),
        json!(20)
    );
    assert_eq!(
        eval_one("SELECT element_at([10, 20, 30], -1) AS v FROM demo", &empty),
        json!(30)
    );
    assert_eq!(
        eval_one("SELECT element_at([10, 20, 30], -3) AS v FROM demo", &empty),
        json!(10)
    );
    assert_eq!(
        eval_one("SELECT element_at([10, 20, 30], 5) AS v FROM demo", &empty),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT element_at([10, 20, 30], -4) AS v FROM demo", &empty),
        Value::Null
    );

    // Containment and set operations.
    assert_eq!(
        eval_one(
            "SELECT array_contains_any([1, 2], [2, 3]) AS v FROM demo",
            &empty
        ),
        json!(true)
    );
    assert_eq!(
        eval_one(
            "SELECT array_contains_any([1, 2], [3, 4]) AS v FROM demo",
            &empty
        ),
        json!(false)
    );
    assert_eq!(
        eval_one("SELECT array_contains_any(5, [1]) AS v FROM demo", &empty),
        json!(false)
    );
    assert_eq!(
        eval_one(
            "SELECT array_remove([1, 2, 1, 3], 1) AS v FROM demo",
            &empty
        ),
        json!([2, 3])
    );
    assert_eq!(
        eval_one("SELECT array_distinct([1, 2, 1, 3]) AS v FROM demo", &empty),
        json!([1, 2, 3])
    );
    assert_eq!(
        eval_one("SELECT array_union([1, 2], [2, 3]) AS v FROM demo", &empty),
        json!([1, 2, 3])
    );
    assert_eq!(
        eval_one(
            "SELECT array_intersect([1, 2, 3], [2, 3, 4]) AS v FROM demo",
            &empty
        ),
        json!([2, 3])
    );
    assert_eq!(
        eval_one("SELECT array_except([1, 2, 3], [2]) AS v FROM demo", &empty),
        json!([1, 3])
    );
    assert_eq!(
        eval_one(
            "SELECT array_except([1, 2], [1, 2, 3]) AS v FROM demo",
            &empty
        ),
        json!([])
    );

    // Numeric aggregations skip non-numeric elements.
    assert_eq!(
        eval_one("SELECT array_max([3, 1, 4]) AS v FROM demo", &empty),
        json!(4)
    );
    assert_eq!(
        eval_one("SELECT array_min([3, 1, 4]) AS v FROM demo", &empty),
        json!(1)
    );
    assert_eq!(
        eval_one("SELECT array_avg([2, 4]) AS v FROM demo", &empty),
        json!(3.0)
    );
    assert_eq!(
        eval_one("SELECT array_max(['a', null]) AS v FROM demo", &empty),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT array_avg([]) AS v FROM demo", &empty),
        Value::Null
    );

    // Flatten (one level) and sort.
    assert_eq!(
        eval_one("SELECT array_flatten([[1, 2], [3]]) AS v FROM demo", &empty),
        json!([1, 2, 3])
    );
    assert_eq!(
        eval_one(
            "SELECT array_flatten([[1, 2], [3], 4]) AS v FROM demo",
            &empty
        ),
        json!([1, 2, 3, 4])
    );
    assert_eq!(
        eval_one("SELECT array_sort([3, 1, 2]) AS v FROM demo", &empty),
        json!([1, 2, 3])
    );
    assert_eq!(
        eval_one("SELECT array_sort(['b', 'a']) AS v FROM demo", &empty),
        json!(["a", "b"])
    );

    // Generators.
    assert_eq!(
        eval_one("SELECT sequence(1, 3) AS v FROM demo", &empty),
        json!([1, 2, 3])
    );
    assert_eq!(
        eval_one("SELECT sequence(3, 1, -1) AS v FROM demo", &empty),
        json!([3, 2, 1])
    );
    assert_eq!(
        eval_one("SELECT sequence(5, 5) AS v FROM demo", &empty),
        json!([5])
    );
    assert_eq!(
        eval_one("SELECT sequence(1, 3, 0) AS v FROM demo", &empty),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT sequence(1, 3, -1) AS v FROM demo", &empty),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT repeat('a', 3) AS v FROM demo", &empty),
        json!(["a", "a", "a"])
    );
    assert_eq!(
        eval_one("SELECT repeat('a', 0) AS v FROM demo", &empty),
        json!([])
    );
    assert_eq!(
        eval_one("SELECT repeat('a', -1) AS v FROM demo", &empty),
        Value::Null
    );

    // Key/value pair expansion, including alternate spellings.
    assert_eq!(
        eval_one(
            "SELECT kvpair_array_to_obj([{\"key\": \"a\", \"value\": 1}]) AS v FROM demo",
            &empty
        ),
        json!({"a": 1})
    );
    assert_eq!(
        eval_one(
            "SELECT kvpair_array_to_obj([{\"Key\": \"x\", \"Value\": true}, {\"k\": \"y\", \"v\": 2}]) AS v FROM demo",
            &empty
        ),
        json!({"x": true, "y": 2})
    );
    assert_eq!(
        eval_one("SELECT kvpair_array_to_obj([]) AS v FROM demo", &empty),
        json!({})
    );
}

// ---------------------------------------------------------------------------
// Datetime calendar parity (MySQL-compatible, UTC)
// ---------------------------------------------------------------------------

fn assert_date_shape(name: &str, value: &Value) {
    let s = value
        .as_str()
        .unwrap_or_else(|| panic!("{} should be a string", name));
    let b = s.as_bytes();
    assert_eq!(s.len(), 10, "{} has wrong length: {:?}", name, s);
    assert_eq!((b[4], b[7]), (b'-', b'-'), "{} separators: {:?}", name, s);
    for (i, c) in b.iter().enumerate() {
        if i == 4 || i == 7 {
            continue;
        }
        assert!(c.is_ascii_digit(), "{} digit at {}: {:?}", name, i, s);
    }
}

fn assert_time_shape(name: &str, value: &Value) {
    let s = value
        .as_str()
        .unwrap_or_else(|| panic!("{} should be a string", name));
    let b = s.as_bytes();
    assert_eq!(s.len(), 8, "{} has wrong length: {:?}", name, s);
    assert_eq!((b[2], b[5]), (b':', b':'), "{} separators: {:?}", name, s);
    for (i, c) in b.iter().enumerate() {
        if i == 2 || i == 5 {
            continue;
        }
        assert!(c.is_ascii_digit(), "{} digit at {}: {:?}", name, i, s);
    }
}

#[test]
fn test_datetime_calendar_parity() {
    let empty = empty();

    // Current date/time shapes (UTC).
    assert_date_shape(
        "current_date()",
        &eval_one("SELECT current_date() AS v FROM demo", &empty),
    );
    assert_date_shape(
        "cur_date()",
        &eval_one("SELECT cur_date() AS v FROM demo", &empty),
    );
    assert_time_shape(
        "current_time()",
        &eval_one("SELECT current_time() AS v FROM demo", &empty),
    );
    assert_time_shape(
        "cur_time()",
        &eval_one("SELECT cur_time() AS v FROM demo", &empty),
    );
    assert_time_shape(
        "local_time()",
        &eval_one("SELECT local_time() AS v FROM demo", &empty),
    );
    // Timestamp aliases track now() (YYYY-MM-DD HH:mm:ss string).
    for func in ["current_timestamp", "local_timestamp"] {
        let v = eval_one(&format!("SELECT {}() AS v FROM demo", func), &empty);
        let s = v.as_str().expect("timestamp alias returns a string");
        assert_eq!(
            s.len(),
            19,
            "{} format should be YYYY-MM-DD HH:mm:ss: {}",
            func,
            s
        );
    }

    // Unix time formatting (1700000000000 = 2023-11-14 22:13:20 UTC).
    assert_eq!(
        eval_one(
            "SELECT from_unix_time(1700000000000) AS v FROM demo",
            &empty
        ),
        json!("2023-11-14 22:13:20")
    );
    assert_eq!(
        eval_one(
            "SELECT from_unix_time(1700000000000, '%Y/%m/%d') AS v FROM demo",
            &empty
        ),
        json!("2023/11/14")
    );
    assert_eq!(
        eval_one("SELECT from_unix_time(null) AS v FROM demo", &empty),
        Value::Null
    );

    // Day of week: 1 = Sunday .. 7 = Saturday.
    for (date_ms, expected) in [
        (1_704_067_200_000i64, 2), // 2024-01-01 Monday
        (1_700_000_000_000i64, 3), // 2023-11-14 Tuesday
        (1_704_499_200_000i64, 7), // 2024-01-06 Saturday
        (1_704_585_600_000i64, 1), // 2024-01-07 Sunday
        (1_709_164_800_000i64, 5), // 2024-02-29 Thursday (leap day)
    ] {
        assert_eq!(
            eval_one(
                &format!("SELECT day_of_week({}) AS v FROM demo", date_ms),
                &empty
            ),
            json!(expected),
            "day_of_week({})",
            date_ms
        );
    }

    // Day of year incl. leap handling.
    assert_eq!(
        eval_one("SELECT day_of_year(1704067200000) AS v FROM demo", &empty),
        json!(1)
    );
    assert_eq!(
        eval_one("SELECT day_of_year(1703980800000) AS v FROM demo", &empty),
        json!(365)
    );
    assert_eq!(
        eval_one("SELECT day_of_year(1709164800000) AS v FROM demo", &empty),
        json!(60)
    );

    // Names.
    assert_eq!(
        eval_one("SELECT day_name(1700035200000) AS v FROM demo", &empty),
        json!("Wednesday")
    );
    assert_eq!(
        eval_one("SELECT month_name(1700000000000) AS v FROM demo", &empty),
        json!("November")
    );
    assert_eq!(
        eval_one("SELECT day_of_month(1700000000000) AS v FROM demo", &empty),
        json!(14)
    );

    // Microsecond component.
    assert_eq!(
        eval_one("SELECT microsecond(1704067200123) AS v FROM demo", &empty),
        json!(123000)
    );
    assert_eq!(
        eval_one("SELECT microsecond(1704067200000) AS v FROM demo", &empty),
        json!(0)
    );

    // Last day of month, leap and common years.
    assert_eq!(
        eval_one("SELECT last_day(1707955200000) AS v FROM demo", &empty),
        json!("2024-02-29")
    );
    assert_eq!(
        eval_one("SELECT last_day(1676419200000) AS v FROM demo", &empty),
        json!("2023-02-28")
    );
    assert_eq!(
        eval_one("SELECT last_day(1704067200000) AS v FROM demo", &empty),
        json!("2024-01-31")
    );

    // to_seconds converts epoch millis to Unix seconds.
    assert_eq!(
        eval_one("SELECT to_seconds(0) AS v FROM demo", &empty),
        json!(0)
    );
    assert_eq!(
        eval_one("SELECT to_seconds(1704067200000) AS v FROM demo", &empty),
        json!(1704067200i64)
    );

    // Null propagation.
    for sql in [
        "SELECT day_of_week(null) AS v FROM demo",
        "SELECT day_name(null) AS v FROM demo",
        "SELECT month_name(null) AS v FROM demo",
        "SELECT microsecond(null) AS v FROM demo",
        "SELECT last_day(null) AS v FROM demo",
        "SELECT to_seconds(null) AS v FROM demo",
        "SELECT from_days(null) AS v FROM demo",
        "SELECT day_of_year(null) AS v FROM demo",
    ] {
        assert_eq!(eval_one(sql, &empty), Value::Null, "{}", sql);
    }
}

// ---------------------------------------------------------------------------
// Object & JSON navigation functions
// ---------------------------------------------------------------------------

#[test]
fn test_object_json_parity() {
    let empty = empty();

    // keys & values, including empty and mistyped inputs.
    assert_eq!(
        eval_one(
            "SELECT keys({\"a\": 1, \"b\": \"val\", \"c\": true}) AS v FROM demo",
            &empty
        ),
        json!(["a", "b", "c"])
    );
    assert_eq!(
        eval_one(
            "SELECT values({\"a\": 1, \"b\": \"val\", \"c\": true}) AS v FROM demo",
            &empty
        ),
        json!([1, "val", true])
    );
    assert_eq!(
        eval_one("SELECT keys({}) AS v FROM demo", &empty),
        json!([])
    );
    assert_eq!(
        eval_one("SELECT values({}) AS v FROM demo", &empty),
        json!([])
    );
    for sql in [
        "SELECT keys(42) AS v FROM demo",
        "SELECT keys('s') AS v FROM demo",
        "SELECT keys([1]) AS v FROM demo",
        "SELECT keys(null) AS v FROM demo",
        "SELECT values(null) AS v FROM demo",
    ] {
        assert_eq!(eval_one(sql, &empty), Value::Null, "{}", sql);
    }

    // object_construct, with Null-valued pairs omitted.
    assert_eq!(
        eval_one(
            "SELECT object_construct('a', 1, 'b', 'test', 'c', null) AS v FROM demo",
            &empty
        ),
        json!({"a": 1, "b": "test"})
    );
    assert_eq!(
        eval_one("SELECT object_construct() AS v FROM demo", &empty),
        json!({})
    );
    assert_eq!(
        eval_one("SELECT object_construct('a') AS v FROM demo", &empty),
        Value::Null
    );

    // object_concat merges left to right, later keys winning.
    assert_eq!(
        eval_one(
            "SELECT object_concat({\"a\": 1, \"b\": 2}, {\"b\": 99, \"c\": 3}) AS v FROM demo",
            &empty
        ),
        json!({"a": 1, "b": 99, "c": 3})
    );
    assert_eq!(
        eval_one(
            "SELECT object_concat({\"a\": 1}, {\"b\": 2}, {\"a\": 10, \"c\": 3}) AS v FROM demo",
            &empty
        ),
        json!({"a": 10, "b": 2, "c": 3})
    );
    assert_eq!(
        eval_one("SELECT object_concat({\"a\": 1}) AS v FROM demo", &empty),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT object_concat({\"a\": 1}, 5) AS v FROM demo", &empty),
        Value::Null
    );

    // erase / object_erase with single, multiple and array keys.
    assert_eq!(
        eval_one(
            "SELECT erase({\"a\": 1, \"b\": 2, \"c\": 3}, 'b') AS v FROM demo",
            &empty
        ),
        json!({"a": 1, "c": 3})
    );
    assert_eq!(
        eval_one(
            "SELECT object_erase({\"a\": 1, \"b\": 2, \"c\": 3}, 'a', 'c') AS v FROM demo",
            &empty
        ),
        json!({"b": 2})
    );
    assert_eq!(
        eval_one(
            "SELECT erase({\"a\": 1, \"b\": 2}, ['a', 'zzz']) AS v FROM demo",
            &empty
        ),
        json!({"b": 2})
    );
    assert_eq!(
        eval_one("SELECT erase({\"a\": 1}, 'missing') AS v FROM demo", &empty),
        json!({"a": 1})
    );
    assert_eq!(
        eval_one("SELECT erase(null, 'a') AS v FROM demo", &empty),
        Value::Null
    );

    // object_pick keeps only requested keys.
    assert_eq!(
        eval_one(
            "SELECT object_pick({\"a\": 1, \"b\": 2, \"c\": 3}, 'a', 'c') AS v FROM demo",
            &empty
        ),
        json!({"a": 1, "c": 3})
    );
    assert_eq!(
        eval_one(
            "SELECT object_pick({\"a\": 1, \"b\": 2, \"c\": 3}, ['b', 'z']) AS v FROM demo",
            &empty
        ),
        json!({"b": 2})
    );
    assert_eq!(
        eval_one("SELECT object_pick(null, 'a') AS v FROM demo", &empty),
        Value::Null
    );

    // kvpair round-trip both directions.
    assert_eq!(
        eval_one(
            "SELECT obj_to_kvpair_array({\"x\": 10, \"y\": 20}) AS v FROM demo",
            &empty
        ),
        json!([{"key": "x", "value": 10}, {"key": "y", "value": 20}])
    );
    assert_eq!(
        eval_one(
            "SELECT object_to_kvpair_array({\"x\": 10}) AS v FROM demo",
            &empty
        ),
        json!([{"key": "x", "value": 10}])
    );
    assert_eq!(
        eval_one(
            "SELECT kvpair_array_to_obj(obj_to_kvpair_array({\"x\": 10, \"y\": 20})) AS v FROM demo",
            &empty
        ),
        json!({"x": 10, "y": 20})
    );
    assert_eq!(
        eval_one("SELECT obj_to_kvpair_array(42) AS v FROM demo", &empty),
        Value::Null
    );

    // to_json / parse_json round-trip plus aliases and error paths.
    assert_eq!(
        eval_one(
            "SELECT to_json({\"msg\": \"hello\", \"code\": 200}) AS v FROM demo",
            &empty
        ),
        json!("{\"code\":200,\"msg\":\"hello\"}")
    );
    assert_eq!(
        eval_one("SELECT tojson(7) AS v FROM demo", &empty),
        json!("7")
    );
    assert_eq!(
        eval_one(
            "SELECT parse_json('{\"code\":200,\"msg\":\"hello\"}') AS v FROM demo",
            &empty
        ),
        json!({"code": 200, "msg": "hello"})
    );
    assert_eq!(
        eval_one("SELECT parsejson('[1, 2]') AS v FROM demo", &empty),
        json!([1, 2])
    );
    assert_eq!(
        eval_one("SELECT json_parse('invalid json{') AS v FROM demo", &empty),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT to_json(null) AS v FROM demo", &empty),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT parse_json(null) AS v FROM demo", &empty),
        Value::Null
    );
    // Structured values pass straight through parse_json.
    assert_eq!(
        eval_one("SELECT parse_json(42) AS v FROM demo", &empty),
        json!(42)
    );
}

// ---------------------------------------------------------------------------
// String, regex & encoding functions
// ---------------------------------------------------------------------------

#[test]
fn test_string_regex_encoding_parity() {
    let empty = empty();

    // regexp_matches: anchored and unanchored patterns, null propagation.
    assert_eq!(
        eval_one(
            "SELECT regexp_matches('device_123', '^[a-z]+_[0-9]+$') AS v FROM demo",
            &empty
        ),
        json!(true)
    );
    assert_eq!(
        eval_one(
            "SELECT regexp_matches('device_123', '^[0-9]+$') AS v FROM demo",
            &empty
        ),
        json!(false)
    );
    assert_eq!(
        eval_one(
            "SELECT regexp_matches('abc123', '[0-9]+') AS v FROM demo",
            &empty
        ),
        json!(true)
    );
    assert_eq!(
        eval_one("SELECT regexp_matches(null, 'abc') AS v FROM demo", &empty),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT regexp_matches('abc', null) AS v FROM demo", &empty),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT regexp_matches('abc', '([') AS v FROM demo", &empty),
        Value::Null
    );

    // regexp_replace: global substitution, no-match passthrough.
    assert_eq!(
        eval_one(
            "SELECT regexp_replace('a1 b2 c3', '[0-9]', 'X') AS v FROM demo",
            &empty
        ),
        json!("aX bX cX")
    );
    assert_eq!(
        eval_one(
            "SELECT regexp_replace('foo bar', 'missing', 'x') AS v FROM demo",
            &empty
        ),
        json!("foo bar")
    );
    assert_eq!(
        eval_one(
            "SELECT regexp_replace('aaa', 'a', 'bb') AS v FROM demo",
            &empty
        ),
        json!("bbbbbb")
    );
    assert_eq!(
        eval_one(
            "SELECT regexp_replace(null, 'a', 'b') AS v FROM demo",
            &empty
        ),
        Value::Null
    );

    // regexp_substring: capture group wins, else whole match, else null.
    assert_eq!(
        eval_one(
            "SELECT regexp_substring('device_001', 'device_([0-9]+)') AS v FROM demo",
            &empty
        ),
        json!("001")
    );
    assert_eq!(
        eval_one(
            "SELECT regexp_substring('temp: 36.5C', '[0-9]+\\.[0-9]+') AS v FROM demo",
            &empty
        ),
        json!("36.5")
    );
    assert_eq!(
        eval_one(
            "SELECT regexp_substring('abc', '[0-9]+') AS v FROM demo",
            &empty
        ),
        Value::Null
    );

    // split_value is 0-based: leading '/' yields an empty first segment.
    assert_eq!(
        eval_one(
            "SELECT split_value('/test/device001/message', '/', 0) AS v FROM demo",
            &empty
        ),
        json!("")
    );
    assert_eq!(
        eval_one(
            "SELECT split_value('/test/device001/message', '/', 1) AS v FROM demo",
            &empty
        ),
        json!("test")
    );
    assert_eq!(
        eval_one(
            "SELECT split_value('/test/device001/message', '/', 2) AS v FROM demo",
            &empty
        ),
        json!("device001")
    );
    assert_eq!(
        eval_one(
            "SELECT split_value('/test/device001/message', '/', 3) AS v FROM demo",
            &empty
        ),
        json!("message")
    );
    assert_eq!(
        eval_one(
            "SELECT split_value('/test/device001/message', '/', 99) AS v FROM demo",
            &empty
        ),
        Value::Null
    );
    assert_eq!(
        eval_one(
            "SELECT split_value('/test/device001/message', '/', -1) AS v FROM demo",
            &empty
        ),
        json!("message")
    );
    assert_eq!(
        eval_one("SELECT split_value('a,b,c', ',', 1) AS v FROM demo", &empty),
        json!("b")
    );

    // numbytes counts UTF-8 bytes, not chars.
    assert_eq!(
        eval_one("SELECT numbytes('hello') AS v FROM demo", &empty),
        json!(5)
    );
    assert_eq!(
        eval_one("SELECT numbytes('你好世界') AS v FROM demo", &empty),
        json!(12)
    );
    assert_eq!(
        eval_one("SELECT numbytes('🚀') AS v FROM demo", &empty),
        json!(4)
    );
    assert_eq!(
        eval_one("SELECT numbytes('') AS v FROM demo", &empty),
        json!(0)
    );
    assert_eq!(
        eval_one("SELECT numbytes(null) AS v FROM demo", &empty),
        Value::Null
    );

    // chr maps code points (incl. non-ASCII) to single-char strings.
    assert_eq!(
        eval_one("SELECT chr(65) AS v FROM demo", &empty),
        json!("A")
    );
    assert_eq!(
        eval_one("SELECT chr(97) AS v FROM demo", &empty),
        json!("a")
    );
    assert_eq!(
        eval_one("SELECT chr(8364) AS v FROM demo", &empty),
        json!("€")
    );
    assert_eq!(
        eval_one("SELECT chr(128640) AS v FROM demo", &empty),
        json!("🚀")
    );
    assert_eq!(
        eval_one("SELECT chr(-1) AS v FROM demo", &empty),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT chr(1114112) AS v FROM demo", &empty),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT chr(null) AS v FROM demo", &empty),
        Value::Null
    );

    // trunc cuts toward zero; whole results come back integral.
    assert_eq!(
        eval_one("SELECT trunc(123.456, 2) AS v FROM demo", &empty),
        json!(123.45)
    );
    assert_eq!(
        eval_one("SELECT trunc(123.456, 0) AS v FROM demo", &empty),
        json!(123)
    );
    assert_eq!(
        eval_one("SELECT trunc(-123.456, 2) AS v FROM demo", &empty),
        json!(-123.45)
    );
    assert_eq!(
        eval_one("SELECT trunc(123.456) AS v FROM demo", &empty),
        json!(123)
    );
    assert_eq!(
        eval_one("SELECT trunc(123.999, 0) AS v FROM demo", &empty),
        json!(123)
    );
    assert_eq!(
        eval_one("SELECT trunc(null, 2) AS v FROM demo", &empty),
        Value::Null
    );

    // hex2dec / dec2hex round-trip.
    assert_eq!(
        eval_one("SELECT hex2dec('0x1A') AS v FROM demo", &empty),
        json!(26)
    );
    assert_eq!(
        eval_one("SELECT hex2dec('1A') AS v FROM demo", &empty),
        json!(26)
    );
    assert_eq!(
        eval_one("SELECT hex2dec('ff') AS v FROM demo", &empty),
        json!(255)
    );
    assert_eq!(
        eval_one("SELECT dec2hex(26) AS v FROM demo", &empty),
        json!("0x1a")
    );
    assert_eq!(
        eval_one("SELECT dec2hex(16) AS v FROM demo", &empty),
        json!("0x10")
    );
    assert_eq!(
        eval_one("SELECT dec2hex(0) AS v FROM demo", &empty),
        json!("0x0")
    );
    assert_eq!(
        eval_one("SELECT dec2hex(-26) AS v FROM demo", &empty),
        json!("-0x1a")
    );
    assert_eq!(
        eval_one("SELECT hex2dec(dec2hex(255)) AS v FROM demo", &empty),
        json!(255)
    );
    assert_eq!(
        eval_one("SELECT hex2dec('zz') AS v FROM demo", &empty),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT dec2hex('abc') AS v FROM demo", &empty),
        Value::Null
    );

    // crc32 IEEE vectors (hex string).
    assert_eq!(
        eval_one("SELECT crc32('123456789') AS v FROM demo", &empty),
        json!("cbf43926")
    );
    assert_eq!(
        eval_one("SELECT crc32('') AS v FROM demo", &empty),
        json!("0")
    );
    assert_eq!(
        eval_one("SELECT crc32(null) AS v FROM demo", &empty),
        Value::Null
    );

    // sha1 / sha384 vectors (verified against Python hashlib).
    assert_eq!(
        eval_one("SELECT sha1('hello world') AS v FROM demo", &empty),
        json!("2aae6c35c94fcfb415dbe95f408b9ce91ee846ed")
    );
    assert_eq!(
        eval_one(
            "SELECT sha384('hello world') AS v FROM demo",
            &empty
        ),
        json!("fdbd8e75a67f29f701a4e040385e2e23986303ea10239211af907fcbb83578b3e417cb71ce646efd0819dd8c088de1bd")
    );
    assert_eq!(
        eval_one("SELECT sha1(null) AS v FROM demo", &empty),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT sha384(null) AS v FROM demo", &empty),
        Value::Null
    );
}

// ---------------------------------------------------------------------------
// Statistical & window aggregate functions
// ---------------------------------------------------------------------------

/// N = 8, sum = 40, mean = 5.0, sum-of-squared-deviations = 32.
fn stats_batch() -> Vec<Record> {
    [2, 4, 4, 4, 5, 5, 7, 9]
        .iter()
        .map(|x| rec(&[("x", json!(x))]))
        .collect()
}

#[test]
fn test_statistical_window_aggregates_parity() {
    let rows = stats_batch();

    // Central tendency and dispersion (population vs sample).
    assert_eq!(
        eval_agg_one("SELECT median(x) AS v FROM demo", &rows),
        json!(4.5)
    );
    assert_eq!(
        eval_agg_one("SELECT stddev(x) AS v FROM demo", &rows),
        json!(2.0)
    );
    assert_eq!(
        eval_agg_one("SELECT var(x) AS v FROM demo", &rows),
        json!(4.0)
    );
    assert_eq!(
        eval_agg_one("SELECT stddevs(x) AS v FROM demo", &rows),
        json!(2.138089935299395)
    );
    assert_eq!(
        eval_agg_one("SELECT vars(x) AS v FROM demo", &rows),
        json!(4.571428571428571)
    );
    // Odd counts keep the middle element untouched.
    let odd = vec![
        rec(&[("x", json!(1))]),
        rec(&[("x", json!(3))]),
        rec(&[("x", json!(5))]),
    ];
    assert_eq!(
        eval_agg_one("SELECT median(x) AS v FROM demo", &odd),
        json!(3)
    );
    // Single-point populations are exact zeros.
    let one = vec![rec(&[("x", json!(7))])];
    assert_eq!(
        eval_agg_one("SELECT stddev(x) AS v FROM demo", &one),
        json!(0.0)
    );
    assert_eq!(
        eval_agg_one("SELECT var(x) AS v FROM demo", &one),
        json!(0.0)
    );
    // Samples need at least two points.
    assert_eq!(
        eval_agg_one("SELECT stddevs(x) AS v FROM demo", &one),
        Value::Null
    );
    assert_eq!(
        eval_agg_one("SELECT vars(x) AS v FROM demo", &one),
        Value::Null
    );

    // Continuous percentiles interpolate; discrete picks exact elements.
    assert_eq!(
        eval_agg_one("SELECT percentile(x, 0.5) AS v FROM demo", &rows),
        json!(4.5)
    );
    assert_eq!(
        eval_agg_one("SELECT percentile(x, 0.0) AS v FROM demo", &rows),
        json!(2.0)
    );
    assert_eq!(
        eval_agg_one("SELECT percentile(x, 1.0) AS v FROM demo", &rows),
        json!(9.0)
    );
    assert_eq!(
        eval_agg_one("SELECT percentile(x, 0.25) AS v FROM demo", &rows),
        json!(4.0)
    );
    assert_eq!(
        eval_agg_one("SELECT percentile_disc(x, 0.5) AS v FROM demo", &rows),
        json!(4)
    );
    assert_eq!(
        eval_agg_one("SELECT percentile_disc(x, 0.0) AS v FROM demo", &rows),
        json!(2)
    );
    assert_eq!(
        eval_agg_one("SELECT percentile_disc(x, 1.0) AS v FROM demo", &rows),
        json!(9)
    );
    assert_eq!(
        eval_agg_one("SELECT percentile(x, 1.5) AS v FROM demo", &rows),
        Value::Null
    );
    assert_eq!(
        eval_agg_one("SELECT percentile(x, -0.1) AS v FROM demo", &rows),
        Value::Null
    );
    assert_eq!(
        eval_agg_one("SELECT percentile_disc(x, 2) AS v FROM demo", &rows),
        Value::Null
    );

    // last_value with and without null-skipping.
    let history = vec![
        rec(&[("a", json!(10)), ("b", json!("first"))]),
        rec(&[("a", Value::Null), ("b", json!("second"))]),
        rec(&[("a", json!(30)), ("b", Value::Null)]),
    ];
    assert_eq!(
        eval_agg_one("SELECT last_value(a, false) AS v FROM demo", &history),
        json!(30)
    );
    assert_eq!(
        eval_agg_one("SELECT last_value(b, false) AS v FROM demo", &history),
        Value::Null
    );
    assert_eq!(
        eval_agg_one("SELECT last_value(b, true) AS v FROM demo", &history),
        json!("second")
    );
    assert_eq!(
        eval_agg_one("SELECT last_value(a) AS v FROM demo", &history),
        json!(30)
    );
    assert_eq!(
        eval_agg_one("SELECT last_value(*) AS v FROM demo", &history),
        json!({"a": 30, "b": Value::Null})
    );

    // merge_agg combines maps left to right; non-objects are skipped.
    let maps = vec![
        rec(&[("sub", json!({"k1": 1, "k2": 2})), ("val", json!(10))]),
        rec(&[("sub", json!({"k2": 99, "k3": 3})), ("val", json!(20))]),
    ];
    assert_eq!(
        eval_agg_one("SELECT merge_agg(sub) AS v FROM demo", &maps),
        json!({"k1": 1, "k2": 99, "k3": 3})
    );
    assert_eq!(
        eval_agg_one("SELECT merge_agg(val) AS v FROM demo", &maps),
        json!({})
    );
    assert_eq!(
        eval_agg_one("SELECT merge_agg(*) AS v FROM demo", &maps),
        json!({"sub": {"k2": 99, "k3": 3}, "val": 20})
    );

    // row_number: 1 per batch, sequential per stream, partitioned per key.
    assert_eq!(
        eval_agg_one("SELECT row_number() AS v FROM demo", &rows),
        json!(1)
    );
    assert_eq!(
        eval_one("SELECT row_number() AS v FROM demo", &empty()),
        json!(1)
    );
    let mut parser = Parser::new("SELECT row_number() AS n FROM demo");
    let stmt = parser.parse_select().expect("Should parse");
    let state = RuleState::default();
    for expected in [1, 2, 3] {
        let out = Evaluator::eval_select_stateful(&stmt, &empty(), &state).expect("row projects");
        assert_eq!(out.get("n"), Some(&json!(expected)));
    }
    let mut parser = Parser::new("SELECT row_number() OVER (PARTITION BY dev) AS n FROM demo");
    let stmt = parser.parse_select().expect("Should parse");
    let state = RuleState::default();
    for (dev, expected) in [("a", 1), ("a", 2), ("b", 1), ("a", 3), ("b", 2)] {
        let out = Evaluator::eval_select_stateful(&stmt, &rec(&[("dev", json!(dev))]), &state)
            .expect("row projects");
        assert_eq!(out.get("n"), Some(&json!(expected)), "dev {}", dev);
    }

    // Empty batches and all-null columns degrade gracefully.
    let empty_batch: Vec<Record> = vec![];
    for sql in [
        "SELECT median(x) AS v FROM demo",
        "SELECT stddev(x) AS v FROM demo",
        "SELECT stddevs(x) AS v FROM demo",
        "SELECT var(x) AS v FROM demo",
        "SELECT vars(x) AS v FROM demo",
        "SELECT percentile(x, 0.5) AS v FROM demo",
        "SELECT percentile_disc(x, 0.5) AS v FROM demo",
        "SELECT latest(x) AS v FROM demo",
    ] {
        assert_eq!(eval_agg_one(sql, &empty_batch), Value::Null, "{}", sql);
    }
    assert_eq!(
        eval_agg_one("SELECT collect(x) AS v FROM demo", &empty_batch),
        json!([])
    );
    assert_eq!(
        eval_agg_one("SELECT merge_agg(x) AS v FROM demo", &empty_batch),
        json!({})
    );
    assert_eq!(
        eval_agg_one("SELECT row_number() AS v FROM demo", &empty_batch),
        json!(1)
    );
    let nulls = vec![rec(&[("x", Value::Null)]), rec(&[("x", Value::Null)])];
    assert_eq!(
        eval_agg_one("SELECT median(x) AS v FROM demo", &nulls),
        Value::Null
    );
    assert_eq!(
        eval_agg_one("SELECT collect(x) AS v FROM demo", &nulls),
        json!([])
    );
}

#[test]
fn test_newly_implemented_functions() {
    rekuiper_core::set_keyed_state("sensor_cal", "100");

    assert_eq!(
        eval_one(
            "SELECT indexof('hello world', 'world') AS v FROM demo",
            &empty()
        ),
        json!(6)
    );
    assert_eq!(
        eval_one(
            "SELECT indexof('hello world', 'xyz') AS v FROM demo",
            &empty()
        ),
        json!(-1)
    );
    assert_eq!(
        eval_one("SELECT format(1234567.891, 2) AS v FROM demo", &empty()),
        json!("1234567.89")
    );
    assert_eq!(
        eval_one(
            "SELECT format(1234567.891, 2, 'en_US') AS v FROM demo",
            &empty()
        ),
        json!("1,234,567.89")
    );
    assert_eq!(
        eval_one(
            "SELECT format_time(1704153600000, 'yyyy-MM-dd') AS v FROM demo",
            &empty()
        ),
        json!("2024-01-02")
    );
    assert_eq!(
        eval_one(
            "SELECT array_last_position([10, 20, 30, 20, 50], 20) AS v FROM demo",
            &empty()
        ),
        json!(3)
    );
    assert_eq!(
        eval_one(
            "SELECT array_map('abs', [-10, 20, -30]) AS v FROM demo",
            &empty()
        ),
        json!([10, 20, 30])
    );
    let shuffled = eval_one(
        "SELECT array_shuffle([1, 2, 3, 4, 5]) AS v FROM demo",
        &empty(),
    );
    assert_eq!(shuffled.as_array().map(|a| a.len()), Some(5));

    assert_eq!(
        eval_one(
            "SELECT get_keyed_state('sensor_cal', 'bigint', 0) AS v FROM demo",
            &empty()
        ),
        json!(100)
    );
    assert_eq!(
        eval_one(
            "SELECT get_keyed_state('missing_key', 'string', 'default_val') AS v FROM demo",
            &empty()
        ),
        json!("default_val")
    );

    // Contextual / stateful tests
    let mut parser = Parser::new("SELECT last_hit_count() AS c, rule_start() AS rs FROM demo");
    let stmt = parser.parse_select().expect("parse");
    let state = RuleState::default();
    let row1 = Evaluator::eval_select_stateful(&stmt, &empty(), &state).expect("eval");
    assert_eq!(row1.get("c"), Some(&json!(0)));
    let row2 = Evaluator::eval_select_stateful(&stmt, &empty(), &state).expect("eval");
    assert_eq!(row2.get("c"), Some(&json!(1)));

    // Aggregate tests
    let batch = vec![rec(&[("x", json!(1))])];
    let agg1 = eval_agg_one("SELECT last_agg_hit_count() AS v FROM demo", &batch);
    assert_eq!(agg1, json!(0));
    let agg2 = eval_agg_one("SELECT last_agg_hit_count() AS v FROM demo", &batch);
    assert_eq!(agg2, json!(1));
}

#[test]
fn test_newly_implemented_functions_all() {
    // 1. Object functions
    assert_eq!(
        eval_one("SELECT object(['a', 'b'], [1, 2]) AS v FROM demo", &empty()),
        json!({"a": 1, "b": 2})
    );
    assert_eq!(
        eval_one("SELECT zip([['a', 1], ['b', 2]]) AS v FROM demo", &empty()),
        json!({"a": 1, "b": 2})
    );
    let items_res = eval_one("SELECT items(object(['a'], [1])) AS v FROM demo", &empty());
    assert_eq!(items_res, json!([["a", 1]]));

    // 2. Delay
    assert_eq!(
        eval_one("SELECT delay(1, 'done') AS v FROM demo", &empty()),
        json!("done")
    );

    // 3. Date diff (2-arg and 3-arg)
    let t1 = 1_704_067_200_000i64; // 2024-01-01 00:00:00 UTC
    let t2 = t1 + 86_400_000 * 3 + 3_600_000 * 2; // 3 days and 2 hours later
    let rec_dates = rec(&[("t1", json!(t1)), ("t2", json!(t2))]);
    assert_eq!(
        eval_one("SELECT date_diff(t1, t2) AS v FROM demo", &rec_dates),
        json!(3)
    );
    assert_eq!(
        eval_one(
            "SELECT date_diff('hour', t1, t2) AS v FROM demo",
            &rec_dates
        ),
        json!(74)
    );

    // 4. Date calc
    assert_eq!(
        eval_one(
            "SELECT date_calc(1704067200000, '1h30m') AS v FROM demo",
            &empty()
        ),
        json!(1704067200000i64 + 90 * 60 * 1000)
    );
    assert_eq!(
        eval_one(
            "SELECT date_calc(1704067200000, '-2d') AS v FROM demo",
            &empty()
        ),
        json!(1704067200000i64 - 2 * 86_400_000)
    );

    // 5. Convert TZ
    assert_eq!(
        eval_one(
            "SELECT convert_tz('2024-01-01 12:00:00', 'Asia/Shanghai') AS v FROM demo",
            &empty()
        ),
        json!("2024-01-01 20:00:00")
    );
    assert_eq!(
        eval_one(
            "SELECT convert_tz('2024-01-01 12:00:00', 'UTC') AS v FROM demo",
            &empty()
        ),
        json!("2024-01-01 12:00:00")
    );

    // 6. Compress and decompress roundtrips
    for method in &["zlib", "gzip", "flate", "zstd"] {
        let compressed = eval_one(
            &format!(
                "SELECT compress('hello rekuiper', '{}') AS v FROM demo",
                method
            ),
            &empty(),
        );
        assert!(compressed.is_string());
        let decompressed = eval_one(
            &format!(
                "SELECT decompress('{}', '{}') AS v FROM demo",
                compressed.as_str().unwrap(),
                method
            ),
            &empty(),
        );
        assert_eq!(decompressed, json!("hello rekuiper"));
    }

    // 7. Acc_collect stateful
    let state = RuleState::default();
    let mut parser = Parser::new("SELECT acc_collect(x) AS v FROM demo");
    let stmt = parser.parse_select().expect("parse");
    let r1 = Evaluator::eval_select_stateful(&stmt, &rec(&[("x", json!(10))]), &state).unwrap();
    assert_eq!(r1.get("v"), Some(&json!([10])));
    let r2 = Evaluator::eval_select_stateful(&stmt, &rec(&[("x", json!(20))]), &state).unwrap();
    assert_eq!(r2.get("v"), Some(&json!([10, 20])));
    let r3 = Evaluator::eval_select_stateful(&stmt, &rec(&[("x", Value::Null)]), &state).unwrap();
    assert_eq!(r3.get("v"), Some(&json!([10, 20])));

    // 8. Extract
    let mut parser_ext = Parser::new("SELECT extract(info) FROM demo");
    let stmt_ext = parser_ext.parse_select().expect("parse");
    let row_ext = Evaluator::eval_select_stateful(
        &stmt_ext,
        &rec(&[("info", json!({"k1": "v1", "k2": "v2"}))]),
        &state,
    )
    .unwrap();
    assert_eq!(row_ext.get("k1"), Some(&json!("v1")));
    assert_eq!(row_ext.get("k2"), Some(&json!("v2")));

    // 9. Changed_cols multi-row/column projection
    let state_cc = RuleState::default();
    let mut parser_cc = Parser::new("SELECT changed_cols('diff_', true, v1, v2) FROM demo");
    let stmt_cc = parser_cc.parse_select().expect("parse");
    let row_cc1 = Evaluator::eval_select_stateful(
        &stmt_cc,
        &rec(&[("v1", json!(100)), ("v2", json!(200))]),
        &state_cc,
    )
    .unwrap();
    assert_eq!(row_cc1.get("diff_v1"), Some(&json!(100)));
    assert_eq!(row_cc1.get("diff_v2"), Some(&json!(200)));
    // Second run with only v2 changed
    let row_cc2 = Evaluator::eval_select_stateful(
        &stmt_cc,
        &rec(&[("v1", json!(100)), ("v2", json!(250))]),
        &state_cc,
    )
    .unwrap();
    assert_eq!(row_cc2.get("diff_v1"), None);
    assert_eq!(row_cc2.get("diff_v2"), Some(&json!(250)));
}

#[test]
fn test_vector_similarity_and_search() {
    let empty = rec(&[]);
    // 1. Cosine similarity
    let r = eval_one(
        "SELECT cosine_similarity([1.0, 0.0], [1.0, 0.0]) AS v FROM demo",
        &empty,
    );
    assert!((r.as_f64().unwrap() - 1.0).abs() < 1e-6);

    let r = eval_one(
        "SELECT cosine_similarity([1.0, 0.0], [0.0, 1.0]) AS v FROM demo",
        &empty,
    );
    assert!((r.as_f64().unwrap() - 0.0).abs() < 1e-6);

    let r = eval_one(
        "SELECT cosine_similarity([1.0, 0.0], [-1.0, 0.0]) AS v FROM demo",
        &empty,
    );
    assert!((r.as_f64().unwrap() - (-1.0)).abs() < 1e-6);

    // 2. Vector L2 (Euclidean distance): sqrt(3^2 + 4^2) = 5.0
    let r = eval_one(
        "SELECT vector_l2([1.0, 2.0], [4.0, 6.0]) AS v FROM demo",
        &empty,
    );
    assert!((r.as_f64().unwrap() - 5.0).abs() < 1e-6);

    // 3. Vector dot product: 1*3 + 2*4 = 11.0
    let r = eval_one(
        "SELECT vector_dot([1.0, 2.0], [3.0, 4.0]) AS v FROM demo",
        &empty,
    );
    assert!((r.as_f64().unwrap() - 11.0).abs() < 1e-6);

    // 4. Vector match: top-k search
    let r = eval_one(
        "SELECT vector_match([1.0, 0.0], [[0.0, 1.0], [0.9, 0.1], [-1.0, 0.0]], 2) AS v FROM demo",
        &empty,
    );
    let arr = r.as_array().expect("array of matches");
    assert_eq!(arr.len(), 2);
    let top1 = &arr[0];
    let top1_sim = top1.get("similarity").unwrap().as_f64().unwrap();
    assert!(top1_sim > 0.95);
}

#[test]
fn test_wasm_function_plugin_sql_execution() {
    let wasm_binary = vec![
        0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, // Magic + version
        0x01, 0x07, 0x01, 0x60, 0x02, 0x7f, 0x7f, 0x01, 0x7f, // Type: (i32, i32) -> i32
        0x03, 0x02, 0x01, 0x00, // Function index 0
        0x07, 0x07, 0x01, 0x03, 0x61, 0x64, 0x64, 0x00, 0x00, // Export "add"
        0x0a, 0x09, 0x01, 0x07, 0x00, 0x20, 0x00, 0x20, 0x01, 0x6a, 0x0b, // Code: add
    ];

    rekuiper_core::get_global_wasm_registry()
        .register_module_as_udfs("math_wasm", &wasm_binary)
        .expect("register wasm module");

    let empty = rec(&[]);

    // 1. Direct call via wasm_run built-in function
    let r1 = eval_one(
        "SELECT wasm_run('math_wasm', 'add', 40, 2) AS v FROM demo",
        &empty,
    );
    assert_eq!(r1, json!(42));

    // 2. Call via registered UDF name
    let r2 = eval_one("SELECT add(15, 25) AS v FROM demo", &empty);
    assert_eq!(r2, json!(40));
}

#[test]
fn test_array_positions() {
    let empty = rec(&[]);
    // Matching indexes: [1, 2, 2, 3, 2] target 2 -> [1, 2, 4] (0-based)
    let r1 = eval_one(
        "SELECT array_positions([1, 2, 2, 3, 2], 2) AS v FROM demo",
        &empty,
    );
    assert_eq!(r1, json!([1, 2, 4]));

    // No match -> empty array
    let r2 = eval_one(
        "SELECT array_positions([1, 2, 3], 99) AS v FROM demo",
        &empty,
    );
    assert_eq!(r2, json!([]));

    // String elements
    let r3 = eval_one(
        "SELECT array_positions(['a', 'b', 'a'], 'a') AS v FROM demo",
        &empty,
    );
    assert_eq!(r3, json!([0, 2]));

    // Null input
    let r4 = eval_one("SELECT array_positions(null, 1) AS v FROM demo", &empty);
    assert_eq!(r4, json!([]));
}

#[test]
fn test_acc_distinct_collect_and_alias() {
    let mut parser = Parser::new("SELECT acc_distinct_collect(val) AS v FROM demo");
    let stmt = parser.parse_select().expect("parse acc_distinct_collect");
    let state = RuleState::default();

    let step = |v: serde_json::Value| {
        let mut r = HashMap::new();
        r.insert("val".to_string(), v);
        Evaluator::eval_select_stateful(&stmt, &r, &state)
            .expect("eval")
            .shift_remove("v")
            .unwrap()
    };

    assert_eq!(step(json!(1)), json!([1]));
    assert_eq!(step(json!(2)), json!([1, 2]));
    assert_eq!(step(json!(2)), json!([1, 2])); // duplicate ignored
    assert_eq!(step(json!(3)), json!([1, 2, 3]));
    assert_eq!(step(serde_json::Value::Null), json!([1, 2, 3])); // null ignored
    assert_eq!(step(json!(1)), json!([1, 2, 3])); // duplicate ignored

    // Test alias distinct_acc
    let mut parser_alias = Parser::new("SELECT distinct_acc(val) AS v FROM demo");
    let stmt_alias = parser_alias.parse_select().expect("parse distinct_acc");
    let state_alias = RuleState::default();

    let step_alias = |v: serde_json::Value| {
        let mut r = HashMap::new();
        r.insert("val".to_string(), v);
        Evaluator::eval_select_stateful(&stmt_alias, &r, &state_alias)
            .expect("eval")
            .shift_remove("v")
            .unwrap()
    };

    assert_eq!(step_alias(json!("apple")), json!(["apple"]));
    assert_eq!(step_alias(json!("banana")), json!(["apple", "banana"]));
    assert_eq!(step_alias(json!("apple")), json!(["apple", "banana"]));
}

#[test]
fn test_lead_with_ignore_null() {
    let rows = vec![
        rec(&[("temp", json!(10))]),
        rec(&[("temp", serde_json::Value::Null)]),
        rec(&[("temp", json!(20))]),
        rec(&[("temp", json!(30))]),
    ];

    // ignoreNull = true (default): skips null row, so offset 1 from row 0 is 20
    assert_eq!(
        eval_agg_one("SELECT lead(temp, 1, -1, true) AS v FROM demo", &rows),
        json!(20)
    );

    // ignoreNull = false: counts raw rows, so offset 1 is null
    assert_eq!(
        eval_agg_one("SELECT lead(temp, 1, -1, false) AS v FROM demo", &rows),
        serde_json::Value::Null
    );

    // Out of bounds with default
    assert_eq!(
        eval_agg_one("SELECT lead(temp, 10, -1, true) AS v FROM demo", &rows),
        json!(-1)
    );
}

#[test]
fn test_issue26_sql_functions_parity() {
    let empty_rec = empty();

    // 1. now([format])
    let now_default = eval_one("SELECT now() AS v FROM demo", &empty_rec);
    let s_def = now_default.as_str().expect("now() string");
    assert_eq!(s_def.len(), 19);

    let now_fmt1 = eval_one("SELECT now('YYYY-MM-DD') AS v FROM demo", &empty_rec);
    let s_fmt1 = now_fmt1.as_str().expect("now(fmt) string");
    assert_eq!(s_fmt1.len(), 10);
    assert_eq!(&s_fmt1[4..5], "-");
    assert_eq!(&s_fmt1[7..8], "-");

    let now_fmt2 = eval_one("SELECT now('%Y/%m/%d') AS v FROM demo", &empty_rec);
    let s_fmt2 = now_fmt2.as_str().expect("now(fmt) string");
    assert_eq!(s_fmt2.len(), 10);
    assert_eq!(&s_fmt2[4..5], "/");

    assert_eq!(
        eval_one("SELECT now(1) AS v FROM demo", &empty_rec),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT now(null) AS v FROM demo", &empty_rec),
        Value::Null
    );

    // current_timestamp and local_timestamp optional format
    let cur_ts_fmt = eval_one(
        "SELECT current_timestamp('YYYY-MM-DD') AS v FROM demo",
        &empty_rec,
    );
    assert_eq!(cur_ts_fmt.as_str().unwrap().len(), 10);

    // 2. to_seconds(ts)
    assert_eq!(
        eval_one(
            "SELECT to_seconds('2024-01-01T00:00:00Z') AS v FROM demo",
            &empty_rec
        ),
        json!(1704067200)
    );
    assert_eq!(
        eval_one(
            "SELECT to_seconds('2024-01-01 00:00:00') AS v FROM demo",
            &empty_rec
        ),
        json!(1704067200)
    );
    assert_eq!(
        eval_one(
            "SELECT to_seconds('2024-01-01 00:00:00.500') AS v FROM demo",
            &empty_rec
        ),
        json!(1704067200)
    );
    assert_eq!(
        eval_one(
            "SELECT to_seconds(1704067200000) AS v FROM demo",
            &empty_rec
        ),
        json!(1704067200)
    );
    assert_eq!(
        eval_one("SELECT to_seconds(null) AS v FROM demo", &empty_rec),
        Value::Null
    );

    // 3. cast(expr, 'datetime') and cast(expr, 'timestamp')
    assert_eq!(
        eval_one(
            "SELECT cast(1700000000000, 'datetime') AS v FROM demo",
            &empty_rec
        ),
        json!("2023-11-14T22:13:20Z")
    );
    assert_eq!(
        eval_one(
            "SELECT cast(1700000000000, 'timestamp') AS v FROM demo",
            &empty_rec
        ),
        json!("2023-11-14T22:13:20Z")
    );
    assert_eq!(
        eval_one(
            "SELECT cast('2023-11-14 22:13:20', 'datetime') AS v FROM demo",
            &empty_rec
        ),
        json!("2023-11-14T22:13:20Z")
    );

    // 4. element_at(arr, index) and element_at(obj, key)
    assert_eq!(
        eval_one(
            "SELECT element_at([10, 20, 30], 0) AS v FROM demo",
            &empty_rec
        ),
        json!(10)
    );
    assert_eq!(
        eval_one(
            "SELECT element_at([10, 20, 30], 1) AS v FROM demo",
            &empty_rec
        ),
        json!(20)
    );
    assert_eq!(
        eval_one(
            "SELECT element_at([10, 20, 30], -1) AS v FROM demo",
            &empty_rec
        ),
        json!(30)
    );
    let mut obj_rec = empty();
    obj_rec.insert("m".to_string(), json!({"a": 1, "b": "hello"}));
    assert_eq!(
        eval_one("SELECT element_at(m, 'a') AS v FROM demo", &obj_rec),
        json!(1)
    );
    assert_eq!(
        eval_one("SELECT element_at(m, 'b') AS v FROM demo", &obj_rec),
        json!("hello")
    );
    assert_eq!(
        eval_one(
            "SELECT element_at(m, 'nonexistent') AS v FROM demo",
            &obj_rec
        ),
        Value::Null
    );

    // 5. min and max on strings
    let str_rows = vec![
        rec(&[("val", json!("v2"))]),
        rec(&[("val", json!("v1"))]),
        rec(&[("val", json!("v3"))]),
    ];
    assert_eq!(
        eval_agg_one("SELECT min(val) AS v FROM demo", &str_rows),
        json!("v1")
    );
    assert_eq!(
        eval_agg_one("SELECT max(val) AS v FROM demo", &str_rows),
        json!("v3")
    );
    assert_eq!(
        eval_agg_one("SELECT concat(max(val), '!') AS v FROM demo", &str_rows),
        json!("v3!")
    );

    // 6. log(x), ln(x), log2(x)
    assert_eq!(
        eval_one("SELECT log(100) AS v FROM demo", &empty_rec),
        json!(2.0)
    );
    assert_eq!(
        eval_one("SELECT log(10, 1000) AS v FROM demo", &empty_rec),
        json!(3.0)
    );
    assert_eq!(
        eval_one("SELECT log(2, 8) AS v FROM demo", &empty_rec),
        json!(3.0)
    );
    assert_eq!(
        eval_one("SELECT ln(1) AS v FROM demo", &empty_rec),
        json!(0.0)
    );
    assert_eq!(
        eval_one("SELECT log2(8) AS v FROM demo", &empty_rec),
        json!(3.0)
    );
    assert_eq!(
        eval_one("SELECT log(0) AS v FROM demo", &empty_rec),
        Value::Null
    );
    assert_eq!(
        eval_one("SELECT log(-10) AS v FROM demo", &empty_rec),
        Value::Null
    );

    // 7. latest with default value
    let null_rows = vec![rec(&[("temp", Value::Null)]), rec(&[("temp", Value::Null)])];
    assert_eq!(
        eval_agg_one("SELECT latest(temp, 99) AS v FROM demo", &null_rows),
        json!(99)
    );
    let mixed_rows = vec![rec(&[("temp", json!(42))]), rec(&[("temp", Value::Null)])];
    assert_eq!(
        eval_agg_one("SELECT latest(temp, 99) AS v FROM demo", &mixed_rows),
        json!(42)
    );
}

#[test]
fn test_issue26_lag_with_ignore_null() {
    let mut state = RuleState::default();
    let step = |val: Value, state: &mut RuleState| -> Value {
        let mut r = empty();
        r.insert("temp".to_string(), val);
        let mut parser = Parser::new("SELECT lag(temp, 1, -1, true) AS v FROM demo");
        let stmt = parser.parse_select().unwrap();
        Evaluator::eval_select_stateful(&stmt, &r, state)
            .unwrap()
            .shift_remove("v")
            .unwrap()
    };

    // First row: no previous, returns default -1
    assert_eq!(step(json!(10), &mut state), json!(-1));
    // Second row is null: ignore_null=true ignores it from history, returns previous non-null 10
    assert_eq!(step(Value::Null, &mut state), json!(10));
    // Third row is 20: previous was 10 (null was skipped)
    assert_eq!(step(json!(20), &mut state), json!(10));
    // Fourth row is 30: previous was 20
    assert_eq!(step(json!(30), &mut state), json!(20));
}
