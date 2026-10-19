use rekuiper_sql::{Evaluator, Parser};
use serde_json::{json, Value};
use std::collections::HashMap;

type Record = HashMap<String, Value>;

fn eval_expr(sql_expr: &str, record: &Record) -> Value {
    let sql = format!("SELECT {} AS v FROM src", sql_expr);
    let mut parser = Parser::new(&sql);
    let stmt = parser
        .parse_select()
        .unwrap_or_else(|e| panic!("Failed to parse {}: {}", sql, e));
    match Evaluator::eval_select(&stmt, record) {
        Some(mut row) => row.shift_remove("v").unwrap_or(Value::Null),
        None => Value::Null,
    }
}

#[test]
fn test_validate_all_missing_functions() {
    let empty = HashMap::new();

    // 1. Math: log2(x)
    let res = eval_expr("log2(8)", &empty);
    println!("VALIDATED: log2(8) = {:?}", res);
    assert_eq!(res, json!(3.0));

    let res_1024 = eval_expr("log2(1024)", &empty);
    println!("VALIDATED: log2(1024) = {:?}", res_1024);
    assert_eq!(res_1024, json!(10.0));

    // 2. Math: log10(x)
    let res = eval_expr("log10(100)", &empty);
    println!("VALIDATED: log10(100) = {:?}", res);
    assert_eq!(res, json!(2.0));

    let res_1m = eval_expr("log10(1000000)", &empty);
    println!("VALIDATED: log10(1000000) = {:?}", res_1m);
    assert_eq!(res_1m, json!(6.0));

    // 3. Array / Object: array_length(arr)
    let mut row_arr = HashMap::new();
    row_arr.insert("arr".to_string(), json!([10, 20, 30, 40]));
    let res = eval_expr("array_length(arr)", &row_arr);
    println!("VALIDATED: array_length([10, 20, 30, 40]) = {:?}", res);
    assert_eq!(res, json!(4));

    // 4. Array / Object: array_slice(arr, start, end) - 1-based indexing
    let res = eval_expr("array_slice(arr, 2, 3)", &row_arr);
    println!("VALIDATED: array_slice([10, 20, 30, 40], 2, 3) = {:?}", res);
    assert_eq!(res, json!([20, 30]));

    // 5. Array / Object: deduplicate(arr)
    let mut row_dup = HashMap::new();
    row_dup.insert("items".to_string(), json!([1, 2, 2, 3, 1, 4]));
    let res = eval_expr("deduplicate(items)", &row_dup);
    println!("VALIDATED: deduplicate([1, 2, 2, 3, 1, 4]) = {:?}", res);
    assert_eq!(res, json!([1, 2, 3, 4]));

    // 6. Object: object_erase(obj, key)
    let mut row_obj = HashMap::new();
    row_obj.insert(
        "dev".to_string(),
        json!({"id": "d1", "temp": 25.4, "status": "active"}),
    );
    let res = eval_expr("object_erase(dev, 'status')", &row_obj);
    println!("VALIDATED: object_erase(dev, 'status') = {:?}", res);
    assert_eq!(res, json!({"id": "d1", "temp": 25.4}));

    // 7. Object: object_to_kvpair_array(obj)
    let mut row_kv = HashMap::new();
    row_kv.insert("kv_obj".to_string(), json!({"a": 1, "b": 2}));
    let res = eval_expr("object_to_kvpair_array(kv_obj)", &row_kv);
    println!(
        "VALIDATED: object_to_kvpair_array({{\"a\": 1, \"b\": 2}}) = {:?}",
        res
    );
    assert!(res.is_array());

    // 8. JSON: tojson(val)
    let mut row_json = HashMap::new();
    row_json.insert(
        "metric".to_string(),
        json!({"temp": 22.5, "unit": "celsius"}),
    );
    let res = eval_expr("tojson(metric)", &row_json);
    println!("VALIDATED: tojson(metric) = {:?}", res);
    assert!(res.is_string());

    // 9. JSON: parsejson(str)
    let mut row_str = HashMap::new();
    row_str.insert(
        "payload".to_string(),
        json!("{\"device\":\"sens_01\",\"val\":99}"),
    );
    let res = eval_expr("parsejson(payload)", &row_str);
    println!("VALIDATED: parsejson(payload) = {:?}", res);
    assert_eq!(res["device"], "sens_01");
    assert_eq!(res["val"], 99);

    // 10. Date / Time: year(ts)
    let mut row_ts = HashMap::new();
    row_ts.insert("ts".to_string(), json!("2026-10-14T20:00:00Z"));
    let res = eval_expr("year(ts)", &row_ts);
    println!("VALIDATED: year('2026-10-14T20:00:00Z') = {:?}", res);
    assert_eq!(res, json!(2026));

    // 11. Date / Time: to_seconds(ts)
    let res = eval_expr("to_seconds(ts)", &row_ts);
    println!("VALIDATED: to_seconds('2026-10-14T20:00:00Z') = {:?}", res);
    assert!(res.as_i64().unwrap_or(0) > 0);

    // 12. Conversion: isnan(val)
    let res = eval_expr("isnan(12.5)", &empty);
    println!("VALIDATED: isnan(12.5) = {:?}", res);
    assert_eq!(res, json!(false));

    // 13. Conversion: isnumeric(val)
    let mut row_num = HashMap::new();
    row_num.insert("val_str".to_string(), json!("42.8"));
    row_num.insert("txt".to_string(), json!("hello"));
    let res1 = eval_expr("isnumeric(val_str)", &row_num);
    let res2 = eval_expr("isnumeric(txt)", &row_num);
    println!(
        "VALIDATED: isnumeric('42.8') = {:?}, isnumeric('hello') = {:?}",
        res1, res2
    );
    assert_eq!(res1, json!(true));
    assert_eq!(res2, json!(false));

    // 14. Conversion: nvl(val, default)
    let mut row_null = HashMap::new();
    row_null.insert("missing".to_string(), Value::Null);
    let res1 = eval_expr("nvl(missing, 100)", &row_null);
    let res2 = eval_expr("nvl(42, 100)", &row_null);
    println!(
        "VALIDATED: nvl(null, 100) = {:?}, nvl(42, 100) = {:?}",
        res1, res2
    );
    assert_eq!(res1, json!(100));
    assert_eq!(res2, json!(42));

    // 15. Conversion: uuid()
    let res = eval_expr("uuid()", &empty);
    println!("VALIDATED: uuid() = {:?}", res);
    assert!(res.is_string());
    assert_eq!(res.as_str().unwrap().len(), 36);

    // 16. Encoding / Crypto: base64_encode(str) & base64_decode(str)
    let mut row_b64 = HashMap::new();
    row_b64.insert("raw".to_string(), json!("Hello, Rekuiper!"));
    let res_enc = eval_expr("base64_encode(raw)", &row_b64);
    println!(
        "VALIDATED: base64_encode('Hello, Rekuiper!') = {:?}",
        res_enc
    );
    let mut row_dec = HashMap::new();
    row_dec.insert("b64".to_string(), res_enc.clone());
    let res_dec = eval_expr("base64_decode(b64)", &row_dec);
    println!("VALIDATED: base64_decode({:?}) = {:?}", res_enc, res_dec);
    assert_eq!(res_dec, json!("Hello, Rekuiper!"));

    // 17. Encoding / String: numbytes(str)
    let res = eval_expr("numbytes(raw)", &row_b64);
    println!("VALIDATED: numbytes('Hello, Rekuiper!') = {:?}", res);
    assert_eq!(res, json!(16));

    // 18. Encoding / String: chr(code)
    let res = eval_expr("chr(65)", &empty);
    println!("VALIDATED: chr(65) = {:?}", res);
    assert_eq!(res, json!("A"));

    // 19. Encoding / Conversion: hex2dec(hex_str) & dec2hex(int_val)
    let res_h2d = eval_expr("hex2dec('1A')", &empty);
    println!("VALIDATED: hex2dec('1A') = {:?}", res_h2d);
    assert_eq!(res_h2d, json!(26));

    let res_d2h = eval_expr("dec2hex(26)", &empty);
    println!("VALIDATED: dec2hex(26) = {:?}", res_d2h);
    assert_eq!(res_d2h, json!("0x1a"));

    // 20. String: split_value(str, delimiter, index) - 0-based indexing
    let mut row_split = HashMap::new();
    row_split.insert(
        "topic".to_string(),
        json!("factory/area1/sensor/temperature"),
    );
    let res1 = eval_expr("split_value(topic, '/', 1)", &row_split);
    let res2 = eval_expr("split_value(topic, '/', 2)", &row_split);
    println!(
        "VALIDATED: split_value('/', 1) = {:?}, split_value('/', 2) = {:?}",
        res1, res2
    );
    assert_eq!(res1, json!("area1"));
    assert_eq!(res2, json!("sensor"));

    // 21. String / Regex: regexp_substr(str, pattern)
    let mut row_re = HashMap::new();
    row_re.insert("log".to_string(), json!("device_id: DEV_4892 status: ok"));
    let res = eval_expr("regexp_substr(log, 'DEV_[0-9]+')", &row_re);
    println!("VALIDATED: regexp_substr(log, 'DEV_[0-9]+') = {:?}", res);
    assert_eq!(res, json!("DEV_4892"));

    println!("=== ALL TESTED FUNCTIONS EXECUTED AND VALIDATED SUCCESSFULLY ===");
}
