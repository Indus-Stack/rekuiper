use rekuiper_connectors::codec::{JsonlCodec, JsonlCodecConfig, JsonlFramingMode};
use serde_json::json;

#[test]
fn p15_t02_strict_jsonl_and_concatenated_modes() {
    let strict = JsonlCodecConfig::default();

    // 1. Two valid lines (including CRLF and escaped newline) decode in order.
    let raw = b"{\"id\":1,\"msg\":\"line1\\nnext\"}\r\n\r\n{\"id\":2}\n";
    let decoded = JsonlCodec::decode_bounded(raw, &strict).unwrap();
    assert_eq!(
        decoded,
        vec![json!({"id": 1, "msg": "line1\nnext"}), json!({"id": 2})]
    );

    // 2. Malformed second line emits no prefix and identifies failing record & byte.
    let bad_second = b"{\"id\":1}\n{bad_json}\n";
    let err = JsonlCodec::decode_bounded(bad_second, &strict).expect_err("must fail atomically");
    assert_eq!(err.record_index, 1);
    assert!(err.byte_offset >= 9);

    // 3. Adjacent objects fail in StrictNewline, succeed in ConcatenatedCompatibility.
    let adjacent = b"{\"a\":1}{\"b\":2}";
    assert!(JsonlCodec::decode_bounded(adjacent, &strict).is_err());

    let compat = JsonlCodecConfig {
        mode: JsonlFramingMode::ConcatenatedCompatibility,
        ..JsonlCodecConfig::default()
    };
    let compat_out = JsonlCodec::decode_bounded(adjacent, &compat).unwrap();
    assert_eq!(compat_out, vec![json!({"a": 1}), json!({"b": 2})]);

    // 4. Oversize message & oversize record rejected.
    let tiny = JsonlCodecConfig {
        max_message_bytes: 8,
        ..JsonlCodecConfig::default()
    };
    assert!(JsonlCodec::decode_bounded(b"{\"long\":12345}", &tiny).is_err());

    // 5. Empty input decodes cleanly when allow_empty_lines is true; non-object rejected when require_objects=true.
    assert!(JsonlCodec::decode_bounded(b"", &strict).unwrap().is_empty());
    assert!(JsonlCodec::decode_bounded(b"[1,2]\n", &strict).is_err());

    // 6. Framed writer handles arrays and rejects non-objects when require_objects=true, round-tripping cleanly.
    let arr = json!([{"k": 1}, {"k": 2}]);
    let encoded = JsonlCodec::encode_value(&arr, true).unwrap();
    let roundtrip = JsonlCodec::decode_bounded(&encoded, &strict).unwrap();
    assert_eq!(roundtrip, vec![json!({"k": 1}), json!({"k": 2})]);

    assert!(JsonlCodec::encode_value(&json!(42), true).is_err());
    let non_obj = JsonlCodec::encode_value(&json!(42), false).unwrap();
    assert_eq!(non_obj, b"42\n");
}
