use base64::Engine as _;
use rekuiper_connectors::kafka::{decode_kafka_record_with_meta, KAFKA_METADATA_CAPABILITIES};
use rskafka::record::{Record, RecordAndOffset};
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;

#[test]
fn p15_t01_kafka_metadata_boundary_binary_key_and_multirow_sharing() {
    const {
        assert!(!KAFKA_METADATA_CAPABILITIES.preserves_duplicate_headers);
    }
    assert!(KAFKA_METADATA_CAPABILITIES.limitation.contains("BTreeMap"));

    let mut headers = BTreeMap::new();
    headers.insert("trace_id".to_owned(), b"abc-123".to_vec());
    headers.insert("raw_hdr".to_owned(), vec![0xff, 0x00, 0xfe]);

    let binary_key = vec![0x00, 0xfe, 0xff, 0x42];
    let ts = std::time::UNIX_EPOCH + std::time::Duration::from_millis(1_700_000_000_123);

    let rec = RecordAndOffset {
        record: Record {
            key: Some(binary_key.clone()),
            value: Some(br#"[{"temp": 21}, {"temp": 22}]"#.to_vec()),
            headers,
            timestamp: ts.into(),
        },
        offset: 42,
    };

    let rows = decode_kafka_record_with_meta(&rec, Some("sensors"), 3).unwrap();
    assert_eq!(rows.len(), 2);

    // Payload projections remain clean (no metadata injected into data).
    assert_eq!(rows[0].data.get("temp"), Some(&json!(21)));
    assert!(!rows[0].data.contains_key("partition"));
    assert!(!rows[0].data.contains_key("offset"));
    assert_eq!(rows[1].data.get("temp"), Some(&json!(22)));

    // Multirow records share the same Arc<RecordMeta>.
    let m0 = rows[0].meta.as_ref().unwrap();
    let m1 = rows[1].meta.as_ref().unwrap();
    assert!(Arc::ptr_eq(m0, m1));
    assert_eq!(m0.topic.as_deref(), Some("sensors"));

    let custom = m0.custom.as_ref().unwrap();
    assert_eq!(custom.get("partition"), Some(&json!(3)));
    assert_eq!(custom.get("offset"), Some(&json!(42)));
    assert_eq!(custom.get("timestamp"), Some(&json!(1_700_000_000_123i64)));
    assert_eq!(custom.get("duplicateHeadersSupported"), Some(&json!(false)));

    let key_b64 = custom.get("keyBase64").unwrap().as_str().unwrap();
    let decoded_key = base64::engine::general_purpose::STANDARD
        .decode(key_b64)
        .unwrap();
    assert_eq!(decoded_key, binary_key);

    assert_eq!(custom["headers"]["trace_id"], json!("abc-123"));
    let hdr_b64 = custom["headersRawBase64"]["raw_hdr"].as_str().unwrap();
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(hdr_b64)
            .unwrap(),
        vec![0xff, 0x00, 0xfe]
    );
}
