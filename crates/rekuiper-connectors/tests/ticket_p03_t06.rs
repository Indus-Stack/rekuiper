use rekuiper_connectors::kafka::{
    resolve_start_offset, KafkaConfig, KafkaPositionCodec, KafkaRawRecord, KafkaReliableError,
    KafkaReliablePartitionReader, KafkaTombstonePolicy,
};
use rekuiper_core::contracts::{Generation, PartitionId, PositionCodec, SourceId, SourcePartition};
use std::cmp::Ordering;

fn test_partition() -> SourcePartition {
    SourcePartition {
        source_id: SourceId::try_from("orders_kafka").expect("valid source id"),
        partition_id: PartitionId::try_from("0").expect("valid partition id"),
    }
}

#[test]
fn kafka_position_codec_orders_and_detects_retention_gaps() {
    let codec = KafkaPositionCodec::new();
    let part = test_partition();
    let generation = Generation(1);

    let p10 = codec.make_position(part.clone(), generation, 10);
    let p11 = codec.make_position(part, generation, 11);
    assert_eq!(p10.compare(&p11, &codec), Ok(Ordering::Less));
    assert_eq!(KafkaPositionCodec::decode_next_offset(&p11.cursor), Ok(11));
    assert_eq!(codec.compare(&[1, 2], &p10.cursor), None);

    // Explicit next-offset restore succeeds when within retained range.
    assert_eq!(resolve_start_offset(5, 20, Some(10), "latest"), Ok(10));

    // Retention gap fails visibly.
    assert_eq!(
        resolve_start_offset(12, 20, Some(10), "latest"),
        Err(KafkaReliableError::RetentionGap {
            requested_offset: 10,
            earliest_available: 12
        })
    );

    // Legacy earliest / latest modes remain preserved.
    assert_eq!(resolve_start_offset(5, 20, None, "earliest"), Ok(5));
    assert_eq!(resolve_start_offset(5, 20, None, "latest"), Ok(20));

    let legacy_cfg: KafkaConfig =
        serde_json::from_str(r#"{"brokers":"127.0.0.1:9092","topic":"t","partition":2}"#)
            .expect("legacy config deserializes");
    assert_eq!(legacy_cfg.next_offset, None);
    assert_eq!(legacy_cfg.offset_reset, "latest");
}

#[test]
fn multirecord_payload_advances_only_on_full_commit_and_handles_tombstones() {
    let part = test_partition();
    let generation = Generation(2);
    let mut reader = KafkaReliablePartitionReader::new(
        part,
        generation,
        Some("orders".to_string()),
        10,
        KafkaTombstonePolicy::Fail,
    );

    // Prepare multirecord payload at offset 10: metadata/position is built before decode.
    let prepared = reader.prepare(KafkaRawRecord {
        offset: 10,
        payload: Some(br#"[{"id":1},{"id":2}]"#.to_vec()),
    });
    assert_eq!(prepared.offset, 10);
    assert_eq!(prepared.next_offset, 11);
    assert_eq!(reader.committed_next_offset(), 10);

    let records = prepared
        .decode(reader.tombstone_policy())
        .expect("multirecord decodes");
    assert_eq!(records.len(), 2);
    // Still not committed until the entire multirecord boundary is accepted.
    assert_eq!(reader.committed_next_offset(), 10);

    let committed_pos = reader.commit_message(&prepared).expect("commit boundary");
    assert_eq!(
        KafkaPositionCodec::decode_next_offset(&committed_pos.cursor),
        Ok(11)
    );
    assert_eq!(reader.committed_next_offset(), 11);

    // Tombstone at offset 11 fails under Fail policy and does not advance.
    let tombstone = reader.prepare(KafkaRawRecord {
        offset: 11,
        payload: None,
    });
    assert!(tombstone.is_tombstone);
    assert_eq!(
        tombstone.decode(KafkaTombstonePolicy::Fail).unwrap_err(),
        KafkaReliableError::Tombstone { offset: 11 }
    );
    assert_eq!(reader.committed_next_offset(), 11);

    // Under SkipAndAdvance, tombstone yields 0 records and can commit offset 12.
    assert_eq!(
        tombstone
            .decode(KafkaTombstonePolicy::SkipAndAdvance)
            .expect("skip tombstone")
            .len(),
        0
    );
    reader.commit_message(&tombstone).expect("commit tombstone");
    assert_eq!(reader.committed_next_offset(), 12);
}
