use rekuiper_core::{
    ActionId, CheckpointId, ConnectionId, DisplayName, ErrorCode, ErrorObject, ErrorPosition,
    ErrorStage, Generation, OperatorId, PartitionId, PositionCodec, PositionCodecId,
    PositionComparisonError, RuleDefinition, RuleId, SourceId, SourcePartition, SourcePosition,
    StructuredError, MAX_ID_LEN,
};
use serde_json::{json, Value};
use std::cmp::Ordering;

const POSITION_JSON: &str = r#"{
    "partition": {"source_id": "line-1", "partition_id": "0"},
    "generation": 7,
    "codec": "signed-offset.v1",
    "cursor": [0,0,0,0,0,0,0,42]
}"#;

const ERROR_JSON: &str = r#"{
    "code": "unavailable",
    "stage": "source",
    "object": {"kind": "connection", "id": "broker.main"},
    "retriable": true,
    "message": "A required resource is unavailable.",
    "position": null
}"#;

struct SignedOffsetCodec(PositionCodecId);

impl PositionCodec for SignedOffsetCodec {
    fn id(&self) -> &PositionCodecId {
        &self.0
    }

    fn compare(&self, left: &[u8], right: &[u8]) -> Option<Ordering> {
        let left = i64::from_be_bytes(left.try_into().ok()?);
        let right = i64::from_be_bytes(right.try_into().ok()?);
        Some(left.cmp(&right))
    }
}

#[test]
fn logical_ids_and_counters_have_scalar_json() {
    macro_rules! check_json {
        ($type:ty, $value:expr, $golden:expr) => {{
            let value: $type = $value;
            assert_eq!(serde_json::to_value(&value).unwrap(), $golden);
            assert_eq!(serde_json::from_value::<$type>($golden).unwrap(), value);
        }};
    }

    check_json!(
        RuleId,
        "plant.alarm".try_into().unwrap(),
        json!("plant.alarm")
    );
    check_json!(OperatorId, "node_2".try_into().unwrap(), json!("node_2"));
    check_json!(ActionId, "log-1".try_into().unwrap(), json!("log-1"));
    check_json!(ConnectionId, "broker".try_into().unwrap(), json!("broker"));
    check_json!(SourceId, "line-1".try_into().unwrap(), json!("line-1"));
    check_json!(PartitionId, "0".try_into().unwrap(), json!("0"));
    check_json!(
        PositionCodecId,
        "offset.v1".try_into().unwrap(),
        json!("offset.v1")
    );
    check_json!(
        DisplayName,
        DisplayName("Line 1 · sensor".into()),
        json!("Line 1 · sensor")
    );
    check_json!(Generation, Generation(u64::MAX), json!(u64::MAX));
    check_json!(CheckpointId, CheckpointId(0), json!(0));
    assert!(serde_json::from_value::<Generation>(json!(-1)).is_err());
    assert!(serde_json::from_value::<CheckpointId>(json!(-1)).is_err());
}

#[test]
fn validation_covers_input_and_deserialization_boundaries() {
    assert!(RuleId::try_from("a/b").is_err());
    assert!(serde_json::from_value::<SourceId>(json!("a/b")).is_err());
    let maximum = "a".repeat(MAX_ID_LEN);
    assert!(RuleId::try_from(maximum.as_str()).is_ok());
    let too_long = "a".repeat(MAX_ID_LEN + 1);
    for invalid in ["", "space id", ".hidden", "a\n", "a\u{202e}b", &too_long] {
        assert!(RuleId::try_from(invalid).is_err());
        assert!(OperatorId::try_from(invalid.to_owned()).is_err());
        assert!(serde_json::from_value::<ActionId>(json!(invalid)).is_err());
        assert!(serde_json::from_value::<ConnectionId>(json!(invalid)).is_err());
        assert!(serde_json::from_value::<SourceId>(json!(invalid)).is_err());
        assert!(serde_json::from_value::<PartitionId>(json!(invalid)).is_err());
        assert!(serde_json::from_value::<PositionCodecId>(json!(invalid)).is_err());
    }
}

#[test]
fn legacy_ids_convert_independently_of_display_names() {
    let mut definition = RuleDefinition {
        id: "_règle_001".into(),
        name: Some("Plant sensor".into()),
        ..RuleDefinition::default()
    };
    let id = RuleId::try_from(&definition).unwrap();
    definition.name = Some("Renamed sensor".into());
    assert_eq!(RuleId::try_from(&definition).unwrap(), id);
    assert_eq!(String::from(id), definition.id);
    assert!(RuleId::try_from(&RuleDefinition::default()).is_err());

    let graph_node_key = String::from("filter_1");
    let operator = OperatorId::try_from(graph_node_key.as_str()).unwrap();
    assert_eq!(String::from(operator), graph_node_key);
}

#[test]
fn legacy_connection_keys_preserve_canonical_and_simple_spellings() {
    let persisted_key = "mqtt/plant";
    let canonical = ConnectionId::try_from(persisted_key.to_owned()).unwrap();
    let json = json!(persisted_key);
    assert_eq!(canonical.as_str(), persisted_key);
    assert_eq!(serde_json::to_value(&canonical).unwrap(), json);
    assert_eq!(
        serde_json::from_value::<ConnectionId>(json).unwrap(),
        canonical
    );
    assert_eq!(String::from(canonical), persisted_key);

    let dot_form = ConnectionId::try_from("mqtt.plant").unwrap();
    assert_eq!(dot_form.as_str(), "mqtt.plant");
    assert_ne!(dot_form, ConnectionId::try_from(persisted_key).unwrap());

    let too_long = format!("mqtt/{}", "a".repeat(MAX_ID_LEN));
    for malformed in [
        "/plant",
        "mqtt/",
        "mqtt//plant",
        "mqtt/plant/other",
        "mqtt/../plant",
        "mqtt/..",
        "mqtt\\plant",
        "mqtt%2fplant",
        &too_long,
    ] {
        assert!(ConnectionId::try_from(malformed).is_err());
        assert!(serde_json::from_value::<ConnectionId>(json!(malformed)).is_err());
    }
}

#[test]
fn position_has_stable_json_and_codec_ordering() {
    let golden: Value = serde_json::from_str(POSITION_JSON).unwrap();
    let position: SourcePosition = serde_json::from_value(golden.clone()).unwrap();
    assert_eq!(serde_json::to_value(&position).unwrap(), golden);

    let codec = SignedOffsetCodec(position.codec.clone());
    let mut earlier = position.clone();
    earlier.cursor = (-1_i64).to_be_bytes().to_vec();
    assert_eq!(earlier.compare(&position, &codec), Ok(Ordering::Less));
    assert_eq!(position.compare(&position, &codec), Ok(Ordering::Equal));
    assert_eq!(position.compare(&earlier, &codec), Ok(Ordering::Greater));
    earlier.cursor.pop();
    assert_eq!(
        earlier.compare(&position, &codec),
        Err(PositionComparisonError::UnorderedCursor)
    );
}

#[test]
fn positions_reject_mismatched_ownership_and_codecs_before_comparing() {
    struct NeverCompare(PositionCodecId);

    impl PositionCodec for NeverCompare {
        fn id(&self) -> &PositionCodecId {
            &self.0
        }

        fn compare(&self, _: &[u8], _: &[u8]) -> Option<Ordering> {
            panic!("foreign cursors must never reach a codec comparator")
        }
    }

    let position: SourcePosition = serde_json::from_str(POSITION_JSON).unwrap();
    let codec = NeverCompare(position.codec.clone());
    let other_codec = NeverCompare("foreign.v1".try_into().unwrap());
    let other_source = SourcePosition {
        partition: SourcePartition {
            source_id: "line-2".try_into().unwrap(),
            ..position.partition.clone()
        },
        ..position.clone()
    };
    let other_partition = SourcePosition {
        partition: SourcePartition {
            partition_id: "1".try_into().unwrap(),
            ..position.partition.clone()
        },
        ..position.clone()
    };
    for foreign in [other_source, other_partition] {
        assert_eq!(
            position.compare(&foreign, &codec),
            Err(PositionComparisonError::PartitionMismatch)
        );
    }

    let other_generation = SourcePosition {
        generation: Generation(8),
        ..position.clone()
    };
    assert_eq!(
        position.compare(&other_generation, &codec),
        Err(PositionComparisonError::GenerationMismatch)
    );

    let foreign_encoding = SourcePosition {
        codec: other_codec.0.clone(),
        ..position.clone()
    };
    assert_eq!(
        position.compare(&foreign_encoding, &codec),
        Err(PositionComparisonError::CodecMismatch)
    );
    assert_eq!(
        position.compare(&position, &other_codec),
        Err(PositionComparisonError::CodecMismatch)
    );
}

#[test]
fn structured_error_has_stable_json_and_optional_position() {
    let error = StructuredError {
        code: ErrorCode::Unavailable,
        stage: ErrorStage::Source,
        object: ErrorObject::Connection("broker.main".try_into().unwrap()),
        retriable: true,
        position: None,
    };
    let mut golden: Value = serde_json::from_str(ERROR_JSON).unwrap();
    assert_eq!(serde_json::to_value(&error).unwrap(), golden);
    assert_eq!(
        serde_json::from_value::<StructuredError>(golden.clone()).unwrap(),
        error
    );

    golden["position"] = json!({
        "partition": {"source_id": "line-1", "partition_id": "0"},
        "generation": 7,
        "codec": "signed-offset.v1"
    });
    let position: SourcePosition = serde_json::from_str(POSITION_JSON).unwrap();
    let with_position = StructuredError {
        position: Some(ErrorPosition::from(&position)),
        ..error
    };
    assert_eq!(serde_json::to_value(&with_position).unwrap(), golden);
    assert_eq!(
        serde_json::from_value::<StructuredError>(golden).unwrap(),
        with_position
    );
}

#[test]
fn wire_input_cannot_inject_secret_error_messages_or_diagnostics() {
    const SECRET: &str = "mqtt://user:secret-password@example.test";
    let mut input: Value = serde_json::from_str(ERROR_JSON).unwrap();
    input["message"] = json!(SECRET);
    input["password"] = json!(SECRET);
    input["raw_connector_error"] = json!(SECRET);
    let mut position: SourcePosition = serde_json::from_str(POSITION_JSON).unwrap();
    position.cursor = SECRET.as_bytes().to_vec();
    input["position"] = serde_json::to_value(&position).unwrap();

    let error: StructuredError = serde_json::from_value(input.clone()).unwrap();
    assert_eq!(error.message(), "A required resource is unavailable.");
    assert_eq!(error.position, Some(ErrorPosition::from(&position)));
    assert!(serde_json::to_value(&error).unwrap()["position"]
        .get("cursor")
        .is_none());
    for rendered in [
        error.to_string(),
        format!("{error:?}"),
        serde_json::to_string(&error).unwrap(),
    ] {
        assert!(!rendered.contains(SECRET));
        assert!(!rendered.contains("password"));
        assert!(!rendered.contains("raw_connector_error"));
        assert!(!rendered.contains("cursor"));
    }

    for field in ["code", "stage", "object", "retriable", "position"] {
        let mut malformed = input.clone();
        malformed[field] = json!(SECRET);
        let error = serde_json::from_value::<StructuredError>(malformed).unwrap_err();
        assert!(error.to_string().contains("invalid structured error"));
        assert!(!error.to_string().contains(SECRET));
    }
}
