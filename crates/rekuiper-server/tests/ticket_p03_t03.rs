use rekuiper_core::contracts::{
    CheckpointId, DeadlineCancellation, EdgeId, Generation, OrderedEnvelope, PartitionId,
    PositionCodecId, SourceId, SourcePartition, SourcePosition,
};
use rekuiper_core::model::StreamRecord;
use rekuiper_core::reliability::BranchOutcome;
use rekuiper_server::engine::reliable_input::{
    reliable_input_channel, ReliableIngressError, ReliableInputItem,
};
use serde_json::json;
use std::collections::HashMap;

fn pos(gen: u64, offset: i64) -> SourcePosition {
    SourcePosition {
        partition: SourcePartition {
            source_id: SourceId::try_from("kafka_in").unwrap(),
            partition_id: PartitionId::try_from("p0").unwrap(),
        },
        generation: Generation(gen),
        codec: PositionCodecId::try_from("kafka_offset_v1").unwrap(),
        cursor: offset.to_be_bytes().to_vec(),
    }
}

#[tokio::test]
async fn data_precedes_barrier_and_eof_and_multirow_is_one_position() {
    let (tx, mut rx) = reliable_input_channel(Generation(2), 8);
    let cancel = DeadlineCancellation::default();

    let p1 = pos(2, 1);
    let row_a = StreamRecord::new(HashMap::from([("id".to_owned(), json!(1))]));
    let row_b = StreamRecord::new(HashMap::from([("id".to_owned(), json!(2))]));
    tx.admit_data(
        ReliableInputItem {
            position: p1.clone(),
            rows: vec![row_a, row_b],
        },
        &cancel,
    )
    .await
    .unwrap();
    tx.admit_barrier(CheckpointId(5), Generation(2), &cancel)
        .await
        .unwrap();
    tx.admit_eof(EdgeId(0), &cancel).await.unwrap();

    // Order is strictly Data -> Barrier -> EndOfInput.
    assert!(matches!(rx.recv().await, Some(OrderedEnvelope::Data(item)) if item.rows.len() == 2));
    assert!(matches!(
        rx.recv().await,
        Some(OrderedEnvelope::Barrier {
            checkpoint: CheckpointId(5),
            generation: Generation(2)
        })
    ));
    assert!(matches!(
        rx.recv().await,
        Some(OrderedEnvelope::EndOfInput { edge: EdgeId(0) })
    ));

    // Multirow position does not complete until both rows complete.
    assert!(tx.complete_branch(&p1, 0, BranchOutcome::TerminalSuccess));
    assert_eq!(tx.contiguous_position(), None);
    assert!(tx.complete_branch(&p1, 1, BranchOutcome::TerminalSuccess));
    assert_eq!(tx.contiguous_position(), Some(p1));
}

#[tokio::test]
async fn stale_generation_and_malformed_input_cannot_advance_and_cancel_wakes_admission() {
    let (tx, _rx) = reliable_input_channel(Generation(3), 1);
    let cancel = DeadlineCancellation::default();

    // Old generation is refused.
    let stale = pos(2, 1);
    let err = tx
        .admit_data(
            ReliableInputItem {
                position: stale,
                rows: vec![],
            },
            &cancel,
        )
        .await
        .expect_err("stale generation must be refused");
    assert!(matches!(err, ReliableIngressError::StaleGeneration { .. }));

    // Malformed message blocks progress and cannot silently advance.
    let bad_pos = pos(3, 1);
    tx.register_malformed(bad_pos.clone()).unwrap();
    assert_eq!(tx.contiguous_position(), None);

    let good_pos = pos(3, 2);
    tx.admit_data(
        ReliableInputItem {
            position: good_pos.clone(),
            rows: vec![],
        },
        &cancel,
    )
    .await
    .unwrap();
    // Even though good_pos has 0 rows, bad_pos at offset 1 blocks contiguous progress.
    assert_eq!(tx.contiguous_position(), None);

    // Fill the capacity-1 channel (good_pos had 1 slot), then verify cancellation unblocks.
    cancel.cancel();
    let err = tx
        .admit_eof(EdgeId(0), &cancel)
        .await
        .expect_err("cancelled admission must return Cancelled");
    assert_eq!(err, ReliableIngressError::Cancelled);
}
