//! Deterministic ordered replay source fake.

use crate::contracts::{
    Generation, OrderedEnvelope, PartitionId, PositionCodec, PositionCodecId,
    PositionComparisonError, SourceId, SourcePartition, SourcePosition,
};
use std::cmp::Ordering;
use std::collections::BTreeSet;

/// Fixed-width signed integer cursor codec for deterministic tests.
#[derive(Debug, Clone)]
pub struct SeqPositionCodec {
    id: PositionCodecId,
}

impl SeqPositionCodec {
    pub fn new(id: &str) -> Self {
        Self {
            id: PositionCodecId::try_from(id).expect("valid codec id"),
        }
    }

    pub fn encode(seq: i64) -> Vec<u8> {
        seq.to_be_bytes().to_vec()
    }

    pub fn decode(bytes: &[u8]) -> Option<i64> {
        let arr: [u8; 8] = bytes.try_into().ok()?;
        Some(i64::from_be_bytes(arr))
    }
}

impl PositionCodec for SeqPositionCodec {
    fn id(&self) -> &PositionCodecId {
        &self.id
    }

    fn compare(&self, left: &[u8], right: &[u8]) -> Option<Ordering> {
        let l = Self::decode(left)?;
        let r = Self::decode(right)?;
        Some(l.cmp(&r))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FakeSourceError {
    #[error("failpoint triggered: {0}")]
    Failpoint(String),
    #[error(transparent)]
    Position(#[from] PositionComparisonError),
}

/// Ordered replayable source for one partition with failpoints and an op log.
pub struct FakeReplaySource<T> {
    partition: SourcePartition,
    generation: Generation,
    codec: SeqPositionCodec,
    records: Vec<(SourcePosition, OrderedEnvelope<T>)>,
    cursor_index: usize,
    failpoints: BTreeSet<String>,
    op_log: Vec<String>,
}

impl<T: Clone> FakeReplaySource<T> {
    pub fn new(
        source_id: SourceId,
        partition_id: PartitionId,
        generation: Generation,
        codec_id: &str,
    ) -> Self {
        Self {
            partition: SourcePartition {
                source_id,
                partition_id,
            },
            generation,
            codec: SeqPositionCodec::new(codec_id),
            records: Vec::new(),
            cursor_index: 0,
            failpoints: BTreeSet::new(),
            op_log: Vec::new(),
        }
    }

    pub fn push(&mut self, seq: i64, envelope: OrderedEnvelope<T>) -> SourcePosition {
        let pos = SourcePosition {
            partition: self.partition.clone(),
            generation: self.generation,
            codec: self.codec.id().clone(),
            cursor: SeqPositionCodec::encode(seq),
        };
        self.records.push((pos.clone(), envelope));
        pos
    }

    pub fn arm_failpoint(&mut self, name: impl Into<String>) {
        self.failpoints.insert(name.into());
    }

    pub fn clear_failpoint(&mut self, name: &str) {
        self.failpoints.remove(name);
    }

    pub fn op_log(&self) -> &[String] {
        &self.op_log
    }

    /// Resumes immediately after `committed_pos` (where `committed_pos` is the
    /// next-to-read or last-committed position depending on `inclusive`).
    /// For `SourcePosition` representing the last committed offset, resumes at the
    /// first record strictly greater than `committed_pos`.
    pub fn resume_after(&mut self, committed_pos: &SourcePosition) -> Result<(), FakeSourceError> {
        self.op_log.push("resume_after".to_owned());
        if let Some((first_pos, _)) = self.records.first() {
            // Validate partition, generation, and codec compatibility up front.
            let _ = first_pos.compare(committed_pos, &self.codec)?;
        }
        let mut next_idx = 0;
        for (idx, (pos, _)) in self.records.iter().enumerate() {
            if pos.compare(committed_pos, &self.codec)? != Ordering::Greater {
                next_idx = idx + 1;
            } else {
                break;
            }
        }
        self.cursor_index = next_idx;
        Ok(())
    }

    pub fn next_envelope(
        &mut self,
    ) -> Result<Option<(SourcePosition, OrderedEnvelope<T>)>, FakeSourceError> {
        if self.failpoints.take("read_error").is_some() {
            self.op_log.push("read_error".to_owned());
            return Err(FakeSourceError::Failpoint("read_error".to_owned()));
        }
        let item = self.records.get(self.cursor_index).cloned();
        if item.is_some() {
            self.cursor_index += 1;
            self.op_log
                .push(format!("read_index:{}", self.cursor_index - 1));
        }
        Ok(item)
    }
}
