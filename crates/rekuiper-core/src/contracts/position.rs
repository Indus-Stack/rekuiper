//! Codec-owned source progress.

use super::identity::{Generation, PartitionId, PositionCodecId, SourceId};
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, fmt};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourcePartition {
    pub source_id: SourceId,
    pub partition_id: PartitionId,
}

/// A position for a source partition owned by a particular generation.
///
/// Commit a position only after completing every row expanded from the input
/// message and every earlier message in that partition.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourcePosition {
    pub partition: SourcePartition,
    pub generation: Generation,
    pub codec: PositionCodecId,
    pub cursor: Vec<u8>,
}

impl fmt::Debug for SourcePosition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourcePosition")
            .field("partition", &self.partition)
            .field("generation", &self.generation)
            .field("codec", &self.codec)
            .field("cursor_len", &self.cursor.len())
            .finish()
    }
}

/// Supplies ordering for the specific opaque cursor format it owns.
pub trait PositionCodec {
    fn id(&self) -> &PositionCodecId;

    /// Returns `None` if either cursor is invalid or the format has no ordering.
    fn compare(&self, left: &[u8], right: &[u8]) -> Option<Ordering>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PositionComparisonError {
    #[error("positions use different source partitions")]
    PartitionMismatch,
    #[error("positions use different ownership generations")]
    GenerationMismatch,
    #[error("positions must use the supplied cursor codec")]
    CodecMismatch,
    #[error("cursor codec could not order the positions")]
    UnorderedCursor,
}

impl SourcePosition {
    pub fn compare(
        &self,
        other: &Self,
        codec: &(impl PositionCodec + ?Sized),
    ) -> Result<Ordering, PositionComparisonError> {
        if self.partition != other.partition {
            return Err(PositionComparisonError::PartitionMismatch);
        }
        if self.generation != other.generation {
            return Err(PositionComparisonError::GenerationMismatch);
        }
        if &self.codec != codec.id() || self.codec != other.codec {
            return Err(PositionComparisonError::CodecMismatch);
        }

        codec
            .compare(&self.cursor, &other.cursor)
            .ok_or(PositionComparisonError::UnorderedCursor)
    }
}
