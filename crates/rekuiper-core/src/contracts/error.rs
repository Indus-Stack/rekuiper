//! Structured errors for reliable execution.

use super::identity::{
    ActionId, ConnectionId, Generation, OperatorId, PositionCodecId, RuleId, SourceId,
};
use super::position::{SourcePartition, SourcePosition};
use serde::{de, ser::SerializeStruct, Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    InvalidConfiguration,
    InvalidData,
    Unsupported,
    Unavailable,
    Timeout,
    StorageFailure,
    Internal,
}

impl ErrorCode {
    pub const fn message(self) -> &'static str {
        match self {
            Self::InvalidConfiguration => "Configuration is invalid.",
            Self::InvalidData => "Input data is invalid.",
            Self::Unsupported => "The requested operation is unsupported.",
            Self::Unavailable => "A required resource is unavailable.",
            Self::Timeout => "The operation timed out.",
            Self::StorageFailure => "A storage operation failed.",
            Self::Internal => "An internal operation failed.",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorStage {
    Configuration,
    Planning,
    Source,
    Operator,
    Action,
    Checkpoint,
    Restore,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum ErrorObject {
    Rule(RuleId),
    Operator(OperatorId),
    Action(ActionId),
    Connection(ConnectionId),
    Source(SourceId),
}

/// Source ownership context safe to attach to a public error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorPosition {
    pub partition: SourcePartition,
    pub generation: Generation,
    pub codec: PositionCodecId,
}

impl From<&SourcePosition> for ErrorPosition {
    fn from(position: &SourcePosition) -> Self {
        Self {
            partition: position.partition.clone(),
            generation: position.generation,
            codec: position.codec.clone(),
        }
    }
}

/// A public error whose diagnostic message is determined by its stable code.
/// Populate object and position fields with logical metadata only. Opaque
/// cursors can carry connector data and are excluded from error context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuredError {
    pub code: ErrorCode,
    pub stage: ErrorStage,
    pub object: ErrorObject,
    pub retriable: bool,
    pub position: Option<ErrorPosition>,
}

impl StructuredError {
    pub fn message(&self) -> &'static str {
        self.code.message()
    }
}

impl fmt::Display for StructuredError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

impl std::error::Error for StructuredError {}

impl Serialize for StructuredError {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("StructuredError", 6)?;
        state.serialize_field("code", &self.code)?;
        state.serialize_field("stage", &self.stage)?;
        state.serialize_field("object", &self.object)?;
        state.serialize_field("retriable", &self.retriable)?;
        state.serialize_field("message", self.message())?;
        state.serialize_field("position", &self.position)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for StructuredError {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct WireError {
            code: ErrorCode,
            stage: ErrorStage,
            object: ErrorObject,
            retriable: bool,
            #[serde(rename = "message", default)]
            _message: Option<de::IgnoredAny>,
            position: Option<ErrorPosition>,
        }

        // Nested serde diagnostics can contain the rejected input value.
        let wire = WireError::deserialize(deserializer)
            .map_err(|_| de::Error::custom("invalid structured error"))?;

        Ok(Self {
            code: wire.code,
            stage: wire.stage,
            object: wire.object,
            retriable: wire.retriable,
            position: wire.position,
        })
    }
}
