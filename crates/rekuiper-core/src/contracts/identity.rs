//! Logical identities and ownership counters.

use crate::RuleDefinition;
use serde::{Deserialize, Serialize};

/// Maximum length in bytes of a logical identifier.
pub const MAX_ID_LEN: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidId {
    #[error("identifier must contain between 1 and 128 bytes")]
    Length,
    #[error("identifier must start with an alphanumeric character or underscore and contain only alphanumeric characters, underscores, hyphens, or periods")]
    Characters,
    #[error(
        "connection identifier must contain either one name or two names separated by one slash"
    )]
    ConnectionSeparator,
}

fn validate_id(value: &str) -> Result<(), InvalidId> {
    if value.is_empty() || value.len() > MAX_ID_LEN {
        return Err(InvalidId::Length);
    }

    let mut chars = value.chars();
    if !chars
        .next()
        .is_some_and(|ch| ch.is_alphanumeric() || ch == '_')
        || !chars.all(|ch| ch.is_alphanumeric() || matches!(ch, '_' | '-' | '.'))
    {
        return Err(InvalidId::Characters);
    }

    Ok(())
}

fn validate_connection_id(value: &str) -> Result<(), InvalidId> {
    if value.len() > MAX_ID_LEN {
        return Err(InvalidId::Length);
    }

    let Some((name, key)) = value.split_once('/') else {
        return validate_id(value);
    };
    if name.is_empty() || key.is_empty() || key.contains('/') {
        return Err(InvalidId::ConnectionSeparator);
    }
    validate_id(name)?;
    validate_id(key)
}

macro_rules! logical_id {
    ($name:ident) => {
        logical_id!($name, validate_id);
    };
    ($name:ident, $validate:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String")]
        pub struct $name(String);

        impl $name {
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = InvalidId;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                $validate(&value)?;
                Ok(Self(value))
            }
        }

        impl TryFrom<&str> for $name {
            type Error = InvalidId;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                $validate(value)?;
                Ok(Self(value.to_owned()))
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

logical_id!(RuleId);
logical_id!(OperatorId);
logical_id!(ActionId);
logical_id!(ConnectionId, validate_connection_id);
logical_id!(SourceId);
logical_id!(PartitionId);
logical_id!(PositionCodecId);

impl TryFrom<&RuleDefinition> for RuleId {
    type Error = InvalidId;

    fn try_from(rule: &RuleDefinition) -> Result<Self, Self::Error> {
        Self::try_from(rule.id.as_str())
    }
}

/// User-facing label that can change independently of a logical identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DisplayName(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Generation(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CheckpointId(pub u64);
