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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ActionIdentityError {
    #[error("action entry must contain exactly one sink type")]
    InvalidActionShape,
    #[error("invalid action name '{0}': {1}")]
    InvalidName(String, InvalidId),
    #[error("duplicate action name '{0}' within rule")]
    DuplicateName(String),
}

/// Resolves stable [`ActionId`] values for a rule's action list.
///
/// Named actions (`props["name"]`) receive `named.<name>` so reordering actions
/// preserves their identity and cache ownership. Duplicate names fail visibly.
/// Unnamed actions preserve positional `index.<idx>` identifiers.
pub fn resolve_action_identities(
    actions: &[std::collections::HashMap<
        String,
        std::collections::HashMap<String, serde_json::Value>,
    >],
) -> Result<Vec<ActionId>, ActionIdentityError> {
    let mut seen_names = std::collections::BTreeSet::new();
    let mut out = Vec::with_capacity(actions.len());

    for (idx, action) in actions.iter().enumerate() {
        if action.len() != 1 {
            return Err(ActionIdentityError::InvalidActionShape);
        }
        let (_, props) = action.iter().next().expect("checked len == 1");
        if let Some(name_val) = props.get("name") {
            let name = name_val
                .as_str()
                .ok_or_else(|| {
                    ActionIdentityError::InvalidName(
                        "<non-string>".to_owned(),
                        InvalidId::Characters,
                    )
                })?
                .trim();
            if name.is_empty() {
                return Err(ActionIdentityError::InvalidName(
                    name.to_owned(),
                    InvalidId::Length,
                ));
            }
            if let Err(err) = validate_id(name) {
                return Err(ActionIdentityError::InvalidName(name.to_owned(), err));
            }
            if !seen_names.insert(name.to_owned()) {
                return Err(ActionIdentityError::DuplicateName(name.to_owned()));
            }
            let id = ActionId::try_from(format!("named.{name}"))
                .map_err(|err| ActionIdentityError::InvalidName(name.to_owned(), err))?;
            out.push(id);
        } else {
            let id = ActionId::try_from(format!("index.{idx}"))
                .map_err(|err| ActionIdentityError::InvalidName(format!("index.{idx}"), err))?;
            out.push(id);
        }
    }

    Ok(out)
}
