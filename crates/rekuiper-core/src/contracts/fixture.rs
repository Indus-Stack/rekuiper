//! Versioned executable fixture schema for deterministic conformance tests.

use super::control::{EdgeId, EventTimestamp};
use super::error::ErrorCode;
use super::identity::{ActionId, CheckpointId, PartitionId, RuleId, SourceId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Current schema version for executable conformance fixtures.
pub const FIXTURE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureStream {
    pub id: SourceId,
    pub ddl: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureRule {
    pub id: RuleId,
    pub sql: String,
    #[serde(default)]
    pub actions: Vec<ActionId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureCatalog {
    pub streams: Vec<FixtureStream>,
    pub rules: Vec<FixtureRule>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FixtureStep {
    Input {
        source_id: SourceId,
        partition_id: PartitionId,
        #[serde(default)]
        event_time: Option<EventTimestamp>,
        #[serde(default)]
        metadata: BTreeMap<String, serde_json::Value>,
        payload: serde_json::Value,
    },
    AdvanceClock {
        millis: u64,
    },
    Watermark {
        edge: EdgeId,
        timestamp: EventTimestamp,
    },
    EndOfInput {
        edge: EdgeId,
    },
    Checkpoint {
        checkpoint_id: CheckpointId,
    },
    Restore {
        checkpoint_id: CheckpointId,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureExpectedRow {
    pub rule_id: RuleId,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureExpectedEffect {
    pub rule_id: RuleId,
    pub action_id: ActionId,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureExpectedError {
    pub rule_id: RuleId,
    pub code: ErrorCode,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureExpectations {
    #[serde(default)]
    pub rows: Vec<FixtureExpectedRow>,
    #[serde(default)]
    pub effects: Vec<FixtureExpectedEffect>,
    #[serde(default)]
    pub errors: Vec<FixtureExpectedError>,
    #[serde(default)]
    pub allow_no_outputs: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureDocument {
    pub schema_version: u32,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub initial_wall_millis: i64,
    pub catalog: FixtureCatalog,
    pub steps: Vec<FixtureStep>,
    pub expected: FixtureExpectations,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FixtureError {
    #[error("malformed fixture at line {line}, column {column}: {message}")]
    InvalidJson {
        line: usize,
        column: usize,
        message: String,
    },
    #[error("unsupported fixture schema_version: {0}")]
    UnsupportedSchemaVersion(u32),
    #[error("fixture validation error: {0}")]
    Validation(String),
}

impl FixtureDocument {
    /// Parses and validates a JSON fixture document, reporting line/column on syntax
    /// or schema errors and rejecting contradictory expectations.
    pub fn from_json_str(raw: &str) -> Result<Self, FixtureError> {
        let doc: Self = serde_json::from_str(raw).map_err(|err| FixtureError::InvalidJson {
            line: err.line(),
            column: err.column(),
            message: err.to_string(),
        })?;
        doc.validate()?;
        Ok(doc)
    }

    pub fn validate(&self) -> Result<(), FixtureError> {
        if self.schema_version != FIXTURE_SCHEMA_VERSION {
            return Err(FixtureError::UnsupportedSchemaVersion(self.schema_version));
        }
        if self.name.trim().is_empty() {
            return Err(FixtureError::Validation(
                "fixture name must not be empty".to_owned(),
            ));
        }
        if self.catalog.streams.is_empty() {
            return Err(FixtureError::Validation(
                "catalog.streams must declare at least one stream".to_owned(),
            ));
        }
        if self.catalog.rules.is_empty() {
            return Err(FixtureError::Validation(
                "catalog.rules must declare at least one rule".to_owned(),
            ));
        }
        if self.steps.is_empty() {
            return Err(FixtureError::Validation(
                "steps must contain at least one step".to_owned(),
            ));
        }

        let mut streams = BTreeSet::new();
        for stream in &self.catalog.streams {
            if !streams.insert(&stream.id) {
                return Err(FixtureError::Validation(format!(
                    "duplicate stream id: {}",
                    stream.id
                )));
            }
        }

        let mut rules: BTreeMap<&RuleId, BTreeSet<&ActionId>> = BTreeMap::new();
        for rule in &self.catalog.rules {
            let actions = rule.actions.iter().collect();
            if rules.insert(&rule.id, actions).is_some() {
                return Err(FixtureError::Validation(format!(
                    "duplicate rule id: {}",
                    rule.id
                )));
            }
        }

        let mut checkpoints = BTreeSet::new();
        for (idx, step) in self.steps.iter().enumerate() {
            match step {
                FixtureStep::Input { source_id, .. } => {
                    if !streams.contains(source_id) {
                        return Err(FixtureError::Validation(format!(
                            "steps[{idx}] references undeclared stream {source_id}"
                        )));
                    }
                }
                FixtureStep::Checkpoint { checkpoint_id } => {
                    if !checkpoints.insert(*checkpoint_id) {
                        return Err(FixtureError::Validation(format!(
                            "steps[{idx}] duplicates checkpoint_id {}",
                            checkpoint_id.0
                        )));
                    }
                }
                FixtureStep::Restore { checkpoint_id } => {
                    if !checkpoints.contains(checkpoint_id) {
                        return Err(FixtureError::Validation(format!(
                            "steps[{idx}] restores unknown checkpoint_id {}",
                            checkpoint_id.0
                        )));
                    }
                }
                FixtureStep::AdvanceClock { .. }
                | FixtureStep::Watermark { .. }
                | FixtureStep::EndOfInput { .. } => {}
            }
        }

        let has_outputs = !self.expected.rows.is_empty()
            || !self.expected.effects.is_empty()
            || !self.expected.errors.is_empty();
        if self.expected.allow_no_outputs && has_outputs {
            return Err(FixtureError::Validation(
                "contradictory expectations: allow_no_outputs is true while rows/effects/errors are non-empty"
                    .to_owned(),
            ));
        }
        if !self.expected.allow_no_outputs && !has_outputs {
            return Err(FixtureError::Validation(
                "expected must declare at least one row, effect, error, or set allow_no_outputs=true"
                    .to_owned(),
            ));
        }

        for row in &self.expected.rows {
            if !rules.contains_key(&row.rule_id) {
                return Err(FixtureError::Validation(format!(
                    "expected.rows references undeclared rule {}",
                    row.rule_id
                )));
            }
        }
        for effect in &self.expected.effects {
            let Some(actions) = rules.get(&effect.rule_id) else {
                return Err(FixtureError::Validation(format!(
                    "expected.effects references undeclared rule {}",
                    effect.rule_id
                )));
            };
            if !actions.contains(&effect.action_id) {
                return Err(FixtureError::Validation(format!(
                    "expected.effects references undeclared action {} on rule {}",
                    effect.action_id, effect.rule_id
                )));
            }
        }
        for error in &self.expected.errors {
            if !rules.contains_key(&error.rule_id) {
                return Err(FixtureError::Validation(format!(
                    "expected.errors references undeclared rule {}",
                    error.rule_id
                )));
            }
        }

        Ok(())
    }
}
