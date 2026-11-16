//! Capability declarations and checks for a complete plan.

use super::identity::{ActionId, ConnectionId, OperatorId, SourceId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputMode {
    AppendOnly,
    Upsert,
    Retract,
    Changelog,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultMode {
    AppendOnly,
    Final,
    Upsert,
    Retract,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderingScope {
    Partition,
    Global,
}

/// An explicit support claim. Declare each supported mode and ordering scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    Input(InputMode),
    Result(ResultMode),
    Ordering(OrderingScope),
    Replay,
    Acknowledgement,
    Durability,
    Transaction,
    Snapshot,
    Restore,
    Migration,
    Deterministic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityLimit {
    Records,
    Bytes,
    ConcurrentTasks,
}

/// Evidence level, independent of the component's support claims.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualificationLevel {
    #[default]
    Unqualified,
    ComponentTested,
    Qualified,
}

/// Support claimed by one component. Omitted capabilities and limits are unsupported.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CapabilityDescriptor {
    pub supported: BTreeSet<Capability>,
    /// Maximum supported capacity for each declared dimension.
    pub limits: BTreeMap<CapabilityLimit, u64>,
    pub qualification: QualificationLevel,
}

/// Requirements for one component, as derived by the planner.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CapabilityRequirements {
    pub required: BTreeSet<Capability>,
    pub minimum_limits: BTreeMap<CapabilityLimit, u64>,
    pub minimum_qualification: QualificationLevel,
}

/// Component identity within a plan. IDs are independent across roles.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum CapabilityComponent {
    Source(SourceId),
    Operator(OperatorId),
    Action(ActionId),
    Connection(ConnectionId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityFailure {
    pub component: CapabilityComponent,
    pub reason: CapabilityFailureReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapabilityFailureReason {
    Unsupported(Capability),
    Limit {
        limit: CapabilityLimit,
        required: u64,
        available: Option<u64>,
    },
    Qualification {
        required: QualificationLevel,
        available: QualificationLevel,
    },
}

/// Checks all component requirements and returns every incompatibility.
///
/// Components are ordered by role (source, operator, action, connection) and
/// then by logical ID. Reasons are ordered by capability, limit, and finally
/// qualification, using declaration order within each category.
/// An absent descriptor provides no support and has unqualified evidence.
pub fn check_plan_requirements(
    requirements: &BTreeMap<CapabilityComponent, CapabilityRequirements>,
    descriptors: &BTreeMap<CapabilityComponent, CapabilityDescriptor>,
) -> Result<(), Vec<CapabilityFailure>> {
    let mut failures = Vec::new();
    for (component, requirements) in requirements {
        let descriptor = descriptors.get(component);
        let mut reject = |reason| {
            failures.push(CapabilityFailure {
                component: component.clone(),
                reason,
            });
        };

        for &capability in &requirements.required {
            if !descriptor.is_some_and(|descriptor| descriptor.supported.contains(&capability)) {
                reject(CapabilityFailureReason::Unsupported(capability));
            }
        }

        for (&limit, &required) in &requirements.minimum_limits {
            let available =
                descriptor.and_then(|descriptor| descriptor.limits.get(&limit).copied());
            if available.is_none_or(|available| available < required) {
                reject(CapabilityFailureReason::Limit {
                    limit,
                    required,
                    available,
                });
            }
        }

        let qualification = descriptor.map_or(QualificationLevel::Unqualified, |descriptor| {
            descriptor.qualification
        });
        if qualification < requirements.minimum_qualification {
            reject(CapabilityFailureReason::Qualification {
                required: requirements.minimum_qualification,
                available: qualification,
            });
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures)
    }
}
