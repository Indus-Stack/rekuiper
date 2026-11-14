//! Capability-aware plan validation translating logical plan nodes into C02 checks.

use crate::ast::Expr;
use crate::planning::types::{
    type_check_expr, FunctionDescriptorRegistry, SchemaEnv, Statefulness, TypeCheckError,
};
use rekuiper_core::contracts::{
    check_plan_requirements, ActionId, Capability, CapabilityComponent, CapabilityDescriptor,
    CapabilityFailure, CapabilityFailureReason, CapabilityRequirements, InputMode, OperatorId,
    QualificationLevel, ResultMode, SourceId,
};
use std::collections::{BTreeMap, BTreeSet};

/// Execution profile controlling whether durability, replay, and snapshots are required.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum PlanExecutionProfile {
    /// Default lightweight execution: ordinary SELECT remains compatible without
    /// requiring durability, replay, or snapshot capabilities.
    #[default]
    Lightweight,
    /// Durable execution profile enforcing replay, snapshot, and qualification guarantees.
    Durable {
        require_replay: bool,
        require_snapshot: bool,
        min_qualification: QualificationLevel,
    },
}

/// Logical plan node translated into C02 capability requirements.
#[derive(Debug, Clone)]
pub enum LogicalPlanNode {
    Source {
        id: SourceId,
        input_mode: InputMode,
        lossy_queue: bool,
    },
    Operator {
        id: OperatorId,
        stateful: bool,
        delayed_or_windowed: bool,
        result_mode: ResultMode,
        exprs: Vec<Expr>,
    },
    Action {
        id: ActionId,
        required_result_mode: ResultMode,
    },
}

/// Distinct diagnostic category for plan capability incompatibilities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanDiagnosticKind {
    DurableWithLossyQueue,
    IncompatibleResultMode(ResultMode),
    MissingSnapshotSupport,
    MissingReplaySupport,
    TypeCheck(TypeCheckError),
    Capability(CapabilityFailureReason),
}

/// Stable-ID diagnostic reported for an incompatible plan node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanCapabilityDiagnostic {
    pub component: CapabilityComponent,
    pub kind: PlanDiagnosticKind,
    pub message: String,
}

/// Translates logical plan nodes into C02 [`CapabilityRequirements`] and reports
/// every incompatible node with stable component IDs.
pub fn validate_plan_capabilities(
    nodes: &[LogicalPlanNode],
    descriptors: &BTreeMap<CapabilityComponent, CapabilityDescriptor>,
    profile: &PlanExecutionProfile,
    schema: &SchemaEnv,
    fn_registry: &FunctionDescriptorRegistry,
) -> Result<(), Vec<PlanCapabilityDiagnostic>> {
    let mut diagnostics = Vec::new();
    let mut requirements_map: BTreeMap<CapabilityComponent, CapabilityRequirements> =
        BTreeMap::new();

    for node in nodes {
        match node {
            LogicalPlanNode::Source {
                id,
                input_mode,
                lossy_queue,
            } => {
                let comp = CapabilityComponent::Source(id.clone());
                let mut req = BTreeSet::new();
                req.insert(Capability::Input(*input_mode));

                if let PlanExecutionProfile::Durable {
                    require_replay,
                    min_qualification,
                    ..
                } = profile
                {
                    if *lossy_queue {
                        diagnostics.push(PlanCapabilityDiagnostic {
                            component: comp.clone(),
                            kind: PlanDiagnosticKind::DurableWithLossyQueue,
                            message: format!(
                                "source `{id}` uses a lossy queue under durable execution"
                            ),
                        });
                    }
                    req.insert(Capability::Durability);
                    if *require_replay {
                        req.insert(Capability::Replay);
                    }
                    requirements_map.insert(
                        comp,
                        CapabilityRequirements {
                            required: req,
                            minimum_limits: BTreeMap::new(),
                            minimum_qualification: *min_qualification,
                        },
                    );
                } else {
                    // Lightweight profile only checks declared input mode if a descriptor exists.
                    if descriptors.contains_key(&comp) {
                        requirements_map.insert(
                            comp,
                            CapabilityRequirements {
                                required: req,
                                minimum_limits: BTreeMap::new(),
                                minimum_qualification: QualificationLevel::Unqualified,
                            },
                        );
                    }
                }
            }
            LogicalPlanNode::Operator {
                id,
                stateful,
                delayed_or_windowed,
                result_mode,
                exprs,
            } => {
                let comp = CapabilityComponent::Operator(id.clone());
                let mut req = BTreeSet::new();
                let mut any_stateful = *stateful || *delayed_or_windowed;

                for expr in exprs {
                    match type_check_expr(expr, schema, fn_registry) {
                        Ok(typed) => {
                            if matches!(typed.statefulness, Statefulness::Stateful { .. }) {
                                any_stateful = true;
                            }
                            if let PlanExecutionProfile::Durable {
                                require_snapshot, ..
                            } = profile
                            {
                                if matches!(
                                    typed.statefulness,
                                    Statefulness::Stateful {
                                        supports_snapshot: false
                                    }
                                ) && *require_snapshot
                                {
                                    diagnostics.push(PlanCapabilityDiagnostic {
                                        component: comp.clone(),
                                        kind: PlanDiagnosticKind::MissingSnapshotSupport,
                                        message: format!(
                                            "operator `{id}` uses a stateful expression without snapshot support"
                                        ),
                                    });
                                }
                            }
                        }
                        Err(err) => {
                            diagnostics.push(PlanCapabilityDiagnostic {
                                component: comp.clone(),
                                kind: PlanDiagnosticKind::TypeCheck(err.clone()),
                                message: err.to_string(),
                            });
                        }
                    }
                }

                if let PlanExecutionProfile::Durable {
                    require_replay,
                    require_snapshot,
                    min_qualification,
                } = profile
                {
                    req.insert(Capability::Result(*result_mode));
                    if *require_replay {
                        req.insert(Capability::Replay);
                    }
                    if any_stateful && *require_snapshot {
                        req.insert(Capability::Snapshot);
                        req.insert(Capability::Restore);
                    }
                    requirements_map.insert(
                        comp,
                        CapabilityRequirements {
                            required: req,
                            minimum_limits: BTreeMap::new(),
                            minimum_qualification: *min_qualification,
                        },
                    );
                } else if *result_mode != ResultMode::AppendOnly && descriptors.contains_key(&comp)
                {
                    req.insert(Capability::Result(*result_mode));
                    requirements_map.insert(
                        comp,
                        CapabilityRequirements {
                            required: req,
                            minimum_limits: BTreeMap::new(),
                            minimum_qualification: QualificationLevel::Unqualified,
                        },
                    );
                }
            }
            LogicalPlanNode::Action {
                id,
                required_result_mode,
            } => {
                let comp = CapabilityComponent::Action(id.clone());
                let mut req = BTreeSet::new();
                req.insert(Capability::Result(*required_result_mode));
                match profile {
                    PlanExecutionProfile::Durable {
                        min_qualification, ..
                    } => {
                        req.insert(Capability::Acknowledgement);
                        requirements_map.insert(
                            comp,
                            CapabilityRequirements {
                                required: req,
                                minimum_limits: BTreeMap::new(),
                                minimum_qualification: *min_qualification,
                            },
                        );
                    }
                    PlanExecutionProfile::Lightweight => {
                        if *required_result_mode != ResultMode::AppendOnly
                            || descriptors.contains_key(&comp)
                        {
                            requirements_map.insert(
                                comp,
                                CapabilityRequirements {
                                    required: req,
                                    minimum_limits: BTreeMap::new(),
                                    minimum_qualification: QualificationLevel::Unqualified,
                                },
                            );
                        }
                    }
                }
            }
        }
    }

    if let Err(failures) = check_plan_requirements(&requirements_map, descriptors) {
        for failure in failures {
            diagnostics.push(map_c02_failure(failure));
        }
    }

    if diagnostics.is_empty() {
        Ok(())
    } else {
        Err(diagnostics)
    }
}

fn map_c02_failure(failure: CapabilityFailure) -> PlanCapabilityDiagnostic {
    let message = format!("{:?}: {:?}", failure.component, failure.reason);
    let kind = match failure.reason {
        CapabilityFailureReason::Unsupported(Capability::Result(mode)) => {
            PlanDiagnosticKind::IncompatibleResultMode(mode)
        }
        CapabilityFailureReason::Unsupported(Capability::Snapshot | Capability::Restore) => {
            PlanDiagnosticKind::MissingSnapshotSupport
        }
        CapabilityFailureReason::Unsupported(Capability::Replay) => {
            PlanDiagnosticKind::MissingReplaySupport
        }
        other => PlanDiagnosticKind::Capability(other),
    };
    PlanCapabilityDiagnostic {
        component: failure.component,
        kind,
        message,
    }
}
