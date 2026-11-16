use rekuiper_core::contracts::{
    ActionId, Capability, CapabilityComponent, CapabilityDescriptor, InputMode, OperatorId,
    QualificationLevel, ResultMode, SourceId,
};
use rekuiper_sql::ast::Expr;
use rekuiper_sql::functions::builtin_function_descriptors;
use rekuiper_sql::planning::{
    validate_plan_capabilities, LogicalPlanNode, PlanDiagnosticKind, PlanExecutionProfile,
    SchemaEnv,
};
use std::collections::{BTreeMap, BTreeSet};

#[test]
fn durable_lossy_retract_append_only_and_stateful_without_snapshot_fail_distinctly() {
    let src_id = SourceId::try_from("kafka_in").unwrap();
    let op_id = OperatorId::try_from("win_op").unwrap();
    let act_id = ActionId::try_from("mqtt_out").unwrap();

    let mut descriptors = BTreeMap::new();
    descriptors.insert(
        CapabilityComponent::Source(src_id.clone()),
        CapabilityDescriptor {
            supported: BTreeSet::from([
                Capability::Input(InputMode::AppendOnly),
                Capability::Durability,
                Capability::Replay,
            ]),
            limits: BTreeMap::new(),
            qualification: QualificationLevel::ComponentTested,
        },
    );
    descriptors.insert(
        CapabilityComponent::Operator(op_id.clone()),
        CapabilityDescriptor {
            supported: BTreeSet::from([
                Capability::Result(ResultMode::Retract),
                Capability::Replay,
            ]),
            limits: BTreeMap::new(),
            qualification: QualificationLevel::ComponentTested,
        },
    );
    descriptors.insert(
        CapabilityComponent::Action(act_id.clone()),
        CapabilityDescriptor {
            supported: BTreeSet::from([
                Capability::Result(ResultMode::AppendOnly),
                Capability::Acknowledgement,
            ]),
            limits: BTreeMap::new(),
            qualification: QualificationLevel::ComponentTested,
        },
    );

    let nodes = vec![
        LogicalPlanNode::Source {
            id: src_id.clone(),
            input_mode: InputMode::AppendOnly,
            lossy_queue: true,
        },
        LogicalPlanNode::Operator {
            id: op_id.clone(),
            stateful: true,
            delayed_or_windowed: true,
            result_mode: ResultMode::Retract,
            exprs: vec![Expr::Call {
                name: "custom_stateful_ext".to_string(),
                args: vec![Expr::Identifier("temp".to_string())],
            }],
        },
        LogicalPlanNode::Action {
            id: act_id.clone(),
            required_result_mode: ResultMode::Retract,
        },
    ];

    let profile = PlanExecutionProfile::Durable {
        require_replay: true,
        require_snapshot: true,
        min_qualification: QualificationLevel::ComponentTested,
    };

    let diagnostics = validate_plan_capabilities(
        &nodes,
        &descriptors,
        &profile,
        &SchemaEnv::schemaless(),
        builtin_function_descriptors(),
    )
    .expect_err("all three incompatibilities must be reported");

    assert!(diagnostics.iter().any(|d| {
        d.component == CapabilityComponent::Source(src_id.clone())
            && d.kind == PlanDiagnosticKind::DurableWithLossyQueue
    }));
    assert!(diagnostics.iter().any(|d| {
        d.component == CapabilityComponent::Operator(op_id.clone())
            && d.kind == PlanDiagnosticKind::MissingSnapshotSupport
    }));
    assert!(diagnostics.iter().any(|d| {
        d.component == CapabilityComponent::Action(act_id.clone())
            && d.kind == PlanDiagnosticKind::IncompatibleResultMode(ResultMode::Retract)
    }));
}

#[test]
fn ordinary_lightweight_select_remains_compatible() {
    let nodes = vec![
        LogicalPlanNode::Source {
            id: SourceId::try_from("stream_in").unwrap(),
            input_mode: InputMode::AppendOnly,
            lossy_queue: true,
        },
        LogicalPlanNode::Operator {
            id: OperatorId::try_from("proj_1").unwrap(),
            stateful: false,
            delayed_or_windowed: false,
            result_mode: ResultMode::AppendOnly,
            exprs: vec![Expr::Identifier("temp".to_string())],
        },
        LogicalPlanNode::Action {
            id: ActionId::try_from("log_out").unwrap(),
            required_result_mode: ResultMode::AppendOnly,
        },
    ];

    validate_plan_capabilities(
        &nodes,
        &BTreeMap::new(),
        &PlanExecutionProfile::Lightweight,
        &SchemaEnv::schemaless(),
        builtin_function_descriptors(),
    )
    .expect("lightweight ordinary SELECT stays compatible");
}
