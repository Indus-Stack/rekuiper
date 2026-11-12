use rekuiper_core::{
    check_plan_requirements, Capability, CapabilityComponent, CapabilityDescriptor,
    CapabilityFailure, CapabilityFailureReason, CapabilityLimit, CapabilityRequirements, InputMode,
    OrderingScope, QualificationLevel, ResultMode,
};
use std::collections::BTreeMap;

#[test]
fn durable_plan_rejects_nonsnapshotting_operator_with_replayable_source() {
    let source = CapabilityComponent::Source("source".try_into().unwrap());
    let operator = CapabilityComponent::Operator("aggregate".try_into().unwrap());
    let requirements = BTreeMap::from([
        (
            source.clone(),
            CapabilityRequirements {
                required: [Capability::Replay].into(),
                ..CapabilityRequirements::default()
            },
        ),
        (
            operator.clone(),
            CapabilityRequirements {
                required: [Capability::Snapshot, Capability::Restore].into(),
                ..CapabilityRequirements::default()
            },
        ),
    ]);
    let mut descriptors = BTreeMap::from([
        (
            source,
            CapabilityDescriptor {
                supported: [Capability::Replay].into(),
                ..CapabilityDescriptor::default()
            },
        ),
        (
            operator.clone(),
            CapabilityDescriptor {
                supported: [Capability::Restore, Capability::Deterministic].into(),
                ..CapabilityDescriptor::default()
            },
        ),
    ]);
    assert_eq!(
        check_plan_requirements(&requirements, &descriptors),
        Err(vec![CapabilityFailure {
            component: operator.clone(),
            reason: CapabilityFailureReason::Unsupported(Capability::Snapshot),
        }])
    );

    descriptors
        .get_mut(&operator)
        .unwrap()
        .supported
        .insert(Capability::Snapshot);
    assert_eq!(check_plan_requirements(&requirements, &descriptors), Ok(()));
}

#[test]
fn retract_plan_rejects_append_only_sink() {
    let action = CapabilityComponent::Action("sink".try_into().unwrap());
    let required = Capability::Input(InputMode::Retract);
    let requirements = BTreeMap::from([(
        action.clone(),
        CapabilityRequirements {
            required: [required].into(),
            ..CapabilityRequirements::default()
        },
    )]);
    let descriptors = BTreeMap::from([(
        action.clone(),
        CapabilityDescriptor {
            supported: [Capability::Input(InputMode::AppendOnly)].into(),
            ..CapabilityDescriptor::default()
        },
    )]);
    assert_eq!(
        check_plan_requirements(&requirements, &descriptors),
        Err(vec![CapabilityFailure {
            component: action,
            reason: CapabilityFailureReason::Unsupported(required),
        }])
    );
}

#[test]
fn legacy_lightweight_components_need_no_descriptor() {
    let operator = CapabilityComponent::Operator("legacy".try_into().unwrap());
    let requirements = BTreeMap::from([(operator, CapabilityRequirements::default())]);
    assert_eq!(
        check_plan_requirements(&requirements, &BTreeMap::new()),
        Ok(())
    );
}

#[test]
fn mixed_declarations_report_all_failures_in_stable_order() {
    let source_a = CapabilityComponent::Source("a".try_into().unwrap());
    let source_z = CapabilityComponent::Source("z".try_into().unwrap());
    let operator = CapabilityComponent::Operator("a".try_into().unwrap());
    let action = CapabilityComponent::Action("a".try_into().unwrap());

    let requirements = BTreeMap::from([
        (
            action.clone(),
            CapabilityRequirements {
                required: [Capability::Transaction].into(),
                ..CapabilityRequirements::default()
            },
        ),
        (
            source_z.clone(),
            CapabilityRequirements {
                required: [Capability::Result(ResultMode::Final)].into(),
                ..CapabilityRequirements::default()
            },
        ),
        (
            operator,
            CapabilityRequirements {
                required: [Capability::Deterministic].into(),
                ..CapabilityRequirements::default()
            },
        ),
        (
            source_a.clone(),
            CapabilityRequirements {
                required: [
                    Capability::Ordering(OrderingScope::Partition),
                    Capability::Acknowledgement,
                    Capability::Replay,
                ]
                .into(),
                minimum_limits: [(CapabilityLimit::Bytes, 256), (CapabilityLimit::Records, 5)]
                    .into(),
                minimum_qualification: QualificationLevel::Qualified,
            },
        ),
    ]);
    let descriptors = BTreeMap::from([
        (
            source_a.clone(),
            CapabilityDescriptor {
                supported: [Capability::Replay].into(),
                limits: [(CapabilityLimit::Bytes, 255)].into(),
                qualification: QualificationLevel::ComponentTested,
            },
        ),
        (
            action.clone(),
            CapabilityDescriptor {
                qualification: QualificationLevel::Qualified,
                ..CapabilityDescriptor::default()
            },
        ),
        (
            CapabilityComponent::Operator("a".try_into().unwrap()),
            CapabilityDescriptor {
                supported: [Capability::Deterministic].into(),
                ..CapabilityDescriptor::default()
            },
        ),
    ]);

    use CapabilityFailureReason::{Limit, Qualification, Unsupported};
    let expected = [
        (
            source_a.clone(),
            Unsupported(Capability::Ordering(OrderingScope::Partition)),
        ),
        (source_a.clone(), Unsupported(Capability::Acknowledgement)),
        (
            source_a.clone(),
            Limit {
                limit: CapabilityLimit::Records,
                required: 5,
                available: None,
            },
        ),
        (
            source_a.clone(),
            Limit {
                limit: CapabilityLimit::Bytes,
                required: 256,
                available: Some(255),
            },
        ),
        (
            source_a,
            Qualification {
                required: QualificationLevel::Qualified,
                available: QualificationLevel::ComponentTested,
            },
        ),
        (source_z, Unsupported(Capability::Result(ResultMode::Final))),
        (action, Unsupported(Capability::Transaction)),
    ]
    .into_iter()
    .map(|(component, reason)| CapabilityFailure { component, reason })
    .collect();
    assert_eq!(
        check_plan_requirements(&requirements, &descriptors),
        Err(expected)
    );
}

#[test]
fn descriptor_wire_defaults_do_not_claim_support_or_qualification() {
    let empty: CapabilityDescriptor = serde_json::from_str("{}").unwrap();
    assert_eq!(empty, CapabilityDescriptor::default());

    let wire = serde_json::json!({
        "supported": ["replay", {"input": "append_only"}],
        "limits": {"bytes": 256},
        "qualification": "component_tested"
    });
    let descriptor: CapabilityDescriptor = serde_json::from_value(wire).unwrap();
    assert_eq!(
        descriptor,
        CapabilityDescriptor {
            supported: [Capability::Input(InputMode::AppendOnly), Capability::Replay].into(),
            limits: [(CapabilityLimit::Bytes, 256)].into(),
            qualification: QualificationLevel::ComponentTested,
        }
    );
    let component = CapabilityComponent::Source("declared".try_into().unwrap());
    let requirements = BTreeMap::from([(
        component.clone(),
        CapabilityRequirements {
            required: [Capability::Replay].into(),
            minimum_limits: [(CapabilityLimit::Bytes, 256)].into(),
            minimum_qualification: QualificationLevel::ComponentTested,
        },
    )]);
    assert_eq!(
        check_plan_requirements(&requirements, &BTreeMap::from([(component, descriptor)])),
        Ok(())
    );
}
