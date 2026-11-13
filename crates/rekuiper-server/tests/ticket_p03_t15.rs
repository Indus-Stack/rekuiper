use rekuiper_core::contracts::{
    ContentChecksum, DeadlineCancellation, EdgeId, Generation, LogicalDeadline, OperatorId,
    PlanIdentity, RuleId, StateError, StateParticipant, StateResourceUsage, StateSchemaVersion,
    StateSnapshot, TestClock,
};
use rekuiper_core::model::{EdgeTargets, GraphDefinition, GraphNode, GraphTopo};
use rekuiper_core::reliability::{ReliableSqliteStore, SqliteDurabilityMode};
use rekuiper_server::engine::dag::validate_reliable_dag_shape;
use rekuiper_server::engine::runtime::reliable::{
    required_participants_for_plan, QualifiedPlanRunner, ReliableRuntimeError,
    RequiredParticipantKind,
};
use rekuiper_server::engine::state::joins_cep::{
    CepSequenceStateParticipant, JoinBufferStateParticipant, JoinSnapshotKind,
};
use rekuiper_server::engine::state::session_state::{
    SessionWindowStateParticipant, StateWindowStateParticipant,
};
use rekuiper_server::engine::state::{
    HoppingWindowStateParticipant, MissedDeadlinePolicy, SlidingWindowStateParticipant,
};
use rekuiper_sql::parser::Parser;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet, HashMap};

fn fp(ch: char) -> ContentChecksum {
    let hex: String = std::iter::repeat_n(ch, 64).collect();
    ContentChecksum::try_from(format!("sha256:{hex}")).unwrap()
}

struct BarrierParticipantStub {
    id: OperatorId,
}

impl StateParticipant for BarrierParticipantStub {
    fn operator_id(&self) -> &OperatorId {
        &self.id
    }

    fn state_version(&self) -> StateSchemaVersion {
        StateSchemaVersion(1)
    }

    fn resource_usage(&self) -> StateResourceUsage {
        StateResourceUsage::default()
    }

    fn capture_snapshot(&self) -> Result<StateSnapshot, StateError> {
        let payload = br#"{"aligned":true}"#.to_vec();
        Ok(StateSnapshot {
            envelope_version: 1,
            operator_id: self.id.clone(),
            state_version: StateSchemaVersion(1),
            checksum: fp('b'),
            usage: StateResourceUsage {
                records: 1,
                bytes: payload.len() as u64,
            },
            payload,
        })
    }

    fn restore_snapshot(&mut self, _snapshot: &StateSnapshot) -> Result<(), StateError> {
        Ok(())
    }
}

#[test]
fn participant_coverage_rejects_omitted_participant_lead_and_unqualified_dag() {
    let hop_stmt = Parser::new("SELECT temp FROM s1 GROUP BY HOPPINGWINDOW(ss, 10, 5)")
        .parse_select()
        .unwrap();
    assert_eq!(
        required_participants_for_plan(&hop_stmt, false, 1).unwrap(),
        vec![RequiredParticipantKind::HoppingWindow]
    );

    let slide_stmt = Parser::new("SELECT temp FROM s1 GROUP BY SLIDINGWINDOW(ss, 10)")
        .parse_select()
        .unwrap();
    assert_eq!(
        required_participants_for_plan(&slide_stmt, false, 1).unwrap(),
        vec![RequiredParticipantKind::SlidingWindow]
    );

    let session_stmt = Parser::new("SELECT temp FROM s1 GROUP BY SESSIONWINDOW(ss, 30, 5)")
        .parse_select()
        .unwrap();
    assert_eq!(
        required_participants_for_plan(&session_stmt, false, 1).unwrap(),
        vec![RequiredParticipantKind::SessionWindow]
    );

    let state_stmt = Parser::new("SELECT temp FROM s1 GROUP BY STATEWINDOW(temp > 10)")
        .parse_select()
        .unwrap();
    assert_eq!(
        required_participants_for_plan(&state_stmt, false, 1).unwrap(),
        vec![RequiredParticipantKind::StateWindow]
    );

    let join_stmt = Parser::new(
        "SELECT s1.temp FROM s1 INNER JOIN s2 ON s1.id = s2.id GROUP BY HOPPINGWINDOW(ss, 10, 5)",
    )
    .parse_select()
    .unwrap();
    assert_eq!(
        required_participants_for_plan(&join_stmt, true, 2).unwrap(),
        vec![
            RequiredParticipantKind::HoppingWindow,
            RequiredParticipantKind::JoinBuffer,
            RequiredParticipantKind::CepSequence,
            RequiredParticipantKind::BarrierAlignment,
        ]
    );

    let lead_stmt = Parser::new("SELECT LEAD(temp, 1) FROM s1")
        .parse_select()
        .unwrap();
    assert!(matches!(
        required_participants_for_plan(&lead_stmt, false, 1),
        Err(ReliableRuntimeError::UnsupportedShape(_))
    ));

    let hop = HoppingWindowStateParticipant::new(
        OperatorId::try_from("hop_1").unwrap(),
        fp('a'),
        1000,
        500,
    );
    let identity = PlanIdentity {
        rule_id: RuleId::try_from("rule_join_hop").unwrap(),
        generation: Generation(1),
        plan_fingerprint: fp('c'),
        schema_fingerprints: BTreeMap::new(),
        extension_digests: BTreeMap::new(),
    };
    let err = QualifiedPlanRunner::new(
        identity,
        &join_stmt,
        false,
        2,
        BTreeSet::from(["mqtt_0".to_string()]),
        &[&hop],
    )
    .err()
    .expect("omitted join/barrier participants must fail validation");
    assert!(matches!(
        err,
        ReliableRuntimeError::MissingParticipant("join")
    ));

    let mut cyclic_edges = HashMap::new();
    cyclic_edges.insert(
        "n1".to_string(),
        EdgeTargets::Single(vec!["n2".to_string()]),
    );
    cyclic_edges.insert(
        "n2".to_string(),
        EdgeTargets::Single(vec!["n1".to_string()]),
    );
    let cyclic_graph = GraphDefinition {
        nodes: HashMap::new(),
        topo: GraphTopo {
            sources: vec!["n1".to_string()],
            edges: cyclic_edges,
        },
    };
    assert!(validate_reliable_dag_shape(&cyclic_graph, &[]).is_err());

    let mut watchdog_nodes = HashMap::new();
    watchdog_nodes.insert(
        "wd1".to_string(),
        GraphNode {
            node_type: "watchdog".to_string(),
            node_category: "operator".to_string(),
            props: HashMap::new(),
        },
    );
    let watchdog_graph = GraphDefinition {
        nodes: watchdog_nodes,
        topo: GraphTopo {
            sources: vec![],
            edges: HashMap::new(),
        },
    };
    assert!(validate_reliable_dag_shape(&watchdog_graph, &[]).is_err());
}

#[tokio::test]
async fn qualified_shapes_pass_snapshot_split_recovery_and_require_all_actions_at_barrier() {
    let store = ReliableSqliteStore::open(":memory:", SqliteDurabilityMode::Full)
        .await
        .unwrap();
    let clock = TestClock::new(10_000);
    let cancellation = DeadlineCancellation::default();

    let stmt = Parser::new(
        "SELECT s1.temp FROM s1 INNER JOIN s2 ON s1.id = s2.id GROUP BY HOPPINGWINDOW(ss, 10, 5)",
    )
    .parse_select()
    .unwrap();
    let identity = PlanIdentity {
        rule_id: RuleId::try_from("rule_qualified_multi").unwrap(),
        generation: Generation(1),
        plan_fingerprint: fp('d'),
        schema_fingerprints: BTreeMap::new(),
        extension_digests: BTreeMap::new(),
    };
    let expected_actions = BTreeSet::from(["mqtt_0".to_string(), "kafka_1".to_string()]);

    let mut hop = HoppingWindowStateParticipant::new(
        OperatorId::try_from("hop_1").unwrap(),
        fp('a'),
        1000,
        500,
    );
    hop.insert_row(1, 200, json!({"v": 1}));
    hop.insert_row(2, 600, json!({"v": 2}));

    let mut join = JoinBufferStateParticipant::new(
        OperatorId::try_from("join_1").unwrap(),
        fp('1'),
        fp('2'),
        JoinSnapshotKind::Inner,
        Some(5_000),
    );
    join.ingest_left("l1", "k1", 200, json!({"id": "k1", "v": 1}));

    let cep_steps = vec!["step_a".to_string(), "step_b".to_string()];
    let cep = CepSequenceStateParticipant::new(
        OperatorId::try_from("cep_1").unwrap(),
        fp('3'),
        fp('c'),
        cep_steps.clone(),
        Some(5_000),
    )
    .unwrap();
    let _slide = SlidingWindowStateParticipant::new(
        OperatorId::try_from("slide_1").unwrap(),
        fp('4'),
        1000,
        0,
    );
    let _session = SessionWindowStateParticipant::new(
        OperatorId::try_from("session_1").unwrap(),
        fp('5'),
        5000,
        Some(1000),
        MissedDeadlinePolicy::FireImmediatelyInOrder,
    );
    let _state =
        StateWindowStateParticipant::new(OperatorId::try_from("state_1").unwrap(), fp('6'), false);
    let barrier = BarrierParticipantStub {
        id: OperatorId::try_from("barrier_1").unwrap(),
    };

    let mut runner = QualifiedPlanRunner::new(
        identity.clone(),
        &stmt,
        true,
        2,
        expected_actions,
        &[&hop, &join, &cep, &barrier],
    )
    .expect("complete qualified participant set should pass validation");

    let partial_acks = BTreeSet::from(["mqtt_0".to_string()]);
    let incomplete_err = runner
        .commit_checkpoint(
            &store,
            &[&hop, &join, &cep, &barrier],
            vec![],
            &partial_acks,
            &clock,
            &cancellation,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        incomplete_err,
        ReliableRuntimeError::IncompleteBarrierActions(_)
    ));

    let all_acks = BTreeSet::from(["mqtt_0".to_string(), "kafka_1".to_string()]);
    let cp_id = runner
        .commit_checkpoint(
            &store,
            &[&hop, &join, &cep, &barrier],
            vec![],
            &all_acks,
            &clock,
            &cancellation,
        )
        .await
        .expect("all-actions barrier commit should succeed");
    assert_eq!(cp_id.0, 1);

    let mut restored_hop = HoppingWindowStateParticipant::new(
        OperatorId::try_from("hop_1").unwrap(),
        fp('a'),
        1000,
        500,
    );
    let mut restored_join = JoinBufferStateParticipant::new(
        OperatorId::try_from("join_1").unwrap(),
        fp('1'),
        fp('2'),
        JoinSnapshotKind::Inner,
        Some(5_000),
    );
    let mut restored_cep = CepSequenceStateParticipant::new(
        OperatorId::try_from("cep_1").unwrap(),
        fp('3'),
        fp('c'),
        cep_steps,
        Some(5_000),
    )
    .unwrap();
    let mut restored_barrier = BarrierParticipantStub {
        id: OperatorId::try_from("barrier_1").unwrap(),
    };

    let mut recovery_runner = QualifiedPlanRunner::new(
        identity,
        &stmt,
        true,
        2,
        BTreeSet::from(["mqtt_0".to_string(), "kafka_1".to_string()]),
        &[
            &restored_hop,
            &restored_join,
            &restored_cep,
            &restored_barrier,
        ],
    )
    .unwrap();
    recovery_runner
        .restore_before_intake(
            &store,
            &mut [
                &mut restored_hop,
                &mut restored_join,
                &mut restored_cep,
                &mut restored_barrier,
            ],
        )
        .await
        .expect("restore_before_intake should succeed");

    hop.insert_row(3, 1100, json!({"v": 3}));
    restored_hop.insert_row(3, 1100, json!({"v": 3}));
    assert_eq!(
        hop.fire_due(2500, MissedDeadlinePolicy::FireImmediatelyInOrder),
        restored_hop.fire_due(2500, MissedDeadlinePolicy::FireImmediatelyInOrder)
    );
    let _ = (EdgeId(1), LogicalDeadline(1_000));
}
