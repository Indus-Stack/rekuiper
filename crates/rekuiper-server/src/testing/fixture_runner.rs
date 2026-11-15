use crate::engine::dag::{validate_graph_acyclic, validate_graph_nodes};
use crate::engine::operators::SwitchNode;
use indexmap::IndexMap;
use rekuiper_core::contracts::{
    ActionId, Clock, EffectDestination, EffectDispatch, EffectId, EffectOutcome, EffectSink,
    Generation, RuleId, TestClock,
};
use rekuiper_core::model::{GraphDefinition, StreamRecord};
use rekuiper_core::testing::{FakeCheckpointOutboxStore, FakeEffectSink};
use rekuiper_sql::{Evaluator, Parser, RuleState};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

/// Execution mode for [`ProductionFixtureRunner`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FixtureMode {
    /// Default deterministic mode: live outbound network effects are prohibited.
    #[default]
    IsolatedDeterministic,
    /// Explicit opt-in for integration harnesses that allow live outbound effects.
    LiveEffectsOptIn,
}

/// Deterministic in-memory lookup fixture replacing external Redis/SQL/HTTP lookups.
#[derive(Debug, Clone, Default)]
pub struct FixtureLookupProvider {
    tables: BTreeMap<String, Vec<HashMap<String, Value>>>,
}

impl FixtureLookupProvider {
    pub fn insert_rows(&mut self, table: impl Into<String>, rows: Vec<HashMap<String, Value>>) {
        self.tables.insert(table.into(), rows);
    }

    pub fn lookup(&self, table: &str, key: &str, value: &Value) -> Option<&HashMap<String, Value>> {
        self.tables
            .get(table)?
            .iter()
            .find(|row| row.get(key) == Some(value))
    }
}

#[derive(Debug, Clone)]
struct ScheduledTimer {
    id: String,
    fire_at: Duration,
    payload: Value,
}

/// Production-backed fixture runner with isolated state, [`TestClock`], fake lookup,
/// and capture sink.
pub struct ProductionFixtureRunner {
    mode: FixtureMode,
    clock: Arc<TestClock>,
    state_store: FakeCheckpointOutboxStore,
    capture_sink: FakeEffectSink,
    lookup: FixtureLookupProvider,
    rule_state: RuleState,
    timers: Vec<ScheduledTimer>,
    window_buffer: Vec<HashMap<String, Value>>,
    window_deadline: Option<Duration>,
}

impl Default for ProductionFixtureRunner {
    fn default() -> Self {
        Self::new(FixtureMode::IsolatedDeterministic)
    }
}

impl ProductionFixtureRunner {
    pub fn new(mode: FixtureMode) -> Self {
        Self {
            mode,
            clock: Arc::new(TestClock::new(0)),
            state_store: FakeCheckpointOutboxStore::default(),
            capture_sink: FakeEffectSink::new(),
            lookup: FixtureLookupProvider::default(),
            rule_state: RuleState::default(),
            timers: Vec::new(),
            window_buffer: Vec::new(),
            window_deadline: None,
        }
    }

    pub fn clock(&self) -> &Arc<TestClock> {
        &self.clock
    }

    pub fn state_store(&self) -> &FakeCheckpointOutboxStore {
        &self.state_store
    }

    pub fn lookup_mut(&mut self) -> &mut FixtureLookupProvider {
        &mut self.lookup
    }

    pub fn advance_clock(&self, delta: Duration) -> Duration {
        self.clock.advance(delta);
        self.clock.now().elapsed
    }

    pub fn schedule_timer(&mut self, id: impl Into<String>, delay: Duration, payload: Value) {
        let fire_at = self.clock.now().elapsed + delay;
        self.timers.push(ScheduledTimer {
            id: id.into(),
            fire_at,
            payload,
        });
    }

    pub fn poll_ready_timers(&mut self) -> Vec<(String, Value)> {
        let now = self.clock.now().elapsed;
        let mut ready = Vec::new();
        let mut pending = Vec::new();
        for t in self.timers.drain(..) {
            if t.fire_at <= now {
                ready.push((t.id, t.payload));
            } else {
                pending.push(t);
            }
        }
        self.timers = pending;
        ready
    }

    /// Executes a SQL transform/filter plan through the production parser + stateful evaluator.
    pub fn run_sql_transform(
        &mut self,
        sql: &str,
        inputs: &[HashMap<String, Value>],
    ) -> Result<Vec<IndexMap<String, Value>>, String> {
        let mut parser = Parser::new(sql);
        let stmt = parser.parse_select().map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for row in inputs {
            if let Some(projected) =
                Evaluator::eval_select_filtered_stateful_fallible(&stmt, row, &self.rule_state)?
            {
                out.push(projected);
            }
        }
        Ok(out)
    }

    /// Steps a windowed aggregate plan using the production `TestClock` and `Evaluator::eval_aggregate`.
    pub fn push_window_event(&mut self, window_size: Duration, row: HashMap<String, Value>) {
        if self.window_deadline.is_none() {
            self.window_deadline = Some(self.clock.now().elapsed + window_size);
        }
        self.window_buffer.push(row);
    }

    pub fn flush_window_if_due(
        &mut self,
        sql: &str,
    ) -> Result<Option<IndexMap<String, Value>>, String> {
        let Some(deadline) = self.window_deadline else {
            return Ok(None);
        };
        if self.clock.now().elapsed < deadline {
            return Ok(None);
        }
        let mut parser = Parser::new(sql);
        let stmt = parser.parse_select().map_err(|e| e.to_string())?;
        let batch = std::mem::take(&mut self.window_buffer);
        self.window_deadline = None;
        Ok(Evaluator::eval_aggregate(&stmt, &batch))
    }

    /// Validates a production `GraphDefinition` and steps a `SwitchNode` operator if present.
    pub fn validate_and_step_dag(
        &self,
        graph: &GraphDefinition,
        switch_node_id: Option<&str>,
        record: &StreamRecord,
    ) -> Result<Option<usize>, String> {
        validate_graph_nodes(graph)?;
        validate_graph_acyclic(graph)?;
        if let Some(id) = switch_node_id {
            let node = graph
                .nodes
                .get(id)
                .ok_or_else(|| format!("missing switch node '{id}'"))?;
            let sw = SwitchNode::from_props(&node.props).map_err(|e| e.to_string())?;
            return Ok(sw.route(record));
        }
        Ok(None)
    }

    /// Attempts a live outbound network effect; fails in default `IsolatedDeterministic` mode.
    pub fn attempt_live_network_effect(&self, endpoint: &str) -> Result<(), String> {
        if self.mode == FixtureMode::IsolatedDeterministic {
            return Err(format!(
                "live outbound network effect prohibited in default fixture mode: {endpoint}"
            ));
        }
        Ok(())
    }

    /// Captures an effect through the deterministic `FakeEffectSink`.
    pub async fn capture_effect(
        &self,
        effect_id: &str,
        action_id: &str,
        payload: Vec<u8>,
    ) -> Result<EffectOutcome, String> {
        let dispatch = EffectDispatch {
            effect_id: EffectId::try_from(effect_id).map_err(|e| e.to_string())?,
            rule_id: RuleId::try_from("fixture_rule").map_err(|e| e.to_string())?,
            generation: Generation(1),
            destination: EffectDestination {
                action_id: ActionId::try_from(action_id).map_err(|e| e.to_string())?,
                connection_id: None,
            },
            order_key: None,
            payload,
        };
        Ok(self.capture_sink.deliver(&dispatch).await)
    }

    pub fn captured_dispatches(&self) -> Vec<EffectDispatch> {
        self.capture_sink.dispatches()
    }
}
