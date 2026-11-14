//! Shared typed-planning and diagnostic adapters for REST and CLI surfaces.

use crate::ast::{Expr, SelectStmt};
use crate::parser::Parser;
use crate::planning::capabilities::{
    validate_plan_capabilities, LogicalPlanNode, PlanCapabilityDiagnostic, PlanDiagnosticKind,
    PlanExecutionProfile,
};
use crate::planning::types::{ExprType, FieldSchema, FunctionDescriptorRegistry, SchemaEnv};
use rekuiper_core::contracts::{
    ActionId, Capability, CapabilityComponent, CapabilityDescriptor, InputMode, OperatorId,
    QualificationLevel, ResultMode, SourceId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// Catalog metadata for a stream or table referenced by a rule.
#[derive(Debug, Clone, Default)]
pub struct StreamCatalogEntry {
    pub name: String,
    pub source_type: Option<String>,
    pub lossy_queue: bool,
    pub replayable: bool,
    pub qualification: QualificationLevel,
    pub schema: BTreeMap<String, ExprType>,
}

/// Canonical input to the shared typed planner adapter.
#[derive(Debug, Clone, Default)]
pub struct PlanCatalogInput {
    pub rule_id: String,
    pub sql: String,
    pub options: Option<HashMap<String, Value>>,
    pub actions: Vec<HashMap<String, Value>>,
    pub streams: Vec<StreamCatalogEntry>,
}

/// Stable diagnostic emitted by typed planning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDiagnosticReport {
    pub code: String,
    pub location: String,
    pub component: Option<String>,
    pub message: String,
}

/// Effective runtime guarantees derived from all plan components.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveGuarantees {
    pub delivery: String,
    pub qualification: QualificationLevel,
    pub limiting_component: Option<String>,
    pub stateful: bool,
    pub lossy_inputs: Vec<String>,
}

/// Secret-redacted typed plan report shared by REST and CLI validate/explain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypedPlanReport {
    pub rule_id: String,
    pub valid: bool,
    pub profile: String,
    pub sources: Vec<String>,
    pub actions: Vec<String>,
    pub redacted_options: BTreeMap<String, Value>,
    pub redacted_actions: Vec<BTreeMap<String, Value>>,
    pub effective_guarantees: EffectiveGuarantees,
    pub diagnostics: Vec<PlanDiagnosticReport>,
}

fn is_secret_key(key: &str) -> bool {
    let k = key.to_ascii_lowercase().replace(['_', '-'], "");
    k.contains("password")
        || k.contains("passwd")
        || k.contains("passphrase")
        || k.contains("secret")
        || k.contains("token")
        || k.contains("apikey")
        || k.contains("credential")
        || k.contains("privatekey")
        || k.contains("accesskey")
        || k.contains("connectionstring")
        || k.contains("authorization")
        || k.contains("cookie")
        || k.contains("sas")
        || k == "dsn"
}

fn is_secret_url_param(key: &str) -> bool {
    if is_secret_key(key) {
        return true;
    }
    let normalized = key.to_ascii_lowercase().replace(['_', '-'], "");
    matches!(
        normalized.as_str(),
        "key" | "sig" | "signature" | "auth" | "jwt" | "bearer" | "session" | "sessionid"
    )
}

fn redact_url_userinfo(s: &str) -> String {
    let out = if let Some(scheme_end) = s.find("://") {
        let rest = &s[scheme_end + 3..];
        let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        if let Some(at_pos) = rest[..authority_end].find('@') {
            format!("{}://[REDACTED]@{}", &s[..scheme_end], &rest[at_pos + 1..])
        } else {
            s.to_string()
        }
    } else {
        s.to_string()
    };

    if !(out.contains("://") || out.starts_with('/')) {
        return out;
    }
    let Some(q_idx) = out.find('?') else {
        return out;
    };
    let (base, query_and_frag) = out.split_at(q_idx);
    let after_q = &query_and_frag[1..];
    let (query, frag) = match after_q.find('#') {
        Some(f_idx) => (&after_q[..f_idx], Some(&after_q[f_idx..])),
        None => (after_q, None),
    };
    let mut changed = false;
    let redacted_params: Vec<String> = query
        .split('&')
        .map(|part| {
            if let Some((k, _v)) = part.split_once('=') {
                if is_secret_url_param(k) {
                    changed = true;
                    return format!("{k}=[REDACTED]");
                }
            }
            part.to_string()
        })
        .collect();
    if changed {
        let mut rebuilt = format!("{base}?{}", redacted_params.join("&"));
        if let Some(f) = frag {
            rebuilt.push_str(f);
        }
        return rebuilt;
    }
    out
}

/// Recursively redacts credentials and secrets from JSON values.
pub fn redact_secrets(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                if is_secret_key(k) {
                    out.insert(k.clone(), Value::String("[REDACTED]".to_string()));
                } else {
                    out.insert(k.clone(), redact_secrets(v));
                }
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(arr.iter().map(redact_secrets).collect()),
        Value::String(s) => Value::String(redact_url_userinfo(s)),
        other => other.clone(),
    }
}

fn redact_map(map: &HashMap<String, Value>) -> BTreeMap<String, Value> {
    map.iter()
        .map(|(k, v)| {
            if is_secret_key(k) {
                (k.clone(), Value::String("[REDACTED]".to_string()))
            } else {
                (k.clone(), redact_secrets(v))
            }
        })
        .collect()
}

pub fn is_durable_options(options: Option<&HashMap<String, Value>>) -> bool {
    options.is_some_and(|opts| {
        opts.iter().any(|(k, v)| {
            ((k.eq_ignore_ascii_case("deliveryGuarantee")
                || k.eq_ignore_ascii_case("delivery_guarantee")
                || k.eq_ignore_ascii_case("reliability"))
                && matches!(
                    v.as_str().map(|s| s.to_ascii_lowercase()).as_deref(),
                    Some("at_least_once" | "durable" | "reliable")
                ))
                || ((k.eq_ignore_ascii_case("checkpointEnabled")
                    || k.eq_ignore_ascii_case("checkpoint_enabled"))
                    && (v.as_bool() == Some(true)
                        || v.as_str().is_some_and(|s| s.eq_ignore_ascii_case("true"))))
        })
    })
}

fn component_label(component: &CapabilityComponent) -> String {
    match component {
        CapabilityComponent::Source(id) => format!("source:{}", id.as_str()),
        CapabilityComponent::Operator(id) => format!("operator:{}", id.as_str()),
        CapabilityComponent::Action(id) => format!("action:{}", id.as_str()),
        CapabilityComponent::Connection(id) => format!("connection:{}", id.as_str()),
    }
}

fn map_diagnostic(diag: &PlanCapabilityDiagnostic) -> PlanDiagnosticReport {
    let comp = component_label(&diag.component);
    let code = match &diag.kind {
        PlanDiagnosticKind::DurableWithLossyQueue => "PLAN_LOSSY_QUEUE_IN_DURABLE",
        PlanDiagnosticKind::IncompatibleResultMode(_) => "PLAN_INCOMPATIBLE_RESULT_MODE",
        PlanDiagnosticKind::MissingSnapshotSupport => "PLAN_MISSING_SNAPSHOT",
        PlanDiagnosticKind::MissingReplaySupport => "PLAN_MISSING_REPLAY",
        PlanDiagnosticKind::TypeCheck(_) => "PLAN_TYPE_CHECK_ERROR",
        PlanDiagnosticKind::Capability(_) => "PLAN_CAPABILITY_ERROR",
    };
    PlanDiagnosticReport {
        code: code.to_string(),
        location: comp.clone(),
        component: Some(comp),
        message: diag.message.clone(),
    }
}

fn collect_stmt_exprs(stmt: &SelectStmt) -> Vec<Expr> {
    let mut exprs = Vec::new();
    exprs.extend(stmt.fields.iter().cloned());
    if let Some(ref w) = stmt.where_clause {
        exprs.push(w.clone());
    }
    exprs.extend(stmt.group_by.iter().cloned());
    if let Some(ref h) = stmt.having {
        exprs.push(h.clone());
    }
    exprs
}

fn source_descriptor(entry: &StreamCatalogEntry) -> CapabilityDescriptor {
    let mut supported = BTreeSet::new();
    supported.insert(Capability::Input(InputMode::AppendOnly));
    if entry.replayable
        || entry
            .source_type
            .as_deref()
            .is_some_and(|t| t.eq_ignore_ascii_case("kafka"))
    {
        supported.insert(Capability::Replay);
    }
    CapabilityDescriptor {
        supported,
        limits: BTreeMap::new(),
        qualification: entry.qualification,
    }
}

fn action_descriptor(kind: &str) -> CapabilityDescriptor {
    let mut supported = BTreeSet::new();
    supported.insert(Capability::Result(ResultMode::AppendOnly));
    supported.insert(Capability::Acknowledgement);
    let qualification = if matches!(
        kind.to_ascii_lowercase().as_str(),
        "log" | "memory" | "mqtt" | "kafka" | "http" | "rest"
    ) {
        QualificationLevel::ComponentTested
    } else {
        QualificationLevel::Unqualified
    };
    CapabilityDescriptor {
        supported,
        limits: BTreeMap::new(),
        qualification,
    }
}

/// Builds a secret-redacted [`TypedPlanReport`] from a rule and catalog metadata.
pub fn build_typed_plan_report(input: &PlanCatalogInput) -> TypedPlanReport {
    let durable = is_durable_options(input.options.as_ref());
    let profile_name = if durable { "durable" } else { "lightweight" };
    let delivery = if durable {
        "at_least_once"
    } else {
        "best_effort"
    };
    let profile = if durable {
        PlanExecutionProfile::Durable {
            require_replay: true,
            require_snapshot: true,
            min_qualification: QualificationLevel::ComponentTested,
        }
    } else {
        PlanExecutionProfile::Lightweight
    };

    let redacted_options = input.options.as_ref().map(redact_map).unwrap_or_default();
    let redacted_actions: Vec<BTreeMap<String, Value>> =
        input.actions.iter().map(redact_map).collect();
    let action_kinds: Vec<String> = input
        .actions
        .iter()
        .flat_map(|a| a.keys().cloned())
        .collect();

    let mut parser = Parser::new(&input.sql);
    let stmt = match parser.parse_select() {
        Ok(s) => s,
        Err(err) => {
            return TypedPlanReport {
                rule_id: input.rule_id.clone(),
                valid: false,
                profile: profile_name.to_string(),
                sources: Vec::new(),
                actions: action_kinds,
                redacted_options,
                redacted_actions,
                effective_guarantees: EffectiveGuarantees {
                    delivery: delivery.to_string(),
                    qualification: QualificationLevel::Unqualified,
                    limiting_component: Some("sql".to_string()),
                    stateful: false,
                    lossy_inputs: Vec::new(),
                },
                diagnostics: vec![PlanDiagnosticReport {
                    code: "PLAN_SQL_PARSE_ERROR".to_string(),
                    location: "sql".to_string(),
                    component: Some("sql".to_string()),
                    message: format!("Invalid rule SQL: {err}"),
                }],
            };
        }
    };

    let mut sources = vec![stmt.from.clone()];
    for join in &stmt.joins {
        if !sources.contains(&join.target) {
            sources.push(join.target.clone());
        }
    }

    let catalog_map: HashMap<String, &StreamCatalogEntry> = input
        .streams
        .iter()
        .map(|s| (s.name.to_ascii_lowercase(), s))
        .collect();

    let mut nodes = Vec::new();
    let mut descriptors = BTreeMap::new();
    let mut schema_env = SchemaEnv::default();
    let mut lossy_inputs = Vec::new();
    let mut min_qual = QualificationLevel::Qualified;
    let mut limiting_component: Option<String> = None;

    let mut update_limiting = |comp: &CapabilityComponent, qual: QualificationLevel| {
        if qual < min_qual || limiting_component.is_none() {
            if qual < min_qual {
                min_qual = qual;
                limiting_component = Some(component_label(comp));
            } else if limiting_component.is_none() && qual < QualificationLevel::Qualified {
                limiting_component = Some(component_label(comp));
            }
        }
    };

    for src_name in &sources {
        let src_id = SourceId::try_from(src_name.clone())
            .unwrap_or_else(|_| SourceId::try_from("unknown_source").unwrap());
        let comp = CapabilityComponent::Source(src_id.clone());
        if let Some(entry) = catalog_map.get(&src_name.to_ascii_lowercase()) {
            if entry.lossy_queue {
                lossy_inputs.push(src_name.clone());
            }
            for (col, ty) in &entry.schema {
                schema_env.fields.insert(
                    col.clone(),
                    FieldSchema {
                        ty: ty.clone(),
                        nullable: true,
                    },
                );
            }
            let desc = source_descriptor(entry);
            update_limiting(&comp, desc.qualification);
            descriptors.insert(comp, desc);
            nodes.push(LogicalPlanNode::Source {
                id: src_id,
                input_mode: InputMode::AppendOnly,
                lossy_queue: entry.lossy_queue,
            });
        } else {
            let desc = CapabilityDescriptor {
                supported: BTreeSet::from([Capability::Input(InputMode::AppendOnly)]),
                limits: BTreeMap::new(),
                qualification: QualificationLevel::ComponentTested,
            };
            update_limiting(&comp, desc.qualification);
            descriptors.insert(comp, desc);
            nodes.push(LogicalPlanNode::Source {
                id: src_id,
                input_mode: InputMode::AppendOnly,
                lossy_queue: false,
            });
        }
    }

    let stateful = stmt.window.is_some() || !stmt.group_by.is_empty() || !stmt.joins.is_empty();
    let op_id = OperatorId::try_from(format!("{}_select", input.rule_id))
        .unwrap_or_else(|_| OperatorId::try_from("rule_select").unwrap());
    let op_comp = CapabilityComponent::Operator(op_id.clone());
    let mut op_caps = BTreeSet::from([Capability::Result(ResultMode::AppendOnly)]);
    if stateful {
        op_caps.insert(Capability::Snapshot);
        op_caps.insert(Capability::Restore);
    }
    let op_qual = QualificationLevel::ComponentTested;
    update_limiting(&op_comp, op_qual);
    descriptors.insert(
        op_comp,
        CapabilityDescriptor {
            supported: op_caps,
            limits: BTreeMap::new(),
            qualification: op_qual,
        },
    );
    nodes.push(LogicalPlanNode::Operator {
        id: op_id,
        stateful,
        delayed_or_windowed: stmt.window.is_some(),
        result_mode: ResultMode::AppendOnly,
        exprs: collect_stmt_exprs(&stmt),
    });

    for (idx, action) in input.actions.iter().enumerate() {
        for kind in action.keys() {
            let action_id = ActionId::try_from(format!("{kind}_{idx}"))
                .unwrap_or_else(|_| ActionId::try_from(format!("action_{idx}")).unwrap());
            let comp = CapabilityComponent::Action(action_id.clone());
            let desc = action_descriptor(kind);
            update_limiting(&comp, desc.qualification);
            descriptors.insert(comp, desc);
            nodes.push(LogicalPlanNode::Action {
                id: action_id,
                required_result_mode: ResultMode::AppendOnly,
            });
        }
    }

    let fn_registry = FunctionDescriptorRegistry::default();
    let diagnostics =
        match validate_plan_capabilities(&nodes, &descriptors, &profile, &schema_env, &fn_registry)
        {
            Ok(()) => Vec::new(),
            Err(diags) => diags.iter().map(map_diagnostic).collect(),
        };

    if let Some(first_diag) = diagnostics.first() {
        if first_diag.component.is_some() {
            limiting_component = first_diag.component.clone();
        }
    }

    TypedPlanReport {
        rule_id: input.rule_id.clone(),
        valid: diagnostics.is_empty(),
        profile: profile_name.to_string(),
        sources,
        actions: action_kinds,
        redacted_options,
        redacted_actions,
        effective_guarantees: EffectiveGuarantees {
            delivery: delivery.to_string(),
            qualification: min_qual,
            limiting_component,
            stateful,
            lossy_inputs,
        },
        diagnostics,
    }
}
