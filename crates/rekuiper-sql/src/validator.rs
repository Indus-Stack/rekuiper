use crate::ast::{Expr, SelectStmt};
use crate::functions::builtin_function_metadata;
use crate::parser::Parser;
use rekuiper_core::model::{
    DeclarativeManifest, DiagnosticLevel, ValidationDiagnostic, ValidationSummary,
};
use std::collections::HashSet;

/// Set of known built-in sink types supported by rekuiper.
pub static KNOWN_SINK_TYPES: &[&str] = &[
    "mqtt",
    "http",
    "rest",
    "log",
    "file",
    "memory",
    "influx",
    "influx2",
    "tdengine",
    "tdengine3",
    "iotdb",
    "kafka",
    "sql",
    "image",
    "nop",
    "edgex",
    "redis",
    "redisPub",
    "rabbitmq",
    "zmq",
    "zeromq",
    "video",
    "websocket",
    "neuron",
    "modbus",
    "opcua",
    "opc_ua",
    "opc-ua",
    "otlp",
];

/// Built-in streams or virtual sources always available to rules.
pub static BUILTIN_STREAMS: &[&str] = &["demo", "demo2", "mockstream", "mock_stream", "table1"];

fn is_known_function(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    // Check metadata catalog
    if builtin_function_metadata()
        .iter()
        .any(|m| m.name.eq_ignore_ascii_case(&lower))
    {
        return true;
    }
    // Pseudo-functions and analytical builtins
    matches!(
        lower.as_str(),
        "meta"
            | "window_start"
            | "window_end"
            | "cast"
            | "coalesce"
            | "isnull"
            | "dedup"
            | "element_at"
            | "cardinality"
            | "array_contains"
            | "array_join"
            | "now"
            | "current_timestamp"
            | "local_timestamp"
            | "to_seconds"
            | "json_path_query"
            | "json_path_exists"
            | "latest"
            | "lag"
            | "echo"
            | "countplusone"
            | "accumulatewordcount"
    )
}

fn collect_functions(expr: &Expr, funcs: &mut Vec<String>) {
    match expr {
        Expr::Call { name, args } => {
            funcs.push(name.clone());
            for arg in args {
                collect_functions(arg, funcs);
            }
        }
        Expr::Over {
            call,
            partition_by,
            when,
        } => {
            collect_functions(call, funcs);
            if let Some(p) = partition_by {
                collect_functions(p, funcs);
            }
            if let Some(w) = when {
                collect_functions(w, funcs);
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_functions(left, funcs);
            collect_functions(right, funcs);
        }
        Expr::UnaryOp { expr, .. } => {
            collect_functions(expr, funcs);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_functions(expr, funcs);
            collect_functions(low, funcs);
            collect_functions(high, funcs);
        }
        Expr::InList { expr, list, .. } => {
            collect_functions(expr, funcs);
            for item in list {
                collect_functions(item, funcs);
            }
        }
        Expr::IsNull { expr, .. } => {
            collect_functions(expr, funcs);
        }
        Expr::FieldAccess { parent, .. } => {
            collect_functions(parent, funcs);
        }
        Expr::Index { base, index } => {
            collect_functions(base, funcs);
            collect_functions(index, funcs);
        }
        Expr::Slice { base, lo, hi } => {
            collect_functions(base, funcs);
            if let Some(l) = lo {
                collect_functions(l, funcs);
            }
            if let Some(h) = hi {
                collect_functions(h, funcs);
            }
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(op) = operand {
                collect_functions(op, funcs);
            }
            for (when_expr, then_expr) in when_clauses {
                collect_functions(when_expr, funcs);
                collect_functions(then_expr, funcs);
            }
            if let Some(el) = else_clause {
                collect_functions(el, funcs);
            }
        }
        Expr::WildcardModified { replace, .. } => {
            for (e, _) in replace {
                collect_functions(e, funcs);
            }
        }
        Expr::Wildcard | Expr::Identifier(_) | Expr::Literal(_) => {}
    }
}

/// Pure offline validation engine for DeclarativeManifest.
/// Inspects:
/// 1. Stream & Table DDL syntax
/// 2. Rule SQL syntax & AST function calls
/// 3. Referential integrity (FROM / JOIN against declared streams/tables, CONNECTION options against connections)
/// 4. Sink action validity
pub fn validate_manifest(
    manifest: &DeclarativeManifest,
) -> Result<ValidationSummary, Vec<ValidationDiagnostic>> {
    let mut diagnostics = Vec::new();

    let mut stream_names = HashSet::new();
    let mut table_names = HashSet::new();

    // 1. Validate Connections
    let connection_names: HashSet<String> = manifest
        .connections
        .keys()
        .map(|k| k.to_ascii_lowercase())
        .collect();

    // 2. Validate Streams DDL
    for (stream_name, ddl) in &manifest.streams {
        let name_lower = stream_name.to_ascii_lowercase();
        stream_names.insert(name_lower);

        let mut parser = Parser::new(ddl);
        match parser.parse_create_stream() {
            Ok(stmt) => {
                // Verify CONNECTION option if specified
                for (opt_key, opt_val) in &stmt.options {
                    if opt_key.eq_ignore_ascii_case("connection")
                        || opt_key.eq_ignore_ascii_case("conf_key")
                    {
                        let opt_val_lower = opt_val.to_ascii_lowercase();
                        if !opt_val.is_empty()
                            && !connection_names.contains(&opt_val_lower)
                            && opt_val_lower != "default"
                        {
                            diagnostics.push(ValidationDiagnostic {
                                level: DiagnosticLevel::Error,
                                entity_type: "stream".to_string(),
                                entity_id: stream_name.clone(),
                                message: format!(
                                    "references undefined connection '{}' in options",
                                    opt_val
                                ),
                            });
                        }
                    }
                    if opt_key.eq_ignore_ascii_case("schema")
                        && !manifest.schemas.contains_key(opt_val)
                    {
                        diagnostics.push(ValidationDiagnostic {
                            level: DiagnosticLevel::Warning,
                            entity_type: "stream".to_string(),
                            entity_id: stream_name.clone(),
                            message: format!(
                                "references schema '{}' not explicitly declared in manifest schemas",
                                opt_val
                            ),
                        });
                    }
                }
            }
            Err(e) => {
                diagnostics.push(ValidationDiagnostic {
                    level: DiagnosticLevel::Error,
                    entity_type: "stream".to_string(),
                    entity_id: stream_name.clone(),
                    message: format!("invalid CREATE STREAM DDL syntax: {}", e),
                });
            }
        }
    }

    // 3. Validate Tables DDL
    for (table_name, ddl) in &manifest.tables {
        let name_lower = table_name.to_ascii_lowercase();
        table_names.insert(name_lower);

        let mut parser = Parser::new(ddl);
        match parser.parse_create_table() {
            Ok(stmt) => {
                for (opt_key, opt_val) in &stmt.options {
                    if opt_key.eq_ignore_ascii_case("connection")
                        || opt_key.eq_ignore_ascii_case("conf_key")
                    {
                        let opt_val_lower = opt_val.to_ascii_lowercase();
                        if !opt_val.is_empty()
                            && !connection_names.contains(&opt_val_lower)
                            && opt_val_lower != "default"
                        {
                            diagnostics.push(ValidationDiagnostic {
                                level: DiagnosticLevel::Error,
                                entity_type: "table".to_string(),
                                entity_id: table_name.clone(),
                                message: format!(
                                    "references undefined connection '{}' in options",
                                    opt_val
                                ),
                            });
                        }
                    }
                }
            }
            Err(e) => {
                diagnostics.push(ValidationDiagnostic {
                    level: DiagnosticLevel::Error,
                    entity_type: "table".to_string(),
                    entity_id: table_name.clone(),
                    message: format!("invalid CREATE TABLE DDL syntax: {}", e),
                });
            }
        }
    }

    // 4. Validate Rules
    let rules_map = match manifest.rules.clone().into_rules_map() {
        Ok(m) => m,
        Err(err) => {
            diagnostics.push(ValidationDiagnostic {
                level: DiagnosticLevel::Error,
                entity_type: "rules".to_string(),
                entity_id: "catalog".to_string(),
                message: err,
            });
            std::collections::BTreeMap::new()
        }
    };

    for (rule_id, rule) in &rules_map {
        // Validate SQL
        if rule.sql.trim().is_empty() {
            diagnostics.push(ValidationDiagnostic {
                level: DiagnosticLevel::Error,
                entity_type: "rule".to_string(),
                entity_id: rule_id.clone(),
                message: "SQL statement cannot be empty".to_string(),
            });
            continue;
        }

        let mut parser = Parser::new(&rule.sql);
        match parser.parse_select() {
            Ok(stmt) => {
                validate_rule_select(
                    &stmt,
                    rule_id,
                    &stream_names,
                    &table_names,
                    &mut diagnostics,
                );
            }
            Err(e) => {
                diagnostics.push(ValidationDiagnostic {
                    level: DiagnosticLevel::Error,
                    entity_type: "rule".to_string(),
                    entity_id: rule_id.clone(),
                    message: format!("SQL parse error: {}", e),
                });
            }
        }

        // Validate Actions
        if rule.actions.is_empty() {
            diagnostics.push(ValidationDiagnostic {
                level: DiagnosticLevel::Error,
                entity_type: "rule".to_string(),
                entity_id: rule_id.clone(),
                message: "must define at least one sink action".to_string(),
            });
        } else {
            for action in &rule.actions {
                for (action_type, props) in action {
                    let type_lower = action_type.to_ascii_lowercase();
                    if !KNOWN_SINK_TYPES
                        .iter()
                        .any(|k| k.eq_ignore_ascii_case(&type_lower))
                    {
                        diagnostics.push(ValidationDiagnostic {
                            level: DiagnosticLevel::Warning,
                            entity_type: "rule".to_string(),
                            entity_id: rule_id.clone(),
                            message: format!(
                                "action '{}' is not a known built-in sink type (may require external plugin)",
                                action_type
                            ),
                        });
                    }
                    if !props.is_object() {
                        diagnostics.push(ValidationDiagnostic {
                            level: DiagnosticLevel::Error,
                            entity_type: "rule".to_string(),
                            entity_id: rule_id.clone(),
                            message: format!(
                                "action '{}' configuration must be an object",
                                action_type
                            ),
                        });
                    }
                }
            }
        }

        // Validate Rule Options (onError, dlq)
        if let Some(opts) = &rule.options {
            if let Some(on_error_val) = opts.get("onError") {
                if let Some(on_error_str) = on_error_val.as_str() {
                    let s = on_error_str.to_ascii_lowercase();
                    if s != "drop" && s != "dlq" && s != "halt" {
                        diagnostics.push(ValidationDiagnostic {
                            level: DiagnosticLevel::Error,
                            entity_type: "rule".to_string(),
                            entity_id: rule_id.clone(),
                            message: format!(
                                "invalid 'onError' option '{}'; must be 'drop', 'dlq', or 'halt'",
                                on_error_str
                            ),
                        });
                    }
                } else {
                    diagnostics.push(ValidationDiagnostic {
                        level: DiagnosticLevel::Error,
                        entity_type: "rule".to_string(),
                        entity_id: rule_id.clone(),
                        message: "'onError' option must be a string ('drop', 'dlq', or 'halt')"
                            .to_string(),
                    });
                }
            }

            if let Some(dlq_val) = opts.get("dlq") {
                if !dlq_val.is_object() {
                    diagnostics.push(ValidationDiagnostic {
                        level: DiagnosticLevel::Error,
                        entity_type: "rule".to_string(),
                        entity_id: rule_id.clone(),
                        message: "'dlq' option must be an object".to_string(),
                    });
                } else if let Some(action) = dlq_val.get("action") {
                    if !action.is_object() {
                        diagnostics.push(ValidationDiagnostic {
                            level: DiagnosticLevel::Error,
                            entity_type: "rule".to_string(),
                            entity_id: rule_id.clone(),
                            message: "'dlq.action' must be an object specifying a sink action"
                                .to_string(),
                        });
                    }
                }
            }
        }
    }

    let errors_count = diagnostics
        .iter()
        .filter(|d| d.level == DiagnosticLevel::Error)
        .count();
    let warnings_count = diagnostics
        .iter()
        .filter(|d| d.level == DiagnosticLevel::Warning)
        .count();

    let summary = ValidationSummary {
        streams_count: manifest.streams.len(),
        tables_count: manifest.tables.len(),
        rules_count: rules_map.len(),
        connections_count: manifest.connections.len(),
        schemas_count: manifest.schemas.len(),
        errors_count,
        warnings_count,
    };

    if errors_count == 0 {
        Ok(summary)
    } else {
        Err(diagnostics)
    }
}

fn validate_rule_select(
    stmt: &SelectStmt,
    rule_id: &str,
    stream_names: &HashSet<String>,
    table_names: &HashSet<String>,
    diagnostics: &mut Vec<ValidationDiagnostic>,
) {
    // 1. Check FROM referential integrity
    let from_source = stmt.from.trim().trim_matches('`').trim_matches('"');
    let from_lower = from_source.to_ascii_lowercase();

    let from_exists = stream_names.contains(&from_lower)
        || table_names.contains(&from_lower)
        || BUILTIN_STREAMS.contains(&from_lower.as_str());

    if !from_exists {
        diagnostics.push(ValidationDiagnostic {
            level: DiagnosticLevel::Error,
            entity_type: "rule".to_string(),
            entity_id: rule_id.to_string(),
            message: format!(
                "references undefined stream or table '{}' in FROM clause",
                from_source
            ),
        });
    }

    // 2. Check JOIN referential integrity
    for join in &stmt.joins {
        let join_target = join.target.trim().trim_matches('`').trim_matches('"');
        let join_lower = join_target.to_ascii_lowercase();
        let join_exists = stream_names.contains(&join_lower)
            || table_names.contains(&join_lower)
            || BUILTIN_STREAMS.contains(&join_lower.as_str());

        if !join_exists {
            diagnostics.push(ValidationDiagnostic {
                level: DiagnosticLevel::Error,
                entity_type: "rule".to_string(),
                entity_id: rule_id.to_string(),
                message: format!(
                    "references undefined stream or table '{}' in JOIN clause",
                    join_target
                ),
            });
        }
    }

    // 3. Check AST function calls
    let mut funcs = Vec::new();
    for f in &stmt.fields {
        collect_functions(f, &mut funcs);
    }
    if let Some(ref w) = stmt.where_clause {
        collect_functions(w, &mut funcs);
    }
    for g in &stmt.group_by {
        collect_functions(g, &mut funcs);
    }
    if let Some(ref h) = stmt.having {
        collect_functions(h, &mut funcs);
    }
    if let Some(ref wf) = stmt.window_filter {
        collect_functions(wf, &mut funcs);
    }
    if let Some(ref wt) = stmt.window_trigger_condition {
        collect_functions(wt, &mut funcs);
    }

    for func in funcs {
        if !is_known_function(&func) {
            diagnostics.push(ValidationDiagnostic {
                level: DiagnosticLevel::Error,
                entity_type: "rule".to_string(),
                entity_id: rule_id.to_string(),
                message: format!("references unknown function '{}'", func),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rekuiper_core::model::DeclarativeManifest;

    #[test]
    fn test_validate_manifest_valid() {
        let yaml = r#"
version: v1
connections:
  local_mqtt:
    server: "tcp://127.0.0.1:1883"
streams:
  telemetry: "CREATE STREAM telemetry () WITH (TYPE=\"mqtt\", CONNECTION=\"local_mqtt\")"
rules:
  - id: high_temp_alert
    sql: "SELECT temperature, humidity, round(temperature, 2) as r_temp FROM telemetry WHERE temperature > 50"
    actions:
      - mqtt:
          topic: "alerts/temp"
"#;
        let manifest = DeclarativeManifest::from_str(yaml, "yaml", false).unwrap();
        let summary = validate_manifest(&manifest).expect("Validation should succeed");
        assert_eq!(summary.streams_count, 1);
        assert_eq!(summary.rules_count, 1);
        assert_eq!(summary.connections_count, 1);
        assert_eq!(summary.errors_count, 0);
    }

    #[test]
    fn test_validate_manifest_missing_stream_and_unknown_func() {
        let yaml = r#"
version: v1
rules:
  - id: bad_rule
    sql: "SELECT non_existent_function_xyz(val) FROM undefined_stream_123"
    actions:
      - log: {}
"#;
        let manifest = DeclarativeManifest::from_str(yaml, "yaml", false).unwrap();
        let errs = validate_manifest(&manifest).expect_err("Validation should fail");
        assert!(errs.iter().any(|d| d
            .message
            .contains("undefined stream or table 'undefined_stream_123'")));
        assert!(errs.iter().any(|d| d
            .message
            .contains("references unknown function 'non_existent_function_xyz'")));
    }

    #[test]
    fn test_validate_manifest_missing_action() {
        let yaml = r#"
version: v1
streams:
  s1: "CREATE STREAM s1 () WITH (TYPE=\"demo\")"
rules:
  - id: no_action_rule
    sql: "SELECT * FROM s1"
    actions: []
"#;
        let manifest = DeclarativeManifest::from_str(yaml, "yaml", false).unwrap();
        let errs = validate_manifest(&manifest).expect_err("Validation should fail");
        assert!(errs
            .iter()
            .any(|d| d.message.contains("must define at least one sink action")));
    }
}
