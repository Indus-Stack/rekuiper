use crate::protocol::{ContentItem, PromptArgument, PromptDefinition, PromptMessage};
use serde_json::Value;

pub fn get_prompt_definitions() -> Vec<PromptDefinition> {
    vec![
        PromptDefinition {
            name: "troubleshoot_rule".to_string(),
            description: Some("Comprehensive edge streaming diagnostics: inspects rule status telemetry (source reads, sink emissions, exception counters, filter drops), validates underlying SQL syntax, examines topological execution graph, and queries active distributed trace spans to isolate pipeline failures.".to_string()),
            arguments: vec![PromptArgument {
                name: "rule_name".to_string(),
                description: Some("Exact identifier of the deployed rekuiper stream processing rule experiencing execution failures, dropped tuples, or unexpected output.".to_string()),
                required: true,
            }],
        },
        PromptDefinition {
            name: "optimize_stream_sql".to_string(),
            description: Some("Performance and memory footprint optimization for rekuiper streaming SQL queries: analyzes window clauses (Tumbling vs Hopping vs Sliding vs Session vs CountWindow), evaluates column pruning opportunities, audits predicate pushdown, and validates memory allocation on constrained edge gateways.".to_string()),
            arguments: vec![PromptArgument {
                name: "sql".to_string(),
                description: Some("The rekuiper streaming SQL query string to optimize for throughput, memory consumption, and latency.".to_string()),
                required: true,
            }],
        },
        PromptDefinition {
            name: "generate_iot_alert_rule".to_string(),
            description: Some("Generates an industrial IoT anomaly detection and alerting pipeline adhering to edge streaming best practices: includes sliding/tumbling window debouncing to eliminate alert flapping, dynamic MQTT topic publishing, and data template formatting.".to_string()),
            arguments: vec![
                PromptArgument {
                    name: "stream_name".to_string(),
                    description: Some("Identifier of the source stream providing incoming telemetry events.".to_string()),
                    required: true,
                },
                PromptArgument {
                    name: "metric".to_string(),
                    description: Some("Telemetry field or property name to evaluate (e.g. 'bearing_vibration', 'motor_temperature', 'current_draw').".to_string()),
                    required: true,
                },
                PromptArgument {
                    name: "threshold".to_string(),
                    description: Some("Threshold predicate or numeric limit triggering the alert condition (e.g. '> 85.0', '< 12.2', 'abs(vibe) > 4.5').".to_string()),
                    required: true,
                },
            ],
        },
        PromptDefinition {
            name: "create_end_to_end_pipeline".to_string(),
            description: Some("Architects a full end-to-end edge pipeline: generates CREATE STREAM DDL with format/connector specifications, continuous aggregation SQL query, MQTT action sinks with JSON templating, and mock event test payload.".to_string()),
            arguments: vec![
                PromptArgument {
                    name: "pipeline_name".to_string(),
                    description: Some("System identifier for the edge streaming pipeline and rule.".to_string()),
                    required: true,
                },
                PromptArgument {
                    name: "datasource_topic".to_string(),
                    description: Some("Inbound transport identifier or MQTT topic pattern (e.g. 'factory/+/sensor/data') from which events are consumed.".to_string()),
                    required: true,
                },
            ],
        },
        PromptDefinition {
            name: "diagnose_data_drop".to_string(),
            description: Some("Systematic root-cause diagnosis for dropped stream records: audits source schema decoding, evaluates WHERE filter conditions against sample payloads, verifies window watermark progression, and tracks sink buffer backpressure.".to_string()),
            arguments: vec![PromptArgument {
                name: "rule_name".to_string(),
                description: Some("Identifier of the rule exhibiting data loss, dropped event counts, or sink delivery gaps.".to_string()),
                required: true,
            }],
        },
        PromptDefinition {
            name: "generate_vector_search_rule".to_string(),
            description: Some("Constructs an edge vector similarity search and anomaly detection rule using cosine_similarity, vector_l2, or vector_dot with real-time threshold filtering.".to_string()),
            arguments: vec![
                PromptArgument {
                    name: "stream_name".to_string(),
                    description: Some("The source stream providing incoming vector embeddings.".to_string()),
                    required: true,
                },
                PromptArgument {
                    name: "vector_field".to_string(),
                    description: Some("Field name containing the embedding array (e.g. 'embedding', 'features').".to_string()),
                    required: true,
                },
                PromptArgument {
                    name: "similarity_threshold".to_string(),
                    description: Some("Minimum similarity threshold between 0.0 and 1.0 (e.g. '0.85').".to_string()),
                    required: true,
                },
            ],
        },
        PromptDefinition {
            name: "configure_rabbitmq_pipeline".to_string(),
            description: Some("Constructs an enterprise pipeline routing stream records to a RabbitMQ AMQP 0-9-1 exchange with durability and dynamic secret resolution.".to_string()),
            arguments: vec![
                PromptArgument {
                    name: "rule_name".to_string(),
                    description: Some("Name of the rule.".to_string()),
                    required: true,
                },
                PromptArgument {
                    name: "stream_name".to_string(),
                    description: Some("Source stream name.".to_string()),
                    required: true,
                },
                PromptArgument {
                    name: "exchange".to_string(),
                    description: Some("RabbitMQ exchange name (e.g. 'events.topic').".to_string()),
                    required: true,
                },
            ],
        },
        PromptDefinition {
            name: "create_wasm_plugin_rule".to_string(),
            description: Some("Guides the registration of a compiled .wasm module and generates a streaming SQL query invoking the exported WebAssembly UDFs.".to_string()),
            arguments: vec![
                PromptArgument {
                    name: "module_name".to_string(),
                    description: Some("Unique name of the WASM module.".to_string()),
                    required: true,
                },
                PromptArgument {
                    name: "function_name".to_string(),
                    description: Some("Exported function name inside the WASM module.".to_string()),
                    required: true,
                },
            ],
        },
        PromptDefinition {
            name: "audit_security_posture".to_string(),
            description: Some("Audits rekuiper daemon runtime security configuration: verifies whether REST (`restIp`) and SSE (`httpServerIp`) listeners are bound to secure loopback (`127.0.0.1`) or exposed externally (`0.0.0.0`), audits REST JWT authentication status, and checks private network access policy (`enable_private_net`).".to_string()),
            arguments: vec![],
        },
        PromptDefinition {
            name: "generate_cep_rule".to_string(),
            description: Some("Constructs a Complex Event Processing (CEP) sequence matching rule using sequence_match or sequence_count across temporal event streams (e.g. detecting state transitions like 'start -> error -> recovery' within a time window).".to_string()),
            arguments: vec![
                PromptArgument {
                    name: "stream_name".to_string(),
                    description: Some("Identifier of the source stream providing incoming events.".to_string()),
                    required: true,
                },
                PromptArgument {
                    name: "pattern".to_string(),
                    description: Some("Sequence regular expression pattern matching event conditions (e.g. '(?1)(?2)*(?3)').".to_string()),
                    required: true,
                },
                PromptArgument {
                    name: "timestamp_field".to_string(),
                    description: Some("Timestamp field or expression used for temporal ordering (e.g. 'ts' or 'tstamp()').".to_string()),
                    required: true,
                },
            ],
        },
        PromptDefinition {
            name: "staged_fleet_rollout".to_string(),
            description: Some("Guides progressive phased canary rollout of rules/binaries across fleet devices with health check verification and rollback validation.".to_string()),
            arguments: vec![
                PromptArgument {
                    name: "version".to_string(),
                    description: Some("Target rollout version tag or configuration release.".to_string()),
                    required: true,
                },
                PromptArgument {
                    name: "group_filter".to_string(),
                    description: Some("Optional fleet group or tag filter (e.g. 'edge-gateways', 'turbine-cluster').".to_string()),
                    required: false,
                },
            ],
        },
        PromptDefinition {
            name: "design_visual_dag_pipeline".to_string(),
            description: Some("Architects a multi-port Visual Stream DAG rule structure with sources, transforms, routers, and sinks, validating graph topology and wire-tap diagnostic points.".to_string()),
            arguments: vec![
                PromptArgument {
                    name: "rule_name".to_string(),
                    description: Some("Unique identifier for the visual DAG rule.".to_string()),
                    required: true,
                },
                PromptArgument {
                    name: "pipeline_goal".to_string(),
                    description: Some("Operational goal (e.g. 'Route high vibration to MQTT alert and raw telemetry to Parquet').".to_string()),
                    required: true,
                },
            ],
        },
    ]
}

pub fn get_prompt_messages(name: &str, args: Option<Value>) -> Result<Vec<PromptMessage>, String> {
    match name {
        "troubleshoot_rule" => {
            let rule_name = args
                .as_ref()
                .and_then(|a| a.get("rule_name"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown_rule");

            Ok(vec![
                PromptMessage {
                    role: "user".to_string(),
                    content: ContentItem::text(format!(
                        "Please troubleshoot the rekuiper stream processing rule '{}'.\n\
                         1. Use `get_rule_status` to inspect runtime counters (messages in, messages out, exceptions, dropped records).\n\
                         2. Use `get_rule` to fetch its current SQL query and sink actions.\n\
                         3. Use `validate_sql` on the query to verify expression correctness.\n\
                         4. If necessary, use `start_rule_trace` and `get_rule_traces` to inspect event execution spans.\n\
                         5. Provide clear recommendations to fix the pipeline, tune buffering, or correct data types.",
                        rule_name
                    )),
                },
            ])
        }

        "optimize_stream_sql" => {
            let sql = args
                .as_ref()
                .and_then(|a| a.get("sql"))
                .and_then(|v| v.as_str())
                .unwrap_or("SELECT * FROM demo");

            Ok(vec![
                PromptMessage {
                    role: "user".to_string(),
                    content: ContentItem::text(format!(
                        "Analyze and optimize the following rekuiper SQL query for high-throughput edge execution:\n\n\
                         ```sql\n{}\n```\n\n\
                         Check for:\n\
                         - Window clause efficiency (Tumbling vs Hopping vs Sliding vs CountWindow)\n\
                         - Appropriate use of HAVING and stateful change detection (had_changed, changed_col) to filter repeated identical aggregate windows\n\
                         - Projection overhead and column pruning\n\
                         - Missing filter predicates to drop irrelevant messages early\n\
                         - Type casting suitability and null safety\n\
                         Use `test_sql_expression` with mock data to demonstrate the difference.",
                        sql
                    )),
                },
            ])
        }

        "generate_iot_alert_rule" => {
            let stream = args
                .as_ref()
                .and_then(|a| a.get("stream_name"))
                .and_then(|v| v.as_str())
                .unwrap_or("telemetry");
            let metric = args
                .as_ref()
                .and_then(|a| a.get("metric"))
                .and_then(|v| v.as_str())
                .unwrap_or("temp");
            let threshold = args
                .as_ref()
                .and_then(|a| a.get("threshold"))
                .and_then(|v| v.as_str())
                .unwrap_or("80.0");

            Ok(vec![
                PromptMessage {
                    role: "user".to_string(),
                    content: ContentItem::text(format!(
                        "Generate a production-ready rekuiper rule JSON configuration for IoT alerting:\n\
                         - Input stream: `{}`\n\
                         - Monitored field: `{}`\n\
                         - Threshold condition: `{}`\n\
                         Include a 30-second deduplication or sliding window to avoid alert spamming, and an MQTT action sink publishing to `alerts/{}/triggered`.\n\
                         Validate the SQL using `validate_sql` before outputting.",
                        stream, metric, threshold, stream
                    )),
                },
            ])
        }

        "create_end_to_end_pipeline" => {
            let pipeline_name = args
                .as_ref()
                .and_then(|a| a.get("pipeline_name"))
                .and_then(|v| v.as_str())
                .unwrap_or("demo_pipeline");
            let topic = args
                .as_ref()
                .and_then(|a| a.get("datasource_topic"))
                .and_then(|v| v.as_str())
                .unwrap_or("devices/data");

            Ok(vec![
                PromptMessage {
                    role: "user".to_string(),
                    content: ContentItem::text(format!(
                        "Design an end-to-end edge pipeline named '{}' consuming from MQTT topic '{}':\n\
                         1. Provide the `create_stream` DDL statement.\n\
                         2. Provide the `create_rule` payload with windowed aggregation (min, max, avg).\n\
                         3. Provide a mock JSON event payload and verify using `test_sql_expression`.\n\
                         4. Provide instructions to deploy using `rekuiper-mcp` tools.",
                        pipeline_name, topic
                    )),
                },
            ])
        }

        "diagnose_data_drop" => {
            let rule_name = args
                .as_ref()
                .and_then(|a| a.get("rule_name"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown_rule");

            Ok(vec![
                PromptMessage {
                    role: "user".to_string(),
                    content: ContentItem::text(format!(
                        "Investigate why data is dropped in rekuiper rule '{}':\n\
                         1. Inspect `get_rule_status` to see if records dropped in `source_dropped` or `sink_dropped`.\n\
                         2. Fetch `get_rule_topo` to trace the node sequence.\n\
                         3. Use `test_sql_expression` to test whether sample payloads satisfy the WHERE clause.\n\
                         4. If necessary, activate `start_rule_trace` to capture dropped payloads.",
                        rule_name
                    )),
                },
            ])
        }

        "generate_vector_search_rule" => {
            let stream = args
                .as_ref()
                .and_then(|a| a.get("stream_name"))
                .and_then(|v| v.as_str())
                .unwrap_or("vector_stream");
            let field = args
                .as_ref()
                .and_then(|a| a.get("vector_field"))
                .and_then(|v| v.as_str())
                .unwrap_or("embedding");
            let threshold = args
                .as_ref()
                .and_then(|a| a.get("similarity_threshold"))
                .and_then(|v| v.as_str())
                .unwrap_or("0.85");

            Ok(vec![
                PromptMessage {
                    role: "user".to_string(),
                    content: ContentItem::text(format!(
                        "Create a real-time vector similarity search rule for stream '{}':\n\
                         1. Use `cosine_similarity({}, target_vector)` in the SELECT projection and WHERE filter.\n\
                         2. Set the predicate `WHERE cosine_similarity({}, target_vector) >= {}`.\n\
                         3. Test the query using `test_sql_expression` with mock embedding vectors.\n\
                         4. Deploy using `create_rule`.",
                        stream, field, field, threshold
                    )),
                },
            ])
        }

        "configure_rabbitmq_pipeline" => {
            let rule = args
                .as_ref()
                .and_then(|a| a.get("rule_name"))
                .and_then(|v| v.as_str())
                .unwrap_or("rabbitmq_rule");
            let stream = args
                .as_ref()
                .and_then(|a| a.get("stream_name"))
                .and_then(|v| v.as_str())
                .unwrap_or("device_stream");
            let exchange = args
                .as_ref()
                .and_then(|a| a.get("exchange"))
                .and_then(|v| v.as_str())
                .unwrap_or("events.topic");

            Ok(vec![
                PromptMessage {
                    role: "user".to_string(),
                    content: ContentItem::text(format!(
                        "Configure a RabbitMQ streaming pipeline for rule '{}' consuming from stream '{}':\n\
                         1. Check the RabbitMQ sink action schema using `read_resource` ('rekuiper://schemas/rabbitmq').\n\
                         2. Use dynamic secrets `{{{{env://RABBITMQ_URI}}}}` or `{{{{vault://secret/rabbitmq#url}}}}` for credentials.\n\
                         3. Validate secret syntax using `validate_secrets`.\n\
                         4. Create the rule sending events to exchange '{}'.",
                        rule, stream, exchange
                    )),
                },
            ])
        }

        "create_wasm_plugin_rule" => {
            let module = args
                .as_ref()
                .and_then(|a| a.get("module_name"))
                .and_then(|v| v.as_str())
                .unwrap_or("custom_wasm");
            let func = args
                .as_ref()
                .and_then(|a| a.get("function_name"))
                .and_then(|v| v.as_str())
                .unwrap_or("process");

            Ok(vec![
                PromptMessage {
                    role: "user".to_string(),
                    content: ContentItem::text(format!(
                        "Configure a WebAssembly UDF streaming pipeline for module '{}' and function '{}':\n\
                         1. Register the plugin using `register_wasm_plugin` (POST /plugins/wasm).\n\
                         2. Verify the registration using `list_wasm_plugins`.\n\
                         3. Construct a query calling `{}(...)` directly or `wasm_run('{}', '{}', ...)`. \n\
                         4. Validate the SQL with `validate_sql`.",
                        module, func, func, module, func
                    )),
                },
            ])
        }

        "audit_security_posture" => {
            Ok(vec![
                PromptMessage {
                    role: "user".to_string(),
                    content: ContentItem::text(
                        "Audit the security posture of the rekuiper deployment:\n\
                         1. Execute `get_configs` to inspect runtime network bindings.\n\
                         2. Verify `restIp` and `httpServerIp` default to secure loopback `127.0.0.1`. If bound to `0.0.0.0`, verify `authentication` is enabled.\n\
                         3. Confirm `enable_private_net` is false unless private network probe egress is required.\n\
                         4. Provide actionable hardening recommendations for production deployments."
                            .to_string(),
                    ),
                },
            ])
        }

        "generate_cep_rule" => {
            let stream = args
                .as_ref()
                .and_then(|a| a.get("stream_name"))
                .and_then(|v| v.as_str())
                .unwrap_or("events");
            let pattern = args
                .as_ref()
                .and_then(|a| a.get("pattern"))
                .and_then(|v| v.as_str())
                .unwrap_or("(?1)(?2)*(?3)");
            let ts_field = args
                .as_ref()
                .and_then(|a| a.get("timestamp_field"))
                .and_then(|v| v.as_str())
                .unwrap_or("ts");

            Ok(vec![
                PromptMessage {
                    role: "user".to_string(),
                    content: ContentItem::text(format!(
                        "Design a Complex Event Processing (CEP) rule for stream '{}' matching pattern '{}' on timestamp '{}':\n\
                         1. Use `sequence_match('{}', {}, cond1, cond2, ...)` or `sequence_count` in the SELECT clause.\n\
                         2. Configure the continuous window (e.g. `SlidingWindow(ss, 60)`) to bound the event matching sequence.\n\
                         3. Validate the streaming query with `validate_sql`.\n\
                         4. Test sample event sequences with `test_sql_expression` to verify positive matches and negative non-matches.",
                        stream, pattern, ts_field, pattern, ts_field
                    )),
                },
            ])
        }

        "staged_fleet_rollout" => {
            let version = args
                .as_ref()
                .and_then(|a| a.get("version"))
                .and_then(|v| v.as_str())
                .unwrap_or("latest");
            let group = args
                .as_ref()
                .and_then(|a| a.get("group_filter"))
                .and_then(|v| v.as_str())
                .unwrap_or("all");

            Ok(vec![
                PromptMessage {
                    role: "user".to_string(),
                    content: ContentItem::text(format!(
                        "Execute a staged canary rollout for fleet devices (group: '{}', target version: '{}'):\n\
                         1. Execute `list_fleet_devices` and read resource `rekuiper://fleet/devices` to audit current device states, versions, and online health.\n\
                         2. Query `list_fleet_snapshots` to confirm backup state before rollout initiation.\n\
                         3. Initiate canary rollout with `trigger_fleet_rollout` targeting a small canary slice.\n\
                         4. Poll `get_fleet_rollouts` to monitor step progression, error rates, and node convergence.\n\
                         5. Proxy runtime rule checks via `proxy_fleet_device_api` on canary nodes before approving global rollout.",
                        group, version
                    )),
                },
            ])
        }

        "design_visual_dag_pipeline" => {
            let rule_name = args
                .as_ref()
                .and_then(|a| a.get("rule_name"))
                .and_then(|v| v.as_str())
                .unwrap_or("visual_rule_01");
            let goal = args
                .as_ref()
                .and_then(|a| a.get("pipeline_goal"))
                .and_then(|v| v.as_str())
                .unwrap_or("Process telemetry and route alerts");

            Ok(vec![
                PromptMessage {
                    role: "user".to_string(),
                    content: ContentItem::text(format!(
                        "Design a visual DAG streaming rule '{}' to achieve: {}\n\
                         1. Query `get_studio_palette` (or read `rekuiper://studio/palette`) to inspect available nodes, input/output port definitions, and properties.\n\
                         2. Construct the DAG JSON representation including nodes (type, id, name, props) and edges (source_node, source_port, target_node, target_port).\n\
                         3. Validate graph topology and connectivity using `validate_graph_rule`.\n\
                         4. Deploy rule and configure real-time wire-tap diagnostic probes using `toggle_wire_tap` and `get_wire_tap_status`.",
                        rule_name, goal
                    )),
                },
            ])
        }

        other => Err(format!("Unknown prompt: '{}'", other)),
    }
}
