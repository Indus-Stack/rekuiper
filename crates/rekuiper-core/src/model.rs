use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

/// Copy-on-write map wrapper backed by `Arc<HashMap<String, Value>>`.
/// Cloning a `StreamRecord` across N subscribers in `StreamBus` is an O(1)
/// atomic refcount increment; mutation via `DerefMut` triggers `Arc::make_mut`.
#[derive(Debug, Clone, Default)]
pub struct SharedMap(pub Arc<HashMap<String, Value>>);

impl SharedMap {
    #[inline]
    pub fn new(map: HashMap<String, Value>) -> Self {
        Self(Arc::new(map))
    }

    #[inline]
    pub fn into_inner(self) -> HashMap<String, Value> {
        Arc::try_unwrap(self.0).unwrap_or_else(|arc| (*arc).clone())
    }
}

impl Deref for SharedMap {
    type Target = HashMap<String, Value>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SharedMap {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::make_mut(&mut self.0)
    }
}

impl PartialEq for SharedMap {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0) || *self.0 == *other.0
    }
}

impl PartialEq<HashMap<String, Value>> for SharedMap {
    fn eq(&self, other: &HashMap<String, Value>) -> bool {
        *self.0 == *other
    }
}

impl PartialEq<SharedMap> for HashMap<String, Value> {
    fn eq(&self, other: &SharedMap) -> bool {
        *self == *other.0
    }
}

impl<'a> IntoIterator for &'a SharedMap {
    type Item = (&'a String, &'a Value);
    type IntoIter = std::collections::hash_map::Iter<'a, String, Value>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl IntoIterator for SharedMap {
    type Item = (String, Value);
    type IntoIter = std::collections::hash_map::IntoIter<String, Value>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.into_inner().into_iter()
    }
}

impl FromIterator<(String, Value)> for SharedMap {
    #[inline]
    fn from_iter<T: IntoIterator<Item = (String, Value)>>(iter: T) -> Self {
        Self::new(iter.into_iter().collect())
    }
}

impl From<HashMap<String, Value>> for SharedMap {
    #[inline]
    fn from(map: HashMap<String, Value>) -> Self {
        Self::new(map)
    }
}

impl From<SharedMap> for HashMap<String, Value> {
    #[inline]
    fn from(shared: SharedMap) -> Self {
        shared.into_inner()
    }
}

impl Serialize for SharedMap {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SharedMap {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        HashMap::<String, Value>::deserialize(deserializer).map(Self::new)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRecord {
    pub timestamp: i64,
    pub data: SharedMap,
}

impl StreamRecord {
    pub fn new(data: HashMap<String, Value>) -> Self {
        Self {
            timestamp: chrono::Utc::now().timestamp_millis(),
            data: SharedMap::new(data),
        }
    }

    pub fn from_shared(data: SharedMap) -> Self {
        Self {
            timestamp: chrono::Utc::now().timestamp_millis(),
            data,
        }
    }

    #[inline]
    pub fn with_timestamp(data: HashMap<String, Value>, timestamp: i64) -> Self {
        Self {
            timestamp,
            data: SharedMap::new(data),
        }
    }
}

/// One declared stream/table column, serialized eKuiper-style as
/// `{"Name": ..., "FieldType": ...}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct StreamField {
    pub name: String,
    pub field_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,
}

impl StreamField {
    pub fn new(name: impl Into<String>, field_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            field_type: field_type.into(),
            default_value: None,
        }
    }

    pub fn with_default(
        name: impl Into<String>,
        field_type: impl Into<String>,
        default_val: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            field_type: field_type.into(),
            default_value: Some(default_val.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamDefinition {
    pub name: String,
    #[serde(default)]
    pub sql: String,
    #[serde(default)]
    pub stream_fields: Vec<StreamField>,
    #[serde(default)]
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaDefinition {
    pub name: String,
    pub kind: String, // e.g. "protobuf"
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub file: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableDefinition {
    pub name: String,
    #[serde(default)]
    pub sql: String,
    #[serde(default)]
    pub stream_fields: Vec<StreamField>,
    #[serde(default)]
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct RuleDefinition {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub sql: String,
    #[serde(default)]
    pub actions: Vec<HashMap<String, Value>>,
    #[serde(default)]
    pub options: Option<HashMap<String, Value>>,
    #[serde(default)]
    pub graph: Option<GraphDefinition>,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct GraphDefinition {
    #[serde(default)]
    pub nodes: HashMap<String, GraphNode>,
    #[serde(default)]
    pub topo: GraphTopo,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct GraphNode {
    #[serde(rename = "type", default)]
    pub node_category: String, // "source", "operator", "sink"
    #[serde(rename = "nodeType", default)]
    pub node_type: String, // e.g. "mqtt", "filter", "pick", "window", "function", "aggfunc", "log"
    #[serde(default)]
    pub props: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct GraphTopo {
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub edges: HashMap<String, Vec<String>>,
}

/// Compile a visual rule DAG into an equivalent SQL string plus sink actions.
///
/// Resulting shape: `SELECT <projections or *> FROM <from> [WHERE
/// <combined_filters>] [GROUP BY <window>]`. Nodes are visited in sorted key
/// order so compilation is deterministic.
pub fn compile_graph_to_sql_and_actions(
    graph: &GraphDefinition,
) -> Result<(String, Vec<HashMap<String, Value>>), String> {
    fn prop_str(props: &HashMap<String, Value>, key: &str) -> Option<String> {
        props
            .get(key)
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    fn prop_u64(props: &HashMap<String, Value>, key: &str, default: u64) -> u64 {
        if let Some(v) = props.get(key) {
            if let Some(n) = v.as_u64() {
                return n;
            }
            if let Some(n) = v.as_i64().and_then(|n| u64::try_from(n).ok()) {
                return n;
            }
            if let Some(s) = v.as_str() {
                if let Ok(n) = s.trim().parse::<u64>() {
                    return n;
                }
            }
        }
        default
    }

    // A visual rule must be acyclic: depth-first search over the topo edges,
    // tracking the recursion stack to catch back edges (including self-loops).
    // Diamond topologies (A -> B, A -> C, B -> D, C -> D) pass: nodes leave
    // the stack once fully explored.
    {
        use std::collections::HashSet;
        fn visit<'a>(
            node: &'a str,
            edges: &'a HashMap<String, Vec<String>>,
            visiting: &mut HashSet<&'a str>,
            done: &mut HashSet<&'a str>,
        ) -> Result<(), String> {
            if !visiting.insert(node) {
                return Err(format!("Graph contains a cycle at node '{}'", node));
            }
            if done.contains(node) {
                visiting.remove(node);
                return Ok(());
            }
            if let Some(next) = edges.get(node) {
                for target in next {
                    visit(target, edges, visiting, done)?;
                }
            }
            visiting.remove(node);
            done.insert(node);
            Ok(())
        }
        let mut visiting = HashSet::new();
        let mut done = HashSet::new();
        let mut endpoints: Vec<&String> = graph.topo.edges.keys().collect();
        for targets in graph.topo.edges.values() {
            endpoints.extend(targets);
        }
        endpoints.sort();
        endpoints.dedup();
        for node in endpoints {
            visit(node, &graph.topo.edges, &mut visiting, &mut done)?;
        }
    }

    // Source stream: prefer `sourceName`/`datasource` props of source nodes
    // (node key as fallback), else the topo entry points.
    let mut source_keys: Vec<&String> = graph
        .nodes
        .iter()
        .filter(|(_, n)| n.node_category == "source")
        .map(|(k, _)| k)
        .collect();
    source_keys.sort();
    let from = match source_keys.first() {
        Some(key) => {
            let node = &graph.nodes[*key];
            prop_str(&node.props, "sourceName")
                .or_else(|| prop_str(&node.props, "datasource"))
                .unwrap_or_else(|| (*key).clone())
        }
        None => graph
            .topo
            .sources
            .first()
            .cloned()
            .ok_or_else(|| "Graph rule has no source".to_string())?,
    };

    let mut filters: Vec<String> = Vec::new();
    let mut projections: Vec<String> = Vec::new();
    let mut window: Option<String> = None;
    let mut actions: Vec<HashMap<String, Value>> = Vec::new();

    let mut node_keys: Vec<&String> = graph.nodes.keys().collect();
    node_keys.sort();
    for key in node_keys {
        let node = &graph.nodes[key];
        match node.node_category.as_str() {
            "operator" => match node.node_type.as_str() {
                "filter" => {
                    if let Some(expr) = prop_str(&node.props, "expr") {
                        filters.push(format!("({})", expr));
                    }
                }
                "pick" => {
                    if let Some(fields) = node.props.get("fields").and_then(|v| v.as_array()) {
                        for field in fields {
                            if let Some(name) = field
                                .as_str()
                                .map(|s| s.trim().to_string())
                                .filter(|s| !s.is_empty())
                            {
                                projections.push(name);
                            }
                        }
                    }
                }
                "function" | "aggfunc" => {
                    if let Some(expr) = prop_str(&node.props, "expr") {
                        projections.push(expr);
                    }
                }
                "window" => {
                    let kind = node
                        .props
                        .get("type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("tumblingwindow")
                        .to_ascii_lowercase();
                    let unit = node
                        .props
                        .get("unit")
                        .and_then(|v| v.as_str())
                        .unwrap_or("ss")
                        .to_ascii_lowercase();
                    let size = prop_u64(&node.props, "size", 10);
                    window = Some(match kind.as_str() {
                        "hoppingwindow" => {
                            let interval = if node.props.contains_key("interval") {
                                prop_u64(&node.props, "interval", size)
                            } else {
                                size
                            };
                            format!("HOPPINGWINDOW({}, {}, {})", unit, size, interval)
                        }
                        "slidingwindow" => format!("SLIDINGWINDOW({}, {})", unit, size),
                        // eKuiper graph session window: size = max duration,
                        // interval = timeout.
                        "sessionwindow" => format!(
                            "SESSIONWINDOW({}, {}, {})",
                            unit,
                            size,
                            prop_u64(&node.props, "interval", size)
                        ),
                        "countwindow" => {
                            if node.props.contains_key("interval") {
                                format!(
                                    "COUNTWINDOW({}, {})",
                                    size,
                                    prop_u64(&node.props, "interval", size)
                                )
                            } else {
                                format!("COUNTWINDOW({})", size)
                            }
                        }
                        _ => format!("TUMBLINGWINDOW({}, {})", unit, size),
                    });
                }
                _ => {}
            },
            "sink" => {
                let action_kind = if node.node_type.is_empty() {
                    (*key).clone()
                } else {
                    node.node_type.clone()
                };
                let mut action = HashMap::new();
                action.insert(
                    action_kind,
                    Value::Object(node.props.clone().into_iter().collect()),
                );
                actions.push(action);
            }
            _ => {}
        }
    }

    let mut sql = format!(
        "SELECT {} FROM {}",
        if projections.is_empty() {
            "*".to_string()
        } else {
            projections.join(", ")
        },
        from
    );
    if !filters.is_empty() {
        sql.push_str(&format!(" WHERE {}", filters.join(" AND ")));
    }
    if let Some(window) = window {
        sql.push_str(&format!(" GROUP BY {}", window));
    }
    Ok((sql, actions))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_compile_graph_to_sql_and_actions() {
        let mut nodes = HashMap::new();
        nodes.insert(
            "src".to_string(),
            GraphNode {
                node_category: "source".to_string(),
                node_type: "stream".to_string(),
                props: [("sourceName".to_string(), json!("demo"))]
                    .into_iter()
                    .collect(),
            },
        );
        nodes.insert(
            "f".to_string(),
            GraphNode {
                node_category: "operator".to_string(),
                node_type: "filter".to_string(),
                props: [("expr".to_string(), json!("temp > 25"))]
                    .into_iter()
                    .collect(),
            },
        );
        nodes.insert(
            "p".to_string(),
            GraphNode {
                node_category: "operator".to_string(),
                node_type: "pick".to_string(),
                props: [("fields".to_string(), json!(["temp"]))]
                    .into_iter()
                    .collect(),
            },
        );
        nodes.insert(
            "out".to_string(),
            GraphNode {
                node_category: "sink".to_string(),
                node_type: "log".to_string(),
                props: HashMap::new(),
            },
        );
        let graph = GraphDefinition {
            nodes,
            topo: GraphTopo {
                sources: vec!["src".to_string()],
                edges: [
                    ("src".to_string(), vec!["f".to_string()]),
                    ("f".to_string(), vec!["p".to_string()]),
                    ("p".to_string(), vec!["out".to_string()]),
                ]
                .into_iter()
                .collect(),
            },
        };
        let (sql, actions) = compile_graph_to_sql_and_actions(&graph).unwrap();
        assert_eq!(sql, "SELECT temp FROM demo WHERE (temp > 25)");
        assert_eq!(actions.len(), 1);
        assert!(actions[0].contains_key("log"));
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuleStatus {
    pub status: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub last_exception: String,
    #[serde(default)]
    pub last_start_timestamp: i64,
    #[serde(default)]
    pub last_stop_timestamp: i64,
    #[serde(default)]
    pub next_start_timestamp: i64,
    #[serde(default)]
    pub source_records_in_total: u64,
    #[serde(default)]
    pub sink_records_out_total: u64,
    #[serde(default)]
    pub exceptions_total: u64,
    /// Input records dropped by the WHERE filter (stateless rules).
    #[serde(default)]
    pub source_records_filtered_total: u64,
    /// Output records enqueued to the sink worker (before completion).
    #[serde(default)]
    pub sink_records_enqueued_total: u64,
    /// Sink operations that failed after dequeue (write/connect errors).
    #[serde(default)]
    pub sink_records_failed_total: u64,
    /// Records dropped by explicit bounded policy (feedback full, no
    /// subscriber, oversized batch rejection accounting).
    #[serde(default)]
    pub dropped_by_policy_total: u64,
    /// Deepest observed sink-queue backlog for this rule.
    #[serde(default)]
    pub sink_queue_high_water: usize,
    /// Total microseconds the evaluation loop spent blocked on sink-queue
    /// backpressure (awaiting capacity).
    #[serde(default)]
    pub sink_blocked_micros_total: u64,
    /// Total records routed to the dead-letter queue (poison pills / errors).
    #[serde(default)]
    pub dlq_records_total: u64,
}

impl Default for RuleStatus {
    fn default() -> Self {
        Self {
            status: "running".to_string(),
            message: "".to_string(),
            last_exception: "".to_string(),
            last_start_timestamp: chrono::Utc::now().timestamp_millis(),
            last_stop_timestamp: 0,
            next_start_timestamp: 0,
            source_records_in_total: 0,
            sink_records_out_total: 0,
            exceptions_total: 0,
            source_records_filtered_total: 0,
            sink_records_enqueued_total: 0,
            sink_records_failed_total: 0,
            dropped_by_policy_total: 0,
            sink_queue_high_water: 0,
            sink_blocked_micros_total: 0,
            dlq_records_total: 0,
        }
    }
}

/// Coerce a JSON value to a target stream data type.
pub fn coerce_value_to_type(val: Value, target_type: &str) -> Value {
    let ty = target_type.trim().to_ascii_lowercase();
    match ty.as_str() {
        "bigint" | "int" | "integer" | "smallint" | "tinyint" => match val {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::from(i)
                } else if let Some(u) = n.as_u64() {
                    Value::from(u as i64)
                } else if let Some(f) = n.as_f64() {
                    Value::from(f as i64)
                } else {
                    Value::Null
                }
            }
            Value::String(s) => {
                let s_trim = s.trim();
                if let Ok(i) = s_trim.parse::<i64>() {
                    Value::from(i)
                } else if let Ok(f) = s_trim.parse::<f64>() {
                    Value::from(f as i64)
                } else {
                    Value::Null
                }
            }
            Value::Bool(b) => Value::from(if b { 1 } else { 0 }),
            Value::Null => Value::Null,
            _ => Value::Null,
        },
        "float" | "double" | "real" => match val {
            Value::Number(n) => {
                if let Some(f) = n.as_f64() {
                    serde_json::Number::from_f64(f)
                        .map(Value::Number)
                        .unwrap_or(Value::Null)
                } else {
                    Value::Null
                }
            }
            Value::String(s) => {
                let s_trim = s.trim();
                if let Ok(f) = s_trim.parse::<f64>() {
                    serde_json::Number::from_f64(f)
                        .map(Value::Number)
                        .unwrap_or(Value::Null)
                } else {
                    Value::Null
                }
            }
            Value::Bool(b) => {
                let f = if b { 1.0 } else { 0.0 };
                serde_json::Number::from_f64(f)
                    .map(Value::Number)
                    .unwrap_or(Value::Null)
            }
            Value::Null => Value::Null,
            _ => Value::Null,
        },
        "string" | "text" | "varchar" | "char" => match val {
            Value::String(_) => val,
            Value::Number(n) => Value::String(n.to_string()),
            Value::Bool(b) => Value::String(b.to_string()),
            Value::Null => Value::Null,
            other => Value::String(other.to_string()),
        },
        "boolean" | "bool" => match val {
            Value::Bool(_) => val,
            Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
                "true" | "1" | "t" => Value::Bool(true),
                "false" | "0" | "f" => Value::Bool(false),
                _ => Value::Null,
            },
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    if i == 1 {
                        Value::Bool(true)
                    } else if i == 0 {
                        Value::Bool(false)
                    } else {
                        Value::Null
                    }
                } else {
                    Value::Null
                }
            }
            Value::Null => Value::Null,
            _ => Value::Null,
        },
        "datetime" | "timestamp" | "date" | "time" => match val {
            Value::Number(n) => Value::Number(n),
            Value::String(s) => {
                let t = s.trim();
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(t) {
                    Value::from(dt.timestamp_millis())
                } else if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S")
                {
                    Value::from(dt.and_utc().timestamp_millis())
                } else if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S")
                {
                    Value::from(dt.and_utc().timestamp_millis())
                } else if let Ok(dt) =
                    chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S%.f")
                {
                    Value::from(dt.and_utc().timestamp_millis())
                } else if let Ok(dt) =
                    chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S%.f")
                {
                    Value::from(dt.and_utc().timestamp_millis())
                } else if let Ok(d) = chrono::NaiveDate::parse_from_str(t, "%Y-%m-%d") {
                    if let Some(dt) = d.and_hms_opt(0, 0, 0) {
                        Value::from(dt.and_utc().timestamp_millis())
                    } else {
                        Value::Null
                    }
                } else if let Ok(n) = t.parse::<i64>() {
                    Value::from(n)
                } else {
                    Value::Null
                }
            }
            Value::Null => Value::Null,
            _ => Value::Null,
        },
        "bytea" | "binary" | "blob" => match val {
            Value::String(_) => val,
            Value::Array(_) => val,
            Value::Null => Value::Null,
            _ => Value::Null,
        },
        _ if ty.starts_with("decimal") || ty.starts_with("numeric") => match val {
            Value::Number(n) => Value::Number(n),
            Value::String(s) => {
                if let Ok(f) = s.trim().parse::<f64>() {
                    serde_json::Number::from_f64(f)
                        .map(Value::Number)
                        .unwrap_or(Value::Null)
                } else {
                    Value::Null
                }
            }
            Value::Null => Value::Null,
            _ => Value::Null,
        },
        _ if ty.starts_with("array") => match val {
            Value::Array(items) => {
                let inner = if (ty.starts_with("array(") && ty.ends_with(')'))
                    || (ty.starts_with("array[") && ty.ends_with(']'))
                {
                    Some(ty[6..ty.len() - 1].trim())
                } else {
                    None
                };
                if let Some(inner_ty) = inner.filter(|s| !s.is_empty()) {
                    let coerced: Vec<Value> = items
                        .into_iter()
                        .map(|item| coerce_value_to_type(item, inner_ty))
                        .collect();
                    Value::Array(coerced)
                } else {
                    Value::Array(items)
                }
            }
            Value::Null => Value::Null,
            _ => Value::Null,
        },
        _ if ty.ends_with("[]") => match val {
            Value::Array(items) => {
                let inner_ty = ty[..ty.len() - 2].trim();
                let coerced: Vec<Value> = items
                    .into_iter()
                    .map(|item| coerce_value_to_type(item, inner_ty))
                    .collect();
                Value::Array(coerced)
            }
            Value::Null => Value::Null,
            _ => Value::Null,
        },
        _ if ty.starts_with("struct") => match val {
            Value::Object(map) => Value::Object(map),
            Value::Null => Value::Null,
            _ => Value::Null,
        },
        _ => val,
    }
}

pub fn parse_default_value(raw: &str, field_type: &str) -> Value {
    let s = raw.trim();
    if s.is_empty() || s.eq_ignore_ascii_case("null") {
        return Value::Null;
    }
    // If wrapped in single or double quotes, extract the string literal
    if (s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2)
        || (s.starts_with('"') && s.ends_with('"') && s.len() >= 2)
    {
        let unquoted = &s[1..s.len() - 1];
        return coerce_value_to_type(Value::String(unquoted.to_string()), field_type);
    }
    // Attempt parsing as JSON literal (numbers, booleans, arrays, objects)
    if let Ok(parsed) = serde_json::from_str::<Value>(s) {
        return coerce_value_to_type(parsed, field_type);
    }
    // Fallback: coerce raw string
    coerce_value_to_type(Value::String(s.to_string()), field_type)
}

/// Coerce an incoming record map according to declared stream fields.
/// When `fields` is empty (schemaless stream), this returns immediately (zero-cost no-op).
pub fn enforce_stream_schema(data: &mut HashMap<String, Value>, fields: &[StreamField]) {
    if fields.is_empty() {
        return;
    }
    for field in fields {
        // Look up either exact match or case-insensitive match
        let existing_key = if data.contains_key(&field.name) {
            Some(field.name.clone())
        } else {
            data.keys()
                .find(|k| k.eq_ignore_ascii_case(&field.name))
                .cloned()
        };

        if let Some(key) = existing_key {
            let val = data.remove(&key).unwrap_or(Value::Null);
            let coerced = if val.is_null() {
                if let Some(ref def_str) = field.default_value {
                    parse_default_value(def_str, &field.field_type)
                } else {
                    Value::Null
                }
            } else {
                coerce_value_to_type(val, &field.field_type)
            };
            data.insert(field.name.clone(), coerced);
        } else if let Some(ref def_str) = field.default_value {
            let default_val = parse_default_value(def_str, &field.field_type);
            data.insert(field.name.clone(), default_val);
        } else if (field.field_type.eq_ignore_ascii_case("bytea")
            || field.field_type.eq_ignore_ascii_case("binary"))
            && data.contains_key("self")
        {
            if let Some(val) = data.get("self").cloned() {
                data.insert(field.name.clone(), val);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Dead-Letter Queue (DLQ) & Poison-Pill Models
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DlqStage {
    CodecDecode = 0,
    SchemaValidation = 1,
    SqlEval = 2,
    DataTemplate = 3,
    SinkDelivery = 4,
}

impl DlqStage {
    pub const ALL: [DlqStage; 5] = [
        DlqStage::CodecDecode,
        DlqStage::SchemaValidation,
        DlqStage::SqlEval,
        DlqStage::DataTemplate,
        DlqStage::SinkDelivery,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            DlqStage::CodecDecode => "CodecDecode",
            DlqStage::SchemaValidation => "SchemaValidation",
            DlqStage::SqlEval => "SqlEval",
            DlqStage::DataTemplate => "DataTemplate",
            DlqStage::SinkDelivery => "SinkDelivery",
        }
    }

    pub fn label_str(&self) -> &'static str {
        match self {
            DlqStage::CodecDecode => "codec_decode",
            DlqStage::SchemaValidation => "schema_validation",
            DlqStage::SqlEval => "sql_eval",
            DlqStage::DataTemplate => "data_template",
            DlqStage::SinkDelivery => "sink_delivery",
        }
    }

    pub fn from_index(idx: usize) -> Option<Self> {
        match idx {
            0 => Some(DlqStage::CodecDecode),
            1 => Some(DlqStage::SchemaValidation),
            2 => Some(DlqStage::SqlEval),
            3 => Some(DlqStage::DataTemplate),
            4 => Some(DlqStage::SinkDelivery),
            _ => None,
        }
    }
}

impl std::fmt::Display for DlqStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

pub const MAX_DLQ_RAW_PAYLOAD_BYTES: usize = 4096;

/// Safely truncates a raw payload string to strictly at most `max_bytes` without slicing inside a UTF-8 character.
pub fn truncate_raw_payload(payload: &str, max_bytes: usize) -> String {
    if payload.len() <= max_bytes {
        return payload.to_string();
    }
    let suffix = "…[truncated]";
    let target = max_bytes.saturating_sub(suffix.len());
    let mut boundary = target;
    while boundary > 0 && !payload.is_char_boundary(boundary) {
        boundary -= 1;
    }
    let truncated = &payload[..boundary];
    format!("{}{}", truncated, suffix)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqEnvelope {
    pub rule_id: String,
    pub timestamp: i64,
    pub stage: DlqStage,
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_payload: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tuple: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink_type: Option<String>,
}

impl DlqEnvelope {
    pub fn new(rule_id: impl Into<String>, stage: DlqStage, error: impl Into<String>) -> Self {
        Self {
            rule_id: rule_id.into(),
            timestamp: chrono::Utc::now().timestamp_millis(),
            stage,
            error: error.into(),
            raw_payload: None,
            tuple: None,
            sink_type: None,
        }
    }

    pub fn with_raw_payload(mut self, raw: &str) -> Self {
        self.raw_payload = Some(truncate_raw_payload(raw, MAX_DLQ_RAW_PAYLOAD_BYTES));
        self
    }

    pub fn with_tuple(mut self, tuple: Value) -> Self {
        self.tuple = Some(tuple);
        self
    }

    pub fn with_sink_type(mut self, sink: impl Into<String>) -> Self {
        self.sink_type = Some(sink.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqStats {
    pub rule_id: String,
    pub total_errors: u64,
    pub codec_decode: u64,
    pub schema_validation: u64,
    pub sql_eval: u64,
    pub data_template: u64,
    pub sink_delivery: u64,
    pub sink_failures: u64,
    pub recent_errors: Vec<DlqEnvelope>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMismatchError {
    pub field: String,
    pub expected: String,
    pub actual_value: String,
}

impl std::fmt::Display for SchemaMismatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Field '{}' expected type '{}', got: {}",
            self.field, self.expected, self.actual_value
        )
    }
}

impl std::error::Error for SchemaMismatchError {}

/// Strict schema validator: validates and coerces declared fields, returning an error
/// if any non-null present value cannot be coerced into the declared type.
pub fn enforce_stream_schema_checked(
    data: &mut HashMap<String, Value>,
    fields: &[StreamField],
) -> Result<(), SchemaMismatchError> {
    if fields.is_empty() {
        return Ok(());
    }
    for field in fields {
        let existing_key = if data.contains_key(&field.name) {
            Some(field.name.clone())
        } else {
            data.keys()
                .find(|k| k.eq_ignore_ascii_case(&field.name))
                .cloned()
        };

        if let Some(key) = existing_key {
            let val = data.remove(&key).unwrap_or(Value::Null);
            if val.is_null() {
                if let Some(ref def_str) = field.default_value {
                    let default_val = parse_default_value(def_str, &field.field_type);
                    data.insert(field.name.clone(), default_val);
                } else {
                    data.insert(field.name.clone(), Value::Null);
                }
            } else {
                let coerced = coerce_value_to_type(val.clone(), &field.field_type);
                if coerced.is_null() {
                    return Err(SchemaMismatchError {
                        field: field.name.clone(),
                        expected: field.field_type.clone(),
                        actual_value: val.to_string(),
                    });
                }
                data.insert(field.name.clone(), coerced);
            }
        } else if let Some(ref def_str) = field.default_value {
            let default_val = parse_default_value(def_str, &field.field_type);
            data.insert(field.name.clone(), default_val);
        } else if (field.field_type.eq_ignore_ascii_case("bytea")
            || field.field_type.eq_ignore_ascii_case("binary"))
            && data.contains_key("self")
        {
            if let Some(val) = data.get("self").cloned() {
                data.insert(field.name.clone(), val);
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Declarative GitOps / Config-as-Code Manifest Models
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DiagnosticLevel {
    Error,
    Warning,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ValidationDiagnostic {
    pub level: DiagnosticLevel,
    pub entity_type: String, // "stream", "table", "rule", "connection", "schema", "manifest"
    pub entity_id: String,
    pub message: String,
}

impl std::fmt::Display for ValidationDiagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let prefix = match self.level {
            DiagnosticLevel::Error => "✗",
            DiagnosticLevel::Warning => "⚠",
        };
        write!(
            f,
            "{} {} '{}': {}",
            prefix, self.entity_type, self.entity_id, self.message
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct ValidationSummary {
    pub streams_count: usize,
    pub tables_count: usize,
    pub rules_count: usize,
    pub connections_count: usize,
    pub schemas_count: usize,
    pub errors_count: usize,
    pub warnings_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum DeclarativeRuleValue {
    Structured(RuleDefinition),
    Stringified(String),
}

impl DeclarativeRuleValue {
    pub fn to_rule_definition(&self, fallback_id: &str) -> Result<RuleDefinition, String> {
        match self {
            DeclarativeRuleValue::Structured(r) => {
                let mut rule = r.clone();
                if rule.id.is_empty() {
                    rule.id = fallback_id.to_string();
                }
                Ok(rule)
            }
            DeclarativeRuleValue::Stringified(s) => {
                let mut rule: RuleDefinition = serde_json::from_str(s).map_err(|e| {
                    format!(
                        "Failed to parse rule '{}' stringified JSON: {}",
                        fallback_id, e
                    )
                })?;
                if rule.id.is_empty() {
                    rule.id = fallback_id.to_string();
                }
                Ok(rule)
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum DeclarativeRules {
    Map(std::collections::BTreeMap<String, DeclarativeRuleValue>),
    List(Vec<RuleDefinition>),
}

impl Default for DeclarativeRules {
    fn default() -> Self {
        DeclarativeRules::Map(std::collections::BTreeMap::new())
    }
}

impl DeclarativeRules {
    pub fn into_rules_map(
        self,
    ) -> Result<std::collections::BTreeMap<String, RuleDefinition>, String> {
        let mut map = std::collections::BTreeMap::new();
        match self {
            DeclarativeRules::Map(m) => {
                for (id, val) in m {
                    let rule = val.to_rule_definition(&id)?;
                    map.insert(id, rule);
                }
            }
            DeclarativeRules::List(list) => {
                for rule in list {
                    if rule.id.is_empty() {
                        return Err("Rule in list missing 'id' field".to_string());
                    }
                    map.insert(rule.id.clone(), rule);
                }
            }
        }
        Ok(map)
    }

    pub fn to_rules_vec(&self) -> Result<Vec<RuleDefinition>, String> {
        let mut vec = Vec::new();
        match self {
            DeclarativeRules::Map(m) => {
                for (id, val) in m {
                    vec.push(val.to_rule_definition(id)?);
                }
            }
            DeclarativeRules::List(list) => {
                vec.extend(list.clone());
            }
        }
        Ok(vec)
    }
}

pub fn interpolate_env_vars(raw: &str, allow_unresolved: bool) -> Result<String, Vec<String>> {
    let re = match regex::Regex::new(r"\$\{([a-zA-Z_][a-zA-Z0-9_]*)(?::-([^}]*))?\}") {
        Ok(r) => r,
        Err(_) => return Ok(raw.to_string()),
    };
    let mut unresolved = Vec::new();
    let mut result = String::with_capacity(raw.len());
    let mut last_idx = 0;

    for cap in re.captures_iter(raw) {
        let mat = cap.get(0).unwrap();
        result.push_str(&raw[last_idx..mat.start()]);
        last_idx = mat.end();

        let var_name = &cap[1];
        let default_opt = cap.get(2).map(|m| m.as_str());

        match std::env::var(var_name) {
            Ok(val) => {
                result.push_str(&val);
            }
            Err(_) => {
                if let Some(def) = default_opt {
                    result.push_str(def);
                } else if allow_unresolved {
                    result.push_str(mat.as_str());
                } else {
                    unresolved.push(format!(
                        "Unresolved environment variable: ${{{}}}",
                        var_name
                    ));
                    result.push_str(mat.as_str());
                }
            }
        }
    }
    result.push_str(&raw[last_idx..]);

    if !unresolved.is_empty() {
        Err(unresolved)
    } else {
        Ok(result)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct DeclarativeManifest {
    #[serde(default = "default_manifest_version")]
    pub version: String,
    #[serde(default)]
    pub connections: std::collections::BTreeMap<String, Value>,
    #[serde(default)]
    pub schemas: std::collections::BTreeMap<String, SchemaDefinition>,
    #[serde(default)]
    pub streams: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub tables: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub rules: DeclarativeRules,
}

fn default_manifest_version() -> String {
    "v1".to_string()
}

impl DeclarativeManifest {
    pub fn from_str(
        content: &str,
        ext_or_format: &str,
        allow_unresolved_env: bool,
    ) -> Result<Self, String> {
        let interpolated =
            interpolate_env_vars(content, allow_unresolved_env).map_err(|errs| errs.join("; "))?;
        let format_lower = ext_or_format.trim_start_matches('.').to_lowercase();
        if format_lower == "toml" {
            toml::from_str::<DeclarativeManifest>(&interpolated)
                .map_err(|e| format!("TOML deserialization error: {}", e))
        } else if format_lower == "json" {
            serde_json::from_str::<DeclarativeManifest>(&interpolated)
                .map_err(|e| format!("JSON deserialization error: {}", e))
        } else {
            // Default to YAML (YAML is a superset of JSON)
            serde_yaml::from_str::<DeclarativeManifest>(&interpolated)
                .map_err(|e| format!("YAML deserialization error: {}", e))
        }
    }

    pub fn load_from_path(
        path: &std::path::Path,
        allow_unresolved_env: bool,
    ) -> Result<Self, Vec<ValidationDiagnostic>> {
        if !path.exists() {
            return Err(vec![ValidationDiagnostic {
                level: DiagnosticLevel::Error,
                entity_type: "manifest".to_string(),
                entity_id: path.to_string_lossy().to_string(),
                message: format!("Manifest path does not exist: {}", path.display()),
            }]);
        }

        if path.is_file() {
            let content = match std::fs::read_to_string(path) {
                Ok(c) => c,
                Err(e) => {
                    return Err(vec![ValidationDiagnostic {
                        level: DiagnosticLevel::Error,
                        entity_type: "manifest".to_string(),
                        entity_id: path.to_string_lossy().to_string(),
                        message: format!("Failed to read manifest file: {}", e),
                    }]);
                }
            };
            let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("yaml");
            match Self::from_str(&content, ext, allow_unresolved_env) {
                Ok(m) => Ok(m),
                Err(err) => Err(vec![ValidationDiagnostic {
                    level: DiagnosticLevel::Error,
                    entity_type: "manifest".to_string(),
                    entity_id: path.to_string_lossy().to_string(),
                    message: err,
                }]),
            }
        } else if path.is_dir() {
            Self::load_from_dir(path, allow_unresolved_env)
        } else {
            Err(vec![ValidationDiagnostic {
                level: DiagnosticLevel::Error,
                entity_type: "manifest".to_string(),
                entity_id: path.to_string_lossy().to_string(),
                message: format!(
                    "Manifest path is neither file nor directory: {}",
                    path.display()
                ),
            }])
        }
    }

    pub fn load_from_dir(
        dir: &std::path::Path,
        allow_unresolved_env: bool,
    ) -> Result<Self, Vec<ValidationDiagnostic>> {
        let mut entries = match std::fs::read_dir(dir) {
            Ok(it) => it.filter_map(|e| e.ok()).collect::<Vec<_>>(),
            Err(e) => {
                return Err(vec![ValidationDiagnostic {
                    level: DiagnosticLevel::Error,
                    entity_type: "manifest".to_string(),
                    entity_id: dir.to_string_lossy().to_string(),
                    message: format!("Failed to read directory {}: {}", dir.display(), e),
                }]);
            }
        };

        // Filter valid manifest file extensions
        entries.retain(|e| {
            if let Some(ext) = e.path().extension().and_then(|s| s.to_str()) {
                let lower = ext.to_lowercase();
                lower == "yaml" || lower == "yml" || lower == "json" || lower == "toml"
            } else {
                false
            }
        });

        // Sort lexicographically by file name
        entries.sort_by_key(|e| e.file_name());

        if entries.is_empty() {
            return Ok(DeclarativeManifest::default());
        }

        let mut combined = DeclarativeManifest::default();
        let mut diagnostics = Vec::new();

        let mut seen_connections: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        let mut seen_schemas: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        let mut seen_streams: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        let mut seen_tables: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        let mut seen_rules: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();

        let mut combined_rules_map = std::collections::BTreeMap::new();

        for entry in entries {
            let file_path = entry.path();
            let file_display = file_path
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| file_path.to_string_lossy().to_string());

            let content = match std::fs::read_to_string(&file_path) {
                Ok(c) => c,
                Err(e) => {
                    diagnostics.push(ValidationDiagnostic {
                        level: DiagnosticLevel::Error,
                        entity_type: "manifest".to_string(),
                        entity_id: file_display,
                        message: format!("Failed to read file: {}", e),
                    });
                    continue;
                }
            };

            let ext = file_path
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or("yaml");

            let manifest = match Self::from_str(&content, ext, allow_unresolved_env) {
                Ok(m) => m,
                Err(e) => {
                    diagnostics.push(ValidationDiagnostic {
                        level: DiagnosticLevel::Error,
                        entity_type: "manifest".to_string(),
                        entity_id: file_display,
                        message: e,
                    });
                    continue;
                }
            };

            if combined.version == "v1" && manifest.version != "v1" {
                combined.version = manifest.version;
            }

            for (conn_name, conn_val) in manifest.connections {
                if let Some(prev) = seen_connections.get(&conn_name) {
                    diagnostics.push(ValidationDiagnostic {
                        level: DiagnosticLevel::Error,
                        entity_type: "connection".to_string(),
                        entity_id: conn_name.clone(),
                        message: format!(
                            "Duplicate connection '{}' defined in both '{}' and '{}'",
                            conn_name, prev, file_display
                        ),
                    });
                } else {
                    seen_connections.insert(conn_name.clone(), file_display.clone());
                    combined.connections.insert(conn_name, conn_val);
                }
            }

            for (schema_name, schema_val) in manifest.schemas {
                if let Some(prev) = seen_schemas.get(&schema_name) {
                    diagnostics.push(ValidationDiagnostic {
                        level: DiagnosticLevel::Error,
                        entity_type: "schema".to_string(),
                        entity_id: schema_name.clone(),
                        message: format!(
                            "Duplicate schema '{}' defined in both '{}' and '{}'",
                            schema_name, prev, file_display
                        ),
                    });
                } else {
                    seen_schemas.insert(schema_name.clone(), file_display.clone());
                    combined.schemas.insert(schema_name, schema_val);
                }
            }

            for (stream_name, stream_ddl) in manifest.streams {
                if let Some(prev) = seen_streams.get(&stream_name) {
                    diagnostics.push(ValidationDiagnostic {
                        level: DiagnosticLevel::Error,
                        entity_type: "stream".to_string(),
                        entity_id: stream_name.clone(),
                        message: format!(
                            "Duplicate stream '{}' defined in both '{}' and '{}'",
                            stream_name, prev, file_display
                        ),
                    });
                } else {
                    seen_streams.insert(stream_name.clone(), file_display.clone());
                    combined.streams.insert(stream_name, stream_ddl);
                }
            }

            for (table_name, table_ddl) in manifest.tables {
                if let Some(prev) = seen_tables.get(&table_name) {
                    diagnostics.push(ValidationDiagnostic {
                        level: DiagnosticLevel::Error,
                        entity_type: "table".to_string(),
                        entity_id: table_name.clone(),
                        message: format!(
                            "Duplicate table '{}' defined in both '{}' and '{}'",
                            table_name, prev, file_display
                        ),
                    });
                } else {
                    seen_tables.insert(table_name.clone(), file_display.clone());
                    combined.tables.insert(table_name, table_ddl);
                }
            }

            match manifest.rules.into_rules_map() {
                Ok(rules_map) => {
                    for (rule_id, rule_def) in rules_map {
                        if let Some(prev) = seen_rules.get(&rule_id) {
                            diagnostics.push(ValidationDiagnostic {
                                level: DiagnosticLevel::Error,
                                entity_type: "rule".to_string(),
                                entity_id: rule_id.clone(),
                                message: format!(
                                    "Duplicate rule ID '{}' defined in both '{}' and '{}'",
                                    rule_id, prev, file_display
                                ),
                            });
                        } else {
                            seen_rules.insert(rule_id.clone(), file_display.clone());
                            combined_rules_map
                                .insert(rule_id, DeclarativeRuleValue::Structured(rule_def));
                        }
                    }
                }
                Err(err) => {
                    diagnostics.push(ValidationDiagnostic {
                        level: DiagnosticLevel::Error,
                        entity_type: "rule".to_string(),
                        entity_id: file_display,
                        message: err,
                    });
                }
            }
        }

        combined.rules = DeclarativeRules::Map(combined_rules_map);

        if diagnostics
            .iter()
            .any(|d| d.level == DiagnosticLevel::Error)
        {
            Err(diagnostics)
        } else {
            Ok(combined)
        }
    }
}

#[cfg(test)]
mod schema_tests {
    use super::*;

    #[test]
    fn test_enforce_stream_schema_coercion() {
        let fields = vec![
            StreamField::new("id", "bigint"),
            StreamField::new("temp", "float"),
            StreamField::new("name", "string"),
            StreamField::new("active", "boolean"),
            StreamField::new("ts", "datetime"),
            StreamField::new("arr", "array(int)"),
        ];

        let mut data = HashMap::new();
        data.insert("id".to_string(), Value::String("101".to_string()));
        data.insert("temp".to_string(), Value::String("25.5".to_string()));
        data.insert("name".to_string(), Value::from(42));
        data.insert("active".to_string(), Value::String("true".to_string()));
        data.insert(
            "ts".to_string(),
            Value::String("2023-01-01T00:00:00Z".to_string()),
        );
        data.insert(
            "arr".to_string(),
            Value::Array(vec![
                Value::String("1".to_string()),
                Value::String("2".to_string()),
            ]),
        );

        enforce_stream_schema(&mut data, &fields);

        assert_eq!(data.get("id"), Some(&Value::from(101i64)));
        assert_eq!(data.get("temp"), Some(&Value::from(25.5f64)));
        assert_eq!(data.get("name"), Some(&Value::String("42".to_string())));
        assert_eq!(data.get("active"), Some(&Value::Bool(true)));
        assert_eq!(data.get("ts"), Some(&Value::from(1672531200000i64)));
        assert_eq!(
            data.get("arr"),
            Some(&Value::Array(vec![Value::from(1i64), Value::from(2i64)]))
        );
    }

    #[test]
    fn test_enforce_stream_schema_mismatched_types() {
        let fields = vec![
            StreamField::new("id", "bigint"),
            StreamField::new("temp", "float"),
            StreamField::new("active", "boolean"),
        ];

        let mut data = HashMap::new();
        data.insert("id".to_string(), Value::String("not_a_number".to_string()));
        data.insert("temp".to_string(), Value::String("not_a_float".to_string()));
        data.insert("active".to_string(), Value::String("invalid".to_string()));

        enforce_stream_schema(&mut data, &fields);

        assert_eq!(data.get("id"), Some(&Value::Null));
        assert_eq!(data.get("temp"), Some(&Value::Null));
        assert_eq!(data.get("active"), Some(&Value::Null));
    }

    #[test]
    fn test_enforce_stream_schema_default_values() {
        let fields = vec![
            StreamField::with_default("id", "bigint", "999"),
            StreamField::with_default("status", "string", "'idle'"),
            StreamField::with_default("temp", "float", "20.5"),
            StreamField::with_default("active", "boolean", "true"),
            StreamField::new("extra", "string"),
        ];

        // 1. Missing fields should receive defaults
        let mut data_missing = HashMap::new();
        enforce_stream_schema(&mut data_missing, &fields);
        assert_eq!(data_missing.get("id"), Some(&Value::from(999i64)));
        assert_eq!(
            data_missing.get("status"),
            Some(&Value::String("idle".to_string()))
        );
        assert_eq!(data_missing.get("temp"), Some(&Value::from(20.5f64)));
        assert_eq!(data_missing.get("active"), Some(&Value::Bool(true)));
        assert!(!data_missing.contains_key("extra"));

        // 2. Explicit null fields should be replaced by defaults
        let mut data_null = HashMap::new();
        data_null.insert("id".to_string(), Value::Null);
        data_null.insert("status".to_string(), Value::Null);
        data_null.insert("temp".to_string(), Value::from(35.0f64));
        enforce_stream_schema(&mut data_null, &fields);
        assert_eq!(data_null.get("id"), Some(&Value::from(999i64)));
        assert_eq!(
            data_null.get("status"),
            Some(&Value::String("idle".to_string()))
        );
        assert_eq!(data_null.get("temp"), Some(&Value::from(35.0f64)));
    }

    #[test]
    fn test_declarative_manifest_env_interpolation() {
        std::env::set_var("TEST_BROKER_HOST", "192.168.1.100");
        let raw = "server: \"tcp://${TEST_BROKER_HOST}:${TEST_PORT:-1883}\"";
        let res = interpolate_env_vars(raw, false).unwrap();
        assert_eq!(res, "server: \"tcp://192.168.1.100:1883\"");

        let unresolved = "token: \"${UNRESOLVED_SECRET}\"";
        assert!(interpolate_env_vars(unresolved, false).is_err());
        assert_eq!(
            interpolate_env_vars(unresolved, true).unwrap(),
            "token: \"${UNRESOLVED_SECRET}\""
        );
    }

    #[test]
    fn test_declarative_manifest_yaml_and_json_parsing() {
        let yaml_content = r#"
version: v1
streams:
  demo_stream: "CREATE STREAM demo_stream () WITH (TYPE=\"demo\")"
rules:
  - id: rule1
    sql: "SELECT * FROM demo_stream"
    actions:
      - log: {}
"#;
        let manifest = DeclarativeManifest::from_str(yaml_content, "yaml", false).unwrap();
        assert_eq!(manifest.version, "v1");
        assert_eq!(manifest.streams.len(), 1);
        let rules_vec = manifest.rules.to_rules_vec().unwrap();
        assert_eq!(rules_vec.len(), 1);
        assert_eq!(rules_vec[0].id, "rule1");

        // Stringified JSON rule map format (as in /ruleset/export)
        let json_export = r#"{
  "version": "v1",
  "streams": {
    "s1": "CREATE STREAM s1 () WITH (TYPE=\"demo\")"
  },
  "rules": {
    "r1": "{\"id\":\"r1\",\"sql\":\"SELECT * FROM s1\",\"actions\":[{\"log\":{}}]}"
  }
}"#;
        let manifest_json = DeclarativeManifest::from_str(json_export, "json", false).unwrap();
        assert_eq!(manifest_json.streams.len(), 1);
        let rules_map = manifest_json.rules.into_rules_map().unwrap();
        assert_eq!(rules_map.len(), 1);
        assert_eq!(rules_map["r1"].sql, "SELECT * FROM s1");
    }
}
