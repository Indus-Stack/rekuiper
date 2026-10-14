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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamDefinition {
    pub name: String,
    #[serde(default)]
    pub sql: String,
    #[serde(default)]
    pub stream_fields: Vec<StreamField>,
    #[serde(default)]
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDefinition {
    pub name: String,
    pub kind: String, // e.g. "protobuf"
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub file: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDefinition {
    pub name: String,
    #[serde(default)]
    pub sql: String,
    #[serde(default)]
    pub stream_fields: Vec<StreamField>,
    #[serde(default)]
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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
}
