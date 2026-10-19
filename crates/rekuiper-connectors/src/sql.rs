use crate::META_KEY;
use anyhow::{bail, Result};
use indexmap::IndexMap;
use parking_lot::RwLock;
use rekuiper_core::model::StreamRecord;
use rekuiper_core::StreamSender;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::LazyLock;

static SQLITE_POOLS: LazyLock<RwLock<HashMap<String, sqlx::sqlite::SqlitePool>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

static PG_POOLS: LazyLock<RwLock<HashMap<String, sqlx::postgres::PgPool>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Resolve or initialize a shared SQLite connection pool for `url`.
pub async fn sqlite_pool(url: &str) -> Result<sqlx::sqlite::SqlitePool> {
    if let Some(pool) = SQLITE_POOLS.read().get(url).cloned() {
        if !pool.is_closed() {
            return Ok(pool);
        }
    }
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(8)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(url)
        .await?;
    let mut guard = SQLITE_POOLS.write();
    let entry = guard.entry(url.to_string()).or_insert_with(|| pool.clone());
    Ok(entry.clone())
}

/// Connect or reuse a shared PostgreSQL pool with a bounded 3s handshake timeout
/// so dead brokers fail fast without stalling rule pipelines.
pub async fn pg_pool(url: &str) -> Result<sqlx::postgres::PgPool> {
    if let Some(pool) = PG_POOLS.read().get(url).cloned() {
        if !pool.is_closed() {
            return Ok(pool);
        }
    }
    use std::str::FromStr;
    let opts = sqlx::postgres::PgConnectOptions::from_str(url)?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(16)
        .acquire_timeout(std::time::Duration::from_secs(3))
        .connect_with(opts)
        .await?;
    let mut guard = PG_POOLS.write();
    let entry = guard.entry(url.to_string()).or_insert_with(|| pool.clone());
    Ok(entry.clone())
}

/// Evict a cached SQL pool on transport/execution error so subsequent calls reconnect cleanly.
pub fn evict_sql_pool(url: &str) {
    if url.starts_with("sqlite") {
        SQLITE_POOLS.write().remove(url);
    } else if url.starts_with("postgres") {
        PG_POOLS.write().remove(url);
    }
}

/// SQL connector configuration (SQLite / PostgreSQL sink, lookup and
/// polling source). Accepts both the native rekuiper shape (`url`/`table`)
/// and the documented eKuiper plugin shape (`dburl`,
/// `templateSqlQueryCfg`/`internalSqlQueryCfg`; see
/// https://ekuiper.org/docs/en/latest/guide/sources/plugin/sql.html).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SqlConnectorConfig {
    /// Database URL, e.g. `"sqlite::memory:"` or
    /// `"postgres://user:pass@localhost:5432/db"`. `dburl` is the
    /// documented plugin spelling.
    #[serde(default, alias = "dburl")]
    pub url: String,
    /// Target table name.
    #[serde(default)]
    pub table: String,
    /// Columns to write (defaults to the record's own keys).
    #[serde(default)]
    pub fields: Vec<String>,
    /// Poll interval in milliseconds (polling sources).
    #[serde(default = "default_sql_interval")]
    pub interval: u64,
    /// Documented template-SQL query config (takes precedence when set).
    #[serde(default)]
    pub template_sql_query_cfg: Option<TemplateSqlQueryCfg>,
    /// Documented internal query-builder config.
    #[serde(default)]
    pub internal_sql_query_cfg: Option<InternalSqlQueryCfg>,
}

/// One indexed column of a SQL source query config.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct IndexFieldCfg {
    #[serde(default)]
    pub index_field: String,
    #[serde(default)]
    pub index_value: serde_json::Value,
    #[serde(default)]
    pub index_field_type: String,
    #[serde(default)]
    pub date_time_format: String,
}

/// Documented `templateSqlQueryCfg`: a raw SQL template where `{{.field}}`
/// placeholders render from the configured index values.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TemplateSqlQueryCfg {
    #[serde(default, alias = "TemplateSql", alias = "templateSql")]
    pub template_sql: String,
    #[serde(default)]
    pub index_field: String,
    #[serde(default)]
    pub index_value: serde_json::Value,
    #[serde(default)]
    pub index_field_type: String,
    #[serde(default)]
    pub index_fields: Vec<IndexFieldCfg>,
    #[serde(default)]
    pub date_time_format: String,
}

impl TemplateSqlQueryCfg {
    fn index_pairs(&self) -> Vec<(String, serde_json::Value)> {
        let mut pairs = Vec::new();
        if !self.index_field.is_empty() {
            pairs.push((self.index_field.clone(), self.index_value.clone()));
        }
        for f in &self.index_fields {
            if !f.index_field.is_empty() {
                pairs.push((f.index_field.clone(), f.index_value.clone()));
            }
        }
        pairs
    }
}

/// Documented `internalSqlQueryCfg`: table + limit + index columns from
/// which the source builds its polling query.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct InternalSqlQueryCfg {
    #[serde(default)]
    pub table: String,
    #[serde(default)]
    pub limit: u64,
    #[serde(default)]
    pub index_field: String,
    #[serde(default)]
    pub index_value: serde_json::Value,
    #[serde(default)]
    pub index_field_type: String,
    #[serde(default)]
    pub index_fields: Vec<IndexFieldCfg>,
    #[serde(default)]
    pub date_time_format: String,
}

impl InternalSqlQueryCfg {
    fn index_pairs(&self) -> Vec<(String, serde_json::Value)> {
        let mut pairs = Vec::new();
        if !self.index_field.is_empty() {
            pairs.push((self.index_field.clone(), self.index_value.clone()));
        }
        for f in &self.index_fields {
            if !f.index_field.is_empty() {
                pairs.push((f.index_field.clone(), f.index_value.clone()));
            }
        }
        pairs
    }
}

fn default_sql_interval() -> u64 {
    1000
}

/// Render a JSON value as a SQL literal for template substitution.
fn sql_literal(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => {
            if *b {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            format!("'{}'", v.to_string().replace('\'', "''"))
        }
    }
}

/// Substitute `{{.field}}` (or `{{ .field }}`) placeholders in a template
/// SQL string with SQL literals. Unknown placeholders are left untouched.
pub fn render_template_sql(template: &str, pairs: &[(String, serde_json::Value)]) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        match after.find("}}") {
            Some(end) => {
                let key = after[..end].trim().trim_start_matches('.').trim();
                match pairs.iter().find(|(k, _)| k == key) {
                    Some((_, v)) => out.push_str(&sql_literal(v)),
                    None => {
                        out.push_str("{{");
                        out.push_str(&after[..end]);
                        out.push_str("}}");
                    }
                }
                rest = &after[end + 2..];
            }
            None => {
                out.push_str(&rest[start..]);
                rest = "";
            }
        }
    }
    out.push_str(rest);
    out
}

/// Build the polling query for a SQL source: the template SQL wins when
/// present, then the internal query-builder config, else `SELECT *`.
pub fn sql_source_query(config: &SqlConnectorConfig) -> String {
    sql_source_query_with(config, &initial_index_pairs(config))
}

/// Initial index pairs from the configured query cfgs (template wins).
fn initial_index_pairs(config: &SqlConnectorConfig) -> Vec<(String, serde_json::Value)> {
    if let Some(t) = &config.template_sql_query_cfg {
        if !t.template_sql.trim().is_empty() {
            let mut pairs = t.index_pairs();
            if pairs.is_empty() && !t.index_field.is_empty() {
                pairs.push((t.index_field.clone(), t.index_value.clone()));
            }
            return pairs;
        }
    }
    if let Some(i) = &config.internal_sql_query_cfg {
        let mut pairs = i.index_pairs();
        if pairs.is_empty() && !i.index_field.is_empty() {
            pairs.push((i.index_field.clone(), i.index_value.clone()));
        }
        return pairs;
    }
    Vec::new()
}

/// Render the polling query with explicit per-poll index values (advanced
/// across polls by [`SqlSource`]).
pub fn sql_source_query_with(
    config: &SqlConnectorConfig,
    pairs: &[(String, serde_json::Value)],
) -> String {
    if let Some(t) = &config.template_sql_query_cfg {
        if !t.template_sql.trim().is_empty() {
            return render_template_sql(&t.template_sql, pairs);
        }
    }
    if let Some(i) = &config.internal_sql_query_cfg {
        let table = if i.table.is_empty() {
            config.table.clone()
        } else {
            i.table.clone()
        };
        let mut sql = format!("SELECT * FROM {}", table);
        if !pairs.is_empty() {
            let conds = pairs
                .iter()
                .map(|(k, v)| format!("{} > {}", k, sql_literal(v)))
                .collect::<Vec<_>>()
                .join(" AND ");
            sql.push_str(&format!(" WHERE {}", conds));
            // Declaration order sets the pagination order (last row wins),
            // mirroring upstream `order by {field} ASC`.
            let order = pairs
                .iter()
                .map(|(k, _)| format!("{} ASC", k))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(" ORDER BY {}", order));
        }
        if i.limit > 0 {
            sql.push_str(&format!(" LIMIT {}", i.limit));
        }
        return sql;
    }
    format!("SELECT * FROM {}", config.table)
}

/// Advance tracked index columns from fetched rows, last row wins (rows
/// arrive in `ORDER BY .. ASC` pagination order for internal queries),
/// mirroring upstream `UpdateMaxIndexValue`.
fn advance_index(index: &mut [(String, serde_json::Value)], rows: &[StreamRecord]) {
    for row in rows {
        for (field, value) in index.iter_mut() {
            if let Some(v) = row.data.get(field) {
                *value = v.clone();
            }
        }
    }
}

/// A dynamically-typed bind parameter: numbers keep their JSON type so
/// drivers infer the right column type (a float must not arrive as text).
/// Null/missing values are NOT bound: PostgreSQL types even a NULL
/// parameter from its Rust type, so `None::<String>` is rejected by
/// non-text columns. Instead the sink omits null columns from the INSERT
/// (database defaults, i.e. NULL, apply).
#[derive(Debug, Clone)]
enum SqlBindVal {
    Int(i64),
    Float(f64),
    Bool(bool),
    Text(String),
}

fn json_to_bind(v: Option<&serde_json::Value>) -> Option<SqlBindVal> {
    match v {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::Bool(b)) => Some(SqlBindVal::Bool(*b)),
        Some(serde_json::Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                Some(SqlBindVal::Int(i))
            } else if let Some(u) = n.as_u64() {
                if u <= i64::MAX as u64 {
                    Some(SqlBindVal::Int(u as i64))
                } else {
                    Some(SqlBindVal::Float(u as f64))
                }
            } else if let Some(f) = n.as_f64() {
                Some(SqlBindVal::Float(f))
            } else {
                Some(SqlBindVal::Text(n.to_string()))
            }
        }
        Some(serde_json::Value::String(s)) => Some(SqlBindVal::Text(s.clone())),
        Some(other) => Some(SqlBindVal::Text(other.to_string())),
    }
}

fn bind_sqlite_arg<'q>(
    query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    val: &'q SqlBindVal,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    match val {
        SqlBindVal::Int(i) => query.bind(*i),
        SqlBindVal::Float(f) => query.bind(*f),
        SqlBindVal::Bool(b) => query.bind(*b),
        SqlBindVal::Text(s) => query.bind(s),
    }
}

fn bind_pg_arg<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    val: &'q SqlBindVal,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    match val {
        SqlBindVal::Int(i) => query.bind(*i),
        SqlBindVal::Float(f) => query.bind(*f),
        SqlBindVal::Bool(b) => query.bind(*b),
        SqlBindVal::Text(s) => query.bind(s),
    }
}

/// SQL sink: executes parameterized row inserts into the database (SQLite
/// `?` placeholders, PostgreSQL `$n` placeholders). Unknown URL schemes
/// are rejected so a rule cannot report a write that never happened.
pub struct SqlSink {
    pub config: SqlConnectorConfig,
}

/// Validates whether a dynamic column name is a safe SQL identifier matching
/// `[A-Za-z_][A-Za-z0-9_]*` to prevent syntax corruption and SQL injection.
pub fn is_valid_sql_identifier(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Validates SQL source table and index column identifiers before query execution.
pub fn validate_sql_source_identifiers(
    config: &SqlConnectorConfig,
    pairs: &[(String, serde_json::Value)],
) -> Result<()> {
    for (k, _) in pairs {
        if !is_valid_sql_identifier(k) {
            bail!(
                "Invalid SQL index field identifier '{}': requires [A-Za-z_][A-Za-z0-9_]*",
                k
            );
        }
    }
    let uses_template = config
        .template_sql_query_cfg
        .as_ref()
        .is_some_and(|t| !t.template_sql.trim().is_empty());
    if !uses_template {
        let table = config
            .internal_sql_query_cfg
            .as_ref()
            .filter(|i| !i.table.is_empty())
            .map(|i| i.table.as_str())
            .unwrap_or(&config.table);
        if !is_valid_sql_identifier(table) {
            bail!(
                "Invalid SQL table identifier '{}': requires [A-Za-z_][A-Za-z0-9_]*",
                table
            );
        }
    }
    Ok(())
}

impl SqlSink {
    pub fn new(config: SqlConnectorConfig) -> Self {
        Self { config }
    }

    pub async fn insert_record(&self, record: &StreamRecord) -> Result<()> {
        if !is_valid_sql_identifier(&self.config.table) {
            bail!(
                "Invalid SQL table identifier '{}': SQL sink requires identifiers matching [A-Za-z_][A-Za-z0-9_]*",
                self.config.table
            );
        }
        let fields = if self.config.fields.is_empty() {
            let mut cols: Vec<String> = Vec::new();
            for k in record
                .data
                .keys()
                .filter(|k| *k != META_KEY && !k.starts_with("__"))
            {
                if !is_valid_sql_identifier(k) {
                    anyhow::bail!(
                        "Invalid dynamic field name '{}': SQL sink requires identifiers matching [A-Za-z_][A-Za-z0-9_]* when fields is unset",
                        k
                    );
                }
                cols.push(k.clone());
            }
            cols.sort();
            cols
        } else {
            for f in &self.config.fields {
                if !is_valid_sql_identifier(f) {
                    bail!(
                        "Invalid field name '{}': SQL sink requires identifiers matching [A-Za-z_][A-Za-z0-9_]*",
                        f
                    );
                }
            }
            self.config.fields.clone()
        };
        // Null/missing columns become an untyped SQL NULL literal (the
        // server infers the column type, so no mistyped-parameter rejection),
        // never a typed NULL parameter and never silent omission (which
        // would wrongly apply column DEFAULTs where baseline stores NULL).
        // Only a completely column-less row falls back to DEFAULT VALUES.
        let cells: Vec<(&String, Option<SqlBindVal>)> = fields
            .iter()
            .map(|f| (f, json_to_bind(record.data.get(f))))
            .collect();
        if self.config.url.starts_with("sqlite") {
            let pool = sqlite_pool(&self.config.url).await?;
            if cells.is_empty() {
                let res = sqlx::query(&format!("INSERT INTO {} DEFAULT VALUES", self.config.table))
                    .execute(&pool)
                    .await;
                if let Err(e) = res {
                    evict_sql_pool(&self.config.url);
                    return Err(e.into());
                }
                return Ok(());
            }
            let cols: Vec<String> = cells.iter().map(|(f, _)| (*f).clone()).collect();
            let nulls: Vec<bool> = cells.iter().map(|(_, v)| v.is_none()).collect();
            let sql = sqlite_insert_row_sql(&self.config.table, &cols, &nulls);
            let mut query = sqlx::query(&sql);
            for v in cells.iter().filter_map(|(_, v)| v.as_ref()) {
                query = bind_sqlite_arg(query, v);
            }
            if let Err(e) = query.execute(&pool).await {
                evict_sql_pool(&self.config.url);
                return Err(e.into());
            }
        } else if self.config.url.starts_with("postgres") {
            let pool = pg_pool(&self.config.url).await?;
            if cells.is_empty() {
                let res = sqlx::query(&format!("INSERT INTO {} DEFAULT VALUES", self.config.table))
                    .execute(&pool)
                    .await;
                if let Err(e) = res {
                    evict_sql_pool(&self.config.url);
                    return Err(e.into());
                }
                return Ok(());
            }
            let cols: Vec<String> = cells.iter().map(|(f, _)| (*f).clone()).collect();
            let nulls: Vec<bool> = cells.iter().map(|(_, v)| v.is_none()).collect();
            let sql = pg_insert_row_sql(&self.config.table, &cols, &nulls);
            let mut query = sqlx::query(&sql);
            for v in cells.iter().filter_map(|(_, v)| v.as_ref()) {
                query = bind_pg_arg(query, v);
            }
            if let Err(e) = query.execute(&pool).await {
                evict_sql_pool(&self.config.url);
                return Err(e.into());
            }
        } else {
            bail!("Unsupported SQL sink URL scheme: {}", self.config.url);
        }
        Ok(())
    }

    /// Multi-row batch insert: batches up to `sub_batch_size` records per query
    /// within parameter limits for dramatic write throughput gains.
    pub async fn insert_batch(&self, records: &[StreamRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        if !is_valid_sql_identifier(&self.config.table) {
            bail!(
                "Invalid SQL table identifier '{}': SQL sink requires identifiers matching [A-Za-z_][A-Za-z0-9_]*",
                self.config.table
            );
        }
        if records.len() == 1 {
            return self.insert_record(&records[0]).await;
        }
        let fields = if self.config.fields.is_empty() {
            let mut set = std::collections::BTreeSet::new();
            for r in records {
                for k in r
                    .data
                    .keys()
                    .filter(|k| *k != META_KEY && !k.starts_with("__"))
                {
                    if !is_valid_sql_identifier(k) {
                        anyhow::bail!(
                            "Invalid dynamic field name '{}': SQL sink requires identifiers matching [A-Za-z_][A-Za-z0-9_]* when fields is unset",
                            k
                        );
                    }
                    set.insert(k.clone());
                }
            }
            set.into_iter().collect::<Vec<_>>()
        } else {
            for f in &self.config.fields {
                if !is_valid_sql_identifier(f) {
                    bail!(
                        "Invalid field name '{}': SQL sink requires identifiers matching [A-Za-z_][A-Za-z0-9_]*",
                        f
                    );
                }
            }
            self.config.fields.clone()
        };
        if fields.is_empty() {
            for r in records {
                self.insert_record(r).await?;
            }
            return Ok(());
        }

        // Limit sub-batch size so total parameters stay comfortably within database limits
        let sub_batch_size = (900 / fields.len().max(1)).clamp(1, 256);
        if self.config.url.starts_with("sqlite") {
            let pool = sqlite_pool(&self.config.url).await?;
            for chunk in records.chunks(sub_batch_size) {
                let mut sql = format!(
                    "INSERT INTO {} ({}) VALUES ",
                    self.config.table,
                    fields.join(", ")
                );
                let mut all_vals = Vec::new();
                for (row_idx, record) in chunk.iter().enumerate() {
                    if row_idx > 0 {
                        sql.push_str(", ");
                    }
                    sql.push('(');
                    for (col_idx, field) in fields.iter().enumerate() {
                        if col_idx > 0 {
                            sql.push_str(", ");
                        }
                        let val = json_to_bind(record.data.get(field));
                        match val {
                            Some(v) => {
                                sql.push('?');
                                all_vals.push(v);
                            }
                            None => {
                                sql.push_str("NULL");
                            }
                        }
                    }
                    sql.push(')');
                }
                let mut query = sqlx::query(&sql);
                for v in &all_vals {
                    query = bind_sqlite_arg(query, v);
                }
                if let Err(e) = query.execute(&pool).await {
                    evict_sql_pool(&self.config.url);
                    return Err(e.into());
                }
            }
        } else if self.config.url.starts_with("postgres") {
            let pool = pg_pool(&self.config.url).await?;
            for chunk in records.chunks(sub_batch_size) {
                let mut sql = format!(
                    "INSERT INTO {} ({}) VALUES ",
                    self.config.table,
                    fields.join(", ")
                );
                let mut all_vals = Vec::new();
                let mut param_idx = 1u32;
                for (row_idx, record) in chunk.iter().enumerate() {
                    if row_idx > 0 {
                        sql.push_str(", ");
                    }
                    sql.push('(');
                    for (col_idx, field) in fields.iter().enumerate() {
                        if col_idx > 0 {
                            sql.push_str(", ");
                        }
                        let val = json_to_bind(record.data.get(field));
                        match val {
                            Some(v) => {
                                sql.push_str(&format!("${}", param_idx));
                                param_idx += 1;
                                all_vals.push(v);
                            }
                            None => {
                                sql.push_str("NULL");
                            }
                        }
                    }
                    sql.push(')');
                }
                let mut query = sqlx::query(&sql);
                for v in &all_vals {
                    query = bind_pg_arg(query, v);
                }
                if let Err(e) = query.execute(&pool).await {
                    evict_sql_pool(&self.config.url);
                    return Err(e.into());
                }
            }
        } else {
            for r in records {
                self.insert_record(r).await?;
            }
        }
        Ok(())
    }
}

/// `INSERT INTO {table} ({cols}) VALUES (?, ...)` for SQLite.
pub fn sqlite_insert_sql(table: &str, fields: &[String]) -> String {
    let cols = fields.join(", ");
    let placeholders = vec!["?"; fields.len()].join(", ");
    format!("INSERT INTO {} ({}) VALUES ({})", table, cols, placeholders)
}

/// Row-aware variant: null columns render as an untyped SQL `NULL` literal
/// (never a typed parameter), other columns keep `?` placeholders.
pub fn sqlite_insert_row_sql(table: &str, cols: &[String], nulls: &[bool]) -> String {
    let names = cols.join(", ");
    let placeholders = cols
        .iter()
        .enumerate()
        .map(|(i, _)| {
            if nulls.get(i).copied().unwrap_or(false) {
                "NULL".to_string()
            } else {
                "?".to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table, names, placeholders
    )
}

/// `INSERT INTO {table} ({cols}) VALUES ($1, ...)` for PostgreSQL, whose
/// wire protocol numbers placeholders instead of accepting `?`.
pub fn pg_insert_sql(table: &str, fields: &[String]) -> String {
    let cols = fields.join(", ");
    let placeholders = (1..=fields.len())
        .map(|i| format!("${}", i))
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO {} ({}) VALUES ({})", table, cols, placeholders)
}

/// Row-aware variant: null columns render as an untyped SQL `NULL` literal
/// (the server infers the column type), other columns keep numbered `$n`
/// placeholders in bind order.
pub fn pg_insert_row_sql(table: &str, cols: &[String], nulls: &[bool]) -> String {
    let names = cols.join(", ");
    let mut next_param = 1u32;
    let placeholders = cols
        .iter()
        .enumerate()
        .map(|(i, _)| {
            if nulls.get(i).copied().unwrap_or(false) {
                "NULL".to_string()
            } else {
                let token = format!("${}", next_param);
                next_param += 1;
                token
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table, names, placeholders
    )
}

/// `SELECT * ... WHERE {key_col} = $1 LIMIT 1` for PostgreSQL. The
/// stream-side key arrives stringified, so the comparison runs on the text
/// image: `integer = text` has no operator in PostgreSQL, while the
/// `CAST(... AS TEXT)` form works for every column type.
pub fn pg_lookup_sql(table: &str, key_col: &str) -> String {
    format!(
        "SELECT * FROM {} WHERE CAST({} AS TEXT) = $1 LIMIT 1",
        table, key_col
    )
}

/// Decode one SQLite column, preserving JSON types from the column's
/// declared affinity (INTEGER → number, REAL → number, TEXT → string),
/// following https://sqlite.org/datatype3.html affinity rules. Untyped
/// columns fall back to an integer-first cascade; NULL becomes Null.
fn sqlite_column_value(row: &sqlx::sqlite::SqliteRow, col: &str) -> serde_json::Value {
    use sqlx::{Column, Row, TypeInfo};
    let affinity = row
        .columns()
        .iter()
        .find(|c| c.name() == col)
        .map(|c| c.type_info().name().to_string())
        .unwrap_or_default();
    let upper = affinity.to_ascii_uppercase();
    if upper.contains("INT") {
        if let Ok(v) = row.try_get::<i64, _>(col) {
            return serde_json::Value::from(v);
        }
    } else if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
        if let Ok(v) = row.try_get::<String, _>(col) {
            return serde_json::Value::String(v);
        }
    } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        if let Ok(v) = row.try_get::<f64, _>(col) {
            return serde_json::json!(v);
        }
    } else if upper.contains("BOOL") {
        if let Ok(v) = row.try_get::<bool, _>(col) {
            return serde_json::Value::Bool(v);
        }
        if let Ok(v) = row.try_get::<i64, _>(col) {
            return serde_json::Value::from(v);
        }
    }
    if let Ok(v) = row.try_get::<i64, _>(col) {
        return serde_json::Value::from(v);
    }
    if let Ok(v) = row.try_get::<f64, _>(col) {
        return serde_json::json!(v);
    }
    if let Ok(v) = row.try_get::<String, _>(col) {
        return serde_json::Value::String(v);
    }
    serde_json::Value::Null
}

/// Decode one PostgreSQL column, preserving JSON types (numbers stay
/// numbers) so downstream typed comparisons keep working. Falls back to
/// `Null` for exotic types the cascade cannot represent.
fn pg_column_value(row: &sqlx::postgres::PgRow, col: &str) -> serde_json::Value {
    use sqlx::Row;
    if let Ok(v) = row.try_get::<String, _>(col) {
        return serde_json::Value::String(v);
    }
    if let Ok(v) = row.try_get::<i32, _>(col) {
        return serde_json::Value::from(v);
    }
    if let Ok(v) = row.try_get::<i64, _>(col) {
        return serde_json::Value::from(v);
    }
    if let Ok(v) = row.try_get::<f64, _>(col) {
        return serde_json::json!(v);
    }
    if let Ok(v) = row.try_get::<f32, _>(col) {
        return serde_json::json!(f64::from(v));
    }
    if let Ok(v) = row.try_get::<bool, _>(col) {
        return serde_json::Value::Bool(v);
    }
    serde_json::Value::Null
}

/// Point lookup against a SQL database, mapping the row columns to
/// type-preserving values (numbers stay numbers on both backends).
/// Returns `None` when no row matches.
pub async fn sql_lookup_key(
    url: &str,
    table: &str,
    key_col: &str,
    key_val: &str,
) -> Result<Option<serde_json::Value>> {
    if !is_valid_sql_identifier(table) {
        bail!(
            "Invalid SQL table identifier '{}': requires [A-Za-z_][A-Za-z0-9_]*",
            table
        );
    }
    if !is_valid_sql_identifier(key_col) {
        bail!(
            "Invalid SQL key column identifier '{}': requires [A-Za-z_][A-Za-z0-9_]*",
            key_col
        );
    }
    if url.starts_with("sqlite") {
        let pool = sqlite_pool(url).await?;
        let sql = format!("SELECT * FROM {} WHERE {} = ? LIMIT 1", table, key_col);
        let row = match sqlx::query(&sql).bind(key_val).fetch_optional(&pool).await {
            Ok(r) => r,
            Err(e) => {
                evict_sql_pool(url);
                return Err(e.into());
            }
        };
        if let Some(r) = row {
            use sqlx::{Column, Row};
            let mut map = serde_json::Map::new();
            for col in r.columns() {
                let name = col.name();
                map.insert(name.to_string(), sqlite_column_value(&r, name));
            }
            return Ok(Some(serde_json::Value::Object(map)));
        }
    } else if url.starts_with("postgres") {
        let pool = pg_pool(url).await?;
        let sql = pg_lookup_sql(table, key_col);
        let row = match sqlx::query(&sql).bind(key_val).fetch_optional(&pool).await {
            Ok(r) => r,
            Err(e) => {
                evict_sql_pool(url);
                return Err(e.into());
            }
        };
        if let Some(r) = row {
            use sqlx::{Column, Row};
            let mut map = serde_json::Map::new();
            for col in r.columns() {
                let name = col.name();
                map.insert(name.to_string(), pg_column_value(&r, name));
            }
            return Ok(Some(serde_json::Value::Object(map)));
        }
    }
    Ok(None)
}

/// Polling SQL source: every `interval` ms runs the configured query and
/// broadcasts each row as a [`StreamRecord`]. Index columns (`indexFields`,
/// plus the legacy singular pair) advance across polls: each poll renders
/// with the current values and the last row wins per column (with `ORDER BY
/// .. ASC` pagination for internal queries), so subsequent polls emit only
/// newer rows — mirroring upstream incremental polling. Mirrors the
/// `HttpPullSource` ticker/cancellation discipline.
pub struct SqlSource {
    pub config: SqlConnectorConfig,
    pub tx: StreamSender,
    index: Vec<(String, serde_json::Value)>,
}

impl SqlSource {
    pub fn new(config: SqlConnectorConfig, tx: StreamSender) -> Self {
        let index = initial_index_pairs(&config);
        Self { config, tx, index }
    }

    pub fn spawn(
        mut self,
        mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_millis(
                self.config.interval.max(1),
            ));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        match self.poll_once().await {
                            Ok(records) => {
                                for record in records {
                                    // All subscribers dropped: shut down cleanly.
                                    if self.tx.send(record).await.is_err() {
                                        return;
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "SQL source poll of {} failed: {}",
                                    self.config.table,
                                    e
                                );
                            }
                        }
                    }
                    changed = cancel_rx.changed() => {
                        match changed {
                            Ok(_) => {
                                if *cancel_rx.borrow() {
                                    return;
                                }
                            }
                            // Cancellation sender dropped: shut down.
                            Err(_) => return,
                        }
                    }
                }
            }
        })
    }

    /// Poll once with the current index values, then advance tracked
    /// indexes from the fetched rows (last row wins).
    pub async fn poll_once(&mut self) -> Result<Vec<StreamRecord>> {
        validate_sql_source_identifiers(&self.config, &self.index)?;
        let sql = sql_source_query_with(&self.config, &self.index);
        if self.config.url.starts_with("sqlite") {
            let pool = sqlite_pool(&self.config.url).await?;
            let rows = match sqlx::query(&sql).fetch_all(&pool).await {
                Ok(r) => r,
                Err(e) => {
                    evict_sql_pool(&self.config.url);
                    return Err(e.into());
                }
            };
            use sqlx::{Column, Row};
            let mut out = Vec::with_capacity(rows.len());
            for r in &rows {
                let mut map = serde_json::Map::new();
                for col in r.columns() {
                    let name = col.name();
                    map.insert(name.to_string(), sqlite_column_value(r, name));
                }
                out.push(StreamRecord::new(
                    map.into_iter().collect::<IndexMap<_, _>>(),
                ));
            }
            advance_index(&mut self.index, &out);
            return Ok(out);
        }
        if self.config.url.starts_with("postgres") {
            let pool = pg_pool(&self.config.url).await?;
            let rows = match sqlx::query(&sql).fetch_all(&pool).await {
                Ok(r) => r,
                Err(e) => {
                    evict_sql_pool(&self.config.url);
                    return Err(e.into());
                }
            };
            use sqlx::{Column, Row};
            let mut out = Vec::with_capacity(rows.len());
            for r in &rows {
                let mut map = serde_json::Map::new();
                for col in r.columns() {
                    let name = col.name();
                    map.insert(name.to_string(), pg_column_value(r, name));
                }
                out.push(StreamRecord::new(
                    map.into_iter().collect::<IndexMap<_, _>>(),
                ));
            }
            advance_index(&mut self.index, &out);
            return Ok(out);
        }
        bail!("Unsupported SQL source URL scheme: {}", self.config.url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sqlite_pool_registry_reuses_pool() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "rekuiper-sql-pool-test-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let url = format!("sqlite://{}?mode=rwc", path.display());

        let pool1 = sqlite_pool(&url).await.expect("connect pool 1");
        let pool2 = sqlite_pool(&url).await.expect("reuse pool 2");
        assert!(SQLITE_POOLS.read().contains_key(&url));
        assert!(!pool1.is_closed());
        assert!(!pool2.is_closed());

        evict_sql_pool(&url);
        assert!(!SQLITE_POOLS.read().contains_key(&url));
        drop(pool1);
        drop(pool2);
        let _ = std::fs::remove_file(&path);
    }
}
