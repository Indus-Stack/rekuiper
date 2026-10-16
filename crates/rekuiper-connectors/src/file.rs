use crate::{parquet_io, Sink, META_KEY};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use rekuiper_core::model::StreamRecord;
use rekuiper_core::StreamSender;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::io::AsyncBufReadExt;

/// File sink following the eKuiper file sink convention.
///
/// By default appends each [`StreamRecord`] as a single JSON object per line
/// (line-delimited JSON) with a trailing newline. With
/// `format: "delimited"` (or `fileType: "csv"`) it writes CSV rows instead,
/// emitting a sorted-key header row first when `hasHeader` is set.
///
/// Deserializable from eKuiper file action options, e.g.
/// `{"path": "data/result.txt"}` or
/// `{"path": "data/result.csv", "format": "delimited", "hasHeader": true}`.
#[derive(Debug, Clone, Deserialize)]
pub struct FileSink {
    pub path: PathBuf,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default, rename = "fileType")]
    pub file_type: Option<String>,
    #[serde(default, rename = "hasHeader")]
    pub has_header: bool,
    #[serde(default)]
    pub delimiter: Option<String>,
    #[serde(default, rename = "allowExternalFileAccess")]
    pub allow_external_file_access: bool,
}

impl FileSink {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            format: None,
            file_type: None,
            has_header: false,
            delimiter: None,
            allow_external_file_access: false,
        }
    }

    pub fn validate_path(&self) -> Result<()> {
        if !self.allow_external_file_access {
            for component in self.path.components() {
                if matches!(component, std::path::Component::ParentDir) {
                    bail!(
                        "Path traversal disallowed without allowExternalFileAccess: {:?}",
                        self.path
                    );
                }
            }
        }
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn is_delimited(&self) -> bool {
        self.format
            .as_deref()
            .is_some_and(|f| f.eq_ignore_ascii_case("delimited"))
            || self
                .file_type
                .as_deref()
                .is_some_and(|t| t.eq_ignore_ascii_case("csv"))
    }

    fn is_parquet(&self) -> bool {
        self.format
            .as_deref()
            .is_some_and(|f| f.eq_ignore_ascii_case("parquet"))
            || self
                .file_type
                .as_deref()
                .is_some_and(|t| t.eq_ignore_ascii_case("parquet"))
            || self
                .path
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("parquet"))
    }

    fn delimiter_str(&self) -> &str {
        match self.delimiter.as_deref() {
            Some(d) if !d.is_empty() => d,
            _ => ",",
        }
    }
}

#[async_trait]
impl Sink for FileSink {
    async fn send(&self, record: &StreamRecord) -> Result<()> {
        self.validate_path()?;
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .with_context(|| format!("Failed to create parent dirs for {:?}", self.path))?;
            }
        }
        if self.is_parquet() {
            parquet_io::append_record_to_parquet_async(record.clone(), self.path.clone()).await
        } else if self.is_delimited() {
            self.send_delimited(record).await
        } else {
            let mut map = std::collections::BTreeMap::new();
            for (k, v) in &record.data {
                if k == META_KEY || k.starts_with("__") {
                    continue;
                }
                map.insert(k.clone(), v.clone());
            }
            let mut line = serde_json::to_string(&map)?;
            line.push('\n');
            append_text(&self.path, line.as_bytes()).await
        }
    }
}

impl FileSink {
    /// Appends a pre-rendered line (e.g. a `dataTemplate` result).
    pub async fn send_text(&self, text: &str) -> Result<()> {
        self.validate_path()?;
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .with_context(|| format!("Failed to create parent dirs for {:?}", self.path))?;
            }
        }
        let mut line = text.to_string();
        line.push('\n');
        append_text(&self.path, line.as_bytes()).await
    }

    async fn send_delimited(&self, record: &StreamRecord) -> Result<()> {
        let delim = self.delimiter_str();
        let mut keys: Vec<String> = record
            .data
            .keys()
            .filter(|k| *k != META_KEY && !k.starts_with("__"))
            .cloned()
            .collect();
        keys.sort();
        let mut out = String::new();
        if self.has_header && is_missing_or_empty(&self.path).await {
            out.push_str(&keys.join(delim));
            out.push('\n');
        }
        let row: String = keys
            .iter()
            .map(|k| csv_field(record.data.get(k), delim))
            .collect::<Vec<_>>()
            .join(delim);
        out.push_str(&row);
        out.push('\n');
        append_text(&self.path, out.as_bytes()).await
    }
}

async fn is_missing_or_empty(path: &Path) -> bool {
    match tokio::fs::metadata(path).await {
        Err(_) => true,
        Ok(meta) => meta.len() == 0,
    }
}

/// Render one CSV cell: missing/`Null` as empty, strings CSV-escaped,
/// numbers/booleans plain, containers as compact JSON.
fn csv_field(value: Option<&Value>, delim: &str) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => csv_escape(s, delim),
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::Bool(b)) => b.to_string(),
        Some(other) => serde_json::to_string(other).unwrap_or_default(),
    }
}

/// Minimal RFC-4180 escaping: quote the cell when it contains the delimiter,
/// a quote, or a line break, doubling embedded quotes.
fn csv_escape(s: &str, delim: &str) -> String {
    if s.contains(delim) || s.contains(['"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

async fn append_text(path: &Path, bytes: &[u8]) -> Result<()> {
    use tokio::fs::OpenOptions;
    use tokio::io::AsyncWriteExt;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .with_context(|| format!("Failed to open file {:?}", path))?;
    file.write_all(bytes)
        .await
        .with_context(|| format!("Failed to write to file {:?}", path))?;
    file.flush().await?;
    Ok(())
}

/// File source following the eKuiper file source convention.
///
/// Streams line-delimited records from a file into [`StreamRecord`]s,
/// decoding each line per the configured [`FileSourceConfig`] format.
pub struct FileSource {
    pub config: FileSourceConfig,
    pub tx: StreamSender,
}

impl FileSource {
    pub fn new(config: FileSourceConfig, tx: StreamSender) -> Self {
        Self { config, tx }
    }

    pub fn path(&self) -> &Path {
        Path::new(&self.config.path)
    }

    /// Read all records from the file without sending them anywhere.
    pub async fn read_records(&self) -> Result<Vec<StreamRecord>> {
        if self.config.format.eq_ignore_ascii_case("parquet")
            || Path::new(&self.config.path)
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("parquet"))
        {
            return parquet_io::read_parquet_file_async(PathBuf::from(&self.config.path)).await;
        }
        let content = tokio::fs::read_to_string(&self.config.path)
            .await
            .with_context(|| format!("Failed to read file {:?}", self.config.path))?;
        parse_ldjson(&content)
    }

    /// Read records from the file and push each one to `sender`.
    ///
    /// Returns the number of records pushed.
    pub async fn run(&self, sender: &tokio::sync::mpsc::Sender<StreamRecord>) -> Result<usize> {
        let records = self.read_records().await?;
        let n = records.len();
        for record in records {
            sender
                .send(record)
                .await
                .map_err(|e| anyhow::anyhow!("FileSource receiver dropped: {}", e))?;
        }
        Ok(n)
    }
}

fn parse_ldjson(content: &str) -> Result<Vec<StreamRecord>> {
    let mut records = Vec::new();
    for (idx, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(line)
            .with_context(|| format!("Invalid JSON on line {} of file source", idx + 1))?;
        let obj = value.as_object().cloned().unwrap_or_else(|| {
            let mut m = serde_json::Map::new();
            m.insert("value".to_string(), value);
            m
        });
        let data: HashMap<String, Value> = obj.into_iter().collect();
        records.push(StreamRecord::new(data));
    }
    Ok(records)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileSourceConfig {
    /// File path or directory path to read (from DATASOURCE or config).
    pub path: String,
    /// Format / file type: "json" (default), "lines", "delimited", or "csv".
    #[serde(default = "default_file_source_format")]
    pub format: String,
    /// Whether the first row is a header for delimited/csv format.
    #[serde(default)]
    pub has_header: bool,
    /// Delimiter character for CSV/TSV (default ',').
    #[serde(default, deserialize_with = "deserialize_delimiter")]
    pub delimiter: Option<char>,
    /// Optional pacing interval between emitted records in milliseconds
    /// (defaults to 0 for immediate replay).
    #[serde(default)]
    pub interval: u64,
}

fn default_file_source_format() -> String {
    "json".to_string()
}

/// Accepts a single delimiter character (`","`, `"\t"`, `"|"`) as well as
/// friendly names (`"comma"`, `"tab"`, `"pipe"`, `"semicolon"`).
fn deserialize_delimiter<'de, D>(deserializer: D) -> Result<Option<char>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    Ok(opt.map(|s| crate::codec::DelimitedCodec::delimiter_from_name(&s)))
}

/// Streaming reader: [`FileSource::spawn`] tails the file, decoding each
/// line per format — JSON lines as objects (arrays fan out per element),
/// delimited/CSV rows mapped positionally onto headers (using the first row
/// when `has_header` is set) — until cancelled.
impl FileSource {
    pub fn spawn(
        self,
        mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let is_parquet = self.config.format.eq_ignore_ascii_case("parquet")
                || Path::new(&self.config.path)
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("parquet"));

            if is_parquet {
                match parquet_io::read_parquet_file_async(PathBuf::from(&self.config.path)).await {
                    Ok(records) => {
                        for record in records {
                            if self.tx.send(record).await.is_err() {
                                return;
                            }
                            if self.config.interval > 0 {
                                tokio::select! {
                                    _ = tokio::time::sleep(std::time::Duration::from_millis(
                                        self.config.interval,
                                    )) => {}
                                    exit = Self::is_cancelled(&mut cancel_rx) => {
                                        if exit {
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            "File source cannot open/parse Parquet file {}: {}",
                            self.config.path,
                            e
                        );
                    }
                }
                return;
            }

            let file = match tokio::fs::File::open(&self.config.path).await {
                Ok(file) => file,
                Err(e) => {
                    tracing::warn!("File source cannot open {}: {}", self.config.path, e);
                    return;
                }
            };
            let mut lines = tokio::io::BufReader::new(file).lines();
            let delimiter = self.config.delimiter.unwrap_or(',');
            let mut headers: Option<Vec<String>> = None;
            loop {
                tokio::select! {
                    line_res = lines.next_line() => {
                        match line_res {
                            Ok(Some(line)) => {
                                let line = line.trim();
                                if line.is_empty() {
                                    continue;
                                }
                                if let Some(records) =
                                    Self::decode_line(&self.config, &mut headers, delimiter, line)
                                {
                                    for record in records {
                                        // All subscribers dropped: shut down cleanly.
                                        if self.tx.send(record).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                                if self.config.interval > 0 {
                                    tokio::select! {
                                        _ = tokio::time::sleep(std::time::Duration::from_millis(
                                            self.config.interval,
                                        )) => {}
                                        exit = Self::is_cancelled(&mut cancel_rx) => {
                                            if exit {
                                                return;
                                            }
                                        }
                                    }
                                }
                            }
                            Ok(None) => {
                                // EOF: pause before re-polling (tail -f) while
                                // staying responsive to cancellation.
                                tokio::select! {
                                    _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {}
                                    exit = Self::is_cancelled(&mut cancel_rx) => {
                                        if exit {
                                            return;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "File source read error on {}: {}",
                                    self.config.path,
                                    e
                                );
                                break;
                            }
                        }
                    }
                    exit = Self::is_cancelled(&mut cancel_rx) => {
                        if exit {
                            return;
                        }
                    }
                }
            }
        })
    }

    async fn is_cancelled(cancel_rx: &mut tokio::sync::watch::Receiver<bool>) -> bool {
        match cancel_rx.changed().await {
            Ok(_) => *cancel_rx.borrow(),
            // Cancellation sender dropped: shut down.
            Err(_) => true,
        }
    }

    /// Decode one non-empty line according to the configured format.
    /// Returns `None` for header rows (consumed, not emitted).
    fn decode_line(
        config: &FileSourceConfig,
        headers: &mut Option<Vec<String>>,
        delimiter: char,
        line: &str,
    ) -> Option<Vec<StreamRecord>> {
        if config.format.eq_ignore_ascii_case("delimited")
            || config.format.eq_ignore_ascii_case("csv")
        {
            if config.has_header && headers.is_none() {
                *headers = Some(
                    line.split(delimiter)
                        .map(|h| h.trim().trim_matches('"').to_string())
                        .collect(),
                );
                return None;
            }
            let headers: Vec<String> = match headers {
                Some(h) => h.clone(),
                None => {
                    tracing::warn!("Delimited file source has no headers; skipping line");
                    return Some(Vec::new());
                }
            };
            let data = parse_delimited_line(line, delimiter, &headers);
            return Some(vec![StreamRecord::new(data)]);
        }
        // JSON / lines: objects emit directly, arrays fan out per element.
        match serde_json::from_str::<Value>(line) {
            Ok(Value::Object(map)) => Some(vec![StreamRecord::new(map.into_iter().collect())]),
            Ok(Value::Array(items)) => {
                let mut records = Vec::new();
                for item in items {
                    if let Value::Object(map) = item {
                        records.push(StreamRecord::new(map.into_iter().collect()));
                    } else {
                        tracing::warn!("Skipping non-object element in file source line");
                    }
                }
                Some(records)
            }
            Ok(_) => {
                tracing::warn!("Skipping non-object JSON line in file source");
                Some(Vec::new())
            }
            Err(e) => {
                tracing::warn!("Skipping invalid JSON line in file source: {}", e);
                Some(Vec::new())
            }
        }
    }
}

/// Delimited-format parser (eKuiper delimited serialization):
/// splits `line` on `delimiter`, trims each token and maps it to the
/// corresponding header, converting numeric tokens to [`Value::Number`].
pub fn parse_delimited_line(
    line: &str,
    delimiter: char,
    headers: &[String],
) -> HashMap<String, Value> {
    let mut map = HashMap::with_capacity(headers.len());
    for (i, header) in headers.iter().enumerate() {
        let token = line.split(delimiter).nth(i).unwrap_or("").trim();
        map.insert(header.clone(), parse_delimited_token(token));
    }
    map
}

fn parse_delimited_token(token: &str) -> Value {
    if let Ok(i) = token.parse::<i64>() {
        return Value::from(i);
    }
    if let Ok(f) = token.parse::<f64>() {
        if let Some(n) = serde_json::Number::from_f64(f) {
            return Value::Number(n);
        }
    }
    Value::String(token.to_string())
}
