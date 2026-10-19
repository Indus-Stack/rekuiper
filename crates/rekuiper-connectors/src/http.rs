use crate::{Sink, META_KEY};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use indexmap::IndexMap;
use rekuiper_core::model::StreamRecord;
use rekuiper_core::StreamSender;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

pub struct HttpSink {
    pub url: String,
    client: reqwest::Client,
}

impl HttpSink {
    pub fn new(url: String) -> Self {
        Self {
            url,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl Sink for HttpSink {
    async fn send(&self, record: &StreamRecord) -> Result<()> {
        let mut map = std::collections::BTreeMap::new();
        for (k, v) in &record.data {
            if k == META_KEY || k.starts_with("__") {
                continue;
            }
            map.insert(k.clone(), v.clone());
        }
        self.client
            .post(&self.url)
            .json(&map)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}

/// Configuration for [`HttpPullSource`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpPullConfig {
    /// Target URL to poll (or stream `DATASOURCE` when no conf key matches).
    pub url: String,
    /// HTTP method, `"get"` (default) or `"post"`.
    #[serde(default = "default_http_method")]
    pub method: String,
    /// Poll interval in milliseconds.
    #[serde(default = "default_http_interval")]
    pub interval: u64,
    /// Extra request headers.
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Optional POST body.
    #[serde(default)]
    pub body: Option<String>,
}

fn default_http_method() -> String {
    "get".to_string()
}

fn default_http_interval() -> u64 {
    1000
}

/// HTTP pull source: periodically GETs (or POSTs) a JSON endpoint and
/// broadcasts each response object as a [`StreamRecord`]. A JSON array
/// response yields one record per object element.
pub struct HttpPullSource {
    pub config: HttpPullConfig,
    pub tx: StreamSender,
}

impl HttpPullSource {
    pub fn spawn(
        self,
        mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let mut ticker = tokio::time::interval(std::time::Duration::from_millis(
                self.config.interval.max(1),
            ));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        match self.fetch_once(&client).await {
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
                                    "HTTP pull source error for {}: {}",
                                    self.config.url,
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

    async fn fetch_once(&self, client: &reqwest::Client) -> Result<Vec<StreamRecord>> {
        let mut req = if self.config.method.eq_ignore_ascii_case("post") {
            let mut req = client.post(&self.config.url);
            if let Some(body) = &self.config.body {
                req = req.body(body.clone());
            }
            req
        } else {
            client.get(&self.config.url)
        };
        for (name, value) in &self.config.headers {
            req = req.header(name, value);
        }
        let resp = req
            .send()
            .await
            .with_context(|| format!("HTTP pull request to {} failed", self.config.url))?;
        let resp = resp
            .error_for_status()
            .with_context(|| format!("HTTP pull request to {} failed", self.config.url))?;
        let payload: Value = resp
            .json()
            .await
            .with_context(|| format!("HTTP pull response from {} is not JSON", self.config.url))?;
        match payload {
            Value::Object(map) => Ok(vec![StreamRecord::new(
                map.into_iter().collect::<IndexMap<_, _>>(),
            )]),
            Value::Array(items) => {
                let mut records = Vec::new();
                for item in items {
                    if let Value::Object(map) = item {
                        records.push(StreamRecord::new(
                            map.into_iter().collect::<IndexMap<_, _>>(),
                        ));
                    } else {
                        tracing::warn!(
                            "Skipping non-object element in HTTP pull response from {}",
                            self.config.url
                        );
                    }
                }
                Ok(records)
            }
            _ => bail!(
                "HTTP pull response from {} must be a JSON object or array",
                self.config.url
            ),
        }
    }
}
