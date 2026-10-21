use axum::{http, Router};
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use futures::{Sink, Stream};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use tower::ServiceExt;

use crate::declarative::rollback_to_known_good;
use crate::state::AppState;

/// Binary frame adapter wrapping a WebSocketStream into `futures::io::AsyncRead + AsyncWrite`
/// for Yamux multiplexing.
pub struct WsStreamAdapter<S> {
    ws: S,
    read_buf: Vec<u8>,
    read_pos: usize,
}

impl<S> WsStreamAdapter<S> {
    pub fn new(ws: S) -> Self {
        Self {
            ws,
            read_buf: Vec::new(),
            read_pos: 0,
        }
    }
}

impl<S> AsyncRead for WsStreamAdapter<S>
where
    S: Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = &mut *self;
        if this.read_pos < this.read_buf.len() {
            let available = this.read_buf.len() - this.read_pos;
            let to_copy = available.min(buf.len());
            buf[..to_copy].copy_from_slice(&this.read_buf[this.read_pos..this.read_pos + to_copy]);
            this.read_pos += to_copy;
            if this.read_pos >= this.read_buf.len() {
                this.read_buf.clear();
                this.read_pos = 0;
            }
            return Poll::Ready(Ok(to_copy));
        }

        loop {
            match Pin::new(&mut this.ws).poll_next(cx) {
                Poll::Ready(Some(Ok(Message::Binary(bin)))) => {
                    if bin.is_empty() {
                        continue;
                    }
                    let to_copy = bin.len().min(buf.len());
                    buf[..to_copy].copy_from_slice(&bin[..to_copy]);
                    if to_copy < bin.len() {
                        this.read_buf = bin[to_copy..].to_vec();
                        this.read_pos = 0;
                    }
                    return Poll::Ready(Ok(to_copy));
                }
                Poll::Ready(Some(Ok(Message::Close(_)))) | Poll::Ready(None) => {
                    return Poll::Ready(Ok(0));
                }
                Poll::Ready(Some(Ok(_))) => {
                    // Ignore text/ping/pong frames
                    continue;
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        e,
                    )));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<S> AsyncWrite for WsStreamAdapter<S>
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = &mut *self;
        match Pin::new(&mut this.ws).poll_ready(cx) {
            Poll::Ready(Ok(())) => {
                let msg = Message::Binary(buf.to_vec());
                match Pin::new(&mut this.ws).start_send(msg) {
                    Ok(()) => Poll::Ready(Ok(buf.len())),
                    Err(e) => Poll::Ready(Err(std::io::Error::other(e))),
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(std::io::Error::other(e))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.ws)
            .poll_flush(cx)
            .map_err(std::io::Error::other)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.ws)
            .poll_close(cx)
            .map_err(std::io::Error::other)
    }
}

/// Compatibility adapter for any `tokio::io::AsyncRead + AsyncWrite` into `futures::io::AsyncRead + AsyncWrite`.
pub struct TokioIoCompat<T>(pub T);

impl<T> TokioIoCompat<T> {
    pub fn new(inner: T) -> Self {
        Self(inner)
    }
}

impl<T: tokio::io::AsyncRead + Unpin> AsyncRead for TokioIoCompat<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let mut read_buf = tokio::io::ReadBuf::new(buf);
        match Pin::new(&mut self.0).poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(read_buf.filled().len())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: tokio::io::AsyncWrite + Unpin> AsyncWrite for TokioIoCompat<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

/// Stream 0 periodic heartbeat envelope
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentHeartbeat {
    pub agent_id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub plant: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub device_type: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    pub cpu_usage: f32,
    pub memory_bytes: u64,
    pub rules_count: usize,
    pub running_rules: usize,
    #[serde(default)]
    pub dlq_count: u64,
    #[serde(default)]
    pub snapshot_id: Option<String>,
    pub snapshot_hash: u64,
    pub timestamp: i64,
    #[serde(default)]
    pub last_rollback_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AgentMetadata {
    pub device_id: String,
    pub name: String,
    pub plant: String,
    pub region: String,
    pub device_type: String,
    pub version: String,
}

impl From<String> for AgentMetadata {
    fn from(s: String) -> Self {
        Self {
            device_id: s.clone(),
            name: s,
            plant: "Plant-Austin".to_string(),
            region: "US-South".to_string(),
            device_type: "IPC-Edge".to_string(),
            version: "2.0.0".to_string(),
        }
    }
}

/// Control plane commands received over Stream 0
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "command")]
pub enum AgentControlCommand {
    ApplySnapshot { manifest: Value },
    Rollback,
    Ping,
}

/// Helper to write length-prefixed bytes
pub async fn send_frame<W: AsyncWrite + Unpin>(writer: &mut W, data: &[u8]) -> std::io::Result<()> {
    let len = data.len() as u32;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(data).await?;
    writer.flush().await?;
    Ok(())
}

/// Helper to read length-prefixed bytes
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<Vec<u8>> {
    let mut len_bytes = [0u8; 4];
    reader.read_exact(&mut len_bytes).await?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Parses raw HTTP/1.1 request bytes into an `axum::http::Request<axum::body::Body>`.
pub fn parse_http_request(bytes: &[u8]) -> Result<http::Request<axum::body::Body>, String> {
    let header_end = bytes
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| "Missing header separator \r\n\r\n".to_string())?;

    let header_str = std::str::from_utf8(&bytes[..header_end])
        .map_err(|e| format!("Invalid UTF-8 in headers: {}", e))?;
    let mut lines = header_str.lines();
    let request_line = lines.next().ok_or("Empty request line")?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("GET");
    let uri = parts.next().unwrap_or("/");

    let mut builder = http::Request::builder().method(method).uri(uri);
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            builder = builder.header(k.trim(), v.trim());
        }
    }

    let body_bytes = &bytes[header_end + 4..];
    let body = axum::body::Body::from(body_bytes.to_vec());
    builder.body(body).map_err(|e| e.to_string())
}

/// Serializes an `http::Response<axum::body::Body>` into HTTP/1.1 wire bytes and flushes to stream.
pub async fn write_http_response<W: AsyncWrite + Unpin>(
    writer: &mut W,
    response: http::Response<axum::body::Body>,
) -> std::io::Result<()> {
    let status = response.status();
    let headers = response.headers().clone();
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))?;

    let mut out = format!(
        "HTTP/1.1 {} {}\r\n",
        status.as_u16(),
        status.canonical_reason().unwrap_or("OK")
    );
    for (k, v) in &headers {
        if let Ok(v_str) = v.to_str() {
            out.push_str(&format!("{}: {}\r\n", k.as_str(), v_str));
        }
    }
    if !headers.contains_key("content-length") {
        out.push_str(&format!("content-length: {}\r\n", body_bytes.len()));
    }
    out.push_str("\r\n");

    writer.write_all(out.as_bytes()).await?;
    writer.write_all(&body_bytes).await?;
    writer.flush().await?;
    Ok(())
}

/// Runs the outbound Yamux multiplexed agent over any underlying transport `T`.
/// Stream 0 is reserved for periodic 15s heartbeats & control commands.
/// Streams 1..N tunnel incoming HTTP requests directly into `axum::Router::oneshot`.
pub async fn run_yamux_agent<T, M>(
    io: T,
    mode: yamux::Mode,
    state: AppState,
    router: Router,
    meta: M,
    heartbeat_interval: Duration,
    mut cancel_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    M: Into<AgentMetadata>,
{
    let meta: AgentMetadata = meta.into();
    let mut cfg = yamux::Config::default();
    cfg.set_read_after_close(true);
    let mut conn = yamux::Connection::new(io, cfg, mode);

    let is_client = mode == yamux::Mode::Client;

    if is_client {
        // Stream 0: Heartbeat & Control Plane
        let mut ctrl_stream = futures::future::poll_fn(|cx| conn.poll_new_outbound(cx))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to open Yamux Stream 0: {}", e))?;

        let state_clone = state.clone();
        let meta_clone = meta.clone();
        let mut cancel_clone = cancel_rx.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(heartbeat_interval);
            ticker.tick().await;

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let total_rules = state_clone.rule_manager.list_rules().len();
                        let running_rules = state_clone.rule_manager.list_rules().into_iter()
                            .filter(|r| state_clone.rule_manager.get_rule_status(&r.id).map(|s| s.status == "running").unwrap_or(false))
                            .count();
                        let active_hash = state_clone.active_rule_hashes.read().values().copied().sum::<u64>();
                        let rollback = state_clone.last_watchdog_rollback.read().clone();
                        let dlq_total = crate::declarative::get_total_dlq_errors(&state_clone);
                        let has_crashes = crate::declarative::check_rule_panics(&state_clone).is_some();

                        let status = if rollback.is_some() || has_crashes {
                            "degraded".to_string()
                        } else {
                            "online".to_string()
                        };

                        let hb = AgentHeartbeat {
                            agent_id: meta_clone.device_id.clone(),
                            name: Some(meta_clone.name.clone()),
                            plant: Some(meta_clone.plant.clone()),
                            region: Some(meta_clone.region.clone()),
                            device_type: Some(meta_clone.device_type.clone()),
                            version: Some(meta_clone.version.clone()),
                            status: Some(status),
                            cpu_usage: 4.2,
                            memory_bytes: 32 * 1024 * 1024,
                            rules_count: total_rules,
                            running_rules,
                            dlq_count: dlq_total,
                            snapshot_id: None,
                            snapshot_hash: active_hash,
                            timestamp: chrono::Utc::now().timestamp(),
                            last_rollback_reason: rollback,
                        };
                        let payload = serde_json::to_vec(&hb).unwrap_or_default();
                        if let Err(e) = send_frame(&mut ctrl_stream, &payload).await {
                            tracing::warn!("Stream 0 heartbeat send failed: {}", e);
                            break;
                        }
                    }
                    cmd_res = read_frame(&mut ctrl_stream) => {
                        match cmd_res {
                            Ok(cmd_bytes) => {
                                if let Ok(cmd) = serde_json::from_slice::<AgentControlCommand>(&cmd_bytes) {
                                    match cmd {
                                        AgentControlCommand::ApplySnapshot { manifest } => {
                                            match serde_json::from_value::<rekuiper_core::model::DeclarativeManifest>(manifest) {
                                                Ok(m) => {
                                                    let res = crate::declarative::reconcile_manifest_with_watchdog(&state_clone, &m, Duration::from_millis(600), 5).await;
                                                    let resp = match res {
                                                        Ok(rep) => serde_json::json!({"status": "ok", "report": rep}),
                                                        Err(err) => serde_json::json!({"status": "error", "message": err}),
                                                    };
                                                    let _ = send_frame(&mut ctrl_stream, &serde_json::to_vec(&resp).unwrap_or_default()).await;
                                                }
                                                Err(e) => {
                                                    let resp = serde_json::json!({"status": "error", "message": format!("Invalid declarative manifest: {}", e)});
                                                    let _ = send_frame(&mut ctrl_stream, &serde_json::to_vec(&resp).unwrap_or_default()).await;
                                                }
                                            }
                                        }
                                        AgentControlCommand::Rollback => {
                                            let res = rollback_to_known_good(&state_clone).await;
                                            let resp = match res {
                                                Ok(rep) => serde_json::json!({"status": "ok", "report": rep}),
                                                Err(err) => serde_json::json!({"status": "error", "message": err}),
                                            };
                                            let _ = send_frame(&mut ctrl_stream, &serde_json::to_vec(&resp).unwrap_or_default()).await;
                                        }
                                        AgentControlCommand::Ping => {
                                            let _ = send_frame(&mut ctrl_stream, b"{\"status\":\"pong\"}").await;
                                        }
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    _ = cancel_clone.changed() => {
                        if *cancel_clone.borrow() {
                            break;
                        }
                    }
                }
            }
        });
    }

    // Streams 1..N: Accept proxied requests and tunnel into local router via oneshot
    loop {
        tokio::select! {
            accept_res = futures::future::poll_fn(|cx| conn.poll_next_inbound(cx)) => {
                match accept_res {
                    Some(Ok(mut stream)) => {
                        let router_clone = router.clone();
                        tokio::spawn(async move {
                            let mut req_buf = vec![0u8; 16384];
                            if let Ok(n) = stream.read(&mut req_buf).await {
                                if n > 0 {
                                    req_buf.truncate(n);
                                    if let Ok(req) = parse_http_request(&req_buf) {
                                        let Ok(resp) = router_clone.oneshot(req).await;
                                        let _ = write_http_response(&mut stream, resp).await;
                                    }
                                }
                            }
                        });
                    }
                    Some(Err(e)) => {
                        tracing::debug!("Yamux accept stream error: {}", e);
                        break;
                    }
                    None => break,
                }
            }
            _ = cancel_rx.changed() => {
                if *cancel_rx.borrow() {
                    break;
                }
            }
        }
    }

    Ok(())
}

/// Connects outbound to a remote WebSocket server, wraps with WsStreamAdapter,
/// and executes the Yamux agent tunnel.
pub async fn connect_outbound_ws_agent<M>(
    ws_url: &str,
    state: AppState,
    router: Router,
    meta: M,
    heartbeat_interval: Duration,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()>
where
    M: Into<AgentMetadata>,
{
    let token = state.config.read().basic.fleet_token.clone();
    let target_url = if let Some(ref t) = token {
        if !ws_url.contains("token=") {
            if ws_url.contains('?') {
                format!("{}&token={}", ws_url, t)
            } else {
                format!("{}?token={}", ws_url, t)
            }
        } else {
            ws_url.to_string()
        }
    } else {
        ws_url.to_string()
    };
    let (ws_stream, _) = tokio_tungstenite::connect_async(&target_url).await?;
    let adapter = WsStreamAdapter::new(ws_stream);
    run_yamux_agent(
        adapter,
        yamux::Mode::Client,
        state,
        router,
        meta,
        heartbeat_interval,
        cancel_rx,
    )
    .await
}

/// Spawns an agent background task with exponential backoff reconnect loop (1s -> 2s -> 4s -> max 30s)
pub fn spawn_agent_reconnect_loop<M>(
    ws_url: String,
    state: AppState,
    router: Router,
    meta: M,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()>
where
    M: Into<AgentMetadata> + Send + 'static,
{
    let meta: AgentMetadata = meta.into();
    tokio::spawn(async move {
        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(30);

        while !*cancel_rx.borrow() {
            tracing::info!(
                "Connecting outbound edge agent to control plane at {}",
                ws_url
            );
            let conn_res = connect_outbound_ws_agent(
                &ws_url,
                state.clone(),
                router.clone(),
                meta.clone(),
                Duration::from_secs(15),
                cancel_rx.clone(),
            )
            .await;

            if *cancel_rx.borrow() {
                break;
            }

            match conn_res {
                Ok(_) => {
                    tracing::warn!(
                        "Agent Yamux connection ended gracefully. Reconnecting in {:?}",
                        backoff
                    );
                    backoff = Duration::from_secs(1);
                }
                Err(e) => {
                    tracing::warn!(
                        "Agent Yamux connection error: {}. Retrying in {:?}",
                        e,
                        backoff
                    );
                }
            }

            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(max_backoff);
        }
    })
}
