use crate::{
    codec::{DelimitedCodec, EdgeXCodec, ProtoMessage, ProtobufCodec},
    secrets::SecretResolver,
    Sink, META_KEY,
};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use indexmap::IndexMap;
use rekuiper_core::model::{RecordMeta, StreamRecord};
use rekuiper_core::{RuleCounters, StreamSender};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// MQTT connection settings (eKuiper MQTT source/sink action format).
///
/// `server` and `topic` both default: source `CONF_KEY` entries typically
/// carry only connection parameters (no topic), and must still deserialize
/// so the stored broker URL is honored instead of silently falling back.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MqttConfig {
    /// Broker URL, e.g. `tcp://127.0.0.1:1883`.
    #[serde(default = "default_mqtt_server")]
    pub server: String,
    /// Topic to publish to / subscribe to.
    #[serde(default)]
    pub topic: String,
    /// Client id (`clientid` in eKuiper source configs, `clientId` in
    /// action JSON). Generated when absent.
    #[serde(default, alias = "clientid", alias = "clientId")]
    pub client_id: Option<String>,
    /// MQTT QoS level (0, 1 or 2). Defaults to 0.
    #[serde(default)]
    pub qos: u8,
    /// Optional username.
    #[serde(default)]
    pub username: Option<String>,
    /// Optional password.
    #[serde(default)]
    pub password: Option<String>,
    /// eKuiper `protocolVersion` (`3.1` / `3.1.1`). Both are served by the
    /// MQTT 3.1.1 wire protocol, which brokers accept for 3.1 clients.
    #[serde(default, alias = "protocolVersion")]
    pub protocol_version: Option<String>,
    /// Clean-session flag (default `true`). `false` with a fixed client id
    /// lets the broker queue QoS 1/2 messages while a vehicle is offline.
    #[serde(default, alias = "cleanSession")]
    pub clean_session: Option<bool>,
    /// Keep-alive interval in seconds (default 30).
    #[serde(default, alias = "keepAlive")]
    pub keep_alive: Option<u64>,
    /// Root CA certificate file path.
    #[serde(default, alias = "rootCaPath")]
    pub root_ca_path: Option<String>,
    /// Root CA certificate PEM content (raw string or base64).
    #[serde(default, alias = "rootCaRaw", alias = "root_ca_raw")]
    pub root_ca_raw: Option<String>,
    /// Client certificate file path for mutual TLS (mTLS).
    #[serde(default, alias = "certificationPath", alias = "certPath")]
    pub certification_path: Option<String>,
    /// Client certificate PEM content (raw string or base64).
    #[serde(
        default,
        alias = "certificationRaw",
        alias = "certficationRaw",
        alias = "certRaw"
    )]
    pub certification_raw: Option<String>,
    /// Client private key file path for mutual TLS (mTLS).
    #[serde(default, alias = "privateKeyPath", alias = "keyPath")]
    pub private_key_path: Option<String>,
    /// Client private key PEM content (raw string or base64).
    #[serde(default, alias = "privateKeyRaw", alias = "keyRaw")]
    pub private_key_raw: Option<String>,
    /// Skip server certificate and hostname verification (insecure, for testing).
    #[serde(default, alias = "insecureSkipVerify")]
    pub insecure_skip_verify: bool,
    /// Source payload decoding (stream `FORMAT`); set by the stream resolver.
    #[serde(skip)]
    pub format: PayloadFormat,
    /// Attach `__meta__` (topic, qos, messageId) to source rows; set when a
    /// rule calls `meta()`/`mqtt()`, so other rules skip the allocation.
    #[serde(skip)]
    pub attach_meta: bool,
    /// Schema-guided field mask to drop unreferenced JSON fields at deserialize time.
    #[serde(skip)]
    pub field_mask: Option<crate::codec::JsonFieldMask>,
}

impl std::fmt::Debug for MqttConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttConfig")
            .field("server", &self.server)
            .field("topic", &self.topic)
            .field("client_id", &self.client_id)
            .field("qos", &self.qos)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "******"))
            .field("protocol_version", &self.protocol_version)
            .field("clean_session", &self.clean_session)
            .field("keep_alive", &self.keep_alive)
            .field("root_ca_path", &self.root_ca_path)
            .field(
                "root_ca_raw",
                &self.root_ca_raw.as_ref().map(|_| "<redacted>"),
            )
            .field("certification_path", &self.certification_path)
            .field(
                "certification_raw",
                &self.certification_raw.as_ref().map(|_| "<redacted>"),
            )
            .field("private_key_path", &self.private_key_path)
            .field(
                "private_key_raw",
                &self.private_key_raw.as_ref().map(|_| "******"),
            )
            .field("insecure_skip_verify", &self.insecure_skip_verify)
            .field("format", &self.format)
            .field("attach_meta", &self.attach_meta)
            .field("field_mask", &self.field_mask)
            .finish()
    }
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            server: default_mqtt_server(),
            topic: String::new(),
            client_id: None,
            qos: 0,
            username: None,
            password: None,
            protocol_version: None,
            clean_session: None,
            keep_alive: None,
            root_ca_path: None,
            root_ca_raw: None,
            certification_path: None,
            certification_raw: None,
            private_key_path: None,
            private_key_raw: None,
            insecure_skip_verify: false,
            format: PayloadFormat::Json,
            attach_meta: false,
            field_mask: None,
        }
    }
}

/// Largest MQTT packet accepted or sent (rumqttc defaults to 10 KiB, which
/// would drop larger vehicle frames by disconnecting).
const MQTT_MAX_PACKET_BYTES: usize = 16 * 1024 * 1024;

/// How an MQTT stream decodes payloads (stream `FORMAT`, eKuiper names).
#[derive(Debug, Clone, PartialEq, Default)]
pub enum PayloadFormat {
    /// JSON object (one row) or array of objects (one row each).
    #[default]
    Json,
    /// Whole payload in the single field `self` (eKuiper BINARY streams):
    /// UTF-8 payloads (ESPHome states such as `23.5` or `ON`) as text,
    /// other bytes as base64.
    Binary,
    /// Delimited text mapped onto the stream columns (or `col0`, `col1`, …).
    Delimited(DelimitedCodec),
    /// Protobuf message resolved from the stream `SCHEMAID`.
    Protobuf(Arc<ProtoMessage>),
    /// EdgeX Foundry event / reading DTO payload format.
    EdgeX,
}

fn default_mqtt_server() -> String {
    "tcp://127.0.0.1:1883".to_string()
}

impl MqttConfig {
    pub fn qos_level(&self) -> QoS {
        qos_from_u8(self.qos)
    }

    /// Subscription topics: `DATASOURCE` may list several, comma separated.
    pub fn topics(&self) -> Vec<String> {
        self.topic
            .split(',')
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .map(str::to_string)
            .collect()
    }

    pub fn effective_client_id(&self) -> String {
        if let Some(id) = &self.client_id {
            if !id.is_empty() {
                return id.clone();
            }
        }
        generate_client_id()
    }

    /// Dynamically resolve any secrets (e.g. `vault://` or `env://`) in password, username, or key.
    pub async fn resolve_secrets(&mut self, resolver: &SecretResolver) -> Result<()> {
        if let Some(pwd) = &self.password {
            self.password = Some(resolver.resolve(pwd).await?);
        }
        if let Some(user) = &self.username {
            self.username = Some(resolver.resolve(user).await?);
        }
        if let Some(key_raw) = &self.private_key_raw {
            self.private_key_raw = Some(resolver.resolve(key_raw).await?);
        }
        Ok(())
    }
}

fn qos_from_u8(qos: u8) -> QoS {
    match qos {
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
    }
}

fn generate_client_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("rekuiper-{}-{}", std::process::id(), nanos)
}

/// Extract `(host, port)` from an eKuiper MQTT server URL.
///
/// Accepts forms like `tcp://127.0.0.1:1883`, `127.0.0.1:1883`,
/// `ssl://mqtt.example.com:8883` or a bare hostname. The port defaults to
/// 1883, or 8883 for `ssl`/`tls`/`tcps`/`mqtts` schemes.
pub fn parse_mqtt_server_url(server: &str) -> Result<(String, u16)> {
    let (_scheme, host, port) = parse_mqtt_server_url_ext(server)?;
    Ok((host, port))
}

/// Extract `(scheme, host, port)` from an MQTT server URL.
pub fn parse_mqtt_server_url_ext(server: &str) -> Result<(String, String, u16)> {
    let s = server.trim();
    if s.is_empty() {
        bail!("MQTT server URL is empty");
    }
    let (scheme, remainder) = match s.split_once("://") {
        Some((scheme, rest)) => (scheme.trim().to_ascii_lowercase(), rest.trim()),
        None => (String::new(), s),
    };
    if remainder.is_empty() {
        bail!("MQTT server URL {:?} has no host", server);
    }
    // Strip any trailing path, query or fragment: host[:port][/...].
    let hostport = remainder.split(['/', '?', '#']).next().unwrap_or("").trim();
    if hostport.is_empty() {
        bail!("MQTT server URL {:?} has no host", server);
    }
    let default_port: u16 = match scheme.as_str() {
        "ssl" | "tls" | "tcps" | "mqtts" => 8883,
        _ => 1883,
    };
    // Split host and port on the last ':' when the suffix is numeric.
    // Bracketed IPv6 (`[::1]:1883`) is unwrapped to `::1`.
    let (mut host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => {
            let port: u16 = p
                .parse()
                .with_context(|| format!("Invalid port in MQTT server URL {:?}", server))?;
            (h.trim().to_string(), port)
        }
        _ => (hostport.to_string(), default_port),
    };
    if host.starts_with('[') && host.ends_with(']') && host.len() >= 2 {
        host = host[1..host.len() - 1].to_string();
    }
    if host.is_empty() {
        bail!("MQTT server URL {:?} has no host", server);
    }
    Ok((scheme, host, port))
}

pub fn is_secure_mqtt_scheme(scheme: &str) -> bool {
    matches!(
        scheme.to_ascii_lowercase().as_str(),
        "ssl" | "tls" | "tcps" | "mqtts"
    )
}

pub fn is_tls_required(config: &MqttConfig, scheme: &str) -> bool {
    is_secure_mqtt_scheme(scheme)
        || config.root_ca_path.is_some()
        || config.root_ca_raw.is_some()
        || config.certification_path.is_some()
        || config.certification_raw.is_some()
        || config.private_key_path.is_some()
        || config.private_key_raw.is_some()
        || config.insecure_skip_verify
}

fn load_pem_bytes(raw: Option<&str>, path: Option<&str>, kind: &str) -> Result<Option<Vec<u8>>> {
    if let Some(p) = path.filter(|s| !s.trim().is_empty()) {
        let path = Path::new(p.trim());
        let bytes = std::fs::read(path)
            .with_context(|| format!("Failed to read MQTT {} file at {:?}", kind, path))?;
        if bytes.is_empty() {
            bail!("MQTT {} file at {:?} is empty", kind, path);
        }
        return Ok(Some(bytes));
    }
    if let Some(r) = raw.filter(|s| !s.trim().is_empty()) {
        let r_trimmed = r.trim();
        if !r_trimmed.starts_with("-----BEGIN ") {
            use base64::Engine as _;
            if let Ok(decoded) =
                base64::engine::general_purpose::STANDARD.decode(r_trimmed.as_bytes())
            {
                return Ok(Some(decoded));
            }
        }
        return Ok(Some(r_trimmed.as_bytes().to_vec()));
    }
    Ok(None)
}

fn parse_client_cert_and_key(
    cert_bytes: &[u8],
    key_bytes: &[u8],
) -> Result<(
    Vec<rumqttc::tokio_rustls::rustls::pki_types::CertificateDer<'static>>,
    rumqttc::tokio_rustls::rustls::pki_types::PrivateKeyDer<'static>,
)> {
    let certs: Vec<_> = rustls_pemfile::certs(&mut std::io::Cursor::new(cert_bytes))
        .collect::<Result<Vec<_>, _>>()
        .context("Failed to parse client certificate PEM")?;
    if certs.is_empty() {
        bail!("No valid client certificate found in PEM data");
    }

    let mut cursor = std::io::Cursor::new(key_bytes);
    let key = loop {
        match rustls_pemfile::read_one(&mut cursor).context("Failed to read private key PEM")? {
            Some(rustls_pemfile::Item::Sec1Key(k)) => break k.into(),
            Some(rustls_pemfile::Item::Pkcs1Key(k)) => break k.into(),
            Some(rustls_pemfile::Item::Pkcs8Key(k)) => break k.into(),
            None => {
                bail!("No valid private key found in PEM data (supported formats: SEC1, PKCS#1, PKCS#8)")
            }
            _ => {}
        }
    };

    Ok((certs, key))
}

fn build_root_cert_store(
    ca_bytes: Option<&[u8]>,
) -> Result<rumqttc::tokio_rustls::rustls::RootCertStore> {
    let mut root_store = rumqttc::tokio_rustls::rustls::RootCertStore::empty();
    if let Some(ca) = ca_bytes {
        let certs: Vec<_> = rustls_pemfile::certs(&mut std::io::Cursor::new(ca))
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to parse root CA PEM")?;
        if certs.is_empty() {
            bail!("No valid CA certificate found in root CA configuration");
        }
        for cert in certs {
            root_store
                .add(cert)
                .context("Failed to add CA certificate to root store")?;
        }
    } else {
        let mut loaded = 0;
        if let Ok(certs) = rustls_native_certs::load_native_certs() {
            for cert in certs {
                if root_store.add(cert).is_ok() {
                    loaded += 1;
                }
            }
        }
        if loaded == 0 {
            root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }
    }
    Ok(root_store)
}

#[derive(Debug)]
struct NoVerifyServerCert;

impl rumqttc::tokio_rustls::rustls::client::danger::ServerCertVerifier for NoVerifyServerCert {
    fn verify_server_cert(
        &self,
        _end_entity: &rumqttc::tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rumqttc::tokio_rustls::rustls::pki_types::CertificateDer<'_>],
        _server_name: &rumqttc::tokio_rustls::rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rumqttc::tokio_rustls::rustls::pki_types::UnixTime,
    ) -> Result<
        rumqttc::tokio_rustls::rustls::client::danger::ServerCertVerified,
        rumqttc::tokio_rustls::rustls::Error,
    > {
        Ok(rumqttc::tokio_rustls::rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rumqttc::tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _dss: &rumqttc::tokio_rustls::rustls::DigitallySignedStruct,
    ) -> Result<
        rumqttc::tokio_rustls::rustls::client::danger::HandshakeSignatureValid,
        rumqttc::tokio_rustls::rustls::Error,
    > {
        Ok(rumqttc::tokio_rustls::rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rumqttc::tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _dss: &rumqttc::tokio_rustls::rustls::DigitallySignedStruct,
    ) -> Result<
        rumqttc::tokio_rustls::rustls::client::danger::HandshakeSignatureValid,
        rumqttc::tokio_rustls::rustls::Error,
    > {
        Ok(rumqttc::tokio_rustls::rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rumqttc::tokio_rustls::rustls::SignatureScheme> {
        vec![
            rumqttc::tokio_rustls::rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rumqttc::tokio_rustls::rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rumqttc::tokio_rustls::rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rumqttc::tokio_rustls::rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rumqttc::tokio_rustls::rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rumqttc::tokio_rustls::rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rumqttc::tokio_rustls::rustls::SignatureScheme::ED25519,
            rumqttc::tokio_rustls::rustls::SignatureScheme::RSA_PSS_SHA256,
            rumqttc::tokio_rustls::rustls::SignatureScheme::RSA_PSS_SHA384,
            rumqttc::tokio_rustls::rustls::SignatureScheme::RSA_PSS_SHA512,
        ]
    }
}

pub fn build_tls_configuration(config: &MqttConfig) -> Result<rumqttc::TlsConfiguration> {
    let ca_bytes = load_pem_bytes(
        config.root_ca_raw.as_deref(),
        config.root_ca_path.as_deref(),
        "root CA certificate",
    )?;

    let client_cert_bytes = load_pem_bytes(
        config.certification_raw.as_deref(),
        config.certification_path.as_deref(),
        "client certificate",
    )?;
    let client_key_bytes = load_pem_bytes(
        config.private_key_raw.as_deref(),
        config.private_key_path.as_deref(),
        "client private key",
    )?;

    match (&client_cert_bytes, &client_key_bytes) {
        (Some(_), None) => {
            bail!("Client certificate was configured for MQTT, but client private key is missing (set privateKeyPath or privateKeyRaw)");
        }
        (None, Some(_)) => {
            bail!("Client private key was configured for MQTT, but client certificate is missing (set certificationPath or certificationRaw)");
        }
        _ => {}
    }

    let client_auth = match (client_cert_bytes.as_deref(), client_key_bytes.as_deref()) {
        (Some(c), Some(k)) => Some(parse_client_cert_and_key(c, k)?),
        _ => None,
    };

    if config.insecure_skip_verify {
        let builder = rumqttc::tokio_rustls::rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifyServerCert));
        let client_config = match client_auth {
            Some((certs, key)) => builder.with_client_auth_cert(certs, key).context(
                "Failed to configure client certificate authentication with insecure verifier",
            )?,
            None => builder.with_no_client_auth(),
        };
        Ok(rumqttc::TlsConfiguration::Rustls(Arc::new(client_config)))
    } else {
        let root_store = build_root_cert_store(ca_bytes.as_deref())?;
        let builder = rumqttc::tokio_rustls::rustls::ClientConfig::builder()
            .with_root_certificates(root_store);
        let client_config = match client_auth {
            Some((certs, key)) => builder
                .with_client_auth_cert(certs, key)
                .context("Failed to configure client certificate authentication")?,
            None => builder.with_no_client_auth(),
        };
        Ok(rumqttc::TlsConfiguration::Rustls(Arc::new(client_config)))
    }
}

pub(crate) fn mqtt_options(config: &MqttConfig) -> Result<MqttOptions> {
    let (scheme, host, port) = parse_mqtt_server_url_ext(&config.server)?;
    if !scheme.is_empty()
        && !matches!(
            scheme.as_str(),
            "tcp" | "mqtt" | "ssl" | "tls" | "tcps" | "mqtts"
        )
    {
        bail!(
            "Unsupported MQTT scheme {:?} in server URL {:?}",
            scheme,
            config.server
        );
    }

    let mut opts = MqttOptions::new(config.effective_client_id(), host, port);
    opts.set_keep_alive(std::time::Duration::from_secs(
        config.keep_alive.unwrap_or(30).max(5),
    ));
    opts.set_max_packet_size(MQTT_MAX_PACKET_BYTES, MQTT_MAX_PACKET_BYTES);
    if let Some(clean) = config.clean_session {
        opts.set_clean_session(clean);
    }
    if let Some(version) = config.protocol_version.as_deref() {
        if !matches!(version.trim(), "" | "3.1" | "3.1.1" | "4") {
            tracing::warn!(
                "MQTT protocolVersion {:?} is not supported; using 3.1.1",
                version
            );
        }
    }
    if let (Some(u), Some(p)) = (config.username.clone(), config.password.clone()) {
        opts.set_credentials(u, p);
    } else if let Some(u) = config.username.clone() {
        opts.set_credentials(u, String::new());
    }

    if is_tls_required(config, &scheme) {
        let tls_cfg = build_tls_configuration(config)?;
        opts.set_transport(rumqttc::Transport::Tls(tls_cfg));
    } else {
        opts.set_transport(rumqttc::Transport::Tcp);
    }

    Ok(opts)
}

/// Drives a sink connection, tracking whether it is currently connected.
/// Exits once every client handle is dropped (the sink is gone) instead of
/// reconnecting forever.
fn spawn_event_loop_driver(
    mut eventloop: rumqttc::EventLoop,
    connected: Arc<std::sync::atomic::AtomicBool>,
) {
    use std::sync::atomic::Ordering::Relaxed;
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => connected.store(true, Relaxed),
                Ok(_) => {}
                Err(rumqttc::ConnectionError::RequestsDone) => break,
                Err(e) => {
                    connected.store(false, Relaxed);
                    tracing::warn!("MQTT event loop error: {}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
        connected.store(false, Relaxed);
    });
}

/// Queued publishes between the sink worker and the network.
const MQTT_SINK_REQUEST_CAPACITY: usize = 1024;
/// How long a new sink waits for its first connection before failing sends.
const MQTT_SINK_CONNECT_GRACE: std::time::Duration = std::time::Duration::from_secs(3);

/// MQTT sink: one persistent connection that publishes each record's `data`
/// as JSON to the configured topic. While the broker is unreachable, sends
/// fail fast so the caller can cache and resend.
pub struct MqttSink {
    pub config: MqttConfig,
    client: AsyncClient,
    connected: Arc<std::sync::atomic::AtomicBool>,
    created: std::time::Instant,
}

impl MqttSink {
    pub fn new(config: MqttConfig) -> Result<Self> {
        let opts = mqtt_options(&config)?;
        let (client, eventloop) = AsyncClient::new(opts, MQTT_SINK_REQUEST_CAPACITY);
        let connected = Arc::new(std::sync::atomic::AtomicBool::new(false));
        spawn_event_loop_driver(eventloop, connected.clone());
        Ok(Self {
            config,
            client,
            connected,
            created: std::time::Instant::now(),
        })
    }

    pub fn config(&self) -> &MqttConfig {
        &self.config
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// A fresh sink waits briefly for its first connection; afterwards a
    /// disconnected sink fails immediately.
    async fn ensure_connected(&self) -> Result<()> {
        while !self.is_connected() {
            if self.created.elapsed() >= MQTT_SINK_CONNECT_GRACE {
                bail!("MQTT sink is not connected to {}", self.config.server);
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        Ok(())
    }
}

#[async_trait]
impl Sink for MqttSink {
    async fn send(&self, record: &StreamRecord) -> Result<()> {
        let mut map = std::collections::BTreeMap::new();
        for (k, v) in &record.data {
            if k == META_KEY || k.starts_with("__") {
                continue;
            }
            map.insert(k.clone(), v.clone());
        }
        let payload = serde_json::to_vec(&map)?;
        self.send_raw(payload).await
    }
}

impl MqttSink {
    /// Publishes pre-rendered bytes (e.g. a `dataTemplate` result).
    pub async fn send_raw(&self, payload: Vec<u8>) -> Result<()> {
        self.send_raw_to(&self.config.topic, payload).await
    }

    /// Publishes to an explicit topic (e.g. a `resendDestination`).
    pub async fn send_raw_to(&self, topic: &str, payload: Vec<u8>) -> Result<()> {
        self.ensure_connected().await?;
        self.client
            .publish(topic, self.config.qos_level(), false, payload)
            .await
            .map_err(|e| anyhow::anyhow!("MQTT publish failed: {}", e))?;
        Ok(())
    }
}

/// Decode a raw MQTT publish payload into a [`StreamRecord`].
///
/// The payload must be a JSON object; its members become the record data.
pub fn decode_mqtt_payload(payload: &[u8]) -> Result<StreamRecord> {
    // Fast path: deserialize straight into the row map (no intermediate DOM,
    // no object clone). Only a failing payload pays for the diagnostic parse
    // that distinguishes invalid JSON from a non-object value.
    match serde_json::from_slice::<IndexMap<String, Value>>(payload) {
        Ok(data) => Ok(StreamRecord::new(data)),
        Err(_) => {
            let value: Value =
                serde_json::from_slice(payload).context("MQTT payload is not valid JSON")?;
            match value {
                Value::Object(map) => Ok(StreamRecord::new(
                    map.into_iter().collect::<IndexMap<_, _>>(),
                )),
                _ => Err(anyhow::anyhow!("MQTT payload must be a JSON object")),
            }
        }
    }
}

/// Decode one payload per `format` into rows appended to `out`. `meta`, when
/// given, is attached to every produced row under `__meta__`.
pub fn decode_payload_into(
    format: &PayloadFormat,
    payload: &[u8],
    meta: Option<Value>,
    out: &mut Vec<StreamRecord>,
) -> Result<()> {
    decode_payload_into_with_mask(format, None, payload, meta, out)
}

/// Decode one payload with optional schema-guided `field_mask` into rows appended to `out`.
pub fn decode_payload_into_with_mask(
    format: &PayloadFormat,
    field_mask: Option<&crate::codec::JsonFieldMask>,
    payload: &[u8],
    meta: Option<Value>,
    out: &mut Vec<StreamRecord>,
) -> Result<()> {
    let first = out.len();
    match format {
        PayloadFormat::Json => {
            if let Some(mask) = field_mask {
                match mask.decode_object(payload) {
                    Ok(data) => out.push(StreamRecord::new(data)),
                    Err(_) => {
                        let value: Value = serde_json::from_slice(payload)
                            .context("MQTT payload is not valid JSON")?;
                        match value {
                            Value::Object(map) => out.push(StreamRecord::new(
                                map.into_iter().collect::<IndexMap<_, _>>(),
                            )),
                            Value::Array(items) => {
                                for item in items {
                                    match item {
                                        Value::Object(map) => out.push(StreamRecord::new(
                                            map.into_iter().collect::<IndexMap<_, _>>(),
                                        )),
                                        _ => {
                                            out.truncate(first);
                                            bail!(
                                                "MQTT JSON array payload must contain only objects"
                                            );
                                        }
                                    }
                                }
                            }
                            _ => bail!("MQTT payload must be a JSON object or an array of objects"),
                        }
                    }
                }
            } else {
                match serde_json::from_slice::<IndexMap<String, Value>>(payload) {
                    Ok(data) => out.push(StreamRecord::new(data)),
                    Err(_) => {
                        let value: Value = serde_json::from_slice(payload)
                            .context("MQTT payload is not valid JSON")?;
                        match value {
                            Value::Object(map) => out.push(StreamRecord::new(
                                map.into_iter().collect::<IndexMap<_, _>>(),
                            )),
                            Value::Array(items) => {
                                for item in items {
                                    match item {
                                        Value::Object(map) => out.push(StreamRecord::new(
                                            map.into_iter().collect::<IndexMap<_, _>>(),
                                        )),
                                        _ => {
                                            out.truncate(first);
                                            bail!(
                                                "MQTT JSON array payload must contain only objects"
                                            );
                                        }
                                    }
                                }
                            }
                            _ => bail!("MQTT payload must be a JSON object or an array of objects"),
                        }
                    }
                }
            }
        }
        PayloadFormat::Binary => {
            let value = match std::str::from_utf8(payload) {
                Ok(text) => Value::String(text.to_string()),
                Err(_) => {
                    use base64::Engine as _;
                    Value::String(base64::engine::general_purpose::STANDARD.encode(payload))
                }
            };
            let mut data = HashMap::with_capacity(2);
            data.insert("self".to_string(), value);
            out.push(StreamRecord::new(data));
        }
        PayloadFormat::Delimited(codec) => {
            let text = std::str::from_utf8(payload).context("delimited payload is not UTF-8")?;
            let line = text.trim_end_matches(['\r', '\n']);
            out.push(StreamRecord::new(codec.decode_row(line)));
        }
        PayloadFormat::Protobuf(message) => match ProtobufCodec::decode(payload, message) {
            Value::Object(map) => out.push(StreamRecord::new(
                map.into_iter().collect::<IndexMap<_, _>>(),
            )),
            _ => bail!("payload is not a valid '{}' protobuf message", message.name),
        },
        PayloadFormat::EdgeX => EdgeXCodec::decode(payload, meta.clone(), out)?,
    }
    if let Some(meta) = meta {
        let topic_str = meta.get("topic").and_then(|v| v.as_str());
        let rec_meta = Arc::new(RecordMeta {
            topic: topic_str.map(Arc::from),
            source: Some(Arc::from("mqtt")),
            custom: None,
        });
        let end = out.len();
        if end > first {
            for record in &mut out[first..end - 1] {
                record.data.insert("__meta__".to_string(), meta.clone());
                record.meta = Some(rec_meta.clone());
            }
            out[end - 1].data.insert("__meta__".to_string(), meta);
            out[end - 1].meta = Some(rec_meta);
        }
    }
    Ok(())
}

/// eKuiper MQTT source metadata for `meta(topic)` / `meta(qos)` /
/// `meta(messageId)`; the topic string is moved, not copied.
fn mqtt_meta(topic: String, qos: QoS, pkid: u16) -> Value {
    let mut meta = serde_json::Map::with_capacity(3);
    meta.insert("topic".to_string(), Value::String(topic));
    meta.insert("qos".to_string(), Value::from(qos as u8));
    meta.insert("messageId".to_string(), Value::from(pkid));
    Value::Object(meta)
}

/// Upper bound on MQTT publishes admitted to the stream bus in one batch.
const MQTT_SOURCE_MAX_BATCH: usize = 1024;

fn push_decoded(
    config: &MqttConfig,
    batch: &mut Vec<StreamRecord>,
    publish: rumqttc::Publish,
    counters: Option<&RuleCounters>,
    meta_flag: Option<&std::sync::atomic::AtomicBool>,
) {
    let mut cache = None;
    push_decoded_with_cache(config, batch, publish, counters, meta_flag, &mut cache);
}

fn push_decoded_with_cache_and_mask(
    config: &MqttConfig,
    field_mask: Option<&crate::codec::JsonFieldMask>,
    batch: &mut Vec<StreamRecord>,
    publish: rumqttc::Publish,
    counters: Option<&RuleCounters>,
    meta_flag: Option<&std::sync::atomic::AtomicBool>,
    meta_cache: &mut Option<(String, u8, Value)>,
) {
    let topic_arc: Arc<str> = Arc::from(publish.topic.as_str());
    let attach = config.attach_meta
        || meta_flag.is_some_and(|f| f.load(std::sync::atomic::Ordering::Relaxed));
    let meta = if attach {
        if publish.pkid == 0 {
            let qos_u8 = publish.qos as u8;
            if let Some((cached_topic, cached_qos, cached_val)) = meta_cache {
                if *cached_topic == publish.topic && *cached_qos == qos_u8 {
                    Some(cached_val.clone())
                } else {
                    let new_meta = mqtt_meta(publish.topic.clone(), publish.qos, 0);
                    *meta_cache = Some((publish.topic, qos_u8, new_meta.clone()));
                    Some(new_meta)
                }
            } else {
                let new_meta = mqtt_meta(publish.topic.clone(), publish.qos, 0);
                *meta_cache = Some((publish.topic, qos_u8, new_meta.clone()));
                Some(new_meta)
            }
        } else {
            Some(mqtt_meta(publish.topic, publish.qos, publish.pkid))
        }
    } else {
        None
    };

    let start_idx = batch.len();
    if let Err(e) =
        decode_payload_into_with_mask(&config.format, field_mask, &publish.payload, meta, batch)
    {
        tracing::warn!("Skipping invalid MQTT payload: {}", e);
        if let Some(counters) = counters {
            counters.inc_exceptions(1);
        }
    } else {
        for record in &mut batch[start_idx..] {
            if record.meta.is_none() {
                record.meta = Some(Arc::new(RecordMeta {
                    topic: Some(topic_arc.clone()),
                    source: Some(Arc::from("mqtt")),
                    custom: None,
                }));
            }
        }
    }
}

fn push_decoded_with_cache(
    config: &MqttConfig,
    batch: &mut Vec<StreamRecord>,
    publish: rumqttc::Publish,
    counters: Option<&RuleCounters>,
    meta_flag: Option<&std::sync::atomic::AtomicBool>,
    meta_cache: &mut Option<(String, u8, Value)>,
) {
    push_decoded_with_cache_and_mask(
        config,
        config.field_mask.as_ref(),
        batch,
        publish,
        counters,
        meta_flag,
        meta_cache,
    );
}

/// Moves publishes that rumqttc has already read from the network and queued
/// (`EventLoop::state.events`) into `batch`, in arrival order. `poll()` pops
/// exactly this queue before touching the network again, so draining it here
/// is equivalent to repeated `poll()` calls without the per-message await, and
/// never waits for more data. Stops at the first non-publish event (left for
/// `poll()`, e.g. ConnAck resubscribe handling) or once `max` records are
/// collected. Acks for these packets were already flushed at read time.
#[allow(dead_code)]
pub(crate) fn drain_buffered_publishes(
    config: &MqttConfig,
    events: &mut std::collections::VecDeque<rumqttc::Event>,
    batch: &mut Vec<StreamRecord>,
    max: usize,
    counters: Option<&RuleCounters>,
    meta_flag: Option<&std::sync::atomic::AtomicBool>,
) {
    let mut meta_cache = None;
    drain_buffered_publishes_with_cache(
        config,
        events,
        batch,
        max,
        counters,
        meta_flag,
        &mut meta_cache,
    );
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn drain_buffered_publishes_with_cache_and_mask(
    config: &MqttConfig,
    field_mask: Option<&crate::codec::JsonFieldMask>,
    events: &mut std::collections::VecDeque<rumqttc::Event>,
    batch: &mut Vec<StreamRecord>,
    max: usize,
    counters: Option<&RuleCounters>,
    meta_flag: Option<&std::sync::atomic::AtomicBool>,
    meta_cache: &mut Option<(String, u8, Value)>,
) {
    while batch.len() < max {
        if !matches!(
            events.front(),
            Some(rumqttc::Event::Incoming(rumqttc::Packet::Publish(_)))
        ) {
            break;
        }
        if let Some(rumqttc::Event::Incoming(rumqttc::Packet::Publish(p))) = events.pop_front() {
            push_decoded_with_cache_and_mask(
                config, field_mask, batch, p, counters, meta_flag, meta_cache,
            );
        }
    }
}

pub(crate) fn drain_buffered_publishes_with_cache(
    config: &MqttConfig,
    events: &mut std::collections::VecDeque<rumqttc::Event>,
    batch: &mut Vec<StreamRecord>,
    max: usize,
    counters: Option<&RuleCounters>,
    meta_flag: Option<&std::sync::atomic::AtomicBool>,
    meta_cache: &mut Option<(String, u8, Value)>,
) {
    drain_buffered_publishes_with_cache_and_mask(
        config,
        config.field_mask.as_ref(),
        events,
        batch,
        max,
        counters,
        meta_flag,
        meta_cache,
    );
}

/// Subscribe to every configured topic in one SUBSCRIBE packet.
async fn subscribe_topics(client: &AsyncClient, config: &MqttConfig) -> Result<()> {
    let filters: Vec<rumqttc::SubscribeFilter> = config
        .topics()
        .into_iter()
        .map(|t| rumqttc::SubscribeFilter::new(t, config.qos_level()))
        .collect();
    if filters.is_empty() {
        bail!("MQTT source has no topic");
    }
    client
        .subscribe_many(filters)
        .await
        .map_err(|e| anyhow::anyhow!("MQTT subscribe to {} failed: {}", config.topic, e))
}

/// MQTT source: subscribes to the configured topic and forwards each
/// incoming JSON message as a [`StreamRecord`].
pub struct MqttSource {
    pub config: MqttConfig,
    pub tx: StreamSender,
    rule_counters: Option<Arc<RuleCounters>>,
    attach_meta_flag: Option<Arc<std::sync::atomic::AtomicBool>>,
    field_mask_flag: Option<Arc<parking_lot::RwLock<Option<crate::codec::JsonFieldMask>>>>,
    status_callback: Option<crate::ConnectorStatusCallback>,
}

impl MqttSource {
    pub fn new(config: MqttConfig, tx: StreamSender) -> Self {
        Self {
            config,
            tx,
            rule_counters: None,
            attach_meta_flag: None,
            field_mask_flag: None,
            status_callback: None,
        }
    }

    pub fn with_status_callback(mut self, cb: Option<crate::ConnectorStatusCallback>) -> Self {
        self.status_callback = cb;
        self
    }

    pub fn with_rule_counters(mut self, counters: Option<Arc<RuleCounters>>) -> Self {
        self.rule_counters = counters;
        self
    }

    pub fn with_meta_flag(mut self, flag: Option<Arc<std::sync::atomic::AtomicBool>>) -> Self {
        self.attach_meta_flag = flag;
        self
    }

    pub fn with_field_mask_flag(
        mut self,
        flag: Option<Arc<parking_lot::RwLock<Option<crate::codec::JsonFieldMask>>>>,
    ) -> Self {
        self.field_mask_flag = flag;
        self
    }

    pub fn config(&self) -> &MqttConfig {
        &self.config
    }

    pub fn spawn(
        self,
        mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            tracing::info!(
                "[MQTT SOURCE] Connecting to broker '{}', topic '{}', qos {}",
                self.config.server,
                self.config.topic,
                self.config.qos
            );
            let opts = match mqtt_options(&self.config) {
                Ok(opts) => opts,
                Err(e) => {
                    tracing::warn!("MQTT source bad broker config: {}", e);
                    if let Some(counters) = self.rule_counters.as_deref() {
                        counters.inc_exceptions(1);
                    }
                    return;
                }
            };
            let (client, mut eventloop) = AsyncClient::new(opts, 64);
            if let Err(e) = subscribe_topics(&client, &self.config).await {
                tracing::warn!("{}", e);
                if let Some(ref cb) = self.status_callback {
                    cb(&self.config.topic, "mqtt", "error", Some(&e.to_string()));
                }
                if let Some(counters) = self.rule_counters.as_deref() {
                    counters.inc_exceptions(1);
                }
                return;
            }
            if let Some(ref cb) = self.status_callback {
                cb(&self.config.topic, "mqtt", "connected", None);
            }
            let mut meta_cache: Option<(String, u8, Value)> = None;
            loop {
                tokio::select! {
                    event = eventloop.poll() => {
                        match event {
                            Ok(Event::Incoming(Packet::Publish(p))) => {
                                // Opportunistic batch: admit this publish plus
                                // every publish already read in the same
                                // network read, in order, with one bus
                                // admission. Backpressure still reaches the
                                // source: send_batch awaits subscriber capacity
                                // instead of overwriting buffered records.
                                let mut batch = Vec::with_capacity(16);
                                let active_mask_owned = if let Some(ref flag) = self.field_mask_flag {
                                    flag.read().clone()
                                } else {
                                    self.config.field_mask.clone()
                                };
                                let active_mask = active_mask_owned.as_ref();
                                push_decoded_with_cache_and_mask(
                                    &self.config,
                                    active_mask,
                                    &mut batch,
                                    p,
                                    self.rule_counters.as_deref(),
                                    self.attach_meta_flag.as_deref(),
                                    &mut meta_cache,
                                );
                                drain_buffered_publishes_with_cache_and_mask(
                                    &self.config,
                                    active_mask,
                                    &mut eventloop.state.events,
                                    &mut batch,
                                    MQTT_SOURCE_MAX_BATCH,
                                    self.rule_counters.as_deref(),
                                    self.attach_meta_flag.as_deref(),
                                    &mut meta_cache,
                                );
                                if !batch.is_empty() {
                                    let _ = self.tx.send_batch(batch).await;
                                }
                            }
                            // rumqttc does not restore subscriptions across
                            // reconnects: every (re)connect handshake must
                            // re-subscribe or the source goes silently deaf
                            // after a broker restart.
                            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                                if let Some(ref cb) = self.status_callback {
                                    cb(&self.config.topic, "mqtt", "connected", None);
                                }
                                if let Err(e) = subscribe_topics(&client, &self.config).await {
                                    tracing::warn!("MQTT resubscribe: {}", e);
                                    if let Some(counters) = self.rule_counters.as_deref() { counters.inc_exceptions(1); }
                                }
                            }
                            Ok(_) => {}
                            Err(e) => {
                                tracing::warn!("MQTT connection error: {}", e);
                                if let Some(ref cb) = self.status_callback {
                                    cb(&self.config.topic, "mqtt", "disconnected", Some(&e.to_string()));
                                }
                                if let Some(counters) = self.rule_counters.as_deref() { counters.inc_exceptions(1); }
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            }
                        }
                    }
                    changed = cancel_rx.changed() => {
                        match changed {
                            Ok(_) => {
                                if *cancel_rx.borrow() {
                                    let _ = client.disconnect().await;
                                    let _ = tokio::time::timeout(std::time::Duration::from_millis(200), eventloop.poll()).await;
                                    return;
                                }
                            }
                            // Cancellation sender dropped: shut down.
                            Err(_) => {
                                let _ = client.disconnect().await;
                                let _ = tokio::time::timeout(std::time::Duration::from_millis(200), eventloop.poll()).await;
                                return;
                            }
                        }
                    }
                }
            }
        })
    }

    pub async fn run(self, tx: tokio::sync::mpsc::Sender<StreamRecord>) -> Result<()> {
        let opts = mqtt_options(&self.config)?;
        let (client, mut eventloop) = AsyncClient::new(opts, 64);
        subscribe_topics(&client, &self.config).await?;
        let mut rows = Vec::new();
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    push_decoded(
                        &self.config,
                        &mut rows,
                        p,
                        self.rule_counters.as_deref(),
                        self.attach_meta_flag.as_deref(),
                    );
                    for record in rows.drain(..) {
                        if tx.send(record).await.is_err() {
                            // Receiver dropped; shut down cleanly.
                            let _ = client.disconnect().await;
                            let _ = tokio::time::timeout(
                                std::time::Duration::from_millis(200),
                                eventloop.poll(),
                            )
                            .await;
                            return Ok(());
                        }
                    }
                }
                // See `spawn`: subscriptions die with the connection.
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    subscribe_topics(&client, &self.config).await?;
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!("MQTT connection error: {}", e);
                    if let Some(counters) = self.rule_counters.as_deref() {
                        counters.inc_exceptions(1);
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }
}
