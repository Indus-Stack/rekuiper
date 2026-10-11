use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct KuiperConfig {
    #[serde(default)]
    pub basic: BasicConfig,
    #[serde(default)]
    pub store: StoreConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BasicConfig {
    #[serde(default = "default_ip")]
    pub ip: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_rest_ip")]
    pub rest_ip: String,
    #[serde(default = "default_rest_port")]
    pub rest_port: u16,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default)]
    pub debug: bool,
    #[serde(default = "default_true")]
    pub console_log: bool,
    #[serde(default)]
    pub authentication: bool,
    #[serde(default = "default_timezone")]
    pub timezone: String,
    #[serde(default)]
    pub prometheus: bool,
    #[serde(default = "default_prometheus_port")]
    pub prometheus_port: u16,
    /// Ruletest SSE server bind IP (`httpServerIp` in kuiper.yaml).
    #[serde(default = "default_http_server_ip")]
    pub http_server_ip: String,
    /// Ruletest SSE server port (`httpServerPort`; documented default 10081).
    #[serde(default = "default_http_server_port")]
    pub http_server_port: u16,
    /// Whether rules and sinks can connect to private networks (default false for security).
    #[serde(default)]
    pub enable_private_net: bool,
}

fn default_ip() -> String {
    "127.0.0.1".to_string()
}
fn default_port() -> u16 {
    20498
}
fn default_rest_ip() -> String {
    "127.0.0.1".to_string()
}
fn default_rest_port() -> u16 {
    9081
}
fn default_log_level() -> String {
    "info".to_string()
}
fn default_true() -> bool {
    true
}
fn default_timezone() -> String {
    "Local".to_string()
}
fn default_prometheus_port() -> u16 {
    20499
}
fn default_http_server_ip() -> String {
    "127.0.0.1".to_string()
}
fn default_http_server_port() -> u16 {
    10081
}

impl Default for BasicConfig {
    fn default() -> Self {
        Self {
            ip: default_ip(),
            port: default_port(),
            rest_ip: default_rest_ip(),
            rest_port: default_rest_port(),
            log_level: default_log_level(),
            debug: false,
            console_log: true,
            authentication: false,
            timezone: default_timezone(),
            prometheus: false,
            prometheus_port: default_prometheus_port(),
            http_server_ip: default_http_server_ip(),
            http_server_port: default_http_server_port(),
            enable_private_net: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoreConfig {
    #[serde(default = "default_store_type")]
    pub r#type: String,
}

fn default_store_type() -> String {
    "memory".to_string()
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            r#type: default_store_type(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PathConfig {
    pub etc_dir: PathBuf,
    pub data_dir: PathBuf,
    pub log_dir: PathBuf,
}

pub type RekuiperConfig = KuiperConfig;

impl PathConfig {
    pub fn new(etc_path: Option<&str>, data_path: Option<&str>, log_path: Option<&str>) -> Self {
        let etc_dir = etc_path
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("etc"));
        let data_dir = data_path
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("data"));
        let log_dir = log_path
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("log"));

        Self {
            etc_dir,
            data_dir,
            log_dir,
        }
    }

    pub fn load_config(&self) -> Result<KuiperConfig> {
        let rekuiper_file = self.etc_dir.join("rekuiper.yaml");
        let kuiper_file = self.etc_dir.join("kuiper.yaml");
        let conf_file = if rekuiper_file.exists() {
            rekuiper_file
        } else if kuiper_file.exists() {
            kuiper_file
        } else {
            tracing::warn!(
                "Configuration file ({:?} or {:?}) not found, using defaults",
                rekuiper_file,
                kuiper_file
            );
            let mut config = KuiperConfig::default();
            apply_env_overrides(&mut config);
            return Ok(config);
        };

        let content = std::fs::read_to_string(&conf_file)
            .with_context(|| format!("Failed to read configuration file: {:?}", conf_file))?;

        let mut config: KuiperConfig = serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse YAML from {:?}", conf_file))?;

        apply_env_overrides(&mut config);
        Ok(config)
    }
}

fn parse_env_bool(val: &str) -> Option<bool> {
    match val.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn apply_basic_override(basic: &mut BasicConfig, key: &str, val: &str) {
    match key {
        "IP" => basic.ip = val.to_string(),
        "PORT" => {
            if let Ok(v) = val.trim().parse() {
                basic.port = v;
                if basic.rest_port == 9081 && v != 20498 {
                    basic.rest_port = v;
                }
            }
        }
        "RESTIP" => basic.rest_ip = val.to_string(),
        "RESTPORT" => {
            if let Ok(v) = val.trim().parse() {
                basic.rest_port = v;
            }
        }
        "LOGLEVEL" => basic.log_level = val.to_string(),
        "DEBUG" => {
            if let Some(v) = parse_env_bool(val) {
                basic.debug = v;
            }
        }
        "CONSOLELOG" => {
            if let Some(v) = parse_env_bool(val) {
                basic.console_log = v;
            }
        }
        "AUTHENTICATION" => {
            if let Some(v) = parse_env_bool(val) {
                basic.authentication = v;
            }
        }
        "TIMEZONE" => basic.timezone = val.to_string(),
        "PROMETHEUS" => {
            if let Some(v) = parse_env_bool(val) {
                basic.prometheus = v;
            }
        }
        "PROMETHEUSPORT" => {
            if let Ok(v) = val.trim().parse() {
                basic.prometheus_port = v;
            }
        }
        "HTTPSERVERIP" => basic.http_server_ip = val.to_string(),
        "HTTPSERVERPORT" => {
            if let Ok(v) = val.trim().parse() {
                basic.http_server_port = v;
            }
        }
        "ENABLEPRIVATENET" => {
            if let Some(v) = parse_env_bool(val) {
                basic.enable_private_net = v;
            }
        }
        _ => {}
    }
}

/// Override configuration from `REKUIPER__<SECTION>__<KEY>` or
/// `KUIPER__<SECTION>__<KEY>` environment variables (container deployments),
/// accepting both `RESTIP`-style and `REST_IP`-style spellings.
/// Unknown sections and keys are ignored, and unparseable numbers/bools leave
/// the current value untouched.
pub fn apply_env_overrides(config: &mut KuiperConfig) {
    apply_env_overrides_from_iter(config, std::env::vars());
}

/// Override configuration from any iterator of key-value pairs (e.g. `std::env::vars()`).
pub fn apply_env_overrides_from_iter<I>(config: &mut KuiperConfig, vars: I)
where
    I: IntoIterator<Item = (String, String)>,
{
    for (key, val) in vars {
        if key == "PORT" {
            if let Ok(p) = val.trim().parse() {
                if config.basic.rest_port == 9081 {
                    config.basic.rest_port = p;
                }
            }
            continue;
        }
        let rest = if let Some(r) = key.strip_prefix("REKUIPER__") {
            r
        } else if let Some(r) = key.strip_prefix("KUIPER__") {
            r
        } else {
            continue;
        };
        let mut parts = rest.split("__");
        let (Some(section), Some(name)) = (parts.next(), parts.next()) else {
            continue;
        };
        if parts.next().is_some() {
            continue;
        }
        let flat: String = name.chars().filter(|c| *c != '_').collect();
        match (
            section.to_ascii_uppercase().as_str(),
            flat.to_ascii_uppercase().as_str(),
        ) {
            ("BASIC", key) => apply_basic_override(&mut config.basic, key, &val),
            ("STORE", "TYPE") => config.store.r#type = val,
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_overrides_basic_network_settings() {
        let vars = vec![
            ("KUIPER__BASIC__RESTIP".to_string(), "0.0.0.0".to_string()),
            ("KUIPER__BASIC__REST_PORT".to_string(), "19081".to_string()),
            ("KUIPER__BASIC__PORT".to_string(), "12001".to_string()),
            ("KUIPER__BASIC__DEBUG".to_string(), "true".to_string()),
        ];
        let mut config = KuiperConfig::default();
        apply_env_overrides_from_iter(&mut config, vars);
        assert_eq!(config.basic.rest_ip, "0.0.0.0");
        assert_eq!(config.basic.rest_port, 19081);
        assert_eq!(config.basic.port, 12001);
        assert!(config.basic.debug);
    }

    #[test]
    fn rekuiper_env_overrides_take_effect() {
        let vars = vec![
            (
                "REKUIPER__BASIC__REST_PORT".to_string(),
                "19082".to_string(),
            ),
            ("REKUIPER__BASIC__LOGLEVEL".to_string(), "warn".to_string()),
        ];
        let mut config = KuiperConfig::default();
        apply_env_overrides_from_iter(&mut config, vars);
        assert_eq!(config.basic.rest_port, 19082);
        assert_eq!(config.basic.log_level, "warn");
    }

    #[test]
    fn edgex_port_env_override_takes_effect() {
        let vars = vec![("PORT".to_string(), "59720".to_string())];
        let mut config = KuiperConfig::default();
        apply_env_overrides_from_iter(&mut config, vars);
        assert_eq!(config.basic.rest_port, 59720);

        let vars_kuiper = vec![("KUIPER__BASIC__PORT".to_string(), "59720".to_string())];
        let mut config2 = KuiperConfig::default();
        apply_env_overrides_from_iter(&mut config2, vars_kuiper);
        assert_eq!(config2.basic.rest_port, 59720);
        assert_eq!(config2.basic.port, 59720);
    }

    #[test]
    fn enable_private_net_env_override() {
        let mut config = KuiperConfig::default();
        assert!(!config.basic.enable_private_net);

        let vars = vec![(
            "KUIPER__BASIC__ENABLEPRIVATENET".to_string(),
            "true".to_string(),
        )];
        apply_env_overrides_from_iter(&mut config, vars);
        assert!(config.basic.enable_private_net);

        let vars_rekuiper = vec![(
            "REKUIPER__BASIC__ENABLE_PRIVATE_NET".to_string(),
            "false".to_string(),
        )];
        apply_env_overrides_from_iter(&mut config, vars_rekuiper);
        assert!(!config.basic.enable_private_net);
    }

    #[test]
    fn default_config_binds_to_loopback() {
        let config = KuiperConfig::default();
        assert_eq!(config.basic.ip, "127.0.0.1");
        assert_eq!(config.basic.rest_ip, "127.0.0.1");
        assert_eq!(config.basic.http_server_ip, "127.0.0.1");
        assert_eq!(config.basic.rest_port, 9081);
        assert!(!config.basic.enable_private_net);
    }
}
