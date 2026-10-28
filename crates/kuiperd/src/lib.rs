use anyhow::Result;
use clap::Parser;
use rekuiper_conf::PathConfig;

#[derive(Parser, Debug)]
#[command(name = "rule-engine")]
#[command(
    about = "rule-engine Daemon - High-performance edge stream processing engine and MQTT rule engine in Rust (rekuiper / eKuiper drop-in)",
    long_about = None
)]
pub struct Args {
    #[command(subcommand)]
    pub command: Option<Commands>,

    /// Path of etc dir
    #[arg(long, default_value = "etc")]
    pub etc: String,

    /// Scaffold default configuration files into the target etc directory and exit
    #[arg(long)]
    pub init: bool,

    /// Path of data dir
    #[arg(long, default_value = "data")]
    pub data: String,

    /// Path of log dir
    #[arg(long, default_value = "log")]
    pub log: String,

    /// How to load path
    #[arg(long, default_value = "relative")]
    pub load_file_type: String,

    /// Path to a declarative manifest file (YAML/TOML/JSON) or directory (pipelines.d/) to run in GitOps mode
    #[arg(long, env = "REKUIPER_DECLARATIVE_PATH")]
    pub declarative: Option<String>,

    /// Watch declarative path and automatically hot-reload when manifest changes
    #[arg(long, env = "REKUIPER_DECLARATIVE_WATCH", default_value_t = false)]
    pub watch: bool,

    /// Control plane WebSocket URL for fleet management (e.g. ws://control-plane:9081/fleet/ws)
    #[arg(long, env = "REKUIPER_CONTROL_PLANE_URL")]
    pub control_plane_url: Option<String>,

    /// Edge device identifier
    #[arg(long, env = "REKUIPER_DEVICE_ID")]
    pub device_id: Option<String>,

    /// Edge device display name
    #[arg(long, env = "REKUIPER_DEVICE_NAME")]
    pub device_name: Option<String>,

    /// Plant or facility tag
    #[arg(long, env = "REKUIPER_PLANT")]
    pub plant: Option<String>,

    /// Geographic region tag
    #[arg(long, env = "REKUIPER_REGION")]
    pub region: Option<String>,

    /// Hardware device type
    #[arg(long, env = "REKUIPER_DEVICE_TYPE")]
    pub device_type: Option<String>,

    /// Optional shared bearer or provisioning token for fleet authentication
    #[arg(long, env = "REKUIPER_FLEET_TOKEN")]
    pub fleet_token: Option<String>,

    /// Enable web console UI at /ui (default: false for security)
    #[arg(
        long,
        visible_alias = "ui",
        env = "REKUIPER_ENABLE_UI",
        default_value_t = false
    )]
    pub enable_ui: bool,

    /// Allowed client IP addresses or CIDR ranges for web console access (comma-separated, e.g. "127.0.0.1,192.168.1.0/24")
    #[arg(long, env = "REKUIPER_UI_ALLOW_IP")]
    pub ui_allow_ip: Option<String>,

    /// Optional password for Web UI access
    #[arg(long, env = "REKUIPER_UI_PASSWORD")]
    pub ui_password: Option<String>,
}

#[derive(clap::Subcommand, Debug)]
pub enum Commands {
    /// Scaffold and write default configuration files (kuiper.yaml and connector configurations)
    Init {
        /// Target directory where configuration files will be written (default: "etc")
        #[arg(short, long, default_value = "etc")]
        dir: String,

        /// Overwrite existing configuration files
        #[arg(short, long, default_value_t = false)]
        force: bool,
    },
}

const DEFAULT_KUIPER_YAML: &str = r#"basic:
  # debug | info | warn | error | fatal | panic
  logLevel: info
  # true|false, with debug level, it prints more debug info
  debug: false
  # true|false, if it's set to true, then the log will be print to console
  consoleLog: false
  # true|false, if it's set to true, then the log will be print to log file
  fileLog: true
  # Whether to disable the log timestamp, useful when output is redirected to logging
  #	system like syslog that already adds timestamps.
  logDisableTimestamp: false
  # syslog settings
  syslog:
    # true|false, if it's set to true, then the log will be print to syslog
    enable: false
    # The syslog protocol, tcp or udp; Leave empty if no remote syslog server is used
    network: udp
    # The syslog server address; Leave empty if no remote syslog server is used
    address: localhost:514
    # The syslog level, supports debug, info, warn, error
    level: info
    # The syslog tag; Leave empty if no tag is used
    tag: kuiper
  # How many hours to split the file
  rotateTime: 24
  # Maximum file storage hours
  maxAge: 72
  # Maximum file size in bytes, if this is set, maxAge will be ignored
  rotateSize: 10485760 # 10 MB
  # Maximum log file count
  rotateCount: 3
  # CLI ip
  ip: 127.0.0.1
  # CLI port
  port: 20498
  # REST service ip (127.0.0.1 loopback default for security; set 0.0.0.0 for external network access)
  restIp: 127.0.0.1
  # REST service port
  restPort: 9081
  # The global time zone from the IANA time zone database, or Local if not set.
  timezone: Local
  # true|false, when true, will check the RSA JWT token for REST API.
  #
  # For local development, authentication can remain disabled.
  # For production deployments, especially when REST API is exposed
  # beyond localhost, authentication should be enabled.
  authentication: false
  #  restTls:
  #    certfile: /var/https-server.crt
  #    keyfile: /var/https-server.key
  # Prometheus settings
  prometheus: false
  prometheusPort: 20499
  # pprof settings
  pprof: true
  pprofIp: 127.0.0.1
  pprofPort: 6060
  # The URL where hosts all of pre-build plugins.
  pluginHosts: https://packages.indramqtt.com
  # Whether to ignore case in SQL processing. Note that, the name of customized function by plugins are case-sensitive.
  ignoreCase: false
  sql:
    # maxConnections indicates the max connections for the certain database instance group by driver and dsn sharing between the sources/sinks
    # 0 indicates unlimited
    maxConnections: 0
  rulePatrolInterval: 10s
  # enableOpenZiti indicates whether to enable OpenZiti for eKuiper REST service. Currently, it is only supported to work with EdgeX secure mode.
  enableOpenZiti: false
  # AES Key, base64 encoded
  ## aesKey: your_base64_encoded_aes_key
  gracefulShutdownTimeout: 10s
  connection:
    backoffMaxElapsedDuration: 3m
  # If it is enabled, the cpu time of the rule will be recorded.
  ResourceProfileConfig:
    enable: false
    interval: 5s
  metricsDumpConfig:
    enable: false
    retainedDuration: 6h
  # If it is enabled, each REST API call will print logs
  enableRestAuditLog: false
  # If it is enabled, the rule functions can access the private network.
  enablePrivateNet: false
  # Allow rule functions to access files outside the eKuiper upload directory.
  #
  # For security reasons, this is disabled by default.
  # Enable only when trusted external file paths are required.
  allowExternalFileAccess: false

# The default options for all rules. Each rule can override this setting by defining its own option
rule:
  # The qos of the rule. The values can be 0: At most once; 1: At least once; 2: Exactly once
  # If qos is bigger than 0, the checkpoint mechanism will launch to save states so that they can be
  # restored for unintended interrupt or planned restart of the rule. The performance may be affected
  # to enable the checkpoint mechanism
  qos: 0
  # The interval duration to run the checkpoint mechanism.
  checkpointInterval: 300s
  # Whether to send errors to sinks
  sendError: false
sink:
  # Control to enable cache or not. If it's set to true, then the cache will be enabled, otherwise, it will be disabled.
  enableCache: false

  # The maximum number of messages to be cached in memory.
  memoryCacheThreshold: 1024

  # The maximum number of messages to be cached in the disk.
  maxDiskCache: 1024000

  # The number of messages for a buffer page which is the unit to read/write to disk in batch to prevent frequent IO
  bufferPageSize: 256

  # The interval duration to resend the cached messages
  resendInterval: 0s

  # Whether to clean the cache when the rule stops
  cleanCacheAtStop: false

source:
  ## Configurations for the global http data server for httppush source
  # HTTP data service ip
  httpServerIp: 127.0.0.1
  # HTTP data service port
  httpServerPort: 10081
  # httpServerTls:
  #    certfile: /var/https-server.crt
  #    keyfile: /var/https-server.key

store:
  #Type of store that will be used for keeping state of the application
  type: sqlite
  extStateType: sqlite
  redis:
    host: localhost
    port: 6379
    ## password: your_redis_password
    #Timeout
    timeout: 1s
  sqlite:
    #Sqlite file name, if left empty name of db will be sqliteKV.db
    name:
  pebble:
    name: "pebble"

# The settings for portable plugin
portable:
  # The executable of python. Specify this if you have multiple python instances in your system
  # or other circumstance where the python executable cannot be successfully invoked through the default command.
  pythonBin: python
  # control init timeout. If the init time is longer than this value, the plugin will be terminated.
  initTimeout: 60s
  sendTimeout: 5s
  recvTimeout: 5s

openTelemetry:
  serviceName: kuiperd-service
  enableRemoteCollector: false
  remoteEndpoint: localhost:4318
  localTraceCapacity: 2048
  enableLocalStorage: false
"#;

const DEFAULT_MQTT_SOURCE_YAML: &str = r#"#Global MQTT configurations
default:
  qos: 1
  server: "tcp://127.0.0.1:1883"
  protocolVersion: "3.1.1"
  insecureSkipVerify: false

e2e_broker:
  qos: 0
  server: "tcp://127.0.0.1:1883"
"#;

const DEFAULT_CLIENT_YAML: &str = r#"basic:
  host: 127.0.0.1
  port: 20498
"#;

/// Scaffolds configuration directory and default YAML files for quickstart.
pub fn scaffold_config(target_dir: &str, force: bool) -> Result<()> {
    let base = std::path::Path::new(target_dir);
    std::fs::create_dir_all(base)?;

    let files = [
        ("kuiper.yaml", DEFAULT_KUIPER_YAML),
        ("mqtt_source.yaml", DEFAULT_MQTT_SOURCE_YAML),
        ("client.yaml", DEFAULT_CLIENT_YAML),
    ];

    println!("Scaffolding configuration in '{}/':", target_dir);
    for (filename, content) in files {
        let file_path = base.join(filename);
        if file_path.exists() && !force {
            println!(
                "  [SKIP] {} already exists (use --force to overwrite)",
                file_path.display()
            );
        } else {
            std::fs::write(&file_path, content)?;
            println!("  [CREATED] {}", file_path.display());
        }
    }

    if target_dir == "etc" {
        let _ = std::fs::create_dir_all("data");
        let _ = std::fs::create_dir_all("log");
        println!("  [CREATED] ./data/");
        println!("  [CREATED] ./log/");
    }

    println!("\nConfiguration complete! You can now start the daemon:");
    println!("  rule-engine");
    println!("  # or: kuiperd\n");

    Ok(())
}

pub async fn run_daemon() -> Result<()> {
    let args = Args::parse();

    if let Some(Commands::Init { dir, force }) = args.command {
        return scaffold_config(&dir, force);
    }
    if args.init {
        return scaffold_config(&args.etc, false);
    }

    tracing_subscriber::fmt::init();

    let paths = PathConfig::new(Some(&args.etc), Some(&args.data), Some(&args.log));
    let config = paths.load_config()?;

    let version = env!("CARGO_PKG_VERSION").to_string();
    let ui_allow_ip_vec = args.ui_allow_ip.map(|s| {
        s.split(',')
            .map(|part| part.trim().to_string())
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
    });

    let options = rekuiper_server::ServerOptions {
        declarative_path: args.declarative.map(std::path::PathBuf::from),
        declarative_watch: args.watch,
        control_plane_url: args.control_plane_url,
        device_id: args.device_id,
        device_name: args.device_name,
        plant: args.plant,
        region: args.region,
        device_type: args.device_type,
        fleet_token: args.fleet_token,
        enable_ui: args.enable_ui,
        ui_allow_ip: ui_allow_ip_vec,
        ui_password: args.ui_password,
    };
    rekuiper_server::start_server_with_options(config, version, options).await?;

    Ok(())
}
