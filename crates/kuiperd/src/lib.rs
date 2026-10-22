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

const DEFAULT_KUIPER_YAML: &str = include_str!("../../../etc/kuiper.yaml");
const DEFAULT_MQTT_SOURCE_YAML: &str = include_str!("../../../etc/mqtt_source.yaml");
const DEFAULT_CLIENT_YAML: &str = include_str!("../../../etc/client.yaml");

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
