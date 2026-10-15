use anyhow::Result;
use clap::Parser;
use rekuiper_conf::PathConfig;

#[derive(Parser, Debug)]
#[command(name = "rekuiperd")]
#[command(about = "rekuiper Daemon - High-performance edge stream processing engine in Rust (kuiperd drop-in)", long_about = None)]
pub struct Args {
    /// Path of etc dir
    #[arg(long, default_value = "etc")]
    pub etc: String,

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
}

pub async fn run_daemon() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let paths = PathConfig::new(Some(&args.etc), Some(&args.data), Some(&args.log));
    let config = paths.load_config()?;

    let version = env!("CARGO_PKG_VERSION").to_string();
    let options = rekuiper_server::ServerOptions {
        declarative_path: args.declarative.map(std::path::PathBuf::from),
        declarative_watch: args.watch,
    };
    rekuiper_server::start_server_with_options(config, version, options).await?;

    Ok(())
}
