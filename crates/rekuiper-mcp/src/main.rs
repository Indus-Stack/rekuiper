use clap::Parser;
use rekuiper_mcp::protocol::JsonRpcRequest;
use rekuiper_mcp::McpHandler;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "rekuiper-mcp",
    about = "Model Context Protocol (MCP) server for rekuiper streaming engine",
    version
)]
struct Args {
    /// Target rekuiper REST endpoint
    #[arg(long, default_value = "http://127.0.0.1:9081")]
    server_url: String,

    /// Bearer authentication token for authenticated rekuiper endpoints
    #[arg(short = 't', long = "token", env = "REKUIPER_TOKEN")]
    token: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Direct all internal logs to stderr so stdout remains exclusively JSON-RPC 2.0 frames
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("rekuiper_mcp=info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();
    info!(
        "Starting rekuiper-mcp server (target engine: {})",
        args.server_url
    );

    let token = args.token.or_else(|| std::env::var("KUIPER_TOKEN").ok());
    let handler = McpHandler::with_token(args.server_url, token);

    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut stdout = tokio::io::stdout();

    let mut line = String::new();

    while let Ok(n) = reader.read_line(&mut line).await {
        if n == 0 {
            // EOF reached
            break;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            line.clear();
            continue;
        }

        match serde_json::from_str::<JsonRpcRequest>(trimmed) {
            Ok(req) => {
                if let Some(resp) = handler.handle_request(req).await {
                    match serde_json::to_string(&resp) {
                        Ok(json_str) => {
                            let _ = stdout.write_all(json_str.as_bytes()).await;
                            let _ = stdout.write_all(b"\n").await;
                            let _ = stdout.flush().await;
                        }
                        Err(e) => {
                            error!("Failed to serialize response: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                error!("Invalid JSON-RPC request received: {}: {}", e, trimmed);
                let err_resp = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": serde_json::Value::Null,
                    "error": {
                        "code": -32700,
                        "message": format!("Parse error: {}", e)
                    }
                });
                let _ = stdout
                    .write_all(serde_json::to_string(&err_resp)?.as_bytes())
                    .await;
                let _ = stdout.write_all(b"\n").await;
                let _ = stdout.flush().await;
            }
        }

        line.clear();
    }

    info!("rekuiper-mcp server shutting down");
    Ok(())
}
