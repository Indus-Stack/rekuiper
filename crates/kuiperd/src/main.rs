use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    kuiperd::run_daemon().await
}
