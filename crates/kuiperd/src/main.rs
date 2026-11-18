use anyhow::Result;
use sqlx::{Connection, SqliteConnection};

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::args_os()
        .nth(1)
        .is_some_and(|arg| arg == "--sqlite-version")
    {
        let mut connection = SqliteConnection::connect("sqlite::memory:").await?;
        let version: String = sqlx::query_scalar("SELECT sqlite_version()")
            .fetch_one(&mut connection)
            .await?;
        println!("{version}");
        return Ok(());
    }
    kuiperd::run_daemon().await
}
