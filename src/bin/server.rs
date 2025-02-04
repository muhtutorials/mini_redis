use clap::Parser;
use tokio::net::TcpListener;
use tokio::signal;

use mini_redis::{server, DEFAULT_PORT};

#[tokio::main]
pub async fn main() -> mini_redis::Result<()> {
    let cli = Cli::parse();
    let port = cli.port.unwrap_or(DEFAULT_PORT);
    // bind a TCP listener
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;
    server::run(listener, signal::ctrl_c()).await;
    Ok(())
}

#[derive(Parser, Debug)]
#[command(name = "mini_redis_server", version, author, about = "A Redis server")]
struct Cli {
    #[arg(long)]
    port: Option<u16>,
}