use clap::Parser;
use tokio::net::TcpListener;
use tokio::signal;

use mini_redis::{server, DEFAULT_PORT};

#[tokio::main]
pub async fn main() -> mini_redis::Result<()> {
    let args = Args::parse();
    let port = args.port.unwrap_or(DEFAULT_PORT);
    let addr = &format!("127.0.0.1:{port}");
    // bind a TCP listener
    let listener = TcpListener::bind(addr).await?;
    println!("`mini_redis` server is listening at: {addr}");
    server::run(listener, signal::ctrl_c()).await;
    Ok(())
}

#[derive(Parser, Debug)]
#[command(name = "mini_redis_server", version, author, about = "Redis server")]
struct Args {
    #[arg(long)]
    port: Option<u16>,
}