use bytes::Bytes;
use clap::{Parser, Subcommand};
use std::num::ParseIntError;
use std::str;
use std::time::Duration;
use mini_redis::{clients::Client, DEFAULT_PORT};

#[derive(Parser, Debug)]
#[command(
    name = "mini_redis_cli",
    version,
    author,
    about = "Issue Redis commands"
)]
struct Cli {
    #[clap(subcommand)]
    command: Command,
    #[arg(id = "hostname", long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,
}

#[derive(Subcommand, Debug)]
enum Command {
    Ping {
        // message to ping
        message: Option<Bytes>
    },
    // get the value of key
    Get {
        // name of key to get
        key: String,
    },
    // set key to hold the value
    Set {
        // name of key to set
        key: String,
        // value to set
        value: Bytes,
        // expire the value after specified amount of time
        #[arg(value_parser = duration_from_ms_str)]
        expiration: Option<Duration>,
    },
    // send a message to a specific channel
    Publish {
        channel: String,
        message: Bytes,
    },
    // subscribe a client to a specific channel
    Subscribe {
        channels: Vec<String>,
    }
}

// Entry point for CLI tool.
//
// The "[tokio::main]" annotation signals that the Tokio runtime should be
// started when the function is called. The body of the function is executed
// within the newly spawned runtime.
//
// `flavor = "current_thread"` is used here to avoid spawning background
// threads. The CLI tool use case benefits more by being lighter instead of
// multithreaded.
#[tokio::main(flavor = "current_thread")]
async fn main() -> mini_redis::Result<()> {
    // parse command line arguments
    let cli = Cli::parse();
    // get the remote address to connect to
    let addr = format!("{}, {}", cli.host, cli.port);
    // establish connection
    let mut client = Client::connect(&addr).await?;
    // process the requested command
    match cli.command {
        Command::Ping { message } => {
            let value = client.ping(message).await?;
            if let Ok(string) = str::from_utf8(&value) {
                println!("\"{}\"", string);
            } else {
                println!("{:?}", value);
            }
        }
        Command::Get { key } => {
            if let Some(value) = client.get(&key).await? {
                if let Ok(string) = str::from_utf8(&value) {
                    println!("\"{}\"", string);
                } else {
                    println!("{:?}", value);
                }
            } else {
                println!("(nil)");
            }
        }
        Command::Set { key, value, expiration: None } => {
            client.set(&key, value).await?;
            println!("OK");
        }
        Command::Set { key, value, expiration: Some(expiration) } => {
            client.set_exp(&key, value, expiration).await?;
            println!("OK");
        }
        Command::Publish { channel, message } => {
            client.publish(&channel, message).await?;
            println!("PUBLISH OK");
        }
        Command::Subscribe { channels } => {
            if channels.is_empty() {
                return Err("channel(s) must be provided".into());
            }
            let mut subscriber = client.subscribe(channels).await?;
            // await messages on channels
            while let Some(msg) = subscriber.next_message().await? {
                println!(
                    "got message from the channel: {}; message: {:?}",
                    msg.channel, msg.content
                )
            }
        }
    }
    Ok(())
}

fn duration_from_ms_str(src: &str) -> Result<Duration, ParseIntError> {
    let ms = src.parse::<u64>()?;
    Ok(Duration::from_millis(ms))
}