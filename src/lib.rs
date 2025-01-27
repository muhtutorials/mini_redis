mod connection;
use connection::Connection;

mod db;
use db::{DB, DBDropGuard};

pub mod frame;
pub use frame::Frame;

mod parse;
use parse::{Parse, ParseError};

pub mod server;

mod shutdown;
use shutdown::Shutdown;

pub mod clients;
pub use clients::{BlockingClient, BufferedClient, Client};

pub mod cmd;
pub use cmd::Command;

pub const DEFAULT_PORT: &str = "6379";

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T> = std::result::Result<T, Error>;
