use async_stream::try_stream;
use bytes::Bytes;
use std::io::{Error, ErrorKind};
use std::time::Duration;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_stream::Stream;
use tracing::{debug, instrument};

use crate::cmd::{Get, Ping, Publish, Set, Subscribe, Unsubscribe};
use crate::{Connection, Frame};

// Established connection with a Redis server.
//
// Backed by a single "TcpStream", "Client" provides basic network client
// functionality (no pooling, retrying, etc.). Connections are established using
// the ["connect"](fn@connect) function.
//
// Requests are issued using the various methods of "Client".
pub struct Client {
    // The TCP connection decorated with the Redis protocol codec is
    // implemented using a buffered "TcpStream".
    //
    // When "Listener" receives an inbound connection, the "TcpStream" is
    // passed to "Connection::new", which initializes the associated buffers.
    // "Connection" allows the handler to operate at the "frame" level and keep
    // the byte level protocol parsing details encapsulated in "Connection".
    connection: Connection,
}

// A client that has entered "pub/sub" mode.
//
// Once client subscribes to a channel, they may only perform "pub/sub" related
// commands. The "Client" type is transitioned to a "Subscriber" type in order
// to prevent non "pub/sub" methods from being called.
pub struct Subscriber {
    client: Client,
    // the set of channels to which the "Subscriber" is currently subscribed
    subscribed_channels: Vec<String>,
}

// message received on the subscribed channel
#[derive(Debug, Clone)]
pub struct Message {
    pub channel: String,
    pub content: Bytes,
}

impl Client {
    /// Establish a connection with the Redis server located at `addr`.
    ///
    /// `addr` may be any type that can be asynchronously converted to a
    /// `SocketAddr`. This includes `SocketAddr` and strings. The `ToSocketAddrs`
    /// trait is the Tokio version and not the `std` version.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use mini_redis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = match Client::connect("localhost:6379").await {
    ///         Ok(client) => client,
    ///         Err(_) => panic!("failed to establish connection"),
    ///     };
    /// # drop(client);
    /// }
    /// ```
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<Client> {
        // The "addr" argument is passed directly to "TcpStream::connect". This
        // performs any asynchronous DNS lookup and attempts to establish a TCP
        // connection. An error at either step returns an error, which is then
        // bubbled up to the caller of "mini_redis" connect.
        let socket = TcpStream::connect(addr).await?;
        // Initialize the connection state. This allocates "read/write" buffers to
        // perform Redis protocol frame parsing.
        let connection = Connection::new(socket);
        Ok(Client { connection })
    }

    /// Ping to the server.
    ///
    /// Returns "PONG" if no argument is provided, otherwise
    /// returns a copy of the argument as a bulk.
    ///
    /// This command is often used to test if a connection
    /// is still alive, or to measure latency.
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage.
    /// ```no_run
    /// use mini_redis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let pong = client.ping(None).await.unwrap();
    ///     assert_eq!(b"PONG", &pong[..]);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn ping(&mut self, message: Option<Bytes>) -> crate::Result<Bytes> {
        let frame = Ping::new(message).into_frame();
        // todo: What does it do?
        debug!(request = ?frame);
        self.connection.write_frame(&frame).await?;
        match self.read_response().await? {
            Frame::Simple(val) => Ok(val.into()),
            Frame::Bulk(val) => Ok(val),
            frame => Err(frame.to_error()),
        }
    }

    /// Get the value of key.
    ///
    /// If the key does not exist the special value `None` is returned.
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage.
    ///
    /// ```no_run
    /// use mini_redis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let val = client.get("foo").await.unwrap();
    ///     println!("Got = {:?}", val);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn get(&mut self, key: &str) -> crate::Result<Option<Bytes>> {
        // create a "Get" command for the key and convert it into a frame
        let frame = Get::new(key).into_frame();
        debug!(request = ?frame);
        // Write the frame to the socket. This writes the full frame to the
        // socket, waiting if necessary.
        self.connection.write_frame(&frame).await?;
        // Wait for the response from the server.
        //
        // Both "Simple" and "Bulk" frames are accepted. "Null" represents the
        // key not being present and "None" is returned.
        match self.read_response().await? {
            Frame::Simple(val) => Ok(Some(val.into())),
            Frame::Bulk(val) => Ok(Some(val)),
            Frame::Null => Ok(None),
            frame => Err(frame.to_error()),
        }
    }


    /// Set `key` to hold the given `value`.
    ///
    /// The `value` is associated with `key` until it is overwritten by the next
    /// call to `SET` or it is removed.
    ///
    /// If key already holds a value, it is overwritten. Any previous "time to live"
    /// associated with the key is discarded on successful `SET` operation.
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage.
    ///
    /// ```no_run
    /// use mini_redis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     client.set("foo", "bar".into()).await.unwrap();
    ///
    ///     // getting the value immediately works
    ///     let val = client.get("foo").await.unwrap().unwrap();
    ///     assert_eq!(val, "bar");
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn set(&mut self, key: &str, value: Bytes) -> crate::Result<()> {
        // Create a "Set" command and pass it to "set_cmd". A separate method is
        // used to set a value with an expiration. The common parts of both
        // methods are implemented by "set_cmd".
        self.set_cmd(Set::new(key, value, None)).await
    }

    /// Set `key` to hold the given `value`. The value expires after `expiration`.
    ///
    /// The `value` is associated with `key` until one of the following:
    /// - it expires;
    /// - it is overwritten by the next call to `SET`;
    /// - it is removed.
    ///
    /// If key already holds a value, it is overwritten. Any previous "time to live"
    /// associated with the key is discarded on a successful "SET" operation.
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage. This example doesn't guarantee to always
    /// work as it relies on time based logic and assumes the client and server
    /// stay relatively synchronized in time. The real world tends to not be so
    /// favorable.
    ///
    /// ```no_run
    /// use mini_redis::clients::Client;
    /// use tokio::time;
    /// use std::time::Duration;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let ttl = Duration::from_millis(500);
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     client.set_exp("foo", "bar".into(), ttl).await.unwrap();
    ///
    ///     // getting the value immediately works
    ///     let val = client.get("foo").await.unwrap().unwrap();
    ///     assert_eq!(val, "bar");
    ///
    ///     // wait for the TTL to expire
    ///     time::sleep(ttl).await;
    ///
    ///     let val = client.get("foo").await.unwrap();
    ///     assert!(val.is_some());
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn set_exp(
        &mut self,
        key: &str,
        value: Bytes,
        expiration: Duration
    ) -> crate::Result<()> {
        // Create a "Set" command and pass it to "set_cmd". A separate method is
        // used to set a value with an expiration. The common parts of both
        // methods are implemented by "set_cmd".
        self.set_cmd(Set::new(key, value, Some(expiration))).await
    }

    // the core "SET" logic, used by both "set" and "set_expires"
    async fn set_cmd(&mut self, cmd: Set) -> crate::Result<()> {
        // convert the "Set" command into a frame
        let frame = cmd.into_frame();
        debug!(request = ?frame);
        // Write the frame to the socket. This writes the full frame to the
        // socket, waiting if necessary.
        self.connection.write_frame(&frame).await?;
        // Wait for the response from the server. On success, the server
        // responds simply with "OK". Any other response indicates an error.
        match self.read_response().await? {
            Frame::Simple(resp) if resp == "OK" => Ok(()),
            frame => Err(frame.to_error()),
        }
    }

    // Reads a response frame from the socket.
    //
    // If an "Error" frame is received, it is converted to "Err".
    async fn read_response(&mut self) -> crate::Result<Frame> {
        let response = self.connection.read_frame().await?;
        debug!(?response);
        match response {
            // "Error" frames are converted to "Err"
            Some(Frame::Error(e)) => Err(e.into()),
            Some(frame) => Ok(frame),
            None => {
                // Receiving "None" here indicates the server has closed the
                // connection without sending a frame. This is unexpected and is
                // represented as a "connection reset by peer" error.
                let err = Error::new(
                    ErrorKind::ConnectionReset,
                    "connection reset by server",
                );
                Err(err.into())
            }
        }
    }
}