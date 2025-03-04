//! Minimal Redis client implementation.
//!
//! Provides an async "connect" and methods for issuing the supported commands.

use std::io::{Error, ErrorKind};
use std::time::Duration;

use async_stream::try_stream;
use bytes::Bytes;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_stream::Stream;

use crate::cmd::{Get, Ping, Publish, Set, Subscribe, Unsubscribe};
use crate::{Connection, Frame};

// Established connection with a Redis server.
//
// Backed by a single "TcpStream", "Client" provides basic network client
// functionality (no pooling, retrying, etc.). Connection is established using
// the "connect" function.
//
// Requests are issued using the various methods of "Client".
pub struct Client {
    // The TCP connection decorated with the Redis protocol codec
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
// Once a client subscribes to a channel, it may only perform "pub/sub" related
// commands. The "Client" type is transitioned to a "Subscriber" type in order
// to prevent non "pub/sub" methods from being called.
pub struct Subscriber {
    client: Client,
    // channels to which the "Subscriber" is currently subscribed
    subs: Vec<String>,
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
    pub async fn ping(&mut self, message: Option<Bytes>) -> crate::Result<Bytes> {
        let frame = Ping::new(message).into_frame();
        self.connection.write_frame(&frame).await?;
        match self.read_response().await? {
            Frame::Simple(val) => Ok(val.into()),
            Frame::Bulk(val) => Ok(val),
            frame => Err(frame.to_error()),
        }
    }

    /// Get the value of a key.
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
    pub async fn get(&mut self, key: &str) -> crate::Result<Option<Bytes>> {
        // create a "Get" command for the key and convert it into a frame
        let frame = Get::new(key).into_frame();
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
    /// If key already holds a value, it is overwritten. Any previous `time to live`
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
    /// If key already holds a value, it is overwritten. Any previous `time to live`
    /// associated with the key is discarded on a successful `SET` operation.
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

    // the core "SET" logic, used by both "set" and "set_exp"
    async fn set_cmd(&mut self, cmd: Set) -> crate::Result<()> {
        // convert the "Set" command into a frame
        let frame = cmd.into_frame();
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

    /// Posts `message` to the given `channel`.
    ///
    /// Returns the number of subscribers currently listening on the channel.
    /// There is no guarantee that these subscribers receive the message as they
    /// may disconnect at any time.
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
    ///     let val = client.publish("foo", "bar".into()).await.unwrap();
    ///     println!("Got = {:?}", val);
    /// }
    /// ```
    pub async fn publish(&mut self, channel: &str, message: Bytes) -> crate::Result<u64> {
        // convert the "Publish" command into a frame
        let frame = Publish::new(channel, message).into_frame();
        // write the frame to the socket
        self.connection.write_frame(&frame).await?;
        // read the response
        match self.read_response().await? {
            Frame::Integer(resp) => Ok(resp),
            frame => Err(frame.to_error()),
        }
    }

    // Subscribes the client to the specified channels.
    //
    // Once a client issues a "Subscribe" command, it may no longer issue any
    // non "pub/sub" commands. The method consumes "self" and returns a "Subscriber".
    //
    // The "Subscriber" value is used to receive messages as well as manage the
    // list of channels the client is subscribed to.
    pub async fn subscribe(mut self, channels: Vec<String>) -> crate::Result<Subscriber> {
        // Issue the "Subscribe" command to the server and wait for confirmation.
        // The client will then have been transitioned into the "Subscriber"
        // state and may only issue "pub/sub" commands from that point on.
        self.subscribe_cmd(&channels).await?;
        Ok(Subscriber {
            client: self,
            subs: channels,
        })
    }

    // the core "SUBSCRIBE" logic, used by miscellaneous subscribe methods
    async fn subscribe_cmd(&mut self, channels: &[String]) -> crate::Result<()> {
        // convert the "Subscribe" command into frame
        let frame = Subscribe::new(channels.to_vec()).into_frame();
        // write the frame to the socket
        self.connection.write_frame(&frame).await?;
        // for each channel being subscribed to, the server responds with a
        // message confirming subscription to that channel
        for channel in channels {
            // read the response
            let response = self.read_response().await?;
            // verify confirmation of subscription
            match response {
                Frame::Array(ref frame) => match frame.as_slice() {
                    // The server responds with an array frame in the form of:
                    //  ["SUBSCRIBE", channel, num_subscribed]
                    // Where channel is the name of the channel and
                    // num_subscribed is the number of channels that the client
                    // is currently subscribed to.
                    [sub, ch, ..] if *sub == "SUBSCRIBE" && *ch == channel => {},
                    _ => return Err(response.to_error()),
                },
                frame => return Err(frame.to_error()),
            }
        }
        Ok(())
    }

    // Reads a response frame from the socket.
    //
    // If an "Error" frame is received, it is converted to "Err".
    async fn read_response(&mut self) -> crate::Result<Frame> {
        let response = self.connection.read_frame().await?;
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

impl Subscriber {
    // returns channels "Subscriber" is subscribed to
    pub fn get_subs(&self) -> &[String] {
        &self.subs
    }

    // Receive the next message published on a subscribed channel, waiting if
    // necessary.
    //
    // "None" indicates the subscription has been terminated.
    pub async fn next_message(&mut self) -> crate::Result<Option<Message>> {
        match self.client.connection.read_frame().await? {
            Some(frame) => {
                match frame {
                    Frame::Array(ref frames) => match frames.as_slice() {
                        [message, channel, content] if *message == "MESSAGE" => {
                            Ok(Some(Message {
                                channel: channel.to_string(),
                                content: Bytes::from(content.to_string()),
                            }))
                        }
                        _ => Err(frame.to_error())
                    },
                    frame => Err(frame.to_error()),
                }
            }
            None => Ok(None)
        }
    }

    // Convert the subscriber into a "Stream" yielding new messages published
    // on subscribed channels.
    //
    // "Subscriber" does not implement stream itself as doing so with safe code
    // is non-trivial. The usage of "async/await" would require a manual "Stream"
    // implementation to use "unsafe" code. Instead, a conversion function is
    // provided and the returned stream is implemented with the help of the
    // "async-stream" crate.
    pub fn into_stream(mut self) -> impl Stream<Item = crate::Result<Message>> {
        // Uses the "try_stream" macro from the "async-stream" crate. Generators
        // are not stable in Rust. The crate uses a macro to simulate generators
        // on top of "async/await". There are limitations, so read the
        // documentation there.
        try_stream! {
            while let Some(msg) = self.next_message().await? {
                yield msg;
            }
        }
    }

    // subscribe to a list of new channels
    pub async fn subscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        self.client.subscribe_cmd(channels).await?;
        // update the set of subscribed channels
        // todo: not totally clear what's going on here
        self.subs.extend(channels.iter().map(Clone::clone));
        Ok(())
    }

    // unsubscribe from a list of channels
    pub async fn unsubscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        let frame = Unsubscribe::new(channels).into_frame();
        // write frame to the socket
        self.client.connection.write_frame(&frame).await?;
        // if the input channel list is empty, server considers it as unsubscribing
        // from all subscribed channels, so we assert that the unsubscribe list received
        // matches the client subscribed one
        let num = if channels.is_empty() {
            self.subs.len()
        } else {
            channels.len()
        };
        // read the response
        for _ in 0..num {
            let resp = self.client.read_response().await?;
            match resp {
                Frame::Array(ref frame) => match frame.as_slice() {
                    [unsubscribe, channel, ..] if *unsubscribe == "UNSUBSCRIBE" => {
                        let len = self.subs.len();
                        if len == 0 {
                            // there must be at least one channel
                            return Err(resp.to_error());
                        }
                        // unsubscribed channel should exist in the subscribed list at this point
                        self.subs.retain(|ch| *channel != &ch[..]);
                        // only a single channel should be removed from the
                        // list of subscribed channels
                        if self.subs.len() != len - 1 {
                            return Err(resp.to_error());
                        }
                    }
                    _ => return Err(resp.to_error()),
                },
                frame => return Err(frame.to_error()),
            }
        }
        Ok(())
    }
}