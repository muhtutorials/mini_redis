//! Minimal blocking Redis client implementation.
//!
//! Provides a blocking "connect" and methods for issuing the supported commands.

use std::time::Duration;

use bytes::Bytes;
use tokio::net::ToSocketAddrs;
use tokio::runtime::Runtime;

pub use crate::clients::Message;

// Established connection with a Redis server.
//
// Backed by a single "TcpStream", "Client" provides basic network client
// functionality (no pooling, retrying, etc.). Connection is established using
// the "connect" function.
//
// Requests are issued using the various methods of "Client".
pub struct BlockingClient {
    // asynchronous "Client"
    inner: crate::clients::Client,
    // "current_thread" runtime for executing operations on the asynchronous
    // client in a blocking manner
    rt: Runtime,
}

// A client that has entered "pub/sub" mode.
//
// Once a client subscribes to a channel, it may only perform "pub/sub" related
// commands. The "BlockingClient" type is transitioned to a
// "BlockingSubscriber" type in order to prevent non "pub/sub" methods from being
// called.
pub struct BlockingSubscriber {
    // asynchronous "Subscriber"
    inner: crate::clients::Subscriber,
    // "current_thread" runtime for executing operations on the asynchronous
    // client in a blocking manner
    rt: Runtime,
}

// iterator returned by "Subscriber::into_iter"
pub struct SubscriberIterator {
    // asynchronous "Subscriber"
    inner: crate::clients::Subscriber,
    // "current_thread" runtime for executing operations on the asynchronous
    // client in a blocking manner
    rt: Runtime,
}

impl BlockingClient {
    /// Establish a connection with the Redis server located at `addr`.
    ///
    /// `addr` may be any type that can be asynchronously converted to a
    /// `SocketAddr`. This includes `SocketAddr` and strings. The `ToSocketAddrs`
    /// trait is the Tokio version and not the `std` version.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use mini_redis::clients::BlockingClient;
    ///
    /// fn main() {
    ///     let client = match BlockingClient::connect("localhost:6379") {
    ///         Ok(client) => client,
    ///         Err(_) => panic!("failed to establish connection"),
    ///     };
    /// # drop(client);
    /// }
    /// ```
    pub fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<BlockingClient> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let inner = rt.block_on(crate::clients::Client::connect(addr))?;
        Ok(BlockingClient { inner, rt })
    }

    /// Get the value of a key.
    ///
    /// If the key does not exist the special value `None` is returned.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use mini_redis::clients::BlockingClient;
    ///
    /// fn main() {
    ///     let mut client = BlockingClient::connect("localhost:6379").unwrap();
    ///     let val = client.get("foo").unwrap();
    ///     println!("Got = {:?}", val);
    /// }
    /// ```
    pub fn get(&mut self, key: &str) -> crate::Result<Option<Bytes>> {
        self.rt.block_on(self.inner.get(key))
    }

    /// Set a `key` to hold the given `value`.
    ///
    /// The `value` is associated with the `key` until it is overwritten by the next
    /// call to `SET` or it is removed.
    ///
    /// If the key already holds a value, it is overwritten. Any previous `time to live`
    /// associated with the key is discarded on successful `SET` operation.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use mini_redis::clients::BlockingClient;
    ///
    /// fn main() {
    ///     let mut client = BlockingClient::connect("localhost:6379").unwrap();
    ///     client.set("foo", "bar".into()).unwrap();
    ///     // getting the value immediately works
    ///     let val = client.get("foo").unwrap().unwrap();
    ///     assert_eq!(val, "bar");
    /// }
    /// ```
    pub fn set(&mut self, key: &str, value: Bytes) -> crate::Result<()> {
        self.rt.block_on(self.inner.set(key, value))
    }

    /// Set a `key` to hold the given `value`. The value expires after `expiration`.
    ///
    /// The `value` is associated with the `key` until one of the following:
    /// - it expires;
    /// - it is overwritten by the next call to `SET`;
    /// - it is removed.
    ///
    /// If the key already holds a value, it is overwritten. Any previous `time to live`
    /// associated with the key is discarded on a successful `SET` operation.
    ///
    /// # Examples
    ///
    /// This example doesn't guarantee to always
    /// work as it relies on time based logic and assumes the client and server
    /// stay relatively synchronized in time. The real world tends to not be so
    /// favorable.
    ///
    /// ```no_run
    /// use mini_redis::clients::BlockingClient;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// fn main() {
    ///     let ttl = Duration::from_millis(500);
    ///     let mut client = BlockingClient::connect("localhost:6379").unwrap();
    ///     client.set_exp("foo", "bar".into(), ttl).unwrap();
    ///     // getting the value immediately works
    ///     let val = client.get("foo").unwrap().unwrap();
    ///     assert_eq!(val, "bar");
    ///     // wait for the TTL to expire
    ///     thread::sleep(ttl);
    ///     let val = client.get("foo").unwrap();
    ///     assert!(val.is_some());
    /// }
    /// ```
    pub async fn set_exp(
        &mut self,
        key: &str,
        value: Bytes,
        expiration: Duration
    ) -> crate::Result<()> {
        self.rt.block_on(self.inner.set_exp(key, value, expiration))
    }

    /// Posts a `message` to the given `channel`.
    ///
    /// Returns the number of subscribers currently listening on the channel.
    /// There is no guarantee that these subscribers receive the message as they
    /// may disconnect at any time.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use mini_redis::clients::BlockingClient;
    ///
    /// fn main() {
    ///     let mut client = BlockingClient::connect("localhost:6379").unwrap();
    ///     let val = client.publish("foo", "bar".into()).unwrap();
    ///     println!("Got = {:?}", val);
    /// }
    /// ```
    pub fn publish(&mut self, channel: &str, message: Bytes) -> crate::Result<u64> {
        self.rt.block_on(self.inner.publish(channel, message))
    }

    // Subscribes the client to the specified channels.
    //
    // Once a client issues a "Subscribe" command, it may no longer issue any
    // non "pub/sub" commands. The method consumes "self" and returns a
    // "BlockingSubscriber".
    //
    // The "BlockingSubscriber" value is used to receive messages as well as
    // manage the list of channels the client is subscribed to.
    pub fn subscribe(self, channels: Vec<String>) -> crate::Result<BlockingSubscriber> {
        let subscriber = self.rt.block_on(self.inner.subscribe(channels))?;
        Ok(BlockingSubscriber {
            inner: subscriber,
            rt: self.rt,
        })
    }
}

impl BlockingSubscriber {
    // returns channels client is subscribed to
    pub fn get_subs(&self) -> &[String] {
        &self.inner.get_subs()
    }

    // Receive the next message published on a subscribed channel, waiting if
    // necessary.
    //
    // "None" indicates the subscription has been terminated.
    pub fn next_message(&mut self) -> crate::Result<Option<Message>> {
        self.rt.block_on(self.inner.next_message())
    }

    // convert the subscriber into an "Iterator" yielding new messages published
    // on subscribed channels.
    pub fn into_iter(self) -> impl Iterator<Item = crate::Result<Message>> {
        SubscriberIterator {
            inner: self.inner,
            rt: self.rt,
        }
    }

    // subscribe to a list of new channels
    pub fn subscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        self.rt.block_on(self.inner.subscribe(channels))
    }

    // unsubscribe from a list of channels
    pub fn unsubscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        self.rt.block_on(self.inner.unsubscribe(channels))
    }
}

impl Iterator for SubscriberIterator {
    type Item = crate::Result<Message>;
    fn next(&mut self) -> Option<Self::Item> {
        self.rt.block_on(self.inner.next_message()).transpose()
    }
}