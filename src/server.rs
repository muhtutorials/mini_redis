use std::future::Future;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};

use crate::{Command, Connection, DB, DBDropGuard, Shutdown};

// Maximum number of concurrent connections the "mini_redis" server will accept.
//
// When this limit is reached, the server will stop accepting connections until
// an active connection terminates.
//
// A real application will want to make this value configurable, but for this
// example it is hardcoded.
const MAX_CONNECTIONS: usize = 250;

// Server listener state. Created in the "run" call. It includes a "run" method
// which performs the TCP listening and initialization of per-connection state.
#[derive(Debug)]
struct Server {
    // Shared database handle.
    //
    // Contains the "key/value" store as well as the broadcast channels for
    // "pub/sub".
    //
    // This holds a wrapper around an "Arc". The internal "DB" can be
    // retrieved and passed into the per-connection state ("Handler").
    db_holder: DBDropGuard,
    // TCP listener supplied by the "run" caller
    listener: TcpListener,
    // A "Semaphore" is used to limit the maximum number of connections. Before
    // attempting to accept a new connection, a permit is acquired from the
    // semaphore. If none are available, the listener waits for one.
    //
    // When handlers complete processing a connection, the permit is returned
    // to the semaphore.
    semaphore: Arc<Semaphore>,
    // Broadcasts a shutdown signal to all active connections.
    //
    // The initial shutdown trigger is provided by the "run" caller. The
    // server is responsible for gracefully shutting down active connections.
    // When a connection task is spawned, it is passed a broadcast receiver
    // handle. When a graceful shutdown is initiated, a "()" value is sent via
    // the "broadcast::Sender". Each active connection receives it, reaches a
    // safe terminal state, and completes the task.
    notify_shutdown: broadcast::Sender<()>,
    // Used as a part of the graceful shutdown process to wait for client
    // connections to complete processing.
    //
    // Tokio channels are closed once all "Sender" handles go out of scope.
    // When a channel is closed, the receiver receives "None". This is
    // leveraged to detect all connection handlers completing. When a
    // connection handler is initialized, it is assigned a clone of
    // "shutdown_complete_tx". When the listener shuts down, it drops the
    // sender held by this "shutdown_complete_tx" field. Once all handler tasks
    // complete, all clones of the "Sender" are also dropped. This results in
    // "shutdown_complete_rx.recv" completing with "None". At this point, it
    // is safe to exit the server process.
    shutdown_complete_tx: mpsc::Sender<()>,
}

// Per-connection handler. Reads requests from "connection" and applies the
// commands to "DB".
#[derive(Debug)]
struct Handler {
    // Shared database handle.
    //
    // When a command is received from "connection", it is applied to "DB".
    // The implementation of the command is in the "cmd" module. Each command
    // will need to interact with "DB" in order to complete the work.
    db: DB,
    // The TCP connection decorated with the Redis protocol codec
    // implemented using a buffered "TcpStream".
    //
    // When "Listener" receives an inbound connection, the "TcpStream" is
    // passed to "Connection::new", which initializes the associated buffers.
    // "Connection" allows the handler to operate at the "frame" level and keep
    // the byte level protocol parsing details encapsulated in "Connection".
    connection: Connection,
    // Listen for shutdown notifications.
    //
    // A wrapper around the "broadcast::Receiver" paired with the sender in
    // "Listener". The connection handler processes requests from the
    // connection until the peer disconnects or a shutdown notification is
    // received from "shutdown". In the latter case, any in-flight work being
    // processed for the peer is continued until it reaches a safe state, at
    // which point the connection is terminated.
    shutdown: Shutdown,
    // Not used directly. Instead, when "Handler" is dropped.
    _shutdown_complete: mpsc::Sender<()>,
}

// Run the "mini_redis" server.
//
// Accepts connections from the supplied listener. For each inbound connection,
// a task is spawned to handle that connection. The server runs until the
// "shutdown" future completes, at which point the server shuts down
// gracefully.
//
// "tokio::signal::ctrl_c()" can be used as the "shutdown" argument. This will
// listen for a "SIGINT" signal.
pub async fn run(listener: TcpListener, shutdown: impl Future) {
    // When the provided "shutdown" future completes, we must send a shutdown
    // message to all active connections. We use a broadcast channel for this
    // purpose. The call below ignores the receiver of the broadcast pair, and when
    // a receiver is needed, the "subscribe" method on the sender is used to create
    // one.
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);
    let mut server = Server {
        db_holder: DBDropGuard::new(),
        listener,
        semaphore: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
    };
    // Concurrently run the server and listen for the "shutdown" signal. The
    // server task runs until an error is encountered, so under normal
    // circumstances, this "select!" statement runs until the "shutdown" signal
    // is received.
    //
    // "select!" statements are written in the form of:
    //
    // ```
    // <result of async op> = <async op> => <step to perform with result>
    // ```
    //
    // All "<async op>" statements are executed concurrently. Once the first
    // "op" completes, its associated "<step to perform with result>" is
    // performed.
    //
    // The "select!" macro is a foundational building block for writing
    // asynchronous Rust. See the API docs for more details:
    //
    // https://docs.rs/tokio/*/tokio/macro.select.html
    tokio::select! {
        res = server.run() => {
            // If an error is received here, accepting connections from the TCP
            // listener failed multiple times and the server is giving up and
            // shutting down.
            //
            // Errors encountered when handling individual connections do not
            // bubble up to this point.
            if let Err(err) = res {
                println!("failed to accept: {err}")
            }
        }
        _ = shutdown => {
            // shutdown signal has been received
            println!("shutting down")
        }
    }
    // Extract the "shutdown_complete" receiver and transmitter,
    // explicitly drop "shutdown_transmitter". This is important, as the
    // "await" below would otherwise never complete.
    let Server {
        notify_shutdown,
        shutdown_complete_tx,
        ..
    } = server;
    // when "notify_shutdown" is dropped, all tasks which have subscribed will
    // receive the shutdown signal and can exit
    drop(notify_shutdown);
    // drop final "Sender" so the "Receiver" below can complete
    drop(shutdown_complete_tx);
    let _ = shutdown_complete_rx.recv().await;
}

impl Server {
    // Run the server.
    //
    // Listen for inbound connections. For each inbound connection, spawn a
    // task to process that connection.
    //
    // Returns "Err" if accepting returns an error. This can happen for a
    // number of reasons that resolve over time. For example, if the underlying
    // operating system has reached an internal limit for maximum number of
    // sockets, accept will fail.
    //
    // The process is not able to detect when a transient error resolves
    // itself. One strategy for handling this is to implement a "backoff"
    // strategy, which is what we do here.
    async fn run(&mut self) -> crate::Result<()> {
        println!("accepting inbound connections");
        loop {
            // Wait for a permit to become available.
            //
            // "acquire_owned" returns a permit that is bound to the semaphore.
            // When the permit value is dropped, it is automatically returned
            // to the semaphore.
            //
            // "acquire_owned" returns an "Err" when the semaphore has been
            // closed. We don't ever close the semaphore, so "unwrap" is safe.
            let permit = self
                .semaphore
                .clone()
                .acquire_owned()
                .await
                .unwrap();
            // Accept a new socket. This will attempt to perform error handling.
            // The "accept" method internally attempts to recover errors, so an
            // error here is non-recoverable.
            let socket = self.accept().await?;
            // create the necessary per-connection handler state
            let mut handler = Handler {
                // get a handle to the shared database
                db: self.db_holder.db(),
                // Initialize the connection state. This allocates "read/write"
                // buffers to perform Redis protocol frame parsing.
                connection: Connection::new(socket),
                // receive shutdown notifications
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                // notifies the receiver half once all clones are dropped
                _shutdown_complete: self.shutdown_complete_tx.clone()
            };
            // Spawn a new task to process the connections. Tokio tasks are like
            // asynchronous green threads and are executed concurrently.
            tokio::spawn(async move {
                // Process the connection. If an error is encountered, log it.
                if let Err(err) = handler.run().await {
                    println!("connection error: {err}");
                }
                // Move the permit into the task and drop it after completion.
                // This returns the permit back to the semaphore.
                drop(permit);
            });
        }
    }

    // Accept an inbound connection.
    //
    // Errors are handled by backing off and retrying. An exponential backoff
    // strategy is used. After the first failure, the task waits for 1 second.
    // After the second failure, the task waits for 2 seconds. Each subsequent
    // failure doubles the wait time. If accepting fails on the 6th try after
    // waiting for 64 seconds, then this function returns an error.
    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 1;
        // try to accept a few times
        loop {
            // Perform the accept operation. If a socket is successfully
            // accepted, return it. Otherwise, save the error.
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        // Accept has failed too many times. Return the error.
                        return Err(err.into());
                    }
                }
            }
            // pause execution until the "backoff" period elapses
            time::sleep(Duration::from_secs(backoff)).await;
            backoff *= 2;
        }
    }
}

impl Handler {
    // Process a single connection.
    //
    // Request frames are read from the socket and processed. Responses are
    // written back to the socket.
    //
    // Currently, pipelining is not implemented. Pipelining is the ability to
    // process more than one request concurrently per connection without
    // interleaving frames. See for more details:
    // https://redis.io/topics/pipelining
    //
    // When the shutdown signal is received, the connection is processed until
    // it reaches a safe state, at which point it is terminated.
    async fn run(&mut self) -> crate::Result<()> {
        // as long as the shutdown signal has not been received, try to read a
        // new request frame
        while !self.shutdown.is_shutdown() {
            // while reading a request frame, also listen for the shutdown signal
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    // If a shutdown signal is received, return from "run".
                    // This will result in the task terminating.
                    return Ok(());
                }
            };
            // If "None" is returned from "read_frame()" then the peer closed
            // the socket. There is no further work to do and the task can be
            // terminated.
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };
            // Convert the Redis frame into a command. This returns an
            // error if the frame is not a valid Redis command, or it is an
            // unsupported command.
            let cmd = Command::from_frame(frame)?;
            // Perform the work needed to apply the command. This may mutate the
            // database state as a result.
            //
            // The connection is passed into the apply function which allows the
            // command to write response frames directly to the connection. In
            // the case of "pub/sub", multiple frames may be sent back to the
            // peer.
            cmd.apply(&self.db, &mut self.connection, &mut self.shutdown).await?;
        };
        Ok(())
    }
}