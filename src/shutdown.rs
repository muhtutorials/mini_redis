use tokio::sync::broadcast;

// Listens for the server shutdown signal.
//
// Shutdown is signalled using a "broadcast::Receiver". Only a single value is
// ever sent. Once a value has been sent via the broadcast channel, the server
// should shut down.
//
// The "Shutdown" struct listens for the signal and tracks that the signal has
// been received. Callers may query for whether the shutdown signal has been
// received or not.
#[derive(Debug)]
pub(crate) struct Shutdown {
    // "true" if the shutdown signal has been received
    is_shutdown: bool,
    // the receiver of the channel used to listen for shutdown
    notify: broadcast::Receiver<()>
}

impl Shutdown {
    // create a new "Shutdown" backed by the given "broadcast::Receiver"
    pub(crate) fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Shutdown {
            is_shutdown: false,
            notify,
        }
    }

    // returns "true" if the shutdown signal has been received
    pub(crate) fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    // receive the shutdown notice, waiting if necessary
    pub(crate) async fn recv(&mut self) {
        // if the shutdown signal has already been received, then return
        // immediately
        if self.is_shutdown {
            return;
        }
        // cannot receive a "lag error" as only one value is ever sent.
        let _ = self.notify.recv().await;
        // remember that the signal has been received
        self.is_shutdown = true;
    }
}