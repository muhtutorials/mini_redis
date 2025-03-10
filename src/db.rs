use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{broadcast, Notify};
use tokio::time;
use tokio::time::Instant;

// A wrapper around a "DB" instance. This exists to allow orderly cleanup
// of the "DB" by signalling the background purge task to shut down when
// this struct is dropped.
#[derive(Debug)]
pub(crate) struct DBDropGuard {
    // the "DB" instance that will be shut down when this "DBDropGuard" struct is dropped
    db: DB,
}

// Server state shared across all connections.
//
// "DB" contains a "HashMap" storing the "key/value" data and all
// "broadcast::Sender" values for active "pub/sub" channels.
//
// A "DB" instance is a handle to shared state. Cloning "DB" is shallow and
// only incurs an atomic ref count increment.
//
// When a "DB" value is created, a background task is spawned. This task is
// used to expire values after the requested duration has elapsed. The task
// runs until all instances of "DB" are dropped, at which point the task
// terminates.
#[derive(Debug, Clone)]
pub(crate) struct DB {
    // Handle to the shared state. The background task will also have an
    // "Arc<Shared>".
    shared: Arc<Shared>
}

#[derive(Debug)]
struct Shared {
    // The shared state is guarded by a mutex. This is a "std::sync::Mutex" and
    // not a Tokio mutex. This is because there are no asynchronous operations
    // being performed while holding the mutex. Additionally, the critical
    // sections are very small.
    //
    // A Tokio mutex is mostly intended to be used when locks need to be held
    // across ".await" yield points. All other cases are usually best
    // served by a "std::sync::Mutex". If the critical section does not include any
    // async operations but is long (CPU intensive or performing blocking
    // operations), then the entire operation, including waiting for the mutex,
    // is considered a "blocking" operation and "tokio::task::spawn_blocking"
    // should be used.
    state: Mutex<State>,
    // Notifies the background task handling entry expiration. The background
    // task waits on this to be notified, then checks for expired values or the
    // shutdown signal.
    background_task: Notify,
}


#[derive(Debug)]
struct State {
    // "key/value" data
    entries: HashMap<String, Entry>,
    // The "pub/sub" key space. Redis uses a separate key space for "key/value"
    // and "pub/sub". "mini_redis" handles this by using a separate "HashMap".
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,
    // Tracks key TTLs.
    //
    // A "BTreeSet" is used to maintain expirations sorted by when they expire.
    // This allows the background task to iterate this map to find the value
    // expiring next.
    //
    // While highly unlikely, it is possible for more than one expiration to be
    // created for the same instant. Because of this, the "Instant" is
    // insufficient for the key. A unique key "String" is used to
    // break these ties.
    expirations: BTreeSet<(Instant, String)>,
    // True when the DB instance is shutting down. This happens when all "DB"
    // values drop. Setting this to "true" signals to the background task to
    // exit.
    shutdown: bool,
}

// entry in the "key/value" store
#[derive(Debug)]
struct Entry {
    // stored data
    data: Bytes,
    // instant at which the entry expires and should be removed from the database
    expires_at: Option<Instant>
}

impl DBDropGuard {
    // Creates a new "DBDropGuard" that wraps a "DB" instance. When it is dropped
    // the "DB"'s purge task will be shut down.
    pub(crate) fn new() -> DBDropGuard {
        DBDropGuard{ db: DB::new() }
    }

    // Returns the shared database. Internally, this is an
    // "Arc", so a clone only increments the ref count.
    pub(crate) fn db(&self) -> DB {
        self.db.clone()
    }
}

impl Drop for DBDropGuard {
    fn drop(&mut self) {
        // signal the "DB" instance to shut down the task that purges expired keys
        self.db.shut_down_purge();
    }
}

impl DB {
    // Create a new, empty "DB" instance. Allocates shared state and spawns a
    // background task to manage key expiration.
    pub(crate) fn new() -> DB {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                pub_sub: HashMap::new(),
                expirations: BTreeSet::new(),
                shutdown: false,
            }),
            background_task: Notify::new(),
        });
        // start the background task
        tokio::spawn(purge_expired_tasks(shared.clone()));
        DB { shared }
    }

    // Get the value associated with a key.
    //
    // Returns "None" if there is no value associated with the key. This may be
    // due to never having assigned a value to the key or a previously assigned
    // value expired.
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        // Acquire the lock, get the entry and clone the value.
        //
        // Because data is stored using "Bytes", a clone here is a shallow
        // clone. Data is not copied.
        let state = self.get_state();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    // Set the value associated with a key along with an optional expiration.
    //
    // If a value is already associated with the key, it is removed.
    pub(crate) fn set(&self, key: String, value: Bytes, expiration: Option<Duration>) {
        let mut state = self.get_state();
        // If this "set" becomes the key that expires next, the background
        // task needs to be notified so it can update its state.
        //
        // Whether the task needs to be notified is computed during the
        // "set" routine.
        let mut notify = false;
        let expires_at = expiration.map(|dur| {
            // "Instant" at which the key expires
            let when = Instant::now() + dur;
            // Only notify the worker task if the newly inserted expiration is the
            // next key to evict. In this case, the worker needs to be woken up
            // to update its state.
            notify = state.next_expiration().map(|exp| exp > when).unwrap_or(true);
            when
        });
        // insert the entry into the "HashMap"
        let old_value = state.entries.insert(
            key.clone(),
            Entry {
                data: value,
                expires_at,
            }
        );
        // If there was a value previously associated with the key, and it
        // had an expiration time the associated entry in the "expirations" map
        // must also be removed. This avoids leaking data.
        if let Some(val) = old_value {
            if let Some(when) = val.expires_at {
                // clear expiration
                state.expirations.remove(&(when, key.clone()));
            }
        }
        // Track the expiration. If we insert before remove that will cause a bug
        // when current "(when, key)" equals previous "(when, key)". Remove then insert
        // can avoid this.
        if let Some(when) = expires_at {
            state.expirations.insert((when, key));
        }
        // Release the mutex before notifying the background task. This helps
        // reduce contention by avoiding the background task waking up only to
        // be unable to acquire the mutex due to this function still holding it.
        drop(state);
        if notify {
            // finally, only notify the background task if it needs to update
            // its state to reflect a new expiration
            self.shared.background_task.notify_one();
        }
    }

    // Returns a "Receiver" for the requested channel.
    //
    // The returned "Receiver" is used to receive values broadcast by "PUBLISH"
    // command.
    pub(crate) fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;
        let mut state = self.get_state();
        // If there is no entry for the requested channel, then create a new
        // broadcast channel and associate it with the key. If one already
        // exists, return an associated receiver.
        match state.pub_sub.entry(key) {
            Entry::Occupied(entry) => entry.get().subscribe(),
            Entry::Vacant(entry) => {
                // No broadcast channel exists yet, so create one.
                //
                // The channel is created with a capacity of "1024" messages. A
                // message is stored in the channel until all subscribers
                // have seen it. This means that a slow subscriber could result
                // in messages being held indefinitely.
                //
                // When the channel's capacity fills up, publishing will result
                // in old messages being dropped. This prevents slow consumers
                // from blocking the entire system.
                let (tx, rx) = broadcast::channel(1024);
                entry.insert(tx);
                rx
            }
        }
    }

    // Publish a message to the channel. Returns the number of subscribers
    // listening on the channel.
    pub(crate) fn publish(&self, key: &str, value: Bytes) -> usize {
        let state = self.get_state();
        state
            .pub_sub
            .get(key)
            // On a successful message sending on the broadcast channel, the number
            // of subscribers is returned. An error indicates there are no
            // receivers, in which case, "0" should be returned.
            .map(|tx| tx.send(value).unwrap_or(0))
            // If there is no entry for the channel key, then there are no
            // subscribers. In this case, return "0".
            .unwrap_or(0)
    }

    // Signals the purge background task to shut down. This is called by the
    // "DBDropGuard"'s "Drop" implementation.
    fn shut_down_purge(&self) {
        // The background task must be signaled to shut down. This is done by
        // setting "State::shutdown" to "true" and signaling the task.
        let mut state = self.get_state();
        state.shutdown = true;
        // Drop the lock before signaling the background task. This helps
        // reduce lock contention by ensuring the background task doesn't
        // wake up only to be unable to acquire the mutex.
        drop(state);
        self.shared.background_task.notify_one();
    }

    fn get_state(&self) -> MutexGuard<State> {
        self.shared.state.lock().unwrap()
    }
}

impl Shared {
    // Purge all expired keys and return the `Instant` at which the next
    // key will expire. The background task will sleep until this instant.
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();
        if state.shutdown {
            // The database is shutting down. All handles to the shared state
            // have dropped. The background task should exit.
            return None;
        }
        // This is needed to make the borrow checker happy. In short, "lock"
        // returns a "MutexGuard" and not a "&mut State". The borrow checker is
        // not able to see "through" the mutex guard and determine that it is
        // safe to access both "state.expirations" and "state.entries" mutably,
        // so we get a "real" mutable reference to "State" outside the loop.
        let state = &mut *state;
        // find all the keys scheduled to expire before now
        let now = Instant::now();
        while let Some(&(when, ref key)) = state.expirations.iter().next() {
            if when > now {
                // Done purging, "when" is the instant at which the next key
                // expires. The worker task will wait until this instant.
                return Some(when);
            }
            // remove expired key
            state.entries.remove(key);
            state.expirations.remove(&(when, key.clone()));
        }
        None
    }
    // Returns "true" if the database is shutting down.
    //
    // The "shutdown" flag is set when all "DB" values have dropped, indicating
    // that the shared state can no longer be accessed.
    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations.iter().next().map(|exp| exp.0)
    }
}

// Routine executed by the background task.
//
// Wait to be notified. On notification, purge any expired keys from the shared
// state handle. If "shutdown" is set, terminate the task.
async fn purge_expired_tasks(shared: Arc<Shared>) {
    // if the shutdown flag is set, then the task should exit
    while !shared.is_shutdown() {
        // Purge all keys that are expired. The function returns the instant at
        // which the next key will expire. The worker should wait until the
        // instant has passed then purge again.
        if let Some(when) = shared.purge_expired_keys() {
            // Wait until the next key expires or until the background task
            // is notified. If the task is notified, then it must reload its
            // state as new keys have been set to expire early. This is done by
            // looping.
            tokio::select! {
                _ = time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            // There are no keys expiring in the future. Wait until the task is
            // notified.
            shared.background_task.notified().await;
        }
    }
    println!("purge background task shut down")
}