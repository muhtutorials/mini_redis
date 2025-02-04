use std::time::Duration;

use bytes::Bytes;
use tracing::{debug, instrument};

use crate::{Connection, DB, Frame, Parse, ParseError};

// Set key to hold the string value.
//
// If the key already holds a value, it is overwritten, regardless of its type.
// Any previous time to live associated with the key is discarded on successful
// "Set" operation.
//
// Currently, the following options are supported:
//  "EX [seconds]" - set the specified expiration time in seconds.
//  "PX [milliseconds]" - set the specified expiration time in milliseconds.
#[derive(Debug)]
pub struct Set {
    // lookup key
    key: String,
    // value to be stored
    value: Bytes,
    // key expiration
    expiration: Option<Duration>
}

impl Set {
    // Create a new "Set" command which sets "key" to "value".
    //
    // If "expiration" is "Some", the value should expire after the specified
    // duration.
    pub fn new(key: impl ToString, value: Bytes, expiration: Option<Duration>) -> Set {
        Set {
            key: key.to_string(),
            value,
            expiration
        }
    }

    // get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    // get the value
    pub fn value(&self) -> &Bytes {
        &self.value
    }

    // get the expiration
    pub fn expiration(&self) -> Option<Duration> {
        self.expiration
    }

    // Parse a "Set" instance from a received frame.
    //
    // The "Parse" argument provides a cursor-like API to read fields from the
    // "Frame". At this point, the entire frame has already been received from
    // the socket.
    //
    // The "SET" string has already been consumed.
    //
    // Returns the "Set" value on success. If the frame is malformed, "Err" is
    // returned.
    //
    // Expects an array frame containing at least 3 entries:
    //  "SET key value [EX seconds | PX milliseconds]"
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Set> {
        // Read the key to set. This is a required field.
        let key = parse.next_string()?;
        // Read the value to set. This is a required field.
        let value = parse.next_bytes()?;
        // The expiration is optional. If nothing else follows, then it is
        // "None".
        let mut expiration = None;
        match parse.next_string() {
            Ok(s) if s.to_uppercase() == "EX" => {
                // Expiration is specified in seconds. The next value is an
                // integer.
                let secs = parse.next_int()?;
                expiration = Some(Duration::from_secs(secs));
            }
            Ok(s) if s.to_uppercase() == "PX" => {
                // Expiration is specified in milliseconds. The next value is an
                // integer.
                let millis = parse.next_int()?;
                expiration = Some(Duration::from_secs(millis));
            }
            // Currently, mini_redis does not support any of the other "SET"
            // options. An error here results in the connection being
            // terminated. Other connections will continue to operate normally.
            Ok(_) => return Err("currently 'SET' only supports the expiration option".into()),
            // The "EndOfStream" error indicates that there is no further data to
            // parse. In this case, it is a normal runtime situation and
            // indicates there are no specified "SET" options.
            Err(ParseError::EndOfStream) => {},
            // all other errors are bubbled up, resulting in the connection
            // being terminated
            Err(err) => return Err(err.into()),
        }
        Ok(Set { key, value, expiration })
    }

    // Apply the "Set" command to the specified "DB" instance.
    //
    // The response is written to "conn". This is called by the server in order
    // to execute a received command.
    #[instrument(skip(self, db, conn))]
    pub(crate) async fn apply(self, db: &DB, conn: &mut Connection) -> crate::Result<()> {
        // set the value in the shared database state
        db.set(self.key, self.value, self.expiration);
        // create a success response and write it to "conn"
        let resp = Frame::Simple("OK".to_string());
        debug!(?resp);
        conn.write_frame(&resp).await?;
        Ok(())
    }

    // Converts the command into an equivalent "Frame".
    //
    // This is called by the client when encoding a "Set" command to send to
    // the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("SET".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame.push_bulk(self.value);
        if let Some(dur) = self.expiration {
            // Expirations in Redis protocol can be specified in two ways:
            // 1. SET key value EX seconds;
            // 2. SET key value PX milliseconds;
            // We chose the second option because it allows greater precision and
            // "src/bin/cli" parses the expiration argument as milliseconds
            // in "duration_from_millis_str()".
            frame.push_bulk(Bytes::from("PX".as_bytes()));
            frame.push_int(dur.as_millis() as u64);
        }
        frame
    }
}