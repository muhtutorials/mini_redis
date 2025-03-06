use bytes::Bytes;

use crate::{Connection, DB, Frame, Parse};

// Get the value of a key.
//
// If the key does not exist the special value nil is returned. An error is
// returned if the value stored at the key is not a string, because "Get" only
// handles string values.
#[derive(Debug)]
pub struct Get {
    // name of the key to get
    key: String,
}

impl Get {
    // create a new "Get" command which fetches the "key"
    pub fn new(key: impl ToString) -> Get {
        Get { key: key.to_string() }
    }

    // get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    // Parse a "Get" instance from a received frame.
    //
    // The "Parse" argument provides a cursor-like API to read fields from the
    // "Frame". At this point the entire frame has already been received from
    // the socket.
    //
    // The "GET" string has already been consumed.
    //
    // Returns the "Get" value on success. If the frame is malformed "Err" is
    // returned.
    //
    // Expects an array frame containing two entries: "GET key".
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Get> {
        // The "GET" string has already been consumed. The next value is the
        // name of the key to get. If the next value is not a string or the
        // input is fully consumed, then an error is returned.
        let key = parse.next_string()?;
        Ok(Get { key })
    }

    // Apply the "Get" command to the specified "DB" instance.
    //
    // The response is written to "conn". This is called by the server in order
    // to execute a received command.
    pub(crate) async fn apply(self, db: &DB, conn: &mut Connection) -> crate::Result<()> {
        // get the value from the shared database state
        let resp = if let Some(value) = db.get(&self.key) {
            // if a value is present, it is written to the client in "Bulk" format
            Frame::Bulk(value)
        } else {
            // if there is no value, "Null" is written
            Frame::Null
        };
        // write response back to the client
        conn.write_frame(&resp).await?;
        Ok(())
    }

    // Converts the command into an equivalent "Frame".
    //
    // This is called by the client when encoding a "Get" command to send to
    // the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("GET".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame
    }
}