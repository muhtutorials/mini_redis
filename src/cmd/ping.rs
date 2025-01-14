use bytes::Bytes;
use tracing::{debug, instrument};

use crate::{Connection, Frame, Parse, ParseError};

// Returns "PONG" if no argument is provided, otherwise
// return a copy of the argument as a "Bulk".
//
// This command is often used to test if a connection
// is still alive, or to measure latency.
#[derive(Debug, Default)]
pub struct Ping {
    // optional message to be returned
    message: Option<Bytes>,
}

impl Ping {
    // create a new "Ping" command with optional message
    pub fn new(message: Option<Bytes>) -> Ping {
        Ping { message }
    }

    // Parse a "Ping" instance from a received frame.
    //
    // The "Parse" argument provides a cursor-like API to read fields from the
    // "Frame". At this point, the entire frame has already been received from
    // the socket.
    //
    // The "PING" string has already been consumed.
    //
    // Returns the "Ping" value on success. If the frame is malformed, "Err" is
    // returned.
    //
    // Expects an array frame containing "PING" and an optional message: "PING [message]".
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Ping> {
        match parse.next_bytes() {
            Ok(msg) => Ok(Ping::new(Some(msg))),
            Err(ParseError::EndOfStream) => Ok(Ping::default()),
            Err(e) => Err(e.into()),
        }
    }

    // Apply the "Ping" command and return the message.
    //
    // The response is written to "conn". This is called by the server in order
    // to execute a received command.
    #[instrument(skip(self, conn))]
    pub(crate) async fn apply(self, conn: &mut Connection) -> crate::Result<()> {
        let resp = match self.message {
            Some(msg) => Frame::Bulk(msg),
            None => Frame::Simple("PONG".to_string()),
        };
        debug!(?resp);
        // write response back to the client
        conn.write_frame(&resp).await?;
        Ok(())
    }

    // Converts the command into an equivalent "Frame".
    //
    // This is called by the client when encoding a "Ping" command to send
    // to the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("PING".as_bytes()));
        if let Some(msg) = self.message {
            frame.push_bulk(msg);
        }
        frame
    }
}