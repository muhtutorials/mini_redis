use bytes::Bytes;

use crate::{Connection, DB, Frame, Parse};

// Posts a message to the given channel.
//
// Sends a message into a channel without any knowledge of individual consumers.
// Consumers may subscribe to channels in order to receive the messages.
//
// Channel names have no relation to the "key/value" namespace. Publishing on a
// channel named "foo" has no relation to setting the "foo" key.
#[derive(Debug)]
pub struct Publish {
    // name of the channel on which the message should be published
    channel: String,
    // message to publish
    message: Bytes,
}

impl Publish {
    // create a new "Publish" command which sends a "message" on a "channel"
    pub(crate) fn new(channel: impl ToString, message: Bytes) -> Publish {
        Publish {
            channel: channel.to_string(),
            message,
        }
    }

    // Parse a "Publish" instance from a received frame.
    //
    // The "Parse" argument provides a cursor-like API to read fields from the
    // "Frame". At this point, the entire frame has already been received from
    // the socket.
    //
    // The "PUBLISH" string has already been consumed.
    //
    // On success, the "Publish" value is returned. If the frame is malformed,
    // "Err" is returned.
    //
    // Expects an array frame containing three entries: "PUBLISH channel message".
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Publish> {
        // The "PUBLISH" string has already been consumed. Extract the "channel"
        // and "message" values from the frame.
        //
        // The "channel" must be a valid string.
        let channel = parse.next_string()?;
        // the "message" is arbitrary bytes
        let message = parse.next_bytes()?;
        Ok(Publish { channel, message })
    }

    // Apply the "Publish" command to the specified "DB" instance.
    //
    // The response is written to "conn". This is called by the server in order
    // to execute a received command.
    pub(crate) async fn apply(self, db: &DB, conn: &mut Connection) -> crate::Result<()> {
        // The shared state contains the "tokio::sync::broadcast::Sender" for
        // all active channels. Calling "db.publish" dispatches the message into
        // the appropriate channel.
        //
        // The number of subscribers currently listening on the channel is
        // returned. This does not mean that "num_subscribers" channels will
        // receive the message. Subscribers may drop before receiving the
        // message. Given this, "num_subscribers" should only be used as a
        // "hint".
        let num_subscribers = db.publish(&self.channel, self.message);
        // the number of subscribers is returned as the response to the publish
        // request
        let resp = Frame::Integer(num_subscribers as u64);
        // write the frame to the client
        conn.write_frame(&resp).await?;
        Ok(())
    }

    // Converts the command into an equivalent "Frame".
    //
    // This is called by the client when encoding a "Publish" command to send
    // to the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("PUBLISH".as_bytes()));
        frame.push_bulk(Bytes::from(self.channel.into_bytes()));
        frame.push_bulk(self.message);
        frame
    }
}