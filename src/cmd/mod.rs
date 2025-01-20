mod get;
pub use get::Get;

mod ping;
pub use ping::Ping;

mod publish;
pub use publish::Publish;

mod set;
pub use set::Set;

mod subscribe;
pub use subscribe::{Subscribe, Unsubscribe};

mod unknown;
pub use unknown::Unknown;

use crate::{Connection, DB, Frame, Parse, Shutdown};

// Enumeration of supported Redis commands.
//
// Methods called on "Command" are delegated to the command implementation.
#[derive(Debug)]
pub enum Command {
    Get(Get),
    Set(Set),
    Publish(Publish),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Ping(Ping),
    Unknown(Unknown),
}

impl Command {
    // Parse a command from a received frame.
    //
    // The "Frame" must represent a Redis command supported by "mini_redis" and
    // be the array variant.
    //
    // On success, the command value is returned, otherwise, "Err" is returned.
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        // The frame value is decorated with "Parse". "Parse" provides a
        // "cursor" like API which makes parsing the command easier.
        //
        // The frame value must be an array variant. Any other frame variants
        // result in an error being returned.
        let mut parse = Parse::new(frame)?;
        // All Redis commands begin with the command name as a string. The name
        // is read and converted to lower case in order to do case-sensitive
        // matching.
        let command_name = parse.next_string()?.to_lowercase();
        // match the command name, delegating the rest of the parsing to the
        // specific command
        let command = match &command_name[..] {
            "get" => Command::Get(Get::parse_frames(&mut parse)?),
            "set" => Command::Set(Set::parse_frames(&mut parse)?),
            "publish" => Command::Publish(Publish::parse_frames(&mut parse)?),
            "subscribe" => Command::Subscribe(Subscribe::parse_frames(&mut parse)?),
            "unsubscribe" => Command::Unsubscribe(Unsubscribe::parse_frames(&mut parse)?),
            "ping" => Command::Ping(Ping::parse_frames(&mut parse)?),
            _ => {
                // The command is not recognized and an "Unknown" command is returned.
                //
                // "return" is called here to skip the "finish()" call below. As
                // the command is not recognized, there is most likely
                // unconsumed fields remaining in the "Parse" instance.
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };
        // Check if there is any remaining unconsumed fields in the "Parse"
        // value. If fields remain, this indicates an unexpected frame format
        // and an error is returned.
        parse.finish()?;
        // the command has been successfully parsed
        Ok(command)
    }

    // Apply the command to the specified "DB" instance.
    //
    // The response is written to "conn". This is called by the server in order
    // to execute a received command.
    pub(crate) async fn apply(
        self,
        db: &DB,
        conn: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        use Command::*;
        match self {
            Get(cmd) => cmd.apply(db, conn).await,
            Set(cmd) => cmd.apply(db, conn).await,
            Publish(cmd) => cmd.apply(db, conn).await,
            Subscribe(cmd) => cmd.apply(db, conn, shutdown).await,
            // "Unsubscribe" cannot be applied. It may only be received from the
            // context of a "Subscribe" command.
            Unsubscribe(_) => Err("'Unsubscribe' is unsupported in this context".into()),
            Ping(cmd) => cmd.apply(conn).await,
            Unknown(cmd) => cmd.apply(conn).await,
        }
    }

    // returns the command name
    pub(crate) fn get_name(&self) -> &str {
        use Command::*;
        match self {
            Get(_) => "get",
            Set(_) => "set",
            Publish(_) => "publish",
            Subscribe(_) => "subscribe",
            // "Unsubscribe" cannot be applied. It may only be received from the
            // context of a "Subscribe" command.
            Unsubscribe(_) => "unsubscribe",
            Ping(_) => "ping",
            Unknown(cmd) => cmd.get_name(),
        }
    }
}