use crate::{Connection, Frame};

// Represents an "Unknown" command. This is not a real Redis command.
#[derive(Debug)]
pub struct Unknown {
    cmd_name: String,
}

impl Unknown {
    // create a new "Unknown" command which responds to unknown commands
    // issued by clients
    pub(crate) fn new(key: impl ToString) -> Unknown {
        Unknown { cmd_name: key.to_string() }
    }

    // returns the command name
    pub(crate) fn get_name(&self) -> &str {
        &self.cmd_name
    }

    // Responds to the client, indicating the command is not recognized.
    //
    // This usually means the command is not yet implemented by "mini_redis".
    pub(crate) async fn apply(self, conn: &mut Connection) -> crate::Result<()> {
        let resp = Frame::Error(format!("unknown command '{}'", self.cmd_name));
        conn.write_frame(&resp).await?;
        Ok(())
    }
}