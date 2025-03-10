use std::{fmt, str, vec};
use std::fmt::{Debug, Display, Formatter};

use bytes::Bytes;

use crate::Frame;

// Utility for parsing a command.
//
// Commands are represented as array frames. Each entry in the frame is a
// "token". A "Parse" is initialized with the array frame and provides a
// cursor-like API. Each command struct includes a "parse_frame" method that
// uses a "Parse" to extract its fields.
#[derive(Debug)]
pub(crate) struct Parse {
    // array frame iterator
    parts: vec::IntoIter<Frame>,
}

// Error encountered while parsing a frame.
//
// Only "EndOfStream" errors are handled at runtime. All other errors result in
// the connection being terminated.
#[derive(Debug)]
pub(crate) enum ParseError {
    // attempting to extract a value failed due to the frame being fully consumed
    EndOfStream,
    // all other errors
    Other(crate::Error),
}

impl Parse {
    // Create a new "Parse" to parse the contents of "Frame".
    //
    // Returns "Err" if "Frame" is not an array frame.
    pub(crate) fn new(frame: Frame) -> Result<Parse, ParseError> {
        let array = match frame {
            Frame::Array(arr) => arr,
            frame => return Err(format!("protocol error; expected array, got {:?}", frame).into()),
        };
        Ok(Parse { parts: array.into_iter() })
    }

    // Return the next entry. Array frames are arrays of frames, so the next
    // entry is a frame.
    fn next(&mut self) -> Result<Frame, ParseError> {
        self.parts.next().ok_or(ParseError::EndOfStream)
    }

    // Return the next entry as a "String".
    //
    // If the next entry cannot be represented as a "String", then an error is returned.
    pub(crate) fn next_string(&mut self) -> Result<String, ParseError> {
        match self.next()? {
            // Both "Simple" and "Bulk" representations may be strings. Strings
            // are parsed into UTF-8.
            //
            // While errors are stored as strings, they are considered separate
            // types.
            Frame::Simple(s) => Ok(s),
            Frame::Bulk(data) => str::from_utf8(&data[..])
                .map(|s| s.to_string())
                .map_err(|_| "protocol error; invalid string".into()),
            frame => Err(format!("protocol error; expected 'Simple' frame or 'Bulk' frame, got {:?}", frame).into()),
        }
    }

    // Return the next entry as raw bytes.
    //
    // If the next entry cannot be represented as raw bytes, then an error is returned.
    pub(crate) fn next_bytes(&mut self) -> Result<Bytes, ParseError> {
        match self.next()? {
            // Both "Simple" and "Bulk" representation may be raw bytes.
            //
            // While errors are stored as strings and could be represented as raw bytes,
            // they are considered separate types.
            Frame::Simple(s) => Ok(Bytes::from(s.into_bytes())),
            Frame::Bulk(data) => Ok(data),
            frame => Err(format!("protocol error; expected 'Simple' frame or 'Bulk' frame, got {:?}", frame).into()),
        }
    }

    // Return the next entry as an integer.
    //
    // This includes "Simple", "Bulk", and "Integer" frame types. "Simple" and
    // "Bulk" frame types are parsed.
    //
    // If the next entry cannot be represented as an integer, then an error is
    // returned.
    pub(crate) fn next_int(&mut self) -> Result<u64, ParseError> {
        use atoi::atoi;
        const MSG: &str = "protocol error; invalid number";
        match self.next()? {
            // an integer frame type is already stored as an integer
            Frame::Integer(v) => Ok(v),
            // "Simple" and "Bulk" frames must be parsed as integers. If the parsing
            // fails, an error is returned.
            Frame::Simple(data) => atoi::<u64>(data.as_bytes()).ok_or_else(|| MSG.into()),
            Frame::Bulk(data) => atoi::<u64>(&data).ok_or_else(|| MSG.into()),
            frame => Err(format!("protocol error; expected 'Integer' frame, got {:?}", frame).into()),
        }
    }

    // ensure there are no more entries in the array
    pub(crate) fn finish(&mut self) -> Result<(), ParseError> {
        if self.parts.next().is_none() {
            Ok(())
        } else {
            Err("protocol error; expected end of frame, but there was more data".into())
        }
    }
}

impl From<String> for ParseError {
    fn from(value: String) -> Self {
        ParseError::Other(value.into())
    }
}

impl From<&str> for ParseError {
    fn from(value: &str) -> Self {
        value.to_string().into()
    }
}

impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::EndOfStream => Debug::fmt(&"protocol error; unexpected end of stream", f),
            ParseError::Other(err) => Debug::fmt(&err, f),
        }
    }
}

impl std::error::Error for ParseError {}