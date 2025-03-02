use std::fmt;
use std::fmt::{Display, Formatter};
use std::io::Cursor;
use std::num::TryFromIntError;
use std::string::FromUtf8Error;

use atoi::atoi;
use bytes::{Buf, Bytes};

// frame in the Redis protocol
#[derive(Clone, Debug)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    Array(Vec<Frame>),
    Null,
}

impl Frame {
    // returns an empty array
    pub(crate) fn array() -> Frame {
        Frame::Array(vec![])
    }

    // Pushes a bulk frame into the array. "self" must be an array frame.
    // Panics if "self" is not an array frame.
    pub(crate) fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Bulk(bytes));
            }
            _ => panic!("not an array frame"),
        }
    }

    // Pushes an integer frame into the array. "self" must be an array frame.
    // Panics if "self" is not an array frame.
    pub(crate) fn push_int(&mut self, int: u64) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Integer(int));
            }
            _ => panic!("not an array frame"),
        }
    }

    // checks if an entire message can be decoded from "src"
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(src)? {
            // simple string
            b'+' => {
                get_line(src)?;
                Ok(())
            }
            // error
            b'-' => {
                get_line(src)?;
                Ok(())
            }
            // integer
            b':' => {
                get_decimal(src)?;
                Ok(())
            }
            // bulk string
            b'$' => {
                if b'-' == peek_u8(src)? {
                    // skip "-1\r\n"
                    skip(src, 4)
                } else {
                    let len: usize = get_decimal(src)?.try_into()?;
                    // skip that number of bytes + 2 (2 is "\r\n")
                    skip(src, len + 2)
                }
            }
            // array
            b'*' => {
                let len = get_decimal(src)?;
                for _ in 0..len {
                    Frame::check(src)?;
                }
                Ok(())
            }
            actual => Err(format!("protocol error; invalid frame type byte `{}`", actual).into()),
        }
    }

    // message has already been validated with "scan"
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match get_u8(src)? {
            // simple string
            b'+' => {
                // read the line and convert it to "Vec<u8>"
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(Frame::Simple(string))
            }
            // error
            b'-' => {
                // read the line and convert it to "Vec<u8>"
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(Frame::Error(string))
            }
            // integer
            b':' => {
                let int = get_decimal(src)?;
                Ok(Frame::Integer(int))
            }
            // bulk string
            b'$' => {
                if b'-' == peek_u8(src)? {
                    let line = get_line(src)?;
                    if line != b"-1" {
                        return Err("protocol error; invalid frame format".into());
                    }
                    Ok(Frame::Null)
                } else {
                    let len = get_decimal(src)?.try_into()?;
                    let n = len + 2; // 2 is "\r\n"
                    if src.remaining() < n {
                        return Err(Error::Incomplete)
                    }
                    let data = Bytes::copy_from_slice(&src.chunk()[..len]);
                    // skip that number of bytes + 2 (2 is "\r\n")
                    skip(src, n)?;
                    Ok(Frame::Bulk(data))
                }
            }
            // array
            b'*' => {
                let len = get_decimal(src)?.try_into()?;
                let mut frames = Vec::with_capacity(len);
                for _ in 0..len {
                    frames.push(Frame::parse(src)?);
                }
                Ok(Frame::Array(frames))
            }
            _ => unimplemented!(),
        }
    }

    // converts the frame to an "unexpected frame" error
    pub(crate) fn to_error(&self) -> crate::Error {
        format!("unexpected frame {self}").into()
    }
}

impl PartialEq<&str> for Frame {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Frame::Simple(s) => s.eq(other),
            Frame::Bulk(s) => s.eq(other),
            _ => false,
        }
    }
}

impl Display for Frame {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use std::str;
        match self {
            Frame::Simple(s) => s.fmt(f),
            Frame::Error(err) => write!(f, "error: {err}"),
            Frame::Integer(num) => num.fmt(f),
            Frame::Bulk(bts) => match str::from_utf8(bts) {
                Ok(s) => s.fmt(f),
                Err(_) => write!(f, "{:?}", bts)
            }
            Frame::Array(elems) => {
                for (i, elem) in elems.iter().enumerate() {
                    if i > 0 {
                        _ = write!(f, " ");
                        elem.fmt(f)?
                    }
                }
                Ok(())
            }
            Frame::Null => "(nil)".fmt(f),
        }
    }
}

fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(src.chunk()[0])
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(src.get_u8())
}

fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    let line = get_line(src)?;
    atoi::<u64>(line).ok_or_else(|| "protocol error; invalid frame format".into())
}

fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    let start = src.position() as usize;
    // scan to the second to last byte
    let end = src.get_ref().len() - 1;
    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            // line is found, update the position to be after the "\r\n"
            src.set_position((i + 2) as u64);
            return Ok(&src.get_ref()[start..i]);
        }
    }
    Err(Error::Incomplete)
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if src.remaining() < n {
        return Err(Error::Incomplete);
    }
    src.advance(n);
    Ok(())
}

#[derive(Debug)]
pub enum Error {
    // not enough data is available to parse a message
    Incomplete,
    // invalid message encoding
    Other(crate::Error),
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Error::Other(value.into())
    }
}

impl From<&str> for Error {
    fn from(value: &str) -> Self {
        value.to_string().into()
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_: FromUtf8Error) -> Self {
        "protocol error; invalid frame".into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_: TryFromIntError) -> Self {
        "protocol error; invalid frame".into()
    }
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(f),
            Error::Other(err) => err.fmt(f),
        }
    }
}
