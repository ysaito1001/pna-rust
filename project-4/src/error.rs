use failure::Fail;

use std::string::FromUtf8Error;
use std::{io, net};

#[derive(Fail, Debug)]
pub enum KvsError {
    #[fail(display = "IO error: {}", _0)]
    Io(io::Error),

    #[fail(display = "serde_json error: {}", _0)]
    Serde(serde_json::Error),

    #[fail(display = "Key not found")]
    KeyNotFound,

    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,

    #[fail(display = "{}", _0)]
    Net(net::AddrParseError),

    #[fail(display = "UTF-8 error: {}", _0)]
    Utf8(FromUtf8Error),

    #[fail(display = "sled error: {}", _0)]
    Sled(sled::Error),

    #[fail(display = "{}", _0)]
    StringError(String),
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::Io(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> KvsError {
        KvsError::Serde(err)
    }
}

impl From<net::AddrParseError> for KvsError {
    fn from(err: net::AddrParseError) -> KvsError {
        KvsError::Net(err)
    }
}

impl From<FromUtf8Error> for KvsError {
    fn from(err: FromUtf8Error) -> KvsError {
        KvsError::Utf8(err)
    }
}

impl From<sled::Error> for KvsError {
    fn from(err: sled::Error) -> KvsError {
        KvsError::Sled(err)
    }
}

pub type Result<T> = std::result::Result<T, KvsError>;
