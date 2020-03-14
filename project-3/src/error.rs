use failure::Fail;

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

pub type Result<T> = std::result::Result<T, KvsError>;
