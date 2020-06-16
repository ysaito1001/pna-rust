use std::{array, io, net, string::FromUtf8Error, sync::PoisonError};

use failure::Fail;

#[derive(Fail, Debug)]
pub enum KvsError {
    #[fail(display = "Bincode error: {}", _0)]
    Bincode(bincode::Error),

    #[fail(display = "Concurrent error when a lock is acquired")]
    ConcurrentError,

    #[fail(display = "IO error: {}", _0)]
    Io(io::Error),

    #[fail(display = "Key not found")]
    KeyNotFound,

    #[fail(display = "{}", _0)]
    Net(net::AddrParseError),

    #[fail(display = "serde_json error: {}", _0)]
    Serde(serde_json::Error),

    #[fail(display = "sled error: {}", _0)]
    Sled(sled::Error),

    #[fail(display = "{}", _0)]
    StringError(String),

    #[fail(display = "TryFromSlice error: {}", _0)]
    TryFromSlice(array::TryFromSliceError),

    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,

    #[fail(display = "UTF-8 error: {}", _0)]
    Utf8(FromUtf8Error),
}

impl From<bincode::Error> for KvsError {
    fn from(err: bincode::Error) -> Self {
        KvsError::Bincode(err)
    }
}

impl<T> From<PoisonError<T>> for KvsError {
    fn from(_: PoisonError<T>) -> Self {
        KvsError::ConcurrentError
    }
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> Self {
        KvsError::Io(err)
    }
}

impl From<net::AddrParseError> for KvsError {
    fn from(err: net::AddrParseError) -> Self {
        KvsError::Net(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> Self {
        KvsError::Serde(err)
    }
}

impl From<sled::Error> for KvsError {
    fn from(err: sled::Error) -> Self {
        KvsError::Sled(err)
    }
}

impl From<array::TryFromSliceError> for KvsError {
    fn from(err: array::TryFromSliceError) -> Self {
        KvsError::TryFromSlice(err)
    }
}

impl From<FromUtf8Error> for KvsError {
    fn from(err: FromUtf8Error) -> Self {
        KvsError::Utf8(err)
    }
}

pub type Result<T> = std::result::Result<T, KvsError>;
