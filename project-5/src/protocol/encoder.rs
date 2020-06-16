use bytes::BytesMut;
use serde::Serialize;

use super::constants;
use crate::Result;

pub struct KvsEncoder {
    buffer: BytesMut,
}

impl KvsEncoder {
    pub fn new(capacity: usize) -> Self {
        let mut buffer = BytesMut::with_capacity(capacity);
        buffer.extend_from_slice(&constants::START_CODE);
        KvsEncoder { buffer }
    }

    pub fn encode<T: Serialize>(&mut self, response: T) -> Result<&[u8]> {
        let serialized = bincode::serialize(&response)?;
        self.buffer.truncate(constants::START_CODE.len());
        self.buffer
            .extend_from_slice(&serialized.len().to_le_bytes());
        self.buffer.extend_from_slice(&serialized);
        Ok(&self.buffer[..])
    }
}
