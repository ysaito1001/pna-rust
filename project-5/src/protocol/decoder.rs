use std::convert::TryInto;

use bytes::BytesMut;
use serde::Deserialize;

use super::constants;
use crate::{KvsError, Result};

pub struct KvsDecoder {
    buffer: BytesMut,
    start_code_bytes_read: usize,
}

impl KvsDecoder {
    pub fn new(capacity: usize) -> Self {
        KvsDecoder {
            buffer: BytesMut::with_capacity(capacity),
            start_code_bytes_read: 0,
        }
    }

    pub fn decode<D: for<'a> Deserialize<'a>>(&mut self) -> Option<Result<D>> {
        if self.start_code_bytes_read == 0 && self.buffer.len() > 2 * constants::USIZE_BYTES {
            if let Err(e) = self.trim_start_code() {
                return Some(Err(e));
            }
        }

        if self.start_code_bytes_read > 0 && self.buffer.len() >= self.start_code_bytes_read {
            let encoded = self.buffer.split_to(self.start_code_bytes_read);
            self.start_code_bytes_read = 0;
            match bincode::deserialize(&encoded[..]) {
                Ok(result) => return Some(Ok(result)),
                Err(e) => return Some(Err(KvsError::Bincode(e))),
            }
        }

        None
    }

    pub fn append(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    fn trim_start_code(&mut self) -> Result<()> {
        if let Some(index) = self
            .buffer
            .windows(constants::START_CODE.len())
            .position(|w| w == constants::START_CODE)
        {
            let _ = self.buffer.split_to(index + constants::START_CODE.len());
            let start_code_bytes = self.buffer.split_to(constants::USIZE_BYTES);
            let start_code: [u8; 8] = start_code_bytes[..].try_into()?;
            self.start_code_bytes_read = usize::from_le_bytes(start_code);
        }

        Ok(())
    }
}
