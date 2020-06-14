use std::{path::PathBuf, sync::Arc};

use async_std::{
    fs::File,
    io::{BufWriter, SeekFrom},
    prelude::*,
};

use super::{command::Command, constants, log_common::*};
use crate::Result;

#[derive(Debug)]
pub struct KvWriter {
    pub writer: BufWriter<File>,
    path: Arc<PathBuf>,
    pub current_generation: u64,
}

impl KvWriter {
    pub async fn open(path: Arc<PathBuf>, generation: u64) -> Result<Self> {
        let mut file = new_log_file(&*path, generation).await?;
        file.seek(SeekFrom::Current(0)).await?;
        Ok(KvWriter {
            writer: BufWriter::new(file),
            path: Arc::clone(&path),
            current_generation: generation,
        })
    }

    pub async fn write_command(&mut self, command: &Command) -> Result<(u64, u64)> {
        let serialized = bincode::serialize(command)?;
        let serialized_bytes = serialized.len();
        let current_position = self.writer.seek(SeekFrom::End(0)).await?;
        self.writer
            .write_all(&serialized_bytes.to_le_bytes())
            .await?;
        self.writer.write_all(&serialized).await?;
        self.writer.flush().await?;
        let data_block_size = constants::USIZE_BYTES + serialized_bytes;

        Ok((current_position, data_block_size as u64))
    }

    pub async fn refresh(&mut self, generation: u64) -> Result<()> {
        let mut file = new_log_file(&self.path, generation).await?;
        file.seek(SeekFrom::Current(0)).await?;
        self.writer = BufWriter::new(file);
        self.current_generation = generation;

        Ok(())
    }
}
