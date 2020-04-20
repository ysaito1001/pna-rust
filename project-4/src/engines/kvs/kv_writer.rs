use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;

use serde_json;

use super::command::Command;
use super::log_common::*;

use crate::Result;

#[derive(Debug)]
pub struct KvWriter {
    pub writer: BufWriter<File>,
    path: Arc<PathBuf>,
    pub current_generation: u64,
}

impl KvWriter {
    pub fn open(path: Arc<PathBuf>, generation: u64) -> Result<Self> {
        let mut file = new_log_file(&*path, generation)?;
        file.seek(SeekFrom::Current(0))?;
        Ok(KvWriter {
            writer: BufWriter::new(file),
            path: Arc::clone(&path),
            current_generation: generation,
        })
    }

    pub fn write_command(&mut self, command: &Command) -> Result<(u64, u64)> {
        let serialized = serde_json::to_string(command)?;
        let current_position = self.writer.seek(SeekFrom::End(0))?;
        let bytes_written = self.writer.write(serialized.as_bytes())? as u64;
        self.writer.flush()?;

        Ok((current_position, bytes_written))
    }

    pub fn refresh(&mut self, generation: u64) -> Result<()> {
        let mut file = new_log_file(&self.path, generation)?;
        file.seek(SeekFrom::Current(0))?;
        self.writer = BufWriter::new(file);
        self.current_generation = generation;

        Ok(())
    }
}
