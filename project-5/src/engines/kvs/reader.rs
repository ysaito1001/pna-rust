use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use async_std::{
    fs::File,
    io::{BufReader, SeekFrom},
    prelude::*,
};
use crossbeam::atomic::AtomicCell;

use super::{command::Command, constants, log_common::*, log_pointer::LogPointer};
use crate::Result;

pub struct KvsReader {
    path: Arc<PathBuf>,
    pub pitr: Arc<AtomicUsize>,
    readers: AtomicCell<BTreeMap<u64, BufReader<File>>>,
}

impl KvsReader {
    pub fn open(
        path: Arc<PathBuf>,
        pitr: Arc<AtomicUsize>,
        readers: BTreeMap<u64, BufReader<File>>,
    ) -> Self {
        KvsReader {
            path,
            pitr,
            readers: AtomicCell::new(readers),
        }
    }

    pub async fn read_command(&self, log_pointer: LogPointer) -> Result<Command> {
        self.close_stale_readers().await;

        let mut readers = self.readers.take();
        let command = self.read_command_helper(&mut readers, log_pointer).await?;
        self.readers.store(readers);

        Ok(command)
    }

    pub async fn close_stale_readers(&self) {
        let mut readers = self.readers.take();
        while !readers.is_empty() {
            let generation = *readers.keys().next().unwrap();
            if generation >= self.pitr.load(Ordering::SeqCst) as u64 {
                break;
            }
            readers.remove(&generation);
        }
        self.readers.store(readers);
    }

    async fn read_command_helper(
        &self,
        readers: &mut BTreeMap<u64, BufReader<File>>,
        log_pointer: LogPointer,
    ) -> Result<Command> {
        if !readers.contains_key(&log_pointer.generation) {
            let reader =
                BufReader::new(File::open(log_path(&self.path, log_pointer.generation)).await?);
            readers.insert(log_pointer.generation, reader);
        }

        let reader = readers.get_mut(&log_pointer.generation).unwrap();
        reader
            .seek(SeekFrom::Start(log_pointer.offset as u64))
            .await?;
        let mut serialized_bytes = [0u8; 8];
        reader.read_exact(&mut serialized_bytes).await?;
        let data_block_size = constants::USIZE_BYTES + usize::from_le_bytes(serialized_bytes);
        deserialize_command(reader, log_pointer.offset, data_block_size).await
    }
}

impl Clone for KvsReader {
    fn clone(&self) -> Self {
        KvsReader {
            path: Arc::clone(&self.path),
            pitr: Arc::clone(&self.pitr),
            readers: AtomicCell::new(BTreeMap::new()),
        }
    }
}

pub(super) async fn deserialize_command(
    reader: &mut BufReader<File>,
    offset: usize,
    data_block_size: usize,
) -> Result<Command> {
    let mut buf = vec![0; data_block_size - constants::USIZE_BYTES];
    reader
        .seek(SeekFrom::Start((offset + constants::USIZE_BYTES) as u64))
        .await?;
    reader.read_exact(&mut buf).await?;

    Ok(bincode::deserialize(&buf)?)
}
