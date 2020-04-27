use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::log_common::*;

use crate::Result;

use super::command::Command;
use super::log_pointer::LogPointer;

#[derive(Debug)]
pub struct KvReader {
    path: Arc<PathBuf>,
    pub pitr: Arc<AtomicU64>,
    readers: RefCell<BTreeMap<u64, BufReader<File>>>,
}

impl KvReader {
    pub fn open(
        path: Arc<PathBuf>,
        pitr: Arc<AtomicU64>,
        readers: BTreeMap<u64, BufReader<File>>,
    ) -> Self {
        KvReader {
            path,
            pitr,
            readers: RefCell::new(readers),
        }
    }

    pub fn read_command(&self, log_pointer: LogPointer) -> Result<Command> {
        self.close_stale_readers();

        let mut readers = self.readers.borrow_mut();
        if !readers.contains_key(&log_pointer.generation) {
            let reader = BufReader::new(File::open(log_path(&self.path, log_pointer.generation))?);
            readers.insert(log_pointer.generation, reader);
        }

        let reader = readers.get_mut(&log_pointer.generation).unwrap();
        reader.seek(SeekFrom::Start(log_pointer.offset))?;
        let serialized = reader.take(log_pointer.length);
        Ok(serde_json::from_reader(serialized)?)
    }

    pub fn close_stale_readers(&self) {
        let mut readers = self.readers.borrow_mut();
        while !readers.is_empty() {
            let generation = *readers.keys().next().unwrap();
            if generation >= self.pitr.load(Ordering::SeqCst) {
                break;
            }
            readers.remove(&generation);
        }
    }
}

impl Clone for KvReader {
    fn clone(&self) -> Self {
        KvReader {
            path: Arc::clone(&self.path),
            pitr: Arc::clone(&self.pitr),
            readers: RefCell::new(BTreeMap::new()),
        }
    }
}
