use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crossbeam_skiplist::SkipMap;
use log::error;
use serde_json::Deserializer;

mod command;
mod kv_reader;
mod kv_writer;
mod log_common;
mod log_pointer;

use super::KvsEngine;
use crate::{KvsError, Result};
use command::Command;
use kv_reader::KvReader;
use kv_writer::KvWriter;
use log_common::*;
use log_pointer::LogPointer;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are persisted to disk in log files. Log files are named after
/// monotonically increasing generation numbers with a `log` extension name.
/// A `BTreeMap` in memory stores the keys and the value locations for fast query.
///
/// ```rust
/// # use kvs::{KvStore, Result};
/// # fn try_main() -> Result<()> {
/// use std::env::current_dir;
/// use kvs::KvsEngine;
/// let mut store = KvStore::open(current_dir()?)?;
/// store.set("key".to_owned(), "value".to_owned())?;
/// let val = store.get("key".to_owned())?;
/// assert_eq!(val, Some("value".to_owned()));
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct KvStore {
    path: Arc<PathBuf>,
    index_map: Arc<SkipMap<String, LogPointer>>,
    kv_reader: KvReader,
    kv_writer: Arc<Mutex<KvWriter>>,
    current_generation: u64,
    uncompacted: Arc<AtomicU64>,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = Arc::new(path.into());
        fs::create_dir_all(&*path)?;

        let mut readers = BTreeMap::new();
        let index_map = Arc::new(SkipMap::new());

        let generations = get_log_generations(&path)?;
        let mut uncompacted = 0;

        for &generation in &generations {
            let mut reader = BufReader::new(File::open(log_path(&path, generation))?);
            uncompacted += load(generation, &mut reader, &*index_map)?;
            readers.insert(generation, reader);
        }

        let current_generation = generations.last().unwrap_or(&0) + 1;
        let pitr = Arc::new(AtomicU64::new(0));

        let kv_reader = KvReader::open(Arc::clone(&path), pitr, readers);
        let kv_writer = KvWriter::open(Arc::clone(&path), current_generation)?;

        Ok(KvStore {
            path,
            index_map,
            kv_reader,
            kv_writer: Arc::new(Mutex::new(kv_writer)),
            current_generation,
            uncompacted: Arc::new(AtomicU64::new(uncompacted)),
        })
    }

    fn run_compaction(&self) -> Result<()> {
        let compaction_generation = self.current_generation + 1;
        let mut compaction_writer = KvWriter::open(Arc::clone(&self.path), compaction_generation)?;

        let index_map = self.index_map.as_ref();
        for entry in index_map.clone().iter() {
            let command = self.kv_reader.borrow().read_command(*entry.value())?;
            let (offset, length) = compaction_writer.write_command(&command)?;
            self.index_map.insert(
                entry.key().clone(),
                (compaction_generation, offset..(offset + length)).into(),
            );
        }

        self.kv_reader
            .pitr
            .store(compaction_generation, Ordering::SeqCst);
        self.kv_reader.close_stale_readers();

        remove_stale_log_files(Arc::clone(&self.path), compaction_generation)?;

        self.uncompacted.store(0, Ordering::SeqCst);
        self.kv_writer.lock()?.refresh(compaction_generation + 1)?;

        Ok(())
    }
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        let command = Command::Set { key, value };
        let mut kv_writer = self.kv_writer.lock()?;
        let (offset, length) = kv_writer.write_command(&command)?;
        if let Command::Set { key, .. } = command {
            if let Some(old_command) = self.index_map.get(&key) {
                self.uncompacted
                    .fetch_add(old_command.value().length, Ordering::SeqCst);
            }
            self.index_map.insert(
                key,
                (kv_writer.current_generation, offset..(offset + length)).into(),
            );
        }

        if self.uncompacted.load(Ordering::SeqCst) > COMPACTION_THRESHOLD {
            drop(kv_writer);
            self.run_compaction()?;
        }

        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        match self.index_map.get(&key) {
            Some(log_pointer) => {
                if let Command::Set { value, .. } =
                    self.kv_reader.read_command(*log_pointer.value())?
                {
                    Ok(Some(value))
                } else {
                    Err(KvsError::UnexpectedCommandType)
                }
            }
            None => Ok(None),
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        if !self.index_map.contains_key(&key) {
            return Err(KvsError::KeyNotFound);
        }

        let command = Command::Remove { key };
        let mut kv_writer = self.kv_writer.lock()?;
        let (_offset, length) = kv_writer.write_command(&command)?;
        if let Command::Remove { key } = command {
            let old_command = self.index_map.remove(&key).unwrap();
            self.uncompacted
                .fetch_add(old_command.value().length, Ordering::SeqCst);
            self.uncompacted.fetch_add(length, Ordering::SeqCst);
        }

        if self.uncompacted.load(Ordering::SeqCst) > COMPACTION_THRESHOLD {
            drop(kv_writer);
            self.run_compaction()?;
        }

        Ok(())
    }
}

fn get_log_generations(path: &Path) -> Result<Vec<u64>> {
    let mut result: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();

    result.sort_unstable();
    Ok(result)
}

fn load(
    generation: u64,
    reader: &mut BufReader<File>,
    index_map: &SkipMap<String, LogPointer>,
) -> Result<u64> {
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    let mut uncompacted = 0;
    let mut position = 0;
    while let Some(command) = stream.next() {
        let new_position = stream.byte_offset() as u64;
        match command? {
            Command::Set { key, .. } => {
                if let Some(old_command) = index_map.get(&key) {
                    uncompacted += old_command.value().length;
                }
                index_map.insert(key, (generation, position..new_position).into());
            }
            Command::Remove { key } => {
                if let Some(old_command) = index_map.remove(&key) {
                    uncompacted += old_command.value().length;
                }
                uncompacted += new_position - position;
            }
        }
        position = new_position;
    }

    Ok(uncompacted)
}

fn remove_stale_log_files(path: Arc<PathBuf>, compaction_generation: u64) -> Result<()> {
    let stale_generations = get_log_generations(&path)?
        .into_iter()
        .filter(|gen| gen < &compaction_generation);

    for stale_generation in stale_generations {
        let stale_log_path = log_path(&path, stale_generation);
        if let Err(e) = fs::remove_file(&stale_log_path) {
            error!("{:?} cannot be deleted: {}e", stale_log_path, e);
        }
    }

    Ok(())
}
