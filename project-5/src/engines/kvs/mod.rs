use std::{
    borrow::Borrow,
    collections::BTreeMap,
    ffi::OsStr,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};

use async_std::{
    fs::{self, File},
    io::{BufReader, SeekFrom},
    prelude::*,
    sync::{Arc, Mutex},
};
use async_trait::async_trait;
use crossbeam_skiplist::SkipMap;
use log::error;

use super::KvsEngine;
use crate::{KvsError, Result};
mod command;
mod constants;
mod kv_reader;
mod kv_writer;
mod log_common;
mod log_pointer;
use command::Command;
use kv_reader::{deserialize_command, KvReader};
use kv_writer::KvWriter;
use log_common::*;
use log_pointer::LogPointer;

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are persisted to disk in log files. Log files are named after
/// monotonically increasing generation numbers with a `log` extension name.
/// A skip list in memory stores the keys and the value locations for fast query.
///
/// ```rust
/// # use async_std::task;
/// # use kvs::{KvStore, Result};
/// # use kvs::thread_pool::{RayonThreadPool, ThreadPool};
/// # fn try_main() -> Result<()> {
/// use kvs::KvsEngine;
/// use std::env::current_dir;
/// task::block_on(async move {
///     let mut store = KvStore::open(current_dir().unwrap()).await.unwrap();
///     store
///         .set("key".to_owned(), "value".to_owned())
///         .await
///         .unwrap();
///     let val = store.get("key".to_owned()).await.unwrap();
///     assert_eq!(val, Some("value".to_owned()));
/// });
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
    uncompacted: Arc<AtomicUsize>,
}

impl KvStore {
    pub async fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = Arc::new(path.into());
        fs::create_dir_all(&*path).await?;

        let mut readers = BTreeMap::new();
        let index_map = Arc::new(SkipMap::new());

        let generations = get_log_generations(&path)?;
        let mut uncompacted = 0;

        for &generation in &generations {
            let log_path = log_path(&path, generation);
            let mut reader = BufReader::new(File::open(&log_path).await?);
            let end_of_file = fs::metadata(log_path).await?.len() as usize;
            uncompacted += load(generation, &mut reader, end_of_file, &*index_map).await?;
            readers.insert(generation, reader);
        }

        let current_generation = generations.last().unwrap_or(&0) + 1;
        let pitr = Arc::new(AtomicUsize::new(0));

        let kv_reader = KvReader::open(Arc::clone(&path), pitr, readers);
        let kv_writer = KvWriter::open(Arc::clone(&path), current_generation).await?;

        Ok(KvStore {
            path,
            index_map,
            kv_reader,
            kv_writer: Arc::new(Mutex::new(kv_writer)),
            current_generation,
            uncompacted: Arc::new(AtomicUsize::new(uncompacted)),
        })
    }

    async fn run_compaction(&self) -> Result<()> {
        let compaction_generation = self.current_generation + 1;
        let mut compaction_writer =
            KvWriter::open(Arc::clone(&self.path), compaction_generation).await?;

        let index_map = self.index_map.as_ref();
        for entry in index_map.clone().iter() {
            let command = self.kv_reader.borrow().read_command(*entry.value()).await?;
            let (offset, length) = compaction_writer.write_command(&command).await?;
            self.index_map.insert(
                entry.key().clone(),
                (compaction_generation, offset..(offset + length)).into(),
            );
        }

        self.kv_reader
            .pitr
            .store(compaction_generation as usize, Ordering::SeqCst);
        self.kv_reader.close_stale_readers().await;

        remove_stale_log_files(Arc::clone(&self.path), compaction_generation).await?;

        self.uncompacted.store(0, Ordering::SeqCst);
        self.kv_writer
            .lock()
            .await
            .refresh(compaction_generation + 1)
            .await?;

        Ok(())
    }
}

#[async_trait]
impl KvsEngine for KvStore {
    async fn set(&self, key: String, value: String) -> Result<()> {
        let command = Command::Set { key, value };
        let mut kv_writer = self.kv_writer.lock().await;
        let (offset, length) = kv_writer.write_command(&command).await?;
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

        if self.uncompacted.load(Ordering::SeqCst) > constants::COMPACTION_THRESHOLD {
            drop(kv_writer);
            self.run_compaction().await?;
        }

        Ok(())
    }

    async fn get(&self, key: String) -> Result<Option<String>> {
        match self.index_map.get(&key) {
            Some(log_pointer) => {
                if let Command::Set { value, .. } =
                    self.kv_reader.read_command(*log_pointer.value()).await?
                {
                    Ok(Some(value))
                } else {
                    Err(KvsError::UnexpectedCommandType)
                }
            }
            None => Ok(None),
        }
    }

    async fn remove(&self, key: String) -> Result<()> {
        if !self.index_map.contains_key(&key) {
            return Err(KvsError::KeyNotFound);
        }

        let command = Command::Remove { key };
        let mut kv_writer = self.kv_writer.lock().await;
        let (_offset, length) = kv_writer.write_command(&command).await?;
        if let Command::Remove { key } = command {
            let old_command = self.index_map.remove(&key).unwrap();
            self.uncompacted
                .fetch_add(old_command.value().length, Ordering::SeqCst);
            self.uncompacted
                .fetch_add(length as usize, Ordering::SeqCst);
        }

        if self.uncompacted.load(Ordering::SeqCst) > constants::COMPACTION_THRESHOLD {
            drop(kv_writer);
            self.run_compaction().await?;
        }

        Ok(())
    }
}

fn get_log_generations(path: &Path) -> Result<Vec<u64>> {
    let mut result: Vec<u64> = std::fs::read_dir(&path)?
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

async fn load(
    generation: u64,
    reader: &mut BufReader<File>,
    end_of_file: usize,
    index_map: &SkipMap<String, LogPointer>,
) -> Result<usize> {
    let mut uncompacted = 0;
    let mut position = 0;
    while position < end_of_file {
        let mut serialized_bytes = [0u8; 8];
        reader.seek(SeekFrom::Start(position as u64)).await?;
        reader.read_exact(&mut serialized_bytes).await?;
        let data_block_size = constants::USIZE_BYTES + usize::from_le_bytes(serialized_bytes);
        let command = deserialize_command(reader, position, data_block_size).await?;
        match command {
            Command::Set { key, .. } => {
                if let Some(old_command) = index_map.get(&key) {
                    uncompacted += old_command.value().length;
                }
                index_map.insert(
                    key,
                    LogPointer {
                        generation,
                        offset: position,
                        length: data_block_size,
                    },
                );
            }
            Command::Remove { key } => {
                if let Some(old_command) = index_map.remove(&key) {
                    uncompacted += old_command.value().length;
                }
                uncompacted += data_block_size;
            }
        }
        position += data_block_size;
    }

    Ok(uncompacted)
}

async fn remove_stale_log_files(path: Arc<PathBuf>, compaction_generation: u64) -> Result<()> {
    let stale_generations = get_log_generations(&path)?
        .into_iter()
        .filter(|gen| gen < &compaction_generation);

    for stale_generation in stale_generations {
        let stale_log_path = log_path(&path, stale_generation);
        if let Err(e) = fs::remove_file(&stale_log_path).await {
            error!("{:?} cannot be deleted: {}e", stale_log_path, e);
        }
    }

    Ok(())
}
