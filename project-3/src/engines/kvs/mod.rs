use command::Command;
use log_pointer::LogPointer;
use log_reader::LogReader;
use log_rotator::LogRotator;
use log_writer::LogWriter;
use serde_json::Deserializer;

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

mod command;
mod log_pointer;
mod log_reader;
mod log_rotator;
mod log_writer;

use crate::{KvsError, Result};

use super::KvsEngine;

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
pub struct KvStore {
    log_rotator: LogRotator,
    index_map: HashMap<String, LogPointer>,
    total_bytes_written: u64,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        let mut log_rotator = LogRotator::new(path)?;
        let (index_map, total_bytes_written) = create_index_map(log_rotator.get_log_reader())?;

        Ok(KvStore {
            log_rotator: log_rotator,
            index_map: index_map,
            total_bytes_written: total_bytes_written,
        })
    }
}

impl KvsEngine for KvStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command::Set {
            key: key.clone(),
            value: value.clone(),
        };
        let serialized = serde_json::to_string(&command)?;
        persist_command_to_log(self, key, serialized)
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.index_map.get(&key) {
            Some(log_pointer) => {
                let reader: &mut LogReader<File> = self.log_rotator.get_log_reader();
                reader.seek(SeekFrom::Start(log_pointer.offset))?;
                let serialized = reader.take(log_pointer.length);
                match serde_json::from_reader(serialized)? {
                    Command::Set { value, .. } => Ok(Some(value)),
                    Command::Remove { .. } => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.index_map.contains_key(&key) {
            let command = Command::Remove { key: key.clone() };
            let serialized = serde_json::to_string(&command)?;
            persist_command_to_log(self, key, serialized)
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
}

fn create_index_map(
    log_reader: &mut LogReader<File>,
) -> Result<(HashMap<String, LogPointer>, u64)> {
    let mut result = HashMap::new();

    let mut current_position = log_reader.seek(SeekFrom::Start(0))?;
    let mut command_stream = Deserializer::from_reader(log_reader).into_iter::<Command>();

    while let Some(command) = command_stream.next() {
        let next_position = command_stream.byte_offset() as u64;
        match command? {
            Command::Set { key, .. } | Command::Remove { key } => {
                result.insert(key, (current_position..next_position).into())
            }
        };
        current_position = next_position;
    }

    Ok((result, current_position))
}

fn persist_command_to_log(kvs: &mut KvStore, key: String, serialized: String) -> Result<()> {
    {
        let writer: &mut LogWriter<File> = kvs.log_rotator.get_log_writer();
        let current_position = writer.seek(SeekFrom::End(0))?;
        let bytes_written = writer.write(serialized.as_bytes())? as u64;
        writer.flush()?;

        kvs.index_map
            .insert(key, (current_position, bytes_written).into());

        kvs.total_bytes_written += bytes_written;
    }

    if kvs.total_bytes_written > COMPACTION_THRESHOLD {
        trigger_compaction(kvs)?;
    }

    Ok(())
}

fn trigger_compaction(kvs: &mut KvStore) -> Result<()> {
    let mut compaction_writer = kvs.log_rotator.create_compaction_writer()?;
    let mut writer_position = compaction_writer.current_position();

    let mut new_index_map: HashMap<String, LogPointer> = HashMap::new();

    let log_reader: &mut LogReader<File> = kvs.log_rotator.get_log_reader();
    let mut reader_position = log_reader.seek(SeekFrom::Start(0))?;
    let mut command_stream = Deserializer::from_reader(log_reader).into_iter::<Command>();

    while let Some(command) = command_stream.next() {
        let next_position = command_stream.byte_offset() as u64;
        let command = command?;
        match &command {
            Command::Set { key, .. } | Command::Remove { key } => {
                if kvs.index_map.get(key).unwrap().offset == reader_position {
                    let serialized = serde_json::to_string(&command)?;
                    let bytes_written = compaction_writer.write(serialized.as_bytes())? as u64;
                    new_index_map.insert(key.clone(), (writer_position, bytes_written).into());
                    writer_position += bytes_written;
                }
            }
        };
        reader_position = next_position;
    }
    compaction_writer.flush()?;

    kvs.log_rotator = kvs.log_rotator.rotate()?;
    kvs.index_map = new_index_map;
    kvs.total_bytes_written = writer_position;

    Ok(())
}
