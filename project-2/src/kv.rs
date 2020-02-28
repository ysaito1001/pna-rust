use serde_json::Deserializer;

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::{Command, KvsError, LogPointer, Result};

const LOG_FILE_BASENAME: &'static str = "0.log";

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `HashMap` in memory and not persisted to disk.
///
/// Example:
///
/// ```rust
/// # use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned());
/// assert_eq!(val, Some("value".to_owned()));
/// ```
pub struct KvStore {
    file_ref: File,
    index_map: HashMap<String, LogPointer>,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        let log_file = path.join(LOG_FILE_BASENAME);

        let mut file_ref = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&log_file)?;

        let index_map = create_index_map(&mut file_ref)?;

        Ok(KvStore {
            file_ref: file_ref,
            index_map: index_map,
        })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command::Set {
            key: key.clone(),
            value: value.clone(),
        };
        let serialized = serde_json::to_string(&command)?;

        let mut writer = BufWriter::new(&mut self.file_ref);
        let current_position = writer.seek(SeekFrom::End(0))?;
        let bytes_written = writer.write(serialized.as_bytes())? as u64;
        writer.flush()?;

        self.index_map
            .insert(key, (current_position, bytes_written).into());

        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.index_map.get(&key) {
            Some(log_pointer) => {
                let mut reader = BufReader::new(&mut self.file_ref);
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

    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index_map.contains_key(&key) {
            let command = Command::Remove { key: key.clone() };
            let serialized = serde_json::to_string(&command)?;

            let mut writer = BufWriter::new(&mut self.file_ref);
            let current_position = writer.seek(SeekFrom::End(0))?;
            let bytes_written = writer.write(serialized.as_bytes())? as u64;
            writer.flush()?;

            self.index_map
                .insert(key, (current_position, bytes_written).into());

            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
}

fn create_index_map(file_ref: &mut File) -> Result<HashMap<String, LogPointer>> {
    let mut result = HashMap::new();

    let mut reader = BufReader::new(file_ref);
    let mut current_position = reader.seek(SeekFrom::Start(0))?;

    let mut command_stream = Deserializer::from_reader(reader).into_iter::<Command>();

    while let Some(command) = command_stream.next() {
        let next_position = command_stream.byte_offset() as u64;
        match command? {
            Command::Set { key, .. } => {
                result.insert(key, (current_position..next_position).into())
            }
            Command::Remove { key } => result.insert(key, (current_position..next_position).into()),
        };
        current_position = next_position;
    }

    Ok(result)
}
