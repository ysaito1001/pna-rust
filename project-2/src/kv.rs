use serde_json::Deserializer;

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::{Command, KvsError, Result};

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
    writer: BufWriter<File>,
    index_map: HashMap<String, String>,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        let log_file = path.join(LOG_FILE_BASENAME);

        let index_map = create_index_map(&log_file)?;

        let writer = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&log_file)?,
        );

        Ok(KvStore {
            writer: writer,
            index_map: index_map,
        })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set {
            key: key.clone(),
            value: value.clone(),
        };
        let serialized = serde_json::to_string(&cmd)?;
        self.writer.write(serialized.as_bytes())?;
        self.writer.flush()?;
        self.index_map.insert(key, value);
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.index_map.get(&key) {
            Some(value) => Ok(Some(value.clone())),
            None => Ok(None),
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index_map.contains_key(&key) {
            let cmd = Command::Remove { key: key.clone() };
            let serialized = serde_json::to_string(&cmd)?;
            self.writer.write(serialized.as_bytes())?;
            self.writer.flush()?;
            self.index_map.remove(&key);
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
}

fn create_index_map(log_filename: &PathBuf) -> Result<HashMap<String, String>> {
    if !Path::new(&log_filename).exists() {
        return Ok(HashMap::new());
    }

    let mut index_map = HashMap::new();
    let mut command_stream = Deserializer::from_reader(BufReader::new(File::open(&log_filename)?))
        .into_iter::<Command>();
    while let Some(command) = command_stream.next() {
        match command? {
            Command::Set { key, value } => {
                index_map.insert(key, value);
            }
            Command::Remove { key } => {
                index_map.remove(&key);
            }
        }
    }
    Ok(index_map)
}
