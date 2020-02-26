mod types;
use types::Command;

use failure::Error;

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;

pub type Result<T> = std::result::Result<T, Error>;

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
        let current_log_file = path.join(format!("0.log"));
        let next_log_file = path.join(format!("1.log"));
        let mut reader = BufReader::new(File::open(&current_log_file)?);
        let writer = BufWriter::new(File::create(&next_log_file)?);

        let mut index_map = HashMap::new();
        populate_map(&mut index_map, &mut reader);

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
        self.index_map.insert(key, value);
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.index_map.get(&key) {
            Some(value) => Ok(Some(value.clone())),
            None => Err(failure::err_msg("Key not found")),
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index_map.contains_key(&key) {
            let cmd = Command::Remove { key: key.clone() };
            let serialized = serde_json::to_string(&cmd)?;
            self.writer.write(serialized.as_bytes())?;
            self.index_map.remove(&key);
            Ok(())
        } else {
            Err(failure::err_msg("Key not found"))
        }
    }
}

fn populate_map<R: BufRead>(index_map: &mut HashMap<String, String>, reader: &mut R) -> Result<()> {
    for line in reader.lines() {
        let line = line?;
        let deserialized = serde_json::from_str(&line)?;
        match deserialized {
            Command::Set { key, value } => {
                index_map.insert(key, value);
            }
            Command::Remove { key } => {
                index_map.remove(&key);
            }
        }
    }
    Ok(())
}
