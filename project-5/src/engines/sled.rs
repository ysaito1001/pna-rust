use async_trait::async_trait;
use sled::{Db, Tree};

use super::KvsEngine;
use crate::{KvsError, Result};

#[derive(Clone)]
pub struct SledKvsEngine(Db);

impl SledKvsEngine {
    pub fn new(db: Db) -> Self {
        SledKvsEngine(db)
    }
}

#[async_trait]
impl KvsEngine for SledKvsEngine {
    async fn set(&self, key: String, value: String) -> Result<()> {
        let tree: &Tree = &self.0;
        tree.insert(key, value.into_bytes()).map(|_| ())?;
        tree.flush()?;
        Ok(())
    }

    async fn get(&self, key: String) -> Result<Option<String>> {
        let tree: &Tree = &self.0;
        Ok(tree
            .get(key)?
            .map(|i_vec| AsRef::<[u8]>::as_ref(&i_vec).to_vec())
            .map(String::from_utf8)
            .transpose()?)
    }

    async fn remove(&self, key: String) -> Result<()> {
        let tree: &Tree = &self.0;
        tree.remove(key)?.ok_or(KvsError::KeyNotFound)?;
        tree.flush()?;
        Ok(())
    }
}
