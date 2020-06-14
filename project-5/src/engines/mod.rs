use async_trait::async_trait;

use super::error::Result;

#[async_trait]
pub trait KvsEngine: Clone + Send + 'static {
    async fn set(&self, key: String, value: String) -> Result<()>;

    async fn get(&self, key: String) -> Result<Option<String>>;

    async fn remove(&self, key: String) -> Result<()>;
}

mod kvs;
mod sled;

pub use self::kvs::KvStore;
pub use self::sled::SledKvsEngine;
