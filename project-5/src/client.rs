use async_std::{
    net::{TcpStream, ToSocketAddrs},
    prelude::*,
};

use crate::{
    error::{KvsError, Result},
    protocol::{KvsStream, Request, Response},
};

pub struct KvsClient {
    kvs_stream: KvsStream<Response>,
}

impl KvsClient {
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(KvsClient {
            kvs_stream: KvsStream::new(stream),
        })
    }

    pub async fn get(&mut self, key: String) -> Result<Option<String>> {
        let request = Request::Get { key };
        self.kvs_stream.send(&request).await?;
        let response = self.kvs_stream.next().await.unwrap();
        match response? {
            Response::Ok(value) => Ok(value),
            Response::Err(e) => Err(KvsError::StringError(e)),
        }
    }

    pub async fn set(&mut self, key: String, value: String) -> Result<()> {
        let request = Request::Set { key, value };
        self.kvs_stream.send(&request).await?;
        let response = self.kvs_stream.next().await.unwrap();
        match response? {
            Response::Ok(_) => Ok(()),
            Response::Err(e) => Err(KvsError::StringError(e)),
        }
    }

    pub async fn remove(&mut self, key: String) -> Result<()> {
        let request = Request::Remove { key };
        self.kvs_stream.send(&request).await?;
        let response = self.kvs_stream.next().await.unwrap();
        match response? {
            Response::Ok(_) => Ok(()),
            Response::Err(e) => Err(KvsError::StringError(e)),
        }
    }
}
