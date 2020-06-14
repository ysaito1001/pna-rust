use std::{
    io::{BufReader, BufWriter, Write},
    net::{TcpStream, ToSocketAddrs},
};

use serde::Deserialize;
use serde_json::de::{Deserializer, IoRead};

use crate::{
    error::{KvsError, Result},
    request::Request,
    response::{GetResponse, RemoveResponse, SetResponse},
};

pub struct KvsClient {
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
    writer: BufWriter<TcpStream>,
}

impl KvsClient {
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        let reader = Deserializer::from_reader(BufReader::new(stream.try_clone()?));
        let writer = BufWriter::new(stream);
        Ok(KvsClient { reader, writer })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        serde_json::to_writer(&mut self.writer, &Request::Get { key })?;
        self.writer.flush()?;
        let response = GetResponse::deserialize(&mut self.reader)?;
        match response {
            GetResponse::Ok(value) => Ok(value),
            GetResponse::Err(e) => Err(KvsError::StringError(e)),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Set { key, value })?;
        self.writer.flush()?;
        let response = SetResponse::deserialize(&mut self.reader)?;
        match response {
            SetResponse::Ok(_) => Ok(()),
            SetResponse::Err(e) => Err(KvsError::StringError(e)),
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Remove { key })?;
        self.writer.flush()?;
        let response = RemoveResponse::deserialize(&mut self.reader)?;
        match response {
            RemoveResponse::Ok(_) => Ok(()),
            RemoveResponse::Err(e) => Err(KvsError::StringError(e)),
        }
    }
}
