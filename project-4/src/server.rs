use crate::error::Result;
use crate::request::Request;
use crate::response::{GetResponse, RemoveResponse, SetResponse};
use crate::KvsEngine;

use log::{debug, error};
use serde_json::Deserializer;

use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};

pub struct KvsServer<E: KvsEngine> {
    engine: E,
}

impl<E: KvsEngine> KvsServer<E> {
    pub fn new(engine: E) -> Self {
        KvsServer { engine }
    }

    pub fn run<A: ToSocketAddrs>(mut self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(e) = self.serve(stream) {
                        error!("Error on serving client: {}", e);
                    }
                }
                Err(e) => error!("Connection failed: {}", e),
            }
        }
        Ok(())
    }

    fn serve(&mut self, stream: TcpStream) -> Result<()> {
        let reader = Deserializer::from_reader(BufReader::new(&stream)).into_iter::<Request>();
        let mut writer = BufWriter::new(&stream);

        let peer_addr = stream.peer_addr()?;

        macro_rules! send_response {
            ($resp:expr) => {{
                let response = $resp;
                serde_json::to_writer(&mut writer, &response)?;
                writer.flush()?;
                debug!("Response sent to {}: {:?}", peer_addr, response);
            };};
        }

        for request in reader {
            let request = request?;
            match request {
                Request::Set { key, value } => send_response!(match self.engine.set(key, value) {
                    Ok(()) => SetResponse::Ok(()),
                    Err(e) => SetResponse::Err(format!("{}", e)),
                }),
                Request::Get { key } => send_response!(match self.engine.get(key) {
                    Ok(value) => GetResponse::Ok(value),
                    Err(e) => GetResponse::Err(format!("{}", e)),
                }),
                Request::Remove { key } => send_response!(match self.engine.remove(key) {
                    Ok(()) => RemoveResponse::Ok(()),
                    Err(e) => RemoveResponse::Err(format!("{}", e)),
                }),
            };
        }

        Ok(())
    }
}
