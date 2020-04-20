use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};

use log::{debug, error};
use serde_json::Deserializer;

use crate::error::Result;
use crate::request::Request;
use crate::response::{GetResponse, RemoveResponse, SetResponse};
use crate::thread_pool::ThreadPool;
use crate::KvsEngine;

pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    pool: P,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    pub fn new(engine: E, pool: P) -> Self {
        KvsServer { engine, pool }
    }

    pub fn run<A: ToSocketAddrs>(self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            let engine = self.engine.clone();
            self.pool.spawn(move || match stream {
                Ok(stream) => {
                    if let Err(e) = serve(engine, stream) {
                        error!("Error on serving client: {}", e);
                    }
                }
                Err(e) => error!("Connection failed: {}", e),
            })
        }
        Ok(())
    }
}

fn serve<E: KvsEngine>(engine: E, stream: TcpStream) -> Result<()> {
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
            Request::Set { key, value } => send_response!(match engine.set(key, value) {
                Ok(()) => SetResponse::Ok(()),
                Err(e) => SetResponse::Err(format!("{}", e)),
            }),
            Request::Get { key } => send_response!(match engine.get(key) {
                Ok(value) => GetResponse::Ok(value),
                Err(e) => GetResponse::Err(format!("{}", e)),
            }),
            Request::Remove { key } => send_response!(match engine.remove(key) {
                Ok(()) => RemoveResponse::Ok(()),
                Err(e) => RemoveResponse::Err(format!("{}", e)),
            }),
        };
    }

    Ok(())
}
