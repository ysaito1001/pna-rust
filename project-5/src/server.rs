use std::{
    io::{BufReader, BufWriter, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
};

use async_std::task;
use log::{debug, error};
use serde_json::Deserializer;

use crate::{
    error::Result,
    request::Request,
    response::{GetResponse, RemoveResponse, SetResponse},
    thread_pool::ThreadPool,
    KvsEngine,
};

pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    pool: Arc<Mutex<P>>,
    addr: SocketAddr,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    pub fn new(engine: E, pool: P, addr: SocketAddr) -> Self {
        KvsServer {
            engine,
            pool: Arc::new(Mutex::new(pool)),
            addr,
        }
    }

    pub fn run(&self) -> JoinHandle<Result<()>> {
        let addr = self.addr;
        let pool = Arc::clone(&self.pool);
        let engine = self.engine.clone();

        thread::spawn(move || {
            let listener = TcpListener::bind(&addr)?;
            let pool = pool.lock().unwrap();
            for stream in listener.incoming() {
                let engine = engine.clone();
                pool.spawn(move || match stream {
                    Ok(stream) => {
                        if let Err(e) = serve(engine, stream) {
                            error!("Error on serving client: {}", e);
                        }
                    }
                    Err(e) => error!("Connection failed: {}", e),
                })
            }

            Ok(())
        })
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
            Request::Set { key, value } => {
                send_response!(match task::block_on(engine.set(key, value)) {
                    Ok(()) => SetResponse::Ok(()),
                    Err(e) => SetResponse::Err(format!("{}", e)),
                })
            }
            Request::Get { key } => send_response!(match task::block_on(engine.get(key)) {
                Ok(value) => GetResponse::Ok(value),
                Err(e) => GetResponse::Err(format!("{}", e)),
            }),
            Request::Remove { key } => send_response!(match task::block_on(engine.remove(key)) {
                Ok(()) => RemoveResponse::Ok(()),
                Err(e) => RemoveResponse::Err(format!("{}", e)),
            }),
        };
    }

    Ok(())
}
