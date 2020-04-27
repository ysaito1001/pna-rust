use std::{
    io::{BufReader, BufWriter, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
};

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
    shutdown_requested: Arc<AtomicBool>,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    pub fn new(engine: E, pool: P, addr: SocketAddr) -> Self {
        KvsServer {
            engine,
            pool: Arc::new(Mutex::new(pool)),
            addr,
            shutdown_requested: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn run(&self) -> JoinHandle<Result<()>> {
        let addr = self.addr;
        let pool = Arc::clone(&self.pool);
        let engine = self.engine.clone();
        let shutdown_requested = Arc::clone(&self.shutdown_requested);

        thread::spawn(move || {
            let listener = TcpListener::bind(&addr)?;
            let pool = pool.lock().unwrap();
            for stream in listener.incoming() {
                if shutdown_requested.load(Ordering::SeqCst) {
                    break;
                }

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

    pub fn initiate_shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(&self.addr);
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
