use async_std::{
    net::{SocketAddr, TcpListener, TcpStream},
    prelude::*,
    task,
};
use log::{debug, error};

use crate::{
    error::Result,
    protocol::{KvsStream, Request, Response},
    KvsEngine,
};

pub struct KvsServer<E: KvsEngine> {
    engine: E,
    addr: SocketAddr,
}

impl<E: KvsEngine + Sync> KvsServer<E> {
    pub fn new(engine: E, addr: SocketAddr) -> Self {
        KvsServer { engine, addr }
    }

    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        let mut incoming = listener.incoming();

        while let Some(stream) = incoming.next().await {
            let engine = self.engine.clone();
            task::spawn(async move {
                match stream {
                    Ok(stream) => {
                        if let Err(e) = serve(engine, stream).await {
                            error!("Error on serving client: {}", e);
                        }
                    }
                    Err(e) => error!("Connection failed: {}", e),
                }
            });
        }

        Ok(())
    }
}

async fn serve<E: KvsEngine>(engine: E, stream: TcpStream) -> Result<()> {
    let peer_addr = stream.peer_addr()?;
    let mut kvs_stream = KvsStream::new(stream);

    macro_rules! send_response {
        ($resp:expr) => {{
            let response = $resp;
            kvs_stream.send(&response).await?;
            debug!("Response sent to {}: {:?}", peer_addr, response);
        };};
    }

    while let Some(request) = kvs_stream.next().await {
        let request = request?;
        match request {
            Request::Set { key, value } => send_response!(match engine.set(key, value).await {
                Ok(()) => Response::Ok(None),
                Err(e) => Response::Err(format!("{}", e)),
            }),
            Request::Get { key } => send_response!(match engine.get(key).await {
                Ok(value) => Response::Ok(value),
                Err(e) => Response::Err(format!("{}", e)),
            }),
            Request::Remove { key } => send_response!(match engine.remove(key).await {
                Ok(()) => Response::Ok(None),
                Err(e) => Response::Err(format!("{}", e)),
            }),
        };
    }

    Ok(())
}
