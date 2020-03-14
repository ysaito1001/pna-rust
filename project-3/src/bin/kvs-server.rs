use kvs::{KvStore, KvsServer, Result};

use clap::{load_yaml, App};
use log::{error, info, LevelFilter};

use std::env::current_dir;
use std::net::SocketAddr;
use std::process::exit;

fn run() -> Result<()> {
    env_logger::builder().filter_level(LevelFilter::Info).init();

    let yaml = load_yaml!("cli-server.yml");
    let matches = App::from_yaml(yaml).get_matches();

    let addr: SocketAddr = matches.value_of("addr").unwrap().parse()?;
    let engine = matches.value_of("engine").unwrap();

    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {}", engine);
    info!("Listening on {}", addr);

    let engine = KvStore::open(current_dir()?)?;
    let server = KvsServer::new(engine);

    server.run(addr)
}

fn main() {
    if let Err(e) = run() {
        error!("{}", e);
        exit(1);
    }
}
