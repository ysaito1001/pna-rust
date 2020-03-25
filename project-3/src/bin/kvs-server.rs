use kvs::{KvStore, KvsError, KvsServer, Result, SledKvsEngine};

use clap::{load_yaml, App};
use log::{error, info, LevelFilter};
use sled;

use std::env::current_dir;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
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

    let engine_file = current_dir()?.join("engine");
    same_engine_as_last_time(&engine_file, &engine)?;
    fs::write(engine_file, format!("{}", engine))?;

    match engine {
        "kvs" => {
            let server = KvsServer::new(KvStore::open(current_dir()?)?);
            server.run(addr)
        }
        "sled" => {
            let server = KvsServer::new(SledKvsEngine::new(sled::open(current_dir()?)?));
            server.run(addr)
        }
        _ => unreachable!(),
    }
}

fn same_engine_as_last_time(engine_file: &PathBuf, engine: &str) -> Result<()> {
    match previous_engine(&engine_file)? {
        Some(previous_engine) if previous_engine != engine => Err(KvsError::StringError(format!(
            "Attempting to use {} while the previous engine was {}",
            engine, previous_engine
        ))),
        _ => Ok(()),
    }
}

fn previous_engine(engine_file: &PathBuf) -> Result<Option<String>> {
    if !engine_file.exists() {
        return Ok(None);
    }

    match fs::read_to_string(engine_file) {
        Ok(engine) => Ok(Some(engine)),
        Err(e) => {
            error!("The content of engine file is invalid: {}", e);
            Ok(None)
        }
    }
}

fn main() {
    if let Err(e) = run() {
        error!("{}", e);
        exit(1);
    }
}
