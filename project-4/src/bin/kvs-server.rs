use std::env::current_dir;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::exit;

use clap::{load_yaml, App};
use log::{error, info, LevelFilter};
use sled;

use kvs::thread_pool::{NaiveThreadPool, RayonThreadPool, SharedQueueThreadPool, ThreadPool};
use kvs::{KvStore, KvsError, KvsServer, Result, SledKvsEngine};

macro_rules! with_engine {
    ($engine: expr, $path: expr, |$name: ident| $block: block) => {{
        match $engine {
            "kvs" => {
                let $name = KvStore::open($path)?;
                let result: Result<()> = $block;
                result
            }
            "sled" => {
                let $name = SledKvsEngine::new(sled::open($path)?);
                let result: Result<()> = $block;
                result
            }
            _ => unreachable!(),
        }
    }};
}

macro_rules! with_pool {
    ($pool: expr, $num_cpus: expr, |$name: ident| $block: block) => {{
        match $pool {
            "naive" => {
                let $name = NaiveThreadPool::new($num_cpus)?;
                let result: Result<()> = $block;
                result
            }
            "shared_queue" => {
                let $name = SharedQueueThreadPool::new($num_cpus)?;
                let result: Result<()> = $block;
                result
            }
            "rayon" => {
                let $name = RayonThreadPool::new($num_cpus)?;
                let result: Result<()> = $block;
                result
            }
            _ => unreachable!(),
        }
    }};
}

fn run() -> Result<()> {
    env_logger::builder().filter_level(LevelFilter::Info).init();

    let yaml = load_yaml!("cli-server.yml");
    let matches = App::from_yaml(yaml).get_matches();

    let addr: SocketAddr = matches.value_of("addr").unwrap().parse()?;
    let engine: &str = matches.value_of("engine").unwrap();
    let pool: &str = matches.value_of("pool").unwrap();

    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {}", engine);
    info!("Listening on {}", addr);

    let engine_file = current_dir()?.join("engine");
    same_engine_as_last_time(&engine_file, &engine)?;
    fs::write(engine_file, format!("{}", engine))?;

    with_engine!(engine, current_dir()?, |engine| {
        with_pool!(pool, num_cpus::get(), |pool| {
            let server = KvsServer::new(engine, pool);
            server.run(addr)?;
            Ok(())
        })
    })?;

    Ok(())
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
