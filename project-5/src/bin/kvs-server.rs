use std::{env::current_dir, path::PathBuf, process::exit};

use async_std::{fs, net::SocketAddr, task};
use clap::{load_yaml, App};
use log::{error, info, LevelFilter};
use sled;

use kvs::{KvStore, KvsError, KvsServer, Result, SledKvsEngine};

macro_rules! with_engine {
    ($engine: expr, $path: expr, |$name: ident| $block: block) => {{
        match $engine {
            "kvs" => {
                let $name = KvStore::open($path).await?;
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

async fn run() -> Result<()> {
    env_logger::builder().filter_level(LevelFilter::Info).init();

    let yaml = load_yaml!("cli-server.yml");
    let matches = App::from_yaml(yaml).get_matches();

    let addr: SocketAddr = matches.value_of("addr").unwrap().parse()?;
    let engine: &str = matches.value_of("engine").unwrap();

    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {}", engine);
    info!("Listening on {}", addr);

    let engine_file = current_dir()?.join("engine");
    same_engine_as_last_time(&engine_file, &engine).await?;
    fs::write(engine_file, format!("{}", engine)).await?;

    with_engine!(engine, current_dir()?, |engine| {
        let server = KvsServer::new(engine, addr);
        server.run().await
    })?;

    Ok(())
}

async fn same_engine_as_last_time(engine_file: &PathBuf, engine: &str) -> Result<()> {
    match previous_engine(&engine_file).await? {
        Some(previous_engine) if previous_engine != engine => Err(KvsError::StringError(format!(
            "Attempting to use {} while the previous engine was {}",
            engine, previous_engine
        ))),
        _ => Ok(()),
    }
}

async fn previous_engine(engine_file: &PathBuf) -> Result<Option<String>> {
    if !engine_file.exists() {
        return Ok(None);
    }

    match fs::read_to_string(engine_file).await {
        Ok(engine) => Ok(Some(engine)),
        Err(e) => {
            error!("The content of engine file is invalid: {}", e);
            Ok(None)
        }
    }
}

fn main() {
    if let Err(e) = task::block_on(run()) {
        error!("{}", e);
        exit(1);
    }
}
