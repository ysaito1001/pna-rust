use kvs::Result;

use clap::{load_yaml, App};
use log::{info, LevelFilter};

use std::net::SocketAddr;

fn main() -> Result<()> {
    env_logger::builder().filter_level(LevelFilter::Info).init();

    let yaml = load_yaml!("cli-server.yml");
    let matches = App::from_yaml(yaml).get_matches();

    let addr: SocketAddr = matches.value_of("addr").unwrap().parse()?;
    let engine = matches.value_of("engine").unwrap();

    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {}", engine);
    info!("Listening on {}", addr);

    Ok(())
}
