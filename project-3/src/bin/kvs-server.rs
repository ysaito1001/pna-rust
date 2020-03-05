use kvs::Result;

use clap::{load_yaml, App};

use std::net::SocketAddr;

fn main() -> Result<()> {
    let yaml = load_yaml!("cli-server.yml");
    let matches = App::from_yaml(yaml).get_matches();

    let _addr: SocketAddr = matches.value_of("addr").unwrap().parse()?;
    let _engine = matches.value_of("engine").unwrap();

    unimplemented!();

    Ok(())
}
