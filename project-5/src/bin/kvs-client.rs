use std::{net::SocketAddr, process::exit};

use async_std::task;
use clap::{load_yaml, App};

use kvs::{KvsClient, Result};

async fn run() -> Result<()> {
    let yaml = load_yaml!("cli-client.yml");
    let matches = App::from_yaml(yaml).get_matches();

    match matches.subcommand() {
        ("set", Some(matches)) => {
            let key = matches
                .value_of("KEY")
                .expect("KEY argument missing")
                .to_string();
            let value = matches
                .value_of("VALUE")
                .expect("VALUE argument missing")
                .to_string();
            let addr: SocketAddr = matches.value_of("addr").unwrap().parse()?;

            let mut client = KvsClient::connect(addr).await?;
            client.set(key, value).await?;
        }
        ("get", Some(matches)) => {
            let key = matches
                .value_of("KEY")
                .expect("KEY argument missing")
                .to_string();
            let addr: SocketAddr = matches.value_of("addr").unwrap().parse()?;

            let mut client = KvsClient::connect(addr).await?;
            if let Some(value) = client.get(key).await? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        ("rm", Some(matches)) => {
            let key = matches
                .value_of("KEY")
                .expect("KEY argument missing")
                .to_string();
            let addr: SocketAddr = matches.value_of("addr").unwrap().parse()?;

            let mut client = KvsClient::connect(addr).await?;
            client.remove(key).await?;
        }
        _ => unreachable!(),
    }

    Ok(())
}

fn main() {
    if let Err(e) = task::block_on(run()) {
        eprintln!("{}", e);
        exit(1);
    }
}
