use kvs::{KvsClient, Result};

use clap::{load_yaml, App};

use std::net::SocketAddr;
use std::process::exit;

fn run() -> Result<()> {
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

            let mut client = KvsClient::connect(addr)?;
            client.set(key, value)?;
        }
        ("get", Some(matches)) => {
            let key = matches
                .value_of("KEY")
                .expect("KEY argument missing")
                .to_string();
            let addr: SocketAddr = matches.value_of("addr").unwrap().parse()?;

            let mut client = KvsClient::connect(addr)?;
            if let Some(value) = client.get(key)? {
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

            let mut client = KvsClient::connect(addr)?;
            client.remove(key)?;
        }
        _ => unreachable!(),
    }

    Ok(())
}

fn main() {
    if let Err(e) = run() {
        eprintln!("{}", e);
        exit(1);
    }
}
