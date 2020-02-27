use kvs::{KvStore, KvsError, Result};

use clap::{load_yaml, App};

use std::env::current_dir;
use std::process::exit;

fn main() -> Result<()> {
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml).get_matches();

    match matches.subcommand() {
        ("set", Some(matches)) => {
            let key = matches.value_of("KEY").expect("KEY argument missing");
            let value = matches.value_of("VALUE").expect("VALUE argument missing");

            let mut kvs = KvStore::open(current_dir()?)?;
            kvs.set(key.to_string(), value.to_string())?
        }
        ("get", Some(matches)) => {
            let key = matches.value_of("KEY").expect("KEY argument missing");

            let mut kvs = KvStore::open(current_dir()?)?;
            match kvs.get(key.to_string())? {
                Some(value) => println!("{}", value),
                None => println!("Key not found"),
            }
            exit(0);
        }
        ("rm", Some(matches)) => {
            let key = matches.value_of("KEY").expect("KEY argument missing");

            let mut kvs = KvStore::open(current_dir()?)?;
            match kvs.remove(key.to_string()) {
                Ok(()) => {}
                Err(KvsError::KeyNotFound) => {
                    println!("Key not found");
                    exit(1);
                }
                Err(e) => return Err(e),
            }
        }
        _ => unreachable!(),
    }

    Ok(())
}
