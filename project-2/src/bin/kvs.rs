use kvs::{KvStore, Result};

use clap::{load_yaml, App};

use std::env::current_dir;

fn main() -> Result<()> {
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml).get_matches();

    match matches.subcommand() {
        ("set", Some(_)) => {
            let key = matches.value_of("KEY").expect("KEY argument missing");
            let value = matches.value_of("VALUE").expect("VALUE argument missing");

            let mut kvs = KvStore::open(current_dir()?)?;
            kvs.set(key.to_string(), value.to_string())?
        }
        ("get", Some(_)) => {
            let key = matches.value_of("KEY").expect("KEY argument missing");

            let mut kvs = KvStore::open(current_dir()?)?;
            match kvs.get(key.to_string())? {
                Some(value) => println!("{}", value),
                None => println!("Key not found"),
            }
        }
        ("rm", Some(_)) => {
            let key = matches.value_of("KEY").expect("KEY argument missing");

            let mut kvs = KvStore::open(current_dir()?)?;
            match kvs.remove(key.to_string()) {
                Ok(()) => {}
                Err(e) => return Err(e),
            }
        }
        _ => unreachable!(),
    }

    Ok(())
}
