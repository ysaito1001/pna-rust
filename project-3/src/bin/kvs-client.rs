use kvs::Result;

use clap::{load_yaml, App};

fn main() -> Result<()> {
    let yaml = load_yaml!("cli-client.yml");
    let matches = App::from_yaml(yaml).get_matches();

    match matches.subcommand() {
        ("set", Some(_matches)) => {
            unimplemented!();
        }
        ("get", Some(_matches)) => {
            unimplemented!();
        }
        ("rm", Some(_matches)) => {
            unimplemented!();
        }
        _ => unreachable!(),
    }

    Ok(())
}
