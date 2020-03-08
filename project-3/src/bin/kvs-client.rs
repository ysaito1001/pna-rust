use kvs::Result;

use clap::{load_yaml, App};

use std::net::{SocketAddr, TcpStream};

fn main() -> Result<()> {
    let yaml = load_yaml!("cli-client.yml");
    let matches = App::from_yaml(yaml).get_matches();

    match matches.subcommand() {
        ("set", Some(matches)) => {
            let addr: SocketAddr = matches.value_of("addr").unwrap().parse()?;
            if let Ok(stream) = TcpStream::connect(addr) {
                println!("Connected to the server: {:?}", stream);
            }
        }
        ("get", Some(matches)) => {
            let addr: SocketAddr = matches.value_of("addr").unwrap().parse()?;
            if let Ok(stream) = TcpStream::connect(addr) {
                println!("Connected to the server: {:?}", stream);
            }
        }
        ("rm", Some(matches)) => {
            let addr: SocketAddr = matches.value_of("addr").unwrap().parse()?;
            if let Ok(stream) = TcpStream::connect(addr) {
                println!("Connected to the server: {:?}", stream);
            }
        }
        _ => unreachable!(),
    }

    Ok(())
}
