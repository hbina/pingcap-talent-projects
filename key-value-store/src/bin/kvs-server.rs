extern crate clap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = clap::App::new(clap::crate_name!())
        .version(clap::crate_version!())
        .author(clap::crate_authors!())
        .about(clap::crate_description!())
        .arg(
            clap::Arg::with_name("addr")
                .long("addr")
                .help("The IP address to listen to. Can be IPv4 or IPv6")
                .takes_value(true)
                .required(false)
                .default_value("127.0.0.1:4000"),
        )
        .arg(
            clap::Arg::with_name("engine")
                .long("engine")
                .help("The backend engine to use. Must either be 'kvs' or 'sled'")
                .takes_value(true)
                .required(false)
                .default_value("kvs"),
        )
        .arg(
            clap::Arg::with_name("log-path")
                .long("log-path")
                .help("The path the log files")
                .takes_value(true)
                .required(false)
                .default_value("."),
        )
        .get_matches();
    let log_path = matches
        .value_of("log-path")
        .map(|p| std::path::PathBuf::from(p))
        .unwrap();
    let engine = matches.value_of("engine").unwrap();
    let addr = matches.value_of("addr").unwrap();
    let listener = std::net::TcpListener::bind(addr)?;
    let mut keystore = kvs::kvs::KvStore::open(log_path)?;
    let mut buffer = String::new();
    println!("Started kvs with engine:{}", engine);
    for stream in listener.incoming() {
        let mut stream = stream.unwrap();
        std::io::Read::read_to_string(&mut stream, &mut buffer)?;
        if let Some(command) = parse_command(buffer.trim_end()) {
            println!(
                "{}",
                match command {
                    kvs::enums::KvsCommand::Set(key, value) => {
                        keystore.set(key.into(), value.into())?;
                        format!("Associated the key:'{}' with value:'{}'", key, value)
                    }
                    kvs::enums::KvsCommand::Get(key) => {
                        if let Some(result) = keystore.get(key.into())? {
                            format!("Retrieved value:'{}' from key:'{}'", result, key)
                        } else {
                            format!("There are no value associated with key:'{}'", key)
                        }
                    }
                    kvs::enums::KvsCommand::Remove(key) => {
                        keystore.remove(key.into())?;
                        format!("Removed value with key:'{}'", key)
                    }
                }
            );
        } else {
            println!("error parsing input");
        };
        buffer.clear();
    }
    Ok(())
}

fn parse_until_whitespace(string: &str) -> Option<(&str, &str)> {
    string.find(' ').map(|x| string.split_at(x + 1))
}

fn parse_command(string: &str) -> Option<kvs::enums::KvsCommand> {
    match parse_until_whitespace(string) {
        Some(("set ", rest)) => {
            match rest.split_ascii_whitespace().collect::<Vec<_>>().as_slice() {
                &[key, value] => Some(kvs::enums::KvsCommand::Set(key, value)),
                _ => None,
            }
        }
        Some(("get ", rest)) => {
            match rest.split_ascii_whitespace().collect::<Vec<_>>().as_slice() {
                &[key] => Some(kvs::enums::KvsCommand::Get(key)),
                _ => None,
            }
        }
        Some(("rm ", rest)) => match rest.split_ascii_whitespace().collect::<Vec<_>>().as_slice() {
            &[key] => Some(kvs::enums::KvsCommand::Remove(key)),
            _ => None,
        },
        _ => None,
    }
}
