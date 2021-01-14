extern crate clap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    std::env::vars().for_each(|(key, value)| log::info!("{} => {}", key, value));
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
    log::info!("Connecting to {} engine at {}", engine, addr);
    let listener = std::net::TcpListener::bind(addr)?;
    let keystore = std::sync::Arc::new(std::sync::Mutex::new(kvs::kvs::KvStore::open(log_path)?));
    for stream in listener.incoming() {
        let mut stream = stream.unwrap();
        let keystore = keystore.clone();
        std::thread::spawn(move || {
            let mut ks = keystore.lock().unwrap();
            let mut deserializer =
                serde_json::Deserializer::from_reader(&mut stream).into_iter::<String>();
            if let Some(command) = deserializer.next() {
                let buffer = command.unwrap();
                let result = execute_command(&mut *ks, buffer.as_str()).unwrap();
                serde_json::to_writer(&mut stream, &result).unwrap();
                std::io::Write::flush(&mut stream).unwrap();
            }
        });
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

fn execute_command(
    keystore: &mut kvs::kvs::KvStore,
    buffer: &str,
) -> kvs::types::Result<Option<String>> {
    if let Some(command) = parse_command(buffer.trim_end()) {
        match command {
            kvs::enums::KvsCommand::Set(key, value) => {
                keystore.set(key.into(), value.into())?;
            }
            kvs::enums::KvsCommand::Get(key) => {
                if let Some(value) = keystore.get(key.into())? {
                    return Ok(Some(format!("{}", value)));
                } else {
                    return Ok(Some(format!("Key not found")));
                }
            }
            kvs::enums::KvsCommand::Remove(key) => {
                if let Err(_) = keystore.remove(key.into()) {
                    return Ok(Some(format!("Key not found")));
                }
            }
        }
    }
    Ok(None)
}
