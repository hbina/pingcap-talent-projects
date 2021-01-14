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
        .subcommand(
            clap::SubCommand::with_name("set")
                .about("Associate a key with a value")
                .arg(
                    clap::Arg::with_name("key")
                        .help("The key must be one continuous word (no whitespaces)")
                        .takes_value(true)
                        .required(true),
                )
                .arg(
                    clap::Arg::with_name("value")
                        .help("The value must be one continuous word (no whitespaces)")
                        .takes_value(true)
                        .required(true),
                )
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
                ),
        )
        .subcommand(
            clap::SubCommand::with_name("get")
                .about("Get the value associated to a key")
                .arg(
                    clap::Arg::with_name("key")
                        .help("The key must be one continuous word (no whitespaces)")
                        .takes_value(true)
                        .required(true),
                )
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
                ),
        )
        .subcommand(
            clap::SubCommand::with_name("rm")
                .about("Dissociate a value from a key")
                .arg(
                    clap::Arg::with_name("key")
                        .help("The key must be one continuous word (no whitespaces)")
                        .takes_value(true)
                        .required(true),
                )
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
                ),
        )
        .get_matches();
    let addr = matches.value_of("addr").unwrap();
    if let Some((set, command_addr)) = convert_matches_to_string(&matches) {
        send_command(&set, &command_addr.unwrap_or(addr))?;
    } else {
        loop {
            print!("kvs>");
            std::io::Write::flush(&mut std::io::stdout())?;
            let input = {
                let mut input = String::new();
                std::io::stdin()
                    .read_line(&mut input)
                    .expect("Failed to read line");
                input
            };
            send_command(&input, addr)?;
        }
    }
    Ok(())
}

fn convert_matches_to_string<'a>(
    matches: &'a clap::ArgMatches,
) -> Option<(String, Option<&'a str>)> {
    if let Some(set) = matches.subcommand_matches("set") {
        let key = set.value_of("key").unwrap();
        let value = set.value_of("value").unwrap();
        Some((format!("set {} {}", key, value), set.value_of("addr")))
    } else if let Some(set) = matches.subcommand_matches("get") {
        let key = set.value_of("key").unwrap();
        Some((format!("get {}", key), set.value_of("addr")))
    } else if let Some(set) = matches.subcommand_matches("rm") {
        let key = set.value_of("key").unwrap();
        Some((format!("rm {}", key), set.value_of("addr")))
    } else {
        None
    }
}

fn send_command(string: &String, addr: &str) -> kvs::types::Result<()> {
    let mut stream = std::net::TcpStream::connect(addr)?;
    serde_json::to_writer(&mut stream, &string)?;
    std::io::Write::flush(&mut stream)?;
    let mut stream =
        serde_json::Deserializer::from_reader(&mut stream).into_iter::<Option<String>>();
    if let Some(Ok(Some(response))) = stream.next() {
        println!("{}", response);
    }
    Ok(())
}
