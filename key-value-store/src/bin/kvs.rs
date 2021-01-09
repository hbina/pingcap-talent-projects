extern crate clap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = clap::App::new(clap::crate_name!())
        .version(clap::crate_version!())
        .author(clap::crate_authors!())
        .about(clap::crate_description!())
        .arg(
            clap::Arg::with_name("LOG_PATH")
                .long("path")
                .help("Path to log folders")
                .takes_value(true)
                .required(false),
        )
        .subcommand(
            clap::SubCommand::with_name("set")
                .about("Set the key to a value")
                .arg(
                    clap::Arg::with_name("key")
                        .help("The key")
                        .takes_value(true)
                        .required(true)
                        .index(1),
                )
                .arg(
                    clap::Arg::with_name("value")
                        .help("The value")
                        .takes_value(true)
                        .required(true)
                        .index(2),
                ),
        )
        .subcommand(
            clap::SubCommand::with_name("get")
                .about("Get the value set by key")
                .arg(
                    clap::Arg::with_name("key")
                        .help("The key")
                        .takes_value(true)
                        .required(true)
                        .index(1),
                ),
        )
        .subcommand(
            clap::SubCommand::with_name("rm")
                .about("Remove a key value")
                .arg(
                    clap::Arg::with_name("key")
                        .help("The key")
                        .takes_value(true)
                        .required(true)
                        .index(1),
                ),
        )
        .get_matches();

    let path = matches
        .value_of("LOG_PATH")
        .map(|p| std::path::PathBuf::from(p))
        .unwrap_or(".".into());

    let mut keystore = kvs::kvs::KvStore::open(path)?;

    if let Some(matches) = matches.subcommand_matches("set") {
        let key = matches.value_of("key").unwrap();
        let value = matches.value_of("value").unwrap();
        keystore.set(key.into(), value.into())?;
    } else if let Some(matches) = matches.subcommand_matches("get") {
        let key = matches.value_of("key").unwrap();
        keystore.get(key.into())?;
    } else if let Some(matches) = matches.subcommand_matches("rm") {
        let key = matches.value_of("key").unwrap();
        keystore.remove(key.into())?;
    } else {
        unimplemented!("No supported command provided")
    }
    Ok(())
}
