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
        .get_matches();
    let addr = matches.value_of("addr").unwrap();
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
        let mut stream = std::net::TcpStream::connect(addr)?;
        std::io::Write::write_all(&mut stream, input.as_bytes())?;
        std::io::Write::flush(&mut stream)?;
    }
}
