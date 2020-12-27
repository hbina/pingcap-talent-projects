use std::io::prelude::*;

mod thread_pool;

fn main() {
    let matches = clap::App::new("My Super Program")
        .version("1.0")
        .author("Kevin K. <kbknapp@gmail.com>")
        .about("Does awesome things")
        .arg(
            clap::Arg::with_name("PORT")
                .value_name("PORT")
                .help("Sets a custom config file")
                .required(true)
                .takes_value(true),
        )
        .get_matches();

    let port = matches.value_of("PORT").unwrap();

    let listener = std::net::TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();

    let pool = thread_pool::ThreadPool::new(4);

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        pool.execute(|| {
            handle_connection(stream);
        });
    }
}

fn handle_connection(mut stream: std::net::TcpStream) {
    let mut buffer = [0; 1024];
    stream.read(&mut buffer).unwrap();

    let get = b"GET / HTTP/1.1\r\n";
    let sleep = b"GET /sleep HTTP/1.1\r\n";

    let (status_line, filename) = if buffer.starts_with(get) {
        ("HTTP/1.1 200 OK\r\n", "hello.html")
    } else if buffer.starts_with(sleep) {
        std::thread::sleep(std::time::Duration::from_secs(5));
        ("HTTP/1.1 200 OK\r\n", "hello.html")
    } else {
        ("HTTP/1.1 404 NOT FOUND\r\n", "404.html")
    };

    let contents = std::fs::read_to_string(filename).unwrap();
    let response = format!(
        "{}Content-Length: {}\r\n\r\n{}",
        status_line,
        contents.len(),
        contents
    );
    stream.write(response.as_bytes()).unwrap();
    stream.flush().unwrap();
}
