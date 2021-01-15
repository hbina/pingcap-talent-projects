#[derive(Debug)]
pub struct KvsNotFound;

impl std::fmt::Display for KvsNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Key not found")
    }
}

impl std::error::Error for KvsNotFound {}
