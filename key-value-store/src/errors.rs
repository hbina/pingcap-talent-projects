#[derive(Debug)]
pub struct KvsNotFound;

impl std::fmt::Display for KvsNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Key not found")
    }
}

impl std::error::Error for KvsNotFound {}

#[derive(Debug)]
pub struct InvalidEngine(pub String);

impl std::fmt::Display for InvalidEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Unknown engine type {}", self.0)
    }
}

impl std::error::Error for InvalidEngine {}
