#[derive(Debug)]
pub enum KvsCommandError {
    KeyNotFound,
    // TODO: Deserialization/Serialization errors
}

impl std::fmt::Display for KvsCommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            KvsCommandError::KeyNotFound => write!(f, "Key not found"),
        }
    }
}

impl std::error::Error for KvsCommandError {}
