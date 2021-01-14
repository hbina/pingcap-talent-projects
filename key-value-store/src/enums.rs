use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum WriteCommand {
    Set(String, String),
    Remove(String),
}

#[derive(Debug, Clone)]
pub enum KvsCommand<'a> {
    Set(&'a str, &'a str),
    Get(&'a str),
    Remove(&'a str),
}
