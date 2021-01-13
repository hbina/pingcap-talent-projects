use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum WriteCommand {
    Set(String, String),
    Remove(String),
}

pub enum KvsCommand<'a> {
    Set(&'a str, &'a str),
    Get(&'a str),
    Remove(&'a str),
}
