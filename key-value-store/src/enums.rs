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

#[derive(Debug, Serialize, Deserialize)]
pub enum KvsResponse {
    Success,
    NotFound,
    BadNotFound,
    Message(String),
}

pub enum KvsEngineType {
    KvStore,
    Sled,
}

impl<'a> std::convert::TryFrom<&'a str> for KvsEngineType {
    type Error = crate::errors::InvalidEngine;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        match value {
            "kvs" => Ok(KvsEngineType::KvStore),
            "sled" => Ok(KvsEngineType::Sled),
            c => Err(crate::errors::InvalidEngine(c.into())),
        }
    }
}

impl std::convert::From<&KvsEngineType> for &'static str {
    fn from(v: &KvsEngineType) -> &'static str {
        match v {
            KvsEngineType::KvStore => "kvslog",
            KvsEngineType::Sled => "sled",
        }
    }
}
