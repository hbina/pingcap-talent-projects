pub trait KvsEngine {
    fn set(&mut self, key: String, value: String) -> crate::types::Result<()>;
    fn get(&mut self, key: String) -> crate::types::Result<Option<String>>;
    fn remove(&mut self, key: String) -> crate::types::Result<()>;
}
