pub struct KvStore {
    storage: std::collections::HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore {
            storage: std::collections::HashMap::new(),
        }
    }

    pub fn set(&mut self, key: String, value: String) {
        self.storage.insert(key, value);
    }

    pub fn get(&self, key: String) -> Option<String> {
        self.storage.get(&key).map(|x| x.into())
    }

    pub fn remove(&mut self, key: String) {
        self.storage.remove(&key);
    }
}
