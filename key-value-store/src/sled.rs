pub struct SledStore(pub sled::Db);

impl SledStore {
    pub fn open(log_path: impl Into<std::path::PathBuf>) -> crate::types::Result<SledStore> {
        let log_path = log_path.into();
        let files = crate::utilities::get_sorted_kvs_log_files(&log_path)?;
        if !files.is_empty() {
            return Err(Box::new(crate::errors::MismatchEngine(
                crate::utilities::SLED_EXT.into(),
                files,
            )));
        }
        Ok(SledStore(sled::open(log_path)?))
    }
}

impl crate::traits::KvsEngine for SledStore {
    fn set(&mut self, key: String, value: String) -> crate::types::Result<()> {
        self.0.insert(key.as_bytes(), value.as_bytes())?;
        Ok(())
    }

    fn get(&self, key: String) -> crate::types::Result<Option<String>> {
        if let Some(result) = self.0.get(key.as_bytes())?.map(convert_ivec_to_string) {
            Ok(Some(result?))
        } else {
            Ok(None)
        }
    }

    fn remove(&mut self, key: String) -> crate::types::Result<()> {
        if let Some(_) = self.0.remove(key.as_bytes())? {
            Ok(())
        } else {
            Err(Box::new(crate::errors::KvsNotFound))
        }
    }
}

fn convert_ivec_to_string(ivec: sled::IVec) -> crate::types::Result<String> {
    Ok(String::from_utf8(Vec::from(&*ivec))?)
}
