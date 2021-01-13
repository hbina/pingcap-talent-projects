pub struct KvStore {
    // TODO: To remove the size of this hashmap, you can calculate the size lazily
    index: std::collections::HashMap<String, (crate::enums::WriteCommand, u64)>,
    log_path: std::path::PathBuf,
    log_writer: crate::buffer::BufWriterWithPos<std::fs::File>,
    total_bytes: u64,
    wasted_bytes: u64,
}

impl KvStore {
    pub fn open(path: impl Into<std::path::PathBuf>) -> crate::types::Result<KvStore> {
        let log_path = path.into();
        std::fs::create_dir_all(&log_path)?;
        let files = crate::utilities::sorted_log_files(&log_path)?;
        let mut wasted_bytes = 0;
        let mut total_bytes = 0;
        let index = files
            .iter()
            .map(|file| crate::utilities::parse_log_reader(file))
            .collect::<crate::types::Result<Vec<_>>>()?
            .iter_mut()
            .fold(std::collections::HashMap::new(), |mut acc, i| {
                i.drain().for_each(|(k, (command, size))| {
                    total_bytes += size;
                    if let Some((_, old_size)) = acc.insert(k, (command, size)) {
                        wasted_bytes += old_size;
                    }
                });
                acc
            });
        let log_writer = crate::utilities::new_log_file(&log_path)?;
        Ok(KvStore {
            index,
            log_path,
            log_writer,
            total_bytes,
            wasted_bytes,
        })
    }

    pub fn set(&mut self, key: String, value: std::string::String) -> crate::types::Result<()> {
        self.insert_command(
            key.clone(),
            crate::enums::WriteCommand::Set(key.clone(), value.clone()),
        )?;
        if self.waste_ratio() > 0.25f64 {
            self.do_compaction()?;
        }
        Ok(())
    }

    pub fn get(&self, key: String) -> crate::types::Result<Option<String>> {
        if let Some((crate::enums::WriteCommand::Set(_, value), _)) = self.index.get(&key) {
            Ok(Some(String::from(value)))
        } else {
            Ok(None)
        }
    }

    pub fn remove(&mut self, key: String) -> crate::types::Result<()> {
        if let Some((crate::enums::WriteCommand::Set(_, _), _)) = self.index.get(&key) {
            self.insert_command(key.clone(), crate::enums::WriteCommand::Remove(key.clone()))?;
            Ok(())
        } else {
            Err(Box::new(crate::errors::KvsCommandError::KeyNotFound))
        }
    }

    fn waste_ratio(&self) -> f64 {
        self.wasted_bytes as f64 / self.total_bytes as f64
    }

    fn do_compaction(&mut self) -> crate::types::Result<()> {
        self.clear_log_file()?;
        self.dump_log_file()?;
        Ok(())
    }

    fn clear_log_file(&mut self) -> crate::types::Result<()> {
        for f in crate::utilities::log_files(&self.log_path)? {
            std::fs::remove_file(f)?;
        }
        Ok(())
    }

    fn dump_log_file(&mut self) -> crate::types::Result<()> {
        let mut writer = crate::utilities::new_log_file(&self.log_path)?;
        for (command, _) in self.index.values() {
            crate::utilities::write_command(&mut writer, command)?;
        }
        self.log_writer = writer;
        Ok(())
    }

    fn insert_command(
        &mut self,
        key: String,
        command: crate::enums::WriteCommand,
    ) -> crate::types::Result<()> {
        let size = crate::utilities::write_command(&mut self.log_writer, &command)?;
        if let Some((_, waste_bytes)) = self.index.insert(key, (command, size)) {
            self.update_wasted_bytes(waste_bytes);
        }
        Ok(())
    }

    fn update_wasted_bytes(&mut self, command_size: u64) {
        self.wasted_bytes += command_size;
    }
}
