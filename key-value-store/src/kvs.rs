pub struct KvStore {
    index: crate::utility::PointerMap,
    path: std::path::PathBuf,
    writer: crate::utility::BufWriterWithPos<std::fs::File>,
    total_bytes: u64,
    wasted_bytes: u64,
}

impl KvStore {
    pub fn open(path: impl Into<std::path::PathBuf>) -> crate::utility::Result<KvStore> {
        let path = path.into();
        std::fs::create_dir_all(&path)?;
        let files = crate::utility::sorted_log_files(&path)?;
        let mut wasted_bytes = 0;
        let mut total_bytes = 0;
        let index = files
            .iter()
            .map(|file| crate::utility::parse_log_reader(file))
            .fold(std::collections::HashMap::new(), |mut acc, i| {
                match i {
                    Ok(mut c) => c.drain().for_each(|(k, v)| {
                        total_bytes += v.2 - v.1;
                        if let Some(left) = acc.insert(k, v) {
                            wasted_bytes += left.2 - left.1;
                        }
                    }),
                    Err(error) => {
                        panic!(
                            "An error occured parsing the previous logs with error:{}",
                            error
                        )
                    }
                }
                acc
            });
        let path = path.join(format!(
            "{}.{}",
            std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                .as_micros(),
            crate::utility::STORE_EXT
        ));
        let writer = crate::utility::BufWriterWithPos::new(
            std::fs::OpenOptions::new()
                .read(true)
                .append(true)
                .write(true)
                .create(true)
                .open(&path)?,
        )?;
        Ok(KvStore {
            index,
            path,
            writer,
            total_bytes,
            wasted_bytes,
        })
    }

    pub fn set(&mut self, key: String, value: std::string::String) -> crate::utility::Result<()> {
        self.write_log_to_file(
            key.clone(),
            &crate::utility::WriteCommand::Set(key.clone(), value.clone()),
        )?;
        if self.waste_ratio() > 0.25f64 {
            self.do_compaction()?;
        }
        Ok(())
    }

    pub fn get(&self, key: String) -> crate::utility::Result<Option<String>> {
        if let Some((log_file, begin, end)) = self.index.get(&key) {
            let mut reader = crate::utility::BufReaderWithPos::new(
                std::fs::OpenOptions::new().read(true).open(&log_file)?,
            )?;
            std::io::Seek::seek(&mut reader, std::io::SeekFrom::Start(*begin))?;
            if let Some(command) = serde_json::Deserializer::from_reader(&mut reader)
                .into_iter::<crate::utility::WriteCommand>()
                .next()
            {
                match command? {
                    crate::utility::WriteCommand::Set(old_key, value) => {
                        assert_eq!(old_key, key);
                        println!("{}", value);
                        Ok(Some(value))
                    }
                    crate::utility::WriteCommand::Remove(old_key) => {
                        assert_eq!(old_key, key);
                        println!("Key not found");
                        Ok(None)
                    }
                }
            } else {
                panic!(
                    r#"Unable to deserialize the command from log {:?} at (begin, end) offset at ({}, {})"#,
                    log_file, begin, end
                );
            }
        } else {
            println!("Key not found");
            Ok(None)
        }
    }

    pub fn remove(&mut self, key: String) -> crate::utility::Result<()> {
        if let Some((log_file, begin, end)) = self.index.get(&key) {
            let mut reader = crate::utility::BufReaderWithPos::new(
                std::fs::OpenOptions::new().read(true).open(&log_file)?,
            )?;
            std::io::Seek::seek(&mut reader, std::io::SeekFrom::Start(*begin))?;
            if let Some(command) = serde_json::Deserializer::from_reader(&mut reader)
                .into_iter::<crate::utility::WriteCommand>()
                .next()
            {
                match command? {
                    crate::utility::WriteCommand::Set(old_key, _) => {
                        assert_eq!(key, old_key);
                        self.write_log_to_file(
                            key.clone(),
                            &crate::utility::WriteCommand::Remove(key.clone()),
                        )?;
                    }
                    crate::utility::WriteCommand::Remove(old_key) => {
                        assert_eq!(key, old_key);
                    }
                }
            } else {
                panic!(
                    r#"Unable to deserialize the command from log {:?} at (begin, end) offset at ({}, {})"#,
                    log_file, begin, end
                );
            }
            Ok(())
        } else {
            println!("Key not found");
            Err(Box::new(crate::utility::KvsCommandError::KeyNotFound))
        }
    }

    fn waste_ratio(&self) -> f64 {
        self.wasted_bytes as f64 / self.total_bytes as f64
    }

    fn do_compaction(&mut self) -> crate::utility::Result<()> {
        println!("doing compaction...");
        Ok(())
    }

    fn clear_log_file(&self) -> crate::utility::Result<()> {
        for f in crate::utility::log_files(&self.path)? {
            std::fs::remove_file(f)?;
        }
        Ok(())
    }

    fn dump_log_file(&self) -> crate::utility::Result<()> {
        Ok(())
    }

    fn write_log_to_file(
        &mut self,
        key: String,
        command: &crate::utility::WriteCommand,
    ) -> crate::utility::Result<()> {
        let current_pos = self.writer.pos();
        serde_json::to_writer(&mut self.writer, &command)?;
        std::io::Write::flush(&mut self.writer)?;
        if let Some((_, begin, end)) = self
            .index
            .insert(key, (self.path.clone(), current_pos, self.writer.pos()))
        {
            self.update_wasted_bytes(end - begin);
        }
        Ok(())
    }

    fn update_wasted_bytes(&mut self, command_size: u64) {
        println!("wasted_space:{}", self.wasted_bytes);
        self.wasted_bytes += command_size;
    }
}
