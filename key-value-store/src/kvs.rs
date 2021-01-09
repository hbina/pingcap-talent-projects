use crate::utility::{
    parse_log_reader, sorted_gen_list, BufReaderWithPos, BufWriterWithPos, PointerMap, Result,
    WriteCommand, STORE_EXT,
};

pub struct KvStore {
    index: PointerMap,
    path: std::path::PathBuf,
    writer: BufWriterWithPos<std::fs::File>,
    leftover: u64,
}

impl KvStore {
    pub fn open(path: impl Into<std::path::PathBuf>) -> Result<KvStore> {
        let path = path.into();
        std::fs::create_dir_all(&path)?;
        let files = sorted_gen_list(&path)?;
        let mut leftover = 0;
        let index = files.iter().map(|file| parse_log_reader(file)).fold(
            std::collections::HashMap::new(),
            |mut acc, i| {
                match i {
                    Ok(mut c) => c.drain().for_each(|(k, v)| {
                        if let Some(left) = acc.insert(k, v) {
                            leftover += left.2 - left.1;
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
            },
        );
        let path = path.join(format!(
            "{}.{}",
            std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)?
                .as_micros(),
            STORE_EXT
        ));
        let writer = BufWriterWithPos::new(
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
            leftover,
        })
    }

    pub fn set(&mut self, key: String, value: std::string::String) -> Result<()> {
        self.write_log_to_file(key.clone(), &WriteCommand::Set(key.clone(), value.clone()))?;
        Ok(())
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        if let Some((log_file, begin, end)) = self.index.get(&key) {
            let mut reader =
                BufReaderWithPos::new(std::fs::OpenOptions::new().read(true).open(&log_file)?)?;
            std::io::Seek::seek(&mut reader, std::io::SeekFrom::Start(*begin))?;
            if let Some(command) = serde_json::Deserializer::from_reader(&mut reader)
                .into_iter::<WriteCommand>()
                .next()
            {
                match command? {
                    WriteCommand::Set(old_key, value) => {
                        assert_eq!(old_key, key);
                        println!("{}", value);
                        Ok(Some(value))
                    }
                    WriteCommand::Remove(old_key) => {
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

    pub fn remove(&mut self, key: String) -> Result<()> {
        if let Some((log_file, begin, _)) = self.index.get(&key) {
            let mut reader =
                BufReaderWithPos::new(std::fs::OpenOptions::new().read(true).open(&log_file)?)?;
            std::io::Seek::seek(&mut reader, std::io::SeekFrom::Start(*begin))?;
            if let Some(command) = serde_json::Deserializer::from_reader(&mut reader)
                .into_iter::<WriteCommand>()
                .next()
            {
                match command? {
                    WriteCommand::Set(old_key, _) => {
                        assert_eq!(key, old_key);
                        self.write_log_to_file(key.clone(), &WriteCommand::Remove(key.clone()))?;
                    }
                    WriteCommand::Remove(old_key) => {
                        assert_eq!(key, old_key);
                        panic!("Already deleted the key{}", old_key);
                    }
                }
            }
            Ok(())
        } else {
            println!("Key not found");
            Err(Box::new(crate::utility::KvsCommandError::KeyNotFound))
        }
    }

    fn write_log_to_file(&mut self, key: String, command: &WriteCommand) -> Result<()> {
        let current_pos = self.writer.pos();
        serde_json::to_writer(&mut self.writer, &command)?;
        std::io::Write::flush(&mut self.writer)?;
        if let Some((_, begin, end)) = self
            .index
            .insert(key, (self.path.clone(), current_pos, self.writer.pos()))
        {
            self.update_left_over(end - begin);
        }
        Ok(())
    }

    fn update_left_over(&mut self, key: u64) {
        self.leftover += key;
    }
}
