pub fn parse_log_reader(
    path: &std::path::PathBuf,
) -> crate::types::Result<std::collections::HashMap<String, (crate::enums::WriteCommand, u64)>> {
    let mut reader =
        crate::buffer::BufReaderWithPos::new(std::fs::OpenOptions::new().read(true).open(path)?)?;
    let mut index = std::collections::HashMap::new();
    let mut pos = std::io::Seek::seek(&mut reader, std::io::SeekFrom::Start(0))?;
    let mut stream = serde_json::Deserializer::from_reader(&mut reader)
        .into_iter::<crate::enums::WriteCommand>();
    while let Some(command) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        let offset_pos = new_pos - pos;
        match command? {
            crate::enums::WriteCommand::Set(key, value) => {
                index.insert(
                    key.clone(),
                    (crate::enums::WriteCommand::Set(key, value), offset_pos),
                );
            }
            crate::enums::WriteCommand::Remove(key) => {
                index.insert(
                    key.clone(),
                    (crate::enums::WriteCommand::Remove(key), offset_pos),
                );
            }
        };
        pos = new_pos;
    }
    Ok(index)
}

pub fn log_files(path: &std::path::PathBuf) -> crate::types::Result<Vec<std::path::PathBuf>> {
    Ok(std::fs::read_dir(path)?
        .flat_map(|res| -> crate::types::Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some(STORE_EXT.as_ref()))
        .collect::<Vec<std::path::PathBuf>>())
}

pub fn sorted_log_files(
    path: &std::path::PathBuf,
) -> crate::types::Result<Vec<std::path::PathBuf>> {
    let mut log_files = log_files(path)?;
    log_files.sort_unstable();
    Ok(log_files)
}

pub const STORE_EXT: &str = "kvslog";

pub fn new_log_file(
    path: &std::path::Path,
) -> crate::types::Result<crate::buffer::BufWriterWithPos<std::fs::File>> {
    let path = path.join(format!(
        "{}.{}",
        std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
            .as_micros(),
        crate::utilities::STORE_EXT
    ));
    Ok(crate::buffer::BufWriterWithPos::new(
        std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .write(true)
            .create(true)
            .open(&path)?,
    )?)
}

pub fn write_command<W: std::io::Write + std::io::Seek>(
    writer: &mut crate::buffer::BufWriterWithPos<W>,
    command: &crate::enums::WriteCommand,
) -> crate::types::Result<u64> {
    let begin_pos = writer.pos();
    serde_json::to_writer(&mut *writer, &command)?;
    std::io::Write::flush(&mut *writer)?;
    let end_pos = writer.pos();
    Ok(end_pos - begin_pos)
}
