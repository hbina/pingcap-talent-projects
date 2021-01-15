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

pub fn get_log_files(
    extension: &str,
    path: &std::path::PathBuf,
) -> crate::types::Result<Vec<std::path::PathBuf>> {
    Ok(std::fs::read_dir(path)?
        .flat_map(|res| -> crate::types::Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some(extension.as_ref()))
        .collect::<Vec<std::path::PathBuf>>())
}

pub fn get_sorted_kvs_log_files(
    path: &std::path::PathBuf,
) -> crate::types::Result<Vec<std::path::PathBuf>> {
    let mut log_files = get_log_files(KVS_EXT, path)?;
    log_files.sort_unstable();
    Ok(log_files)
}

pub fn sled_log_files_exist(path: &std::path::PathBuf) -> crate::types::Result<bool> {
    Ok(get_log_files(SLED_EXT, path)?
        .iter()
        .any(|x| check_pathbuf_is_not(x, "conf"))
        && get_log_files(SLED_EXT, path)?
            .iter()
            .any(|x| check_pathbuf_is_not(x, "db")))
}

fn check_pathbuf_is_not(path: &std::path::PathBuf, is_not: &str) -> bool {
    path.file_name()
        .map_or(false, |x| x == &std::path::PathBuf::from(is_not))
}

pub const KVS_EXT: &str = "kvs";
pub const SLED_EXT: &str = "sled";

pub fn get_log_name(extension: &str) -> crate::types::Result<String> {
    Ok(format!(
        "{}.{}",
        std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
            .as_micros(),
        extension
    ))
}

pub fn new_kvs_log_file(
    path: &std::path::Path,
) -> crate::types::Result<crate::buffer::BufWriterWithPos<std::fs::File>> {
    let path = path.join(get_log_name(KVS_EXT)?);
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
