use serde::{Deserialize, Serialize};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub fn parse_log_reader(
    path: &std::path::PathBuf,
) -> Result<std::collections::HashMap<String, (WriteCommand, u64)>> {
    let mut reader = BufReaderWithPos::new(std::fs::OpenOptions::new().read(true).open(path)?)?;
    let mut index = std::collections::HashMap::new();
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = serde_json::Deserializer::from_reader(&mut reader).into_iter::<WriteCommand>();
    while let Some(command) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        let offset_pos = new_pos - pos;
        match command? {
            WriteCommand::Set(key, value) => {
                index.insert(key.clone(), (WriteCommand::Set(key, value), offset_pos));
            }
            WriteCommand::Remove(key) => {
                index.insert(key.clone(), (WriteCommand::Remove(key), offset_pos));
            }
        };
        pos = new_pos;
    }
    Ok(index)
}

pub struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    pub fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }

    pub fn pos(&self) -> u64 {
        self.pos
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

pub struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    pub fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }

    pub fn pos(&self) -> u64 {
        self.pos
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}

pub fn log_files(path: &std::path::PathBuf) -> Result<Vec<std::path::PathBuf>> {
    Ok(std::fs::read_dir(path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some(STORE_EXT.as_ref()))
        .collect::<Vec<std::path::PathBuf>>())
}

pub fn sorted_log_files(path: &std::path::PathBuf) -> Result<Vec<std::path::PathBuf>> {
    let mut log_files = log_files(path)?;
    log_files.sort_unstable();
    Ok(log_files)
}

pub fn grab_log_files(path: &std::path::PathBuf) -> Result<Vec<std::path::PathBuf>> {
    let mut entries = std::fs::read_dir(path)?
        .map(|res| res.map(|e| e.path()).map_err(|e| Box::new(e) as _))
        .collect::<Result<Vec<std::path::PathBuf>>>()?;
    entries.sort();
    Ok(entries)
}

#[derive(Debug, Serialize, Deserialize)]
pub enum WriteCommand {
    Set(String, String),
    Remove(String),
}

pub const STORE_EXT: &str = "kvslog";

#[derive(Debug)]
pub enum KvsCommandError {
    KeyNotFound,
    // TODO: Deserialization/Serialization errors
}

impl std::fmt::Display for KvsCommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            KvsCommandError::KeyNotFound => write!(f, "Key not found"),
        }
    }
}

impl std::error::Error for KvsCommandError {}

pub fn new_log_file(
    path: &std::path::Path,
) -> crate::utility::Result<BufWriterWithPos<std::fs::File>> {
    let path = path.join(format!(
        "{}.{}",
        std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
            .as_micros(),
        crate::utility::STORE_EXT
    ));
    Ok(crate::utility::BufWriterWithPos::new(
        std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .write(true)
            .create(true)
            .open(&path)?,
    )?)
}

pub fn write_command<W: std::io::Write + std::io::Seek>(
    writer: &mut crate::utility::BufWriterWithPos<W>,
    command: &crate::utility::WriteCommand,
) -> crate::utility::Result<u64> {
    let begin_pos = writer.pos();
    serde_json::to_writer(&mut *writer, &command)?;
    std::io::Write::flush(&mut *writer)?;
    let end_pos = writer.pos();
    Ok(end_pos - begin_pos)
}
