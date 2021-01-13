pub struct BufReaderWithPos<R: std::io::Read + std::io::Seek> {
    reader: std::io::BufReader<R>,
    pos: u64,
}

impl<R: std::io::Read + std::io::Seek> BufReaderWithPos<R> {
    pub fn new(mut inner: R) -> crate::types::Result<Self> {
        let pos = inner.seek(std::io::SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: std::io::BufReader::new(inner),
            pos,
        })
    }
}

impl<R: std::io::Read + std::io::Seek> std::io::Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: std::io::Read + std::io::Seek> std::io::Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

pub struct BufWriterWithPos<W: std::io::Write + std::io::Seek> {
    writer: std::io::BufWriter<W>,
    pos: u64,
}

impl<W: std::io::Write + std::io::Seek> BufWriterWithPos<W> {
    pub fn new(mut inner: W) -> crate::types::Result<Self> {
        let pos = inner.seek(std::io::SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: std::io::BufWriter::new(inner),
            pos,
        })
    }

    pub fn pos(&self) -> u64 {
        self.pos
    }
}

impl<W: std::io::Write + std::io::Seek> std::io::Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

impl<W: std::io::Write + std::io::Seek> std::io::Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
