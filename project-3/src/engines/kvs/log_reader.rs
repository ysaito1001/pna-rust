use std::io::{self, BufReader, Read, Seek, SeekFrom};

use crate::Result;

#[derive(Debug)]
pub struct LogReader<R: Read + Seek> {
    reader: BufReader<R>,
    current_position: u64,
}

impl<R: Read + Seek> LogReader<R> {
    pub fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(LogReader {
            reader: BufReader::new(inner),
            current_position: pos,
        })
    }
}

impl<R: Read + Seek> Read for LogReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let bytes_read = self.reader.read(buf)?;
        self.current_position += bytes_read as u64;
        Ok(bytes_read)
    }
}

impl<R: Read + Seek> Seek for LogReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.current_position = self.reader.seek(pos)?;
        Ok(self.current_position)
    }
}
