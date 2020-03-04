use std::io::{self, BufWriter, Seek, SeekFrom, Write};

use crate::Result;

#[derive(Debug)]
pub struct LogWriter<W: Write + Seek> {
    writer: BufWriter<W>,
    current_position: u64,
}

impl<W: Write + Seek> LogWriter<W> {
    pub fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(LogWriter {
            writer: BufWriter::new(inner),
            current_position: pos,
        })
    }

    pub fn current_position(&self) -> u64 {
        self.current_position
    }
}

impl<W: Write + Seek> Write for LogWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let bytes_written = self.writer.write(buf)?;
        self.current_position += bytes_written as u64;
        Ok(bytes_written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for LogWriter<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.current_position = self.writer.seek(pos)?;
        Ok(self.current_position)
    }
}
