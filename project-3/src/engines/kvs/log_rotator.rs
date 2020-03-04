use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

use super::{log_reader::LogReader, log_writer::LogWriter};

use crate::Result;

const LOG_BASENAME: &'static str = "0.log";
const ALTERNATIVE_LOG_BASENAME: &'static str = "1.log";

#[derive(Debug)]
pub struct LogRotator {
    log_directory: PathBuf,
    active_log_basename: &'static str,
    log_reader: LogReader<File>,
    log_writer: LogWriter<File>,
}

impl LogRotator {
    pub fn new(path: std::path::PathBuf) -> Result<LogRotator> {
        let log_file = path.join(LOG_BASENAME);

        let (active_log_file, active_log_basename) = if Path::new(&log_file).exists() {
            (log_file, LOG_BASENAME)
        } else {
            (
                path.join(ALTERNATIVE_LOG_BASENAME),
                ALTERNATIVE_LOG_BASENAME,
            )
        };

        let log_writer = LogWriter::new(new_log_file(&active_log_file)?)?;
        let log_reader = LogReader::new(File::open(active_log_file)?)?;

        Ok(LogRotator {
            log_directory: path,
            active_log_basename: active_log_basename,
            log_reader: log_reader,
            log_writer: log_writer,
        })
    }

    pub fn rotate(&self) -> Result<Self> {
        fs::remove_file(&self.log_directory.join(self.active_log_basename))?;
        Self::new(self.log_directory.clone())
    }

    pub fn get_log_reader(&mut self) -> &mut LogReader<File> {
        &mut self.log_reader
    }

    pub fn get_log_writer(&mut self) -> &mut LogWriter<File> {
        &mut self.log_writer
    }

    pub fn create_compaction_writer(&self) -> Result<LogWriter<File>> {
        Ok(LogWriter::new(new_log_file(
            &self.get_compaction_filename(),
        )?)?)
    }

    fn get_compaction_filename(&self) -> PathBuf {
        if self.active_log_basename == LOG_BASENAME {
            self.log_directory.join(ALTERNATIVE_LOG_BASENAME)
        } else {
            self.log_directory.join(LOG_BASENAME)
        }
    }
}

fn new_log_file(path: &PathBuf) -> Result<File> {
    Ok(OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .append(true)
        .open(&path)?)
}
