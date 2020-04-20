use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use crate::Result;

pub(super) fn new_log_file(path: &Path, generation: u64) -> Result<File> {
    let path = log_path(&path, generation);
    Ok(OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(&path)?)
}

pub(super) fn log_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("{}.log", generation))
}
