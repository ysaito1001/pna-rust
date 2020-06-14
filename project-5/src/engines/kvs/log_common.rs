use std::path::{Path, PathBuf};

use async_std::fs::{File, OpenOptions};

use crate::Result;

pub(super) async fn new_log_file(path: &Path, generation: u64) -> Result<File> {
    let path = log_path(&path, generation);
    Ok(OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(&path)
        .await?)
}

pub(super) fn log_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("{}.log", generation))
}
