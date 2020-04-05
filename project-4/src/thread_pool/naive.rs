use super::ThreadPool;

use crate::Result;

pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(_number_of_threads: u32) -> Result<Self> {
        unimplemented!();
    }

    fn spawn<F>(&self, _job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        unimplemented!();
    }
}
