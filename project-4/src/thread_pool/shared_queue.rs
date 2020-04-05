use super::ThreadPool;

use crate::Result;

pub struct SharedQueueThreadPool;

impl ThreadPool for SharedQueueThreadPool {
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
