use super::ThreadPool;

use crate::Result;

pub struct RayonThreadPool;

impl ThreadPool for RayonThreadPool {
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
