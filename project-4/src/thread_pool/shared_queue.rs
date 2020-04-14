use super::ThreadPool;
use crate::Result;

use crossbeam::channel::{self, Receiver, Sender};
use log::{debug, error};

use std::thread;

pub struct SharedQueueThreadPool {
    tx: Sender<Box<dyn FnOnce() + Send + 'static>>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(number_of_threads: usize) -> Result<Self> {
        let (tx, rx) = channel::unbounded();
        for _ in 0..number_of_threads {
            let rx = TaskReceiver(rx.clone());
            thread::Builder::new().spawn(move || run_task(rx))?;
        }
        Ok(SharedQueueThreadPool { tx })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.tx.send(Box::new(job)).unwrap();
    }
}

#[derive(Clone)]
struct TaskReceiver(Receiver<Box<dyn FnOnce() + Send + 'static>>);

impl Drop for TaskReceiver {
    fn drop(&mut self) {
        if thread::panicking() {
            let rx = self.clone();
            if let Err(e) = thread::Builder::new().spawn(move || run_task(rx)) {
                error!("Failed to spawn a thread: {}", e)
            }
        }
    }
}

fn run_task(rx: TaskReceiver) {
    loop {
        match rx.0.recv() {
            Ok(task) => {
                task();
            }
            Err(_) => debug!("Thread exits because the thread pool is destroyed."),
        }
    }
}
