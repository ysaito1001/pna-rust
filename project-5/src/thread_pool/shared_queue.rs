use std::thread;

use crossbeam::channel::{self, Receiver, Sender};
use log::{debug, error};

use super::{ThreadPool, ThreadPoolMessage};
use crate::Result;

pub struct SharedQueueThreadPool {
    number_of_threads: usize,
    tx: Sender<ThreadPoolMessage>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(number_of_threads: usize) -> Result<Self> {
        let (tx, rx) = channel::unbounded();
        for _ in 0..number_of_threads {
            let rx = JobReceiver(rx.clone());
            thread::Builder::new().spawn(move || run_task(rx))?;
        }
        Ok(SharedQueueThreadPool {
            number_of_threads,
            tx,
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.tx
            .send(ThreadPoolMessage::RunJob(Box::new(job)))
            .unwrap();
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        for _ in 0..self.number_of_threads {
            self.tx.send(ThreadPoolMessage::Shutdown).unwrap();
        }
    }
}

#[derive(Clone)]
struct JobReceiver(Receiver<ThreadPoolMessage>);

impl Drop for JobReceiver {
    fn drop(&mut self) {
        if thread::panicking() {
            let rx = self.clone();
            if let Err(e) = thread::Builder::new().spawn(move || run_task(rx)) {
                error!("Failed to spawn a thread: {}", e)
            }
        }
    }
}

fn run_task(rx: JobReceiver) {
    loop {
        match rx.0.recv() {
            Ok(message) => match message {
                ThreadPoolMessage::RunJob(job) => job(),
                ThreadPoolMessage::Shutdown => break,
            },
            Err(_) => debug!("Thread exits because the thread pool is destroyed."),
        }
    }
}
