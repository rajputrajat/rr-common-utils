use std::{
    fmt::Debug,
    mem,
    sync::RwLock,
    thread::{self, JoinHandle},
};
use tracing::{error, trace};

#[derive(Debug)]
pub enum Future<T> {
    Ready(T),
    Pending(oneshot::Receiver<T>),
}

impl<T: Debug> Future<T> {
    pub fn poll(&mut self) {
        if let Future::Pending(receiver) = self {
            match receiver.try_recv() {
                Ok(t) => {
                    trace!("data: {t:?} in the future is now available");
                    *self = Future::Ready(t);
                }
                Err(oneshot::TryRecvError::Empty) => {}
                Err(oneshot::TryRecvError::Disconnected) => {
                    error!("the sender thread seems to have crashed.")
                }
            }
        }
    }
}

pub struct ThreadPool {
    threads: Vec<JoinHandle<()>>,
    distributor: crossbeam::channel::Sender<Box<dyn FnOnce() + Send + 'static>>,
}

impl ThreadPool {
    pub fn run_async<F, T>(f: F) -> Future<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        let future = Future::Pending(receiver);
        THREAD_POOL.read().unwrap().as_ref().map(|thread_pool| {
            thread_pool
                .distributor
                .send(Box::new(|| {
                    trace!("this is a job cb being scheduled to run");
                    sender.send(f()).unwrap();
                }))
                .unwrap()
        });
        future
    }

    fn create() -> Self {
        let (distributor, receiver) = crossbeam::channel::unbounded();
        let num_threads = thread::available_parallelism().unwrap().get();
        let num_of_threads_to_use = if num_threads <= 2 { 1 } else { num_threads - 2 };
        ThreadPool {
            threads: (0..num_of_threads_to_use)
                .map(|thread_id| {
                    let receiver: crossbeam::channel::Receiver<Box<dyn FnOnce() + Send>> =
                        receiver.clone();
                    thread::spawn(move || {
                        trace!("thread # {thread_id} is spawned!");
                        while let Ok(f) = receiver.recv() {
                            trace!("thread # {thread_id} has received a job!");
                            f();
                            trace!("thread # {thread_id} has completed running the job");
                        }
                    })
                })
                .collect(),
            distributor,
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        let mut threads = vec![];
        mem::swap(&mut self.threads, &mut threads);
        for th in threads {
            th.join().unwrap();
        }
    }
}

lazy_static::lazy_static! {
    static ref THREAD_POOL: RwLock<Option<ThreadPool>> = RwLock::new(Some(ThreadPool::create()));
}

impl<T: PartialEq> PartialEq for Future<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Future::Ready(a), Future::Ready(b)) if a == b => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result as AnyResult;
    use std::time::Duration;

    #[test]
    fn simple_test() -> AnyResult<()> {
        let _ = tracing_subscriber::fmt::try_init();
        let magic_number = || {
            thread::sleep(Duration::from_millis(100));
            42
        };
        trace!("pushing a function to run asynchronously");
        let mut value = ThreadPool::run_async(magic_number);
        trace!("first check");
        if let Future::Pending(_) = value {
            assert!(true);
        } else {
            assert!(false);
        }
        thread::sleep(Duration::from_millis(101));
        trace!("second check");
        if let Future::Pending(_) = value {
            assert!(true);
        } else {
            assert!(false);
        }
        value.poll();
        trace!("third check");
        assert_eq!(value, Future::Ready(42));
        Ok(())
    }
}
