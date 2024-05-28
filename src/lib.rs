use core::panic;
use std::{
    fmt::Debug,
    mem,
    sync::RwLock,
    thread::{self, JoinHandle},
};
use thiserror::Error as ThisError;
use tracing::{error, trace};

#[derive(Debug)]
pub enum Future<T> {
    Ready(T),
    Pending(oneshot::Receiver<T>),
}

impl<T> Future<T>
where
    T: Debug + Send + 'static,
{
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

    pub fn map<U, F>(self, f: F) -> Future<U>
    where
        U: Send + 'static,
        F: FnOnce(T) -> U + Send + 'static,
    {
        match self {
            Future::Ready(t) => ThreadPool::run_async(move || f(t)),
            Future::Pending(fut) => ThreadPool::run_async(move || match fut.recv() {
                Ok(t) => f(t),
                Err(_) => {
                    error!("execution of mapped future failed");
                    panic!("mapped future problem")
                }
            }),
        }
    }
}

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("the future being mapped was ready already")]
    FutureWasReady,
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
        if let Some(thread_pool) = THREAD_POOL.read().unwrap().as_ref() {
            thread_pool
                .distributor
                .send(Box::new(|| {
                    trace!("this is a job cb being scheduled to run");
                    sender.send(f()).unwrap();
                }))
                .unwrap()
        }
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
        matches!((self, other), (Future::Ready(a), Future::Ready(b)) if a == b)
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
        if let Future::Pending(_) = value {
            trace!("before sleep and poll the value should be pending and it is");
        } else {
            unreachable!("before sleep and poll the future must be pending");
        }
        thread::sleep(Duration::from_millis(101));
        if let Future::Pending(_) = value {
            trace!("after sleep but before polling, the future should be pending. and it is");
        } else {
            unreachable!("after sleep but before polling, the future should be pending");
        }
        value.poll();
        trace!("third check");
        assert_eq!(value, Future::Ready(42));
        Ok(())
    }

    #[test]
    fn nested_future() -> AnyResult<()> {
        let _ = tracing_subscriber::fmt::try_init();
        let magic_num_fut = ThreadPool::run_async(|| {
            thread::sleep(Duration::from_millis(100));
            42
        });
        if let Future::Pending(_) = magic_num_fut {
            trace!("before polling the first future should be a pending");
        } else {
            unreachable!("before polling shouldn't be 'ready'");
        }
        let mut double_num_fut = magic_num_fut.map(|n| {
            thread::sleep(Duration::from_millis(100));
            n * 2
        });
        if let Future::Pending(_) = double_num_fut {
            trace!("before polling the nested future should be pending");
        } else {
            unreachable!("before polling the nested future shouldn't be 'ready'");
        }
        double_num_fut.poll();
        if let Future::Pending(_) = double_num_fut {
            trace!( "first polling is done, but both the future are going to resolve in 100 msec each, so pending it is");
        } else {
            unreachable!("after first polling, before 200 msec delay, no ready");
        }
        thread::sleep(Duration::from_millis(105));
        double_num_fut.poll();
        if let Future::Pending(_) = double_num_fut {
            trace!( "second polling is done, but only the future is going to resolve in 100 msec, so pending it is");
        } else {
            unreachable!("after second polling, before 100 msec delay, no ready");
        }
        thread::sleep(Duration::from_millis(105));
        double_num_fut.poll();
        assert_eq!(double_num_fut, Future::Ready(84));
        Ok(())
    }
}
