use std::{
    fmt::Debug,
    mem,
    sync::RwLock,
    thread::{self, JoinHandle},
};

use tracing::{error, trace};

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
        THREAD_POOL
            .read()
            .unwrap()
            .as_ref()
            .map_or_else(Self::create, |thread_pool| {
                thread_pool
                    .distributor
                    .send(Box::new(|| {
                        sender.send(f()).unwrap();
                    }))
                    .unwrap()
            });
        future
    }

    fn create() {
        let (distributor, receiver) = crossbeam::channel::unbounded();
        let num_threads = thread::available_parallelism().unwrap().get();
        let num_of_threads_to_use = if num_threads <= 2 { 1 } else { num_threads - 2 };
        let _ = THREAD_POOL.write().unwrap().insert(ThreadPool {
            threads: (0..num_of_threads_to_use)
                .map(|_| {
                    let receiver: crossbeam::channel::Receiver<Box<dyn FnOnce() + Send>> =
                        receiver.clone();
                    thread::spawn(move || {
                        while let Ok(f) = receiver.recv() {
                            f();
                        }
                    })
                })
                .collect(),
            distributor,
        });
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

static THREAD_POOL: RwLock<Option<ThreadPool>> = RwLock::new(None);
