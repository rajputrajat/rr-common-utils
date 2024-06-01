use std::{
    borrow::Cow,
    fmt::Debug,
    mem,
    sync::{mpsc, Arc, Mutex, OnceLock},
    thread::{self, JoinHandle},
    time::Instant,
};
use thiserror::Error as ThisError;
use tracing::{debug, error, trace, warn};

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

    pub fn try_map<U, F, E>(self, f: F, job_desc: JobDesc) -> Future<Result<U, E>>
    where
        U: Debug + Send + 'static,
        F: FnOnce(T) -> Result<U, E> + Send + 'static,
        E: Debug + Send + 'static + From<oneshot::RecvError>,
    {
        let job_desc_ = job_desc.clone();
        match self {
            Future::Ready(t) => ThreadPool::global().run_async(move || f(t), job_desc),
            Future::Pending(fut) => ThreadPool::global().run_async(
                move || match fut.recv() {
                    Ok(t) => f(t),
                    Err(e) => {
                        warn!("{:?}: {e:?}, execution of mapped future failed", job_desc_);
                        Err(e.into())
                    }
                },
                job_desc,
            ),
        }
    }

    pub fn finish(self) -> Result<T, oneshot::RecvError> {
        match self {
            Self::Ready(t) => Ok(t),
            Self::Pending(receiver) => receiver.recv(),
        }
    }
}

pub trait SelectFuture<T>: Sized
where
    T: Send + 'static + Debug,
{
    fn select<F>(self, f: F) -> Future<T>
    where
        F: Fn(&T) -> bool + Send + 'static;
}

impl<T> SelectFuture<T> for Vec<Future<T>>
where
    T: Send + 'static + Debug,
{
    #[tracing::instrument(skip_all)]
    fn select<F>(self, f: F) -> Future<T>
    where
        F: Fn(&T) -> bool + Send + 'static,
    {
        let (sender, receiver) = mpsc::channel();
        for fut in self {
            let sender = sender.clone();
            ThreadPool::global().run_async(
                move || {
                    match fut {
                        Future::Ready(t) => {
                            trace!("data '{t:?}' was ready so send, without waiting");
                            let _ = sender.send(t);
                        }
                        Future::Pending(one_receiver) => {
                            if let Ok(t) = one_receiver.recv() {
                                trace!("data '{t:?}' received from future now send");
                                let _ = sender.send(t);
                            }
                        }
                    };
                },
                JobDesc::create("SelectFuture", "forward data for select"),
            );
        }
        let (oneshot_sender, oneshot_receiver) = oneshot::channel();
        ThreadPool::global().run_async(
            move || {
                let data = loop {
                    if let Ok(data) = receiver.recv() {
                        if f(&data) {
                            break data;
                        }
                    }
                };
                oneshot_sender.send(data).unwrap();
            },
            JobDesc::create("SelectFuture", "return the selected"),
        );
        Future::Pending(oneshot_receiver)
    }
}

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("the future being mapped was ready already")]
    FutureWasReady,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JobDesc {
    owner: Cow<'static, str>,
    desc: Cow<'static, str>,
}

impl JobDesc {
    pub fn create<C: Into<Cow<'static, str>>>(owner: C, desc: C) -> Self {
        Self {
            owner: owner.into(),
            desc: desc.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum ThreadStatus {
    Idle,
    Busy(Instant, JobDesc),
}

#[derive(Clone)]
struct ArcThreadData(Arc<Mutex<ThreadData>>);

struct ThreadData {
    name: String,
    handle: Option<JoinHandle<()>>,
    status: ThreadStatus,
}

pub struct ThreadPool {
    threads: Vec<ArcThreadData>,
    distributor: crossbeam::channel::Sender<Box<dyn FnOnce(ArcThreadData) + Send + 'static>>,
}

impl Debug for ThreadPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("ThreadPool {{ num: {} }}", self.threads.len()))
    }
}

impl ThreadPool {
    #[tracing::instrument(skip(self, f))]
    pub fn run_async<F, T>(&self, f: F, job_desc: JobDesc) -> Future<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Debug + Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        let job_desc_ = job_desc.clone();
        if let Err(e) = self.distributor
            .send(Box::new(move |thread_data: ArcThreadData| {
                let thread_name = (*thread_data.0.lock().unwrap()).name.clone();
                if sender.is_closed() {
                    warn!("{thread_name}: receiver of {sender:?} is closed, so skipping run of attached callback");
                } else {
                    trace!("{thread_name}: this is a job cb being scheduled to run");
                    if let Err(e) = sender
                        .send({
                            (*thread_data.0.lock().unwrap()).status =
                                ThreadStatus::Busy(Instant::now(), job_desc_.clone());
                            let out = f();
                            (*thread_data.0.lock().unwrap()).status = ThreadStatus::Idle;
                            out
                        })
                        {
                            error!("{thread_name}: {job_desc_:?}, {e:?}");
                        }
                }
            }))
        {
            error!("{job_desc:?}, {e:?}");
        }
        Future::Pending(receiver)
    }

    #[tracing::instrument(skip(self, f))]
    pub fn try_run_async<F, T, E>(&self, f: F, job_desc: JobDesc) -> Future<Result<T, E>>
    where
        F: FnOnce() -> Result<T, E> + Send + 'static,
        T: Debug + Send + 'static,
        E: Debug + Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        let job_desc_ = job_desc.clone();
        if let Err(e) = self.distributor
            .send(Box::new(move |thread_data: ArcThreadData| {
                let thread_name = (*thread_data.0.lock().unwrap()).name.clone();
                if sender.is_closed() {
                    warn!("{thread_name}: receiver of {sender:?} is closed, so skipping run of attached callback");
                } else {
                    trace!("{thread_name}: this is a job cb being scheduled to run");
                    if let Err(e) = sender
                        .send({
                            (*thread_data.0.lock().unwrap()).status =
                                ThreadStatus::Busy(Instant::now(), job_desc_.clone());
                            let out = f();
                            (*thread_data.0.lock().unwrap()).status = ThreadStatus::Idle;
                            out
                        })
                        {
                            error!("{thread_name}: {job_desc_:?}, {e:?}");
                        }
                }
            }))
        {
            error!("{job_desc:?}, {e:?}");
        }
        Future::Pending(receiver)
    }

    #[tracing::instrument]
    fn create(num_of_threads: usize) -> Self {
        let (distributor, receiver) = crossbeam::channel::unbounded();
        ThreadPool {
            threads: (0..num_of_threads)
                .map(|thread_id| {
                    let receiver: crossbeam::channel::Receiver<
                        Box<dyn FnOnce(ArcThreadData) + Send>,
                    > = receiver.clone();
                    let thread_data = ArcThreadData(Arc::new(Mutex::new(ThreadData {
                        name: format!("Thread # {thread_id}"),
                        status: ThreadStatus::Idle,
                        handle: None,
                    })));
                    let thread_data_ = thread_data.clone();
                    let handle = thread::spawn(move || {
                        trace!("thread # {thread_id} is spawned!");
                        while let Ok(f) = receiver.recv() {
                            let thread_data = thread_data_.clone();
                            trace!("thread # {thread_id} has received a job!");
                            f(thread_data);
                            trace!("thread # {thread_id} has completed running the job");
                        }
                    });
                    let _ = (thread_data.0.lock().unwrap()).handle.insert(handle);
                    thread_data
                })
                .collect(),
            distributor,
        }
    }

    pub fn global() -> &'static Self {
        GLOBAL_THREAD_POOL.get_or_init(|| ThreadPool::create(128))
    }

    pub fn debug_print_brief_status(&self) {
        let free = self.threads.iter().fold(0, |acc, th| {
            if (*th.0.lock().unwrap()).status == ThreadStatus::Idle {
                acc + 1
            } else {
                acc
            }
        });
        debug!(
            "{} / {} threads of the ThreadPool are available",
            free,
            self.threads.len()
        );
    }

    pub fn debug_print_detailed_status(&self) {
        let mut to_log = String::new();
        let mut busy_cnt = 0;
        for th in &self.threads {
            let th_status = (*th.0.lock().unwrap()).status.clone();
            let name = (*th.0.lock().unwrap()).name.clone();
            if let ThreadStatus::Busy(instant, job_desc) = th_status {
                busy_cnt += 1;
                to_log.push_str(&format!(
                    "{}, {:?}, is busy since {:?}",
                    &name,
                    job_desc,
                    Instant::now().duration_since(instant)
                ));
                to_log.push('\n');
            }
        }
        debug!(
            "{busy_cnt} / {} threads are busy. Here is the list of busy threads:\n{to_log}",
            self.threads.len()
        );
    }
}

static GLOBAL_THREAD_POOL: OnceLock<ThreadPool> = OnceLock::new();

impl Drop for ThreadPool {
    #[allow(clippy::unwrap_used)]
    fn drop(&mut self) {
        let mut threads = vec![];
        mem::swap(&mut self.threads, &mut threads);
        for th in threads {
            let th = Arc::into_inner(th.0).unwrap();
            let th = th.into_inner().unwrap();
            th.handle.unwrap().join().unwrap();
        }
    }
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
        let mut value = ThreadPool::global().run_async(
            magic_number,
            JobDesc::create("simple_test", "returns the magic number"),
        );
        if let Future::Pending(_) = value {
            trace!("before sleep and poll the value should be pending and it is");
        } else {
            unreachable!("before sleep and poll the future must be pending");
        }
        thread::sleep(Duration::from_millis(103));
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
        ThreadPool::global().debug_print_detailed_status();
        let magic_num_fut = ThreadPool::global().run_async(
            || {
                thread::sleep(Duration::from_millis(100));
                42
            },
            JobDesc::create("nested_future", "sleep then return the magic number"),
        );
        ThreadPool::global().debug_print_detailed_status();
        if let Future::Pending(_) = magic_num_fut {
            trace!("before polling the first future should be a pending");
        } else {
            unreachable!("before polling shouldn't be 'ready'");
        }
        let mut double_num_fut = magic_num_fut.map(
            |n| {
                thread::sleep(Duration::from_millis(100));
                n * 2
            },
            JobDesc::create("double_num_fut_map", "sleep. then return the double number"),
        );
        ThreadPool::global().debug_print_detailed_status();
        if let Future::Pending(_) = double_num_fut {
            trace!("before polling the nested future should be pending");
        } else {
            unreachable!("before polling the nested future shouldn't be 'ready'");
        }
        double_num_fut.poll();
        ThreadPool::global().debug_print_detailed_status();
        if let Future::Pending(_) = double_num_fut {
            trace!( "first polling is done, but both the future are going to resolve in 100 msec each, so pending it is");
        } else {
            unreachable!("after first polling, before 200 msec delay, no ready");
        }
        thread::sleep(Duration::from_millis(105));
        double_num_fut.poll();
        ThreadPool::global().debug_print_detailed_status();
        if let Future::Pending(_) = double_num_fut {
            trace!( "second polling is done, but only the future is going to resolve in 100 msec, so pending it is");
        } else {
            unreachable!("after second polling, before 100 msec delay, no ready");
        }
        thread::sleep(Duration::from_millis(105));
        double_num_fut.poll();
        assert_eq!(double_num_fut, Future::Ready(84));
        ThreadPool::global().debug_print_detailed_status();
        Ok(())
    }
}
