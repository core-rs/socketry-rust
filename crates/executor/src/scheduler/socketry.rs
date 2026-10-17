// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Socketry's work-stealing executor and scheduler context.
#[cfg(feature = "native")]
mod operations;
#[cfg(all(unix, feature = "signals", feature = "native"))]
mod signals;

use crate::blocking_pool::BlockingPool;
use crate::task::{Completion, Runnable, TaskFuture, TaskState, UNASSIGNED_WORKER};
use crate::worker::{self, WorkerState};
use crate::{
    Blocking, BlockingError, BlockingOptions, BlockingTask, Cancellation, Spawn, SpawnError, Task,
    TaskHandle,
};
use crossbeam_deque::{Injector, Worker};
use event_listener::{Event, Listener};
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, JoinHandle};

thread_local! {
    static CURRENT_SCHEDULER: RefCell<Weak<Shared>> = const { RefCell::new(Weak::new()) };
}

struct Registration {
    state: Arc<TaskState>,
    waker: Waker,
}

struct Registry {
    closed: bool,
    next_identifier: u64,
    tasks: HashMap<u64, Registration>,
}

pub(crate) struct Shared {
    blocking: BlockingPool,
    #[cfg(feature = "native")]
    cleanup: BlockingPool,
    #[cfg(feature = "native")]
    selector: std::sync::OnceLock<io::Result<super::selector::DefaultSelector>>,
    registry: Mutex<Registry>,
    pub(crate) ready: Injector<Runnable>,
    pub(crate) workers: Vec<WorkerState>,
    pub(crate) closed: AtomicBool,
    pub(crate) idle_workers: AtomicUsize,
    remaining: AtomicUsize,
    completion: Event,
}

impl Shared {
    fn reserve_identifier(registry: &mut Registry) -> Result<u64, SpawnError> {
        if registry.closed {
            return Err(SpawnError::SchedulerClosed);
        }
        let identifier = registry.next_identifier;
        registry.next_identifier = identifier
            .checked_add(1)
            .ok_or(SpawnError::IdentifiersExhausted)?;
        Ok(identifier)
    }

    pub(crate) fn spawn<FutureType>(
        self: &Arc<Self>,
        future: FutureType,
    ) -> Result<TaskHandle<FutureType::Output>, SpawnError>
    where
        FutureType: Future + Send + 'static,
        FutureType::Output: Send + 'static,
    {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let identifier = Self::reserve_identifier(&mut registry)?;
        let state = Arc::new(TaskState {
            identifier,
            scheduler: Arc::downgrade(self),
            cancelled: AtomicBool::new(false),
            finished: AtomicBool::new(false),
            worker: AtomicUsize::new(UNASSIGNED_WORKER),
        });
        let scheduler = Arc::downgrade(self);
        let (runnable, handle) = async_task::Builder::new()
            .metadata(Arc::clone(&state))
            .spawn(
                |state| TaskFuture {
                    future,
                    completion: Completion(Arc::clone(state)),
                },
                move |runnable| {
                    // Workers stay alive until every registered future is destroyed.
                    // A completed task's late wakers never schedule it again.
                    let shared = scheduler.upgrade().expect("live task lost its scheduler");
                    worker::schedule(&shared, runnable);
                },
            );
        registry.tasks.insert(
            identifier,
            Registration {
                state: Arc::clone(&state),
                waker: runnable.waker(),
            },
        );
        self.remaining.fetch_add(1, Ordering::Release);
        // Publish ownership before execution, and never invoke scheduling under this lock.
        drop(registry);
        runnable.schedule();
        Ok(TaskHandle {
            inner: Some(handle.fallible()),
            task: Task { state },
        })
    }

    pub(crate) fn finish(&self, state: &TaskState) {
        let registration = self
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .tasks
            .remove(&state.identifier);
        // Drop the retained waker outside the registry lock.
        drop(registration);
        if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.completion.notify(usize::MAX);
            if self.closed.load(Ordering::Acquire) {
                self.wake_all();
            }
        }
    }

    pub(crate) fn wake(&self, identifier: u64) {
        let waker = self
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .tasks
            .get(&identifier)
            .map(|task| task.waker.clone());
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn close(&self) {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if registry.closed {
            return;
        }
        registry.closed = true;
        self.closed.store(true, Ordering::Release);
        let wakers = registry
            .tasks
            .values()
            .map(|task| {
                task.state.cancelled.store(true, Ordering::Release);
                task.waker.clone()
            })
            .collect::<Vec<_>>();
        drop(registry);
        self.blocking.close();
        #[cfg(feature = "native")]
        self.cleanup.close();
        for waker in wakers {
            waker.wake();
        }
        self.wake_all();
        #[cfg(all(feature = "io-uring", target_os = "linux"))]
        if let Some(Ok(selector)) = self.selector.get() {
            selector.close();
        }
    }

    pub(crate) fn finished(&self) -> bool {
        self.closed.load(Ordering::Acquire) && self.remaining.load(Ordering::Acquire) == 0
    }

    pub(crate) fn wake_all(&self) {
        for worker in &self.workers {
            worker.wake();
        }
    }

    pub(crate) fn wake_idle(&self, preferred: usize) {
        // A store/load barrier is required between queue publication and the
        // idle check. A SeqCst load alone does not flush preceding queue stores.
        // The worker's SeqCst idle registration precedes its second queue search.
        std::sync::atomic::fence(Ordering::SeqCst);
        // Avoid scanning worker flags on the common path where everyone is busy.
        if self.idle_workers.load(Ordering::SeqCst) == 0 {
            return;
        }
        if let Some(worker) = self.workers.get(preferred)
            && worker.wake()
        {
            return;
        }
        for worker in &self.workers {
            if worker.wake() {
                return;
            }
        }
    }
}

/// A clonable, thread-safe reference for submitting work to a scheduler.
/// Handles do not keep worker threads running after the owning Scheduler closes.
#[derive(Clone)]
pub struct SchedulerHandle {
    pub(crate) shared: Arc<Shared>,
}

impl SchedulerHandle {
    /// Wait for bounded admission to the dedicated pool; await the returned handle for completion. See [`Blocking::spawn_blocking`] for cancellation and ownership.
    pub async fn spawn_blocking<F, Output>(
        &self,
        operation: F,
        cancellation: &Cancellation,
    ) -> Result<BlockingTask<Output>, BlockingError>
    where
        F: FnOnce() -> Output + Send + 'static,
        Output: Send + 'static,
    {
        Blocking::spawn_blocking(self, operation, cancellation).await
    }

    pub(crate) fn from_shared(shared: Arc<Shared>) -> Self {
        Self { shared }
    }

    /// Spawn a task whose returned handle owns its execution lifetime.
    pub fn spawn<FutureType>(
        &self,
        future: FutureType,
    ) -> Result<TaskHandle<FutureType::Output>, SpawnError>
    where
        FutureType: Future + Send + 'static,
        FutureType::Output: Send + 'static,
    {
        Spawn::spawn(self, future)
    }

    /// Number of worker threads configured for this scheduler.
    pub fn worker_count(&self) -> usize {
        self.shared.workers.len()
    }
}

impl Spawn for SchedulerHandle {
    type Handle<Output>
        = TaskHandle<Output>
    where
        Output: Send + 'static;

    fn spawn<FutureType>(
        &self,
        future: FutureType,
    ) -> Result<TaskHandle<FutureType::Output>, SpawnError>
    where
        FutureType: Future + Send + 'static,
        FutureType::Output: Send + 'static,
    {
        self.shared.spawn(future)
    }
}

/// A futures executor with worker queues, remote inboxes and idle work stealing.
///
/// Workers start at construction. Tasks run until completion or abandonment.
/// Dropping an owning task handle requests destruction after an active poll. There are no private task stacks.
/// Tasks must yield through pending futures; blocking calls block their worker.
/// The `native` feature supplies I/O selectors and timers. The `io-uring`
/// feature selects native completion reads/writes on Linux.
///
/// Spawned futures must be Send, including values retained across suspension:
///
/// ```compile_fail
/// use socketry_executor::{Scheduler, yield_now};
/// use std::rc::Rc;
/// let scheduler = Scheduler::with_workers(1).unwrap();
/// let value = Rc::new(42);
/// scheduler.spawn(async move {
///     yield_now().await;
///     *value
/// }).unwrap();
/// ```
///
/// Independently owned tasks cannot borrow local data:
///
/// ```compile_fail
/// use socketry_executor::Scheduler;
/// let scheduler = Scheduler::with_workers(1).unwrap();
/// let value = String::from("borrowed");
/// scheduler.spawn(async { value.len() }).unwrap();
/// ```
pub struct Scheduler {
    handle: SchedulerHandle,
    workers: Vec<JoinHandle<()>>,
}

impl Scheduler {
    /// Wait for bounded admission to the dedicated pool; await the returned handle for completion. See [`Blocking::spawn_blocking`] for cancellation and ownership.
    pub async fn spawn_blocking<F, Output>(
        &self,
        operation: F,
        cancellation: &Cancellation,
    ) -> Result<BlockingTask<Output>, BlockingError>
    where
        F: FnOnce() -> Output + Send + 'static,
        Output: Send + 'static,
    {
        Blocking::spawn_blocking(self, operation, cancellation).await
    }

    /// Start one worker per available hardware parallelism unit (at least one).
    pub fn new() -> io::Result<Self> {
        Self::with_workers(thread::available_parallelism().map_or(1, usize::from))
    }

    /// Start a specified, nonzero number of workers.
    pub fn with_workers(worker_count: usize) -> io::Result<Self> {
        Self::with_workers_and_blocking(worker_count, BlockingOptions::default())
    }

    /// Configure future workers and the separate, lazily started blocking and cleanup pools. Each pool has these limits.
    pub fn with_workers_and_blocking(
        worker_count: usize,
        blocking: BlockingOptions,
    ) -> io::Result<Self> {
        Self::with_worker_spawner(worker_count, blocking, |shared, identifier, queue| {
            thread::Builder::new()
                .name(format!("socketry-worker-{identifier}"))
                .spawn(move || worker::run(shared, identifier, queue))
        })
    }

    fn with_worker_spawner(
        worker_count: usize,
        blocking: BlockingOptions,
        mut spawn_worker: impl FnMut(Arc<Shared>, usize, Worker<Runnable>) -> io::Result<JoinHandle<()>>,
    ) -> io::Result<Self> {
        if worker_count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker count must be nonzero",
            ));
        }
        let blocking = blocking.validate()?;
        let queues: Vec<_> = (0..worker_count).map(|_| Worker::new_fifo()).collect();
        let shared = Arc::new(Shared {
            blocking: BlockingPool::new(blocking),
            #[cfg(feature = "native")]
            cleanup: BlockingPool::new(blocking),
            #[cfg(feature = "native")]
            selector: std::sync::OnceLock::new(),
            registry: Mutex::new(Registry {
                closed: false,
                next_identifier: 0,
                tasks: HashMap::new(),
            }),
            ready: Injector::new(),
            workers: queues
                .iter()
                .map(|queue| WorkerState::new(queue.stealer()))
                .collect(),
            closed: AtomicBool::new(false),
            idle_workers: AtomicUsize::new(0),
            remaining: AtomicUsize::new(0),
            completion: Event::new(),
        });
        let mut scheduler = Self {
            handle: SchedulerHandle::from_shared(shared),
            workers: Vec::with_capacity(worker_count),
        };
        for (identifier, queue) in queues.into_iter().enumerate() {
            let shared = Arc::clone(&scheduler.handle.shared);
            let thread = spawn_worker(shared, identifier, queue)?;
            scheduler.workers.push(thread);
        }
        Ok(scheduler)
    }

    /// Return the scheduler associated with this worker or `block_on` invocation.
    pub fn current() -> Option<SchedulerHandle> {
        CURRENT_SCHEDULER
            .with(|current| current.borrow().upgrade())
            .map(SchedulerHandle::from_shared)
    }

    /// Return a handle usable from any thread.
    pub fn handle(&self) -> SchedulerHandle {
        self.handle.clone()
    }

    /// Number of workers configured for this scheduler.
    pub fn worker_count(&self) -> usize {
        self.handle.worker_count()
    }

    /// Spawn a task whose returned handle owns its execution lifetime.
    pub fn spawn<FutureType>(
        &self,
        future: FutureType,
    ) -> Result<TaskHandle<FutureType::Output>, SpawnError>
    where
        FutureType: Future + Send + 'static,
        FutureType::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }

    /// Block the caller until all currently owned future tasks finish. External submissions may extend this wait. The scheduler remains open. Task failures are retrieved separately through their join handles. Blocking jobs are independent; await their handles explicitly.
    ///
    /// # Panics
    /// Panics on a Socketry worker, which would block task progress.
    pub fn run(&self) {
        worker::assert_outside_worker();
        loop {
            let listener = self.handle.shared.completion.listen();
            if self.handle.shared.remaining.load(Ordering::Acquire) == 0 {
                return;
            }
            listener.wait();
        }
    }

    /// Poll a root future on the calling thread while workers execute tasks.
    /// The root may borrow local data and need not be Send. It is not an owned
    /// task: `Task::current()` is None. Returning drops task handles still owned
    /// by the root; handles moved elsewhere retain their execution lifetimes.
    ///
    /// # Panics
    /// Panics on a Socketry worker. Use `.await` inside tasks.
    pub fn block_on<FutureType: Future>(&self, future: FutureType) -> FutureType::Output {
        worker::assert_outside_worker();
        let _scope = enter(&self.handle.shared);
        let notification = Arc::new(Notification(Event::new()));
        let waker = Waker::from(Arc::clone(&notification));
        let mut context = Context::from_waker(&waker);
        let mut future = pin!(future);
        loop {
            let listener = notification.0.listen();
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => return output,
                Poll::Pending => {
                    listener.wait();
                }
            }
        }
    }

    /// Close admission, cancel all future tasks, and join their worker threads. In-progress polls must return; this cannot interrupt a blocking call.
    ///
    /// Queued `spawn_blocking` jobs are discarded. Started blocking jobs finish independently; await their handles first for graceful shutdown. Accepted file-close jobs always finish; shutdown outside a worker waits for them.
    ///
    /// # Panics
    /// Panics on a Socketry worker. Dropping a scheduler on a worker instead
    /// requests shutdown without joining, allowing that worker to finish.
    pub fn shutdown(self) {
        worker::assert_outside_worker();
        drop(self);
    }
}

impl Spawn for Scheduler {
    type Handle<Output>
        = TaskHandle<Output>
    where
        Output: Send + 'static;

    fn spawn<FutureType>(
        &self,
        future: FutureType,
    ) -> Result<TaskHandle<FutureType::Output>, SpawnError>
    where
        FutureType: Future + Send + 'static,
        FutureType::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        self.handle.shared.close();
        if !worker::is_worker() {
            for worker in self.workers.drain(..) {
                let _ = worker.join();
            }
            #[cfg(feature = "native")]
            self.handle.shared.cleanup.wait_submitted_blocking();
            #[cfg(all(feature = "io-uring", target_os = "linux"))]
            if let Some(Ok(selector)) = self.handle.shared.selector.get() {
                selector.wait_closed();
            }
        }
    }
}

struct Notification(Event);

impl Wake for Notification {
    fn wake(self: Arc<Self>) {
        self.0.notify(1);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.notify(1);
    }
}

pub(crate) struct SchedulerScope(Weak<Shared>);

pub(crate) fn enter(shared: &Arc<Shared>) -> SchedulerScope {
    SchedulerScope(CURRENT_SCHEDULER.with(|current| current.replace(Arc::downgrade(shared))))
}

impl Drop for SchedulerScope {
    fn drop(&mut self) {
        CURRENT_SCHEDULER.with(|current| current.replace(std::mem::take(&mut self.0)));
    }
}

impl Blocking for Scheduler {
    type Handle<Output>
        = BlockingTask<Output>
    where
        Output: Send + 'static;

    async fn spawn_blocking<F, Output>(
        &self,
        operation: F,
        cancellation: &Cancellation,
    ) -> Result<BlockingTask<Output>, BlockingError>
    where
        F: FnOnce() -> Output + Send + 'static,
        Output: Send + 'static,
    {
        self.handle
            .shared
            .blocking
            .spawn(operation, cancellation)
            .await
    }
}

impl Blocking for SchedulerHandle {
    type Handle<Output>
        = BlockingTask<Output>
    where
        Output: Send + 'static;

    async fn spawn_blocking<F, Output>(
        &self,
        operation: F,
        cancellation: &Cancellation,
    ) -> Result<BlockingTask<Output>, BlockingError>
    where
        F: FnOnce() -> Output + Send + 'static,
        Output: Send + 'static,
    {
        self.shared.blocking.spawn(operation, cancellation).await
    }
}

#[cfg(test)]
mod tests;
