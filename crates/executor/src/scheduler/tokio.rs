// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Adapter for an existing, running Tokio runtime with I/O and time enabled.
//!
//! This adapter owns the tasks it spawns, but does not own or drive the Tokio
//! runtime. Dropping it closes admission and requests cancellation. Await
//! shutdown to join task destruction. Socketry's Task::current and
//! Scheduler::current describe Socketry execution, not Tokio tasks.
#[cfg(all(unix, feature = "signals"))]
mod signals;

use super::operation::cancellable;
use super::{BufferResult, Clock, File, Interest, Socket as SocketOperations};
use crate::SyncMode;
use crate::blocking_pool::BlockingPool;
use crate::{
    Blocking, BlockingError, BlockingOptions, BlockingTask, Cancellation, FileOptions, IoBuf,
    IoBufMut, OperationResult, SocketRegistration, Spawn, SpawnError, TaskError,
};
use ::tokio::runtime::Handle;
use ::tokio::task::{AbortHandle, JoinHandle};
use event_listener::Event;
use pin_project_lite::pin_project;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File as StdFile;
use std::future::Future;
use std::io::{self, SeekFrom};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use std::time::Instant;

thread_local! {
    static CURRENT_SCHEDULER: RefCell<Weak<Shared>> = const { RefCell::new(Weak::new()) };
}

struct SchedulerScope(Weak<Shared>);
impl Drop for SchedulerScope {
    fn drop(&mut self) {
        CURRENT_SCHEDULER.with(|current| current.replace(std::mem::take(&mut self.0)));
    }
}

struct TaskState {
    cancelled: AtomicBool,
}

struct Registration {
    state: Arc<TaskState>,
    abort: Option<AbortHandle>,
}

struct Registry {
    closed: bool,
    next_identifier: u64,
    tasks: HashMap<u64, Registration>,
}

struct Shared {
    blocking: BlockingPool,
    runtime: Handle,
    closed: AtomicBool,
    registry: Mutex<Registry>,
    remaining: AtomicUsize,
    completion: Event,
}

impl Shared {
    fn spawn<FutureType>(
        self: &Arc<Self>,
        future: FutureType,
    ) -> Result<TaskHandle<FutureType::Output>, SpawnError>
    where
        FutureType: Future + Send + 'static,
        FutureType::Output: Send + 'static,
    {
        self.spawn_with_registration_hook(future, || {})
    }

    fn spawn_with_registration_hook<FutureType>(
        self: &Arc<Self>,
        future: FutureType,
        after_registration: impl FnOnce(),
    ) -> Result<TaskHandle<FutureType::Output>, SpawnError>
    where
        FutureType: Future + Send + 'static,
        FutureType::Output: Send + 'static,
    {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if registry.closed {
            return Err(SpawnError::SchedulerClosed);
        }
        let identifier = registry.next_identifier;
        registry.next_identifier = identifier
            .checked_add(1)
            .ok_or(SpawnError::IdentifiersExhausted)?;
        let state = Arc::new(TaskState {
            cancelled: AtomicBool::new(false),
        });
        registry.tasks.insert(
            identifier,
            Registration {
                state: Arc::clone(&state),
                abort: None,
            },
        );
        self.remaining.fetch_add(1, Ordering::Release);
        drop(registry);
        after_registration();
        // Publish ownership before spawn, but do not hold the registry lock:
        // a shut-down runtime can destroy the submitted future immediately.
        let inner = self.runtime.spawn(TrackedFuture {
            future,
            completion: Completion {
                scheduler: Arc::downgrade(self),
                identifier,
                state,
            },
        });
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let cancel = if let Some(registration) = registry.tasks.get_mut(&identifier) {
            registration.abort = Some(inner.abort_handle());
            registration.state.cancelled.load(Ordering::Acquire)
        } else {
            false
        };
        drop(registry);
        abort_if_cancelled(cancel, || inner.abort());
        Ok(TaskHandle { inner })
    }

    fn close(&self) {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        registry.closed = true;
        self.closed.store(true, Ordering::Release);
        let aborts: Vec<_> = registry
            .tasks
            .values()
            .filter_map(|task| {
                task.state.cancelled.store(true, Ordering::Release);
                task.abort.clone()
            })
            .collect();
        drop(registry);
        self.blocking.close();
        for abort in aborts {
            abort.abort();
        }
    }

    async fn wait(&self) {
        loop {
            let listener = self.completion.listen();
            if self.remaining.load(Ordering::Acquire) == 0 {
                return;
            }
            listener.await;
        }
    }

    fn check_open(&self) -> io::Result<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "scheduler is closed",
            ))
        } else {
            Ok(())
        }
    }
}

fn register_socket_with(
    runtime: &Handle,
    socket: TcpStream,
    set_nonblocking: impl FnOnce(&TcpStream) -> io::Result<()>,
    convert: impl FnOnce(TcpStream) -> io::Result<::tokio::net::TcpStream>,
) -> io::Result<Socket> {
    set_nonblocking(&socket)?;
    let _scope = runtime.enter();
    Ok(Socket {
        inner: Arc::new(convert(socket)?),
        runtime: runtime.id(),
    })
}

fn register_listener_with(
    runtime: &Handle,
    listener: TcpListener,
    set_nonblocking: impl FnOnce(&TcpListener) -> io::Result<()>,
    convert: impl FnOnce(TcpListener) -> io::Result<::tokio::net::TcpListener>,
) -> io::Result<Listener> {
    set_nonblocking(&listener)?;
    let _scope = runtime.enter();
    Ok(Listener {
        inner: convert(listener)?,
        runtime: runtime.id(),
    })
}

async fn connect_with<FutureType>(
    runtime: Handle,
    runtime_identifier: ::tokio::runtime::Id,
    future: FutureType,
) -> io::Result<Socket>
where
    FutureType: Future<Output = io::Result<::tokio::net::TcpStream>>,
{
    let inner = InRuntime { runtime, future }.await?;
    Ok(Socket {
        inner: Arc::new(inner),
        runtime: runtime_identifier,
    })
}

async fn accept_with<FutureType>(
    runtime: Handle,
    runtime_identifier: ::tokio::runtime::Id,
    future: FutureType,
) -> io::Result<(Socket, SocketAddr)>
where
    FutureType: Future<Output = io::Result<(::tokio::net::TcpStream, SocketAddr)>>,
{
    let (inner, address) = InRuntime { runtime, future }.await?;
    Ok((
        Socket {
            inner: Arc::new(inner),
            runtime: runtime_identifier,
        },
        address,
    ))
}

struct Completion {
    scheduler: Weak<Shared>,
    identifier: u64,
    state: Arc<TaskState>,
}

impl Drop for Completion {
    fn drop(&mut self) {
        if let Some(shared) = self.scheduler.upgrade() {
            let registration = shared
                .registry
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .tasks
                .remove(&self.identifier);
            drop(registration);
            if shared.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
                shared.completion.notify(usize::MAX);
            }
        }
    }
}

pin_project! {
    struct TrackedFuture<FutureType> {
        #[pin]
        future: FutureType,
        // Future fields must be destroyed before ownership completion.
        completion: Completion,
    }
}
impl<FutureType: Future> Future for TrackedFuture<FutureType> {
    type Output = Result<FutureType::Output, TaskError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if this.completion.state.cancelled.load(Ordering::Acquire) {
            return Poll::Ready(Err(TaskError::Cancelled));
        }
        let _scope = SchedulerScope(
            CURRENT_SCHEDULER.with(|current| current.replace(this.completion.scheduler.clone())),
        );
        this.future.poll(context).map(Ok)
    }
}

fn abort_if_cancelled(cancelled: bool, abort: impl FnOnce()) {
    if cancelled {
        abort();
    }
}

async fn read_with<Read, Readable, ReadableFuture>(
    buffer: &mut [u8],
    mut read: Read,
    mut readable: Readable,
) -> io::Result<usize>
where
    Read: FnMut(&mut [u8]) -> io::Result<usize>,
    Readable: FnMut() -> ReadableFuture,
    ReadableFuture: Future<Output = io::Result<()>>,
{
    loop {
        match read(buffer) {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if let Err(error) = readable().await {
                    break Err(error);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => break result,
        }
    }
}

async fn write_with<Write, Writable, WritableFuture>(
    buffer: &[u8],
    mut write: Write,
    mut writable: Writable,
) -> io::Result<usize>
where
    Write: FnMut(&[u8]) -> io::Result<usize>,
    Writable: FnMut() -> WritableFuture,
    WritableFuture: Future<Output = io::Result<()>>,
{
    loop {
        match write(buffer) {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if let Err(error) = writable().await {
                    break Err(error);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => break result,
        }
    }
}

fn file_operation_result<Output, Error: From<io::Error>>(
    result: Result<Result<Output, Error>, ::tokio::task::JoinError>,
) -> Result<Output, Error> {
    match result {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => Err(io::Error::new(io::ErrorKind::Interrupted, error).into()),
    }
}

fn with_file_buffer<B, Output>(
    buffer: &Mutex<B>,
    operation: impl FnOnce(&mut B) -> Output,
) -> Output {
    let mut buffer = buffer.lock().unwrap_or_else(|error| error.into_inner());
    operation(&mut buffer)
}

fn take_file_buffer<B: Default>(buffer: &Mutex<B>) -> B {
    std::mem::take(&mut *buffer.lock().unwrap_or_else(|error| error.into_inner()))
}

/// An owning Tokio task handle. Drop requests destruction after the current
/// poll returns. Move the handle to transfer ownership; await it for completion.
#[must_use = "await or retain the task; dropping its handle abandons execution"]
pub struct TaskHandle<Output> {
    inner: JoinHandle<Result<Output, TaskError>>,
}

impl<Output> TaskHandle<Output> {
    pub fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }

    pub async fn cancel(self) -> Result<Output, TaskError> {
        self.inner.abort();
        self.await
    }
}

impl<Output> Drop for TaskHandle<Output> {
    fn drop(&mut self) {
        self.inner.abort();
    }
}

impl<Output> Future for TaskHandle<Output> {
    type Output = Result<Output, TaskError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner)
            .poll(context)
            .map(|result| match result {
                Ok(result) => result,
                Err(error) if error.is_panic() => Err(TaskError::Panicked(error.into_panic())),
                Err(_) => Err(TaskError::Cancelled),
            })
    }
}

/// Owns top-level tasks submitted through a Tokio runtime adapter.
pub struct Scheduler {
    handle: SchedulerHandle,
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

    /// The runtime must remain alive and driven, with I/O and time enabled.
    pub fn new(runtime: Handle) -> Self {
        Self::with_pool(runtime, BlockingPool::new(BlockingOptions::default()))
    }

    /// Configure a dedicated blocking pool, independent of Tokio's global blocking pool.
    pub fn with_blocking(runtime: Handle, options: BlockingOptions) -> io::Result<Self> {
        Ok(Self::with_pool(
            runtime,
            BlockingPool::new(options.validate()?),
        ))
    }

    fn with_pool(runtime: Handle, blocking: BlockingPool) -> Self {
        Self {
            handle: SchedulerHandle {
                shared: Arc::new(Shared {
                    blocking,
                    runtime,
                    closed: AtomicBool::new(false),
                    registry: Mutex::new(Registry {
                        closed: false,
                        next_identifier: 0,
                        tasks: HashMap::new(),
                    }),
                    remaining: AtomicUsize::new(0),
                    completion: Event::new(),
                }),
            },
        }
    }

    pub fn handle(&self) -> SchedulerHandle {
        self.handle.clone()
    }

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

    /// Close admission, cancel all owned future tasks, and wait for their futures to be destroyed.
    ///
    /// Queued blocking jobs are discarded. Started blocking jobs finish independently; await their handles first for graceful shutdown.
    pub async fn shutdown(self) {
        assert_outside_scheduler(&self.handle.shared);
        self.handle.shared.close();
        self.handle.shared.wait().await;
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        self.handle.shared.close();
    }
}

/// A clonable handle. It does not keep admission open after Scheduler drops.
#[derive(Clone)]
pub struct SchedulerHandle {
    shared: Arc<Shared>,
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

    pub fn spawn<FutureType>(
        &self,
        future: FutureType,
    ) -> Result<TaskHandle<FutureType::Output>, SpawnError>
    where
        FutureType: Future + Send + 'static,
        FutureType::Output: Send + 'static,
    {
        self.shared.spawn(future)
    }

    fn check_runtime(&self, runtime: ::tokio::runtime::Id) -> io::Result<()> {
        self.shared.check_open()?;
        if runtime != self.shared.runtime.id() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "resource belongs to another Tokio runtime",
            ));
        }
        Ok(())
    }
}

fn assert_outside_scheduler(shared: &Arc<Shared>) {
    assert!(
        CURRENT_SCHEDULER.with(|current| !Weak::ptr_eq(&current.borrow(), &Arc::downgrade(shared))),
        "a task cannot shut down its own scheduler"
    );
}

/// A Tokio socket registered with the adapter's runtime.
#[derive(Clone)]
pub struct Socket {
    inner: Arc<::tokio::net::TcpStream>,
    runtime: ::tokio::runtime::Id,
}

impl Socket {
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner.peer_addr()
    }
}

pub struct Listener {
    inner: ::tokio::net::TcpListener,
    runtime: ::tokio::runtime::Id,
}

impl Listener {
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

pin_project! {
    struct InRuntime<FutureType> {
        runtime: Handle,
        #[pin]
        future: FutureType,
    }
}
impl<FutureType: Future> Future for InRuntime<FutureType> {
    type Output = FutureType::Output;
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let _scope = this.runtime.enter();
        this.future.poll(context)
    }
}

impl SocketRegistration for SchedulerHandle {
    fn register_socket(&self, socket: TcpStream) -> io::Result<Socket> {
        self.shared.check_open()?;
        register_socket_with(
            &self.shared.runtime,
            socket,
            |socket| socket.set_nonblocking(true),
            ::tokio::net::TcpStream::from_std,
        )
    }

    fn register_listener(&self, listener: TcpListener) -> io::Result<Listener> {
        self.shared.check_open()?;
        register_listener_with(
            &self.shared.runtime,
            listener,
            |listener| listener.set_nonblocking(true),
            ::tokio::net::TcpListener::from_std,
        )
    }
}
impl SocketOperations for SchedulerHandle {
    type Socket = Socket;
    type Listener = Listener;
    async fn socket_listen(
        &self,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<Listener> {
        cancellation.check()?;
        self.shared.check_open()?;
        self.register_listener(TcpListener::bind(address)?)
            .map_err(Into::into)
    }
    fn listener_addr(&self, listener: &Listener) -> io::Result<SocketAddr> {
        self.check_runtime(listener.runtime)?;
        listener.local_addr()
    }
    fn socket_local_addr(&self, socket: &Socket) -> io::Result<SocketAddr> {
        self.check_runtime(socket.runtime)?;
        socket.local_addr()
    }
    fn socket_peer_addr(&self, socket: &Socket) -> io::Result<SocketAddr> {
        self.check_runtime(socket.runtime)?;
        socket.peer_addr()
    }
    async fn connect(
        &self,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<Socket> {
        self.shared.check_open()?;
        cancellable(
            cancellation,
            connect_with(
                self.shared.runtime.clone(),
                self.shared.runtime.id(),
                ::tokio::net::TcpStream::connect(address),
            ),
        )
        .await
    }
    async fn accept(
        &self,
        listener: &Listener,
        cancellation: &Cancellation,
    ) -> OperationResult<(Socket, SocketAddr)> {
        self.check_runtime(listener.runtime)?;
        cancellable(
            cancellation,
            accept_with(
                self.shared.runtime.clone(),
                listener.runtime,
                listener.inner.accept(),
            ),
        )
        .await
    }
    async fn io_read<B: IoBufMut>(
        &self,
        socket: &Socket,
        mut buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        if let Err(error) = self.check_runtime(socket.runtime) {
            return (Err(error.into()), buffer);
        }
        let result = cancellable(cancellation, async {
            if buffer.as_ref().is_empty() {
                return Ok(0);
            }
            read_with(
                buffer.as_mut(),
                |buffer| socket.inner.try_read(buffer),
                || socket.inner.readable(),
            )
            .await
        })
        .await;
        (result, buffer)
    }
    async fn io_write<B: IoBuf>(
        &self,
        socket: &Socket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        if let Err(error) = self.check_runtime(socket.runtime) {
            return (Err(error.into()), buffer);
        }
        let bytes = buffer.as_ref();
        let result = cancellable(cancellation, async {
            if bytes.is_empty() {
                return Ok(0);
            }
            write_with(
                bytes,
                |buffer| socket.inner.try_write(buffer),
                || socket.inner.writable(),
            )
            .await
        })
        .await;
        (result, buffer)
    }
    async fn io_wait(
        &self,
        socket: &Socket,
        interest: Interest,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.check_runtime(socket.runtime)?;
        cancellable(cancellation, async {
            match interest {
                Interest::Readable => socket.inner.readable().await,
                Interest::Writable => socket.inner.writable().await,
            }
        })
        .await
    }
}

impl File for SchedulerHandle {
    type File = Arc<StdFile>;
    async fn file_truncate(
        &self,
        file: &Self::File,
        length: u64,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        self.shared.check_open()?;
        let file = Arc::clone(file);
        let cancellation = cancellation.clone();
        file_operation_result(
            self.shared
                .runtime
                .spawn_blocking(move || {
                    super::blocking::run(&cancellation, || super::file_io::truncate(&file, length))
                })
                .await,
        )
    }

    async fn file_sync(
        &self,
        file: &Self::File,
        mode: SyncMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        self.shared.check_open()?;
        let file = Arc::clone(file);
        let cancellation = cancellation.clone();
        file_operation_result(
            self.shared
                .runtime
                .spawn_blocking(move || {
                    super::blocking::run(&cancellation, || super::file_io::sync(&file, mode))
                })
                .await,
        )
    }

    async fn file_seek(
        &self,
        file: &Self::File,
        position: SeekFrom,
        cancellation: &Cancellation,
    ) -> OperationResult<u64> {
        cancellation.check()?;
        self.shared.check_open()?;
        let file = Arc::clone(file);
        let cancellation = cancellation.clone();
        file_operation_result(
            self.shared
                .runtime
                .spawn_blocking(move || {
                    super::blocking::run(&cancellation, || super::file_io::seek(&file, position))
                })
                .await,
        )
    }

    async fn file_open(
        &self,
        path: &Path,
        options: FileOptions,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::File> {
        cancellation.check()?;
        self.shared.check_open()?;
        let path = path.to_owned();
        let cancellation = cancellation.clone();
        file_operation_result(
            self.shared
                .runtime
                .spawn_blocking(move || {
                    super::blocking::run(&cancellation, || options.open(&path).map(Arc::new))
                })
                .await,
        )
    }
    async fn file_read<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let file = Arc::clone(file);
        self.file_operation(buffer, cancellation, move |buffer| {
            crate::scheduler::file_io::read(&file, buffer.as_mut())
        })
        .await
    }
    async fn file_write<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let file = Arc::clone(file);
        self.file_operation(buffer, cancellation, move |buffer| {
            crate::scheduler::file_io::write(&file, buffer.as_ref())
        })
        .await
    }
    async fn file_read_at<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let file = Arc::clone(file);
        self.file_operation(buffer, cancellation, move |buffer| {
            super::file_io::read_at(&file, buffer.as_mut(), offset)
        })
        .await
    }
    async fn file_write_at<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let file = Arc::clone(file);
        self.file_operation(buffer, cancellation, move |buffer| {
            super::file_io::write_at(&file, buffer.as_ref(), offset)
        })
        .await
    }
}

impl SchedulerHandle {
    async fn file_operation<B: IoBuf>(
        &self,
        buffer: B,
        cancellation: &Cancellation,
        operation: impl FnOnce(&mut B) -> io::Result<usize> + Send + 'static,
    ) -> BufferResult<B> {
        if let Err(error) = cancellation.check() {
            return (Err(error.into()), buffer);
        }
        if let Err(error) = self.shared.check_open() {
            return (Err(error.into()), buffer);
        }
        // A queued job may be destroyed by the external runtime. The waiter
        // retains the buffer too, and only takes it after the job has ended.
        let buffer = Arc::new(Mutex::new(Some(buffer)));
        let operation_buffer = Arc::clone(&buffer);
        let cancellation = cancellation.clone();
        let result = self
            .shared
            .runtime
            .spawn_blocking(move || {
                with_file_buffer(&operation_buffer, |buffer| {
                    super::blocking::run(&cancellation, || {
                        operation(buffer.as_mut().expect("job retains its buffer"))
                    })
                })
            })
            .await;
        let result = file_operation_result(result);
        let buffer = take_file_buffer(&buffer).expect("completed job returns its buffer");
        (result, buffer)
    }
}

impl Clock for SchedulerHandle {
    fn now(&self) -> Instant {
        let _scope = self.shared.runtime.enter();
        ::tokio::time::Instant::now().into_std()
    }
    async fn sleep_until(
        &self,
        deadline: Instant,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        super::operation::cancellable(cancellation, async {
            let future = {
                let _scope = self.shared.runtime.enter();
                ::tokio::time::sleep_until(::tokio::time::Instant::from_std(deadline))
            };
            InRuntime {
                runtime: self.shared.runtime.clone(),
                future,
            }
            .await;
            Ok(())
        })
        .await
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

impl SocketRegistration for Scheduler {
    fn register_socket(&self, socket: TcpStream) -> io::Result<Self::Socket> {
        self.handle.register_socket(socket)
    }
    fn register_listener(&self, listener: TcpListener) -> io::Result<Self::Listener> {
        self.handle.register_listener(listener)
    }
}
impl SocketOperations for Scheduler {
    type Socket = <SchedulerHandle as SocketOperations>::Socket;
    type Listener = <SchedulerHandle as SocketOperations>::Listener;
    async fn socket_listen(
        &self,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Listener> {
        self.handle.socket_listen(address, cancellation).await
    }
    fn listener_addr(&self, listener: &Self::Listener) -> io::Result<SocketAddr> {
        self.handle.listener_addr(listener)
    }
    fn socket_local_addr(&self, socket: &Self::Socket) -> io::Result<SocketAddr> {
        self.handle.socket_local_addr(socket)
    }
    fn socket_peer_addr(&self, socket: &Self::Socket) -> io::Result<SocketAddr> {
        self.handle.socket_peer_addr(socket)
    }
    async fn connect(
        &self,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Socket> {
        self.handle.connect(address, cancellation).await
    }
    async fn accept(
        &self,
        listener: &Self::Listener,
        cancellation: &Cancellation,
    ) -> OperationResult<(Self::Socket, SocketAddr)> {
        self.handle.accept(listener, cancellation).await
    }
    async fn io_read<B: IoBufMut>(
        &self,
        socket: &Self::Socket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.handle.io_read(socket, buffer, cancellation).await
    }
    async fn io_write<B: IoBuf>(
        &self,
        socket: &Self::Socket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.handle.io_write(socket, buffer, cancellation).await
    }
    async fn io_wait(
        &self,
        socket: &Self::Socket,
        interest: Interest,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.handle.io_wait(socket, interest, cancellation).await
    }
}
impl File for Scheduler {
    type File = <SchedulerHandle as File>::File;
    async fn file_truncate(
        &self,
        file: &Self::File,
        length: u64,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.handle.file_truncate(file, length, cancellation).await
    }

    async fn file_sync(
        &self,
        file: &Self::File,
        mode: SyncMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.handle.file_sync(file, mode, cancellation).await
    }

    async fn file_seek(
        &self,
        file: &Self::File,
        position: SeekFrom,
        cancellation: &Cancellation,
    ) -> OperationResult<u64> {
        self.handle.file_seek(file, position, cancellation).await
    }

    async fn file_open(
        &self,
        path: &Path,
        options: FileOptions,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::File> {
        self.handle.file_open(path, options, cancellation).await
    }
    async fn file_read<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.handle.file_read(file, buffer, cancellation).await
    }
    async fn file_write<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.handle.file_write(file, buffer, cancellation).await
    }
    async fn file_read_at<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.handle
            .file_read_at(file, buffer, offset, cancellation)
            .await
    }
    async fn file_write_at<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.handle
            .file_write_at(file, buffer, offset, cancellation)
            .await
    }
}
impl Clock for Scheduler {
    fn now(&self) -> Instant {
        self.handle.now()
    }
    async fn sleep_until(
        &self,
        deadline: Instant,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.handle.sleep_until(deadline, cancellation).await
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
