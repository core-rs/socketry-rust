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
use crate::blocking_pool::BlockingPool;
#[cfg(target_os = "linux")]
use crate::{AllocationMode, FileAllocate};
use crate::{
    Blocking, BlockingError, BlockingOptions, BlockingTask, Cancellation, FileOptions, IoBuf,
    IoBufMut, OperationResult, SocketRegistration, Spawn, SpawnError, TaskError,
};
use crate::{FileDescriptor, SyncMode};
use crate::{SocketMessage, SocketMessages, SocketRecvFlags, SocketRecvResult, SocketSendFlags};
use ::tokio::runtime::Handle;
use ::tokio::task::{AbortHandle, JoinHandle};
use event_listener::Event;
use pin_project_lite::pin_project;
use std::cell::RefCell;
use std::collections::HashMap;
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

mod socket_descriptor;
pub use socket_descriptor::SocketDescriptor;

async fn accept_with(
    socket: &SocketDescriptor,
    runtime: &Handle,
    cancellation: &Cancellation,
    accept: impl FnMut(&socket2::Socket) -> io::Result<(socket2::Socket, socket2::SockAddr)> + Send,
    register: impl FnOnce(&Handle, socket2::Socket) -> io::Result<SocketDescriptor>,
) -> OperationResult<(SocketDescriptor, SocketAddr)> {
    let (accepted, address) =
        cancellable(cancellation, socket.io(Interest::Readable, accept)).await?;
    register_accepted(runtime, accepted, address, register).map_err(Into::into)
}

fn register_accepted(
    runtime: &Handle,
    accepted: socket2::Socket,
    address: socket2::SockAddr,
    register: impl FnOnce(&Handle, socket2::Socket) -> io::Result<SocketDescriptor>,
) -> io::Result<(SocketDescriptor, SocketAddr)> {
    Ok((
        register(runtime, accepted)?,
        crate::scheduler::socket_io::address(address),
    ))
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
    cleanup: BlockingPool,
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
        self.cleanup.close();
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

#[cfg(test)]
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

#[cfg(test)]
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

    /// Configure dedicated blocking and cleanup pools, independent of Tokio's global blocking pool. Each pool has these limits.
    pub fn with_blocking(runtime: Handle, options: BlockingOptions) -> io::Result<Self> {
        Ok(Self::with_pool(
            runtime,
            BlockingPool::new(options.validate()?),
        ))
    }

    fn with_pool(runtime: Handle, blocking: BlockingPool) -> Self {
        let cleanup = BlockingPool::new(blocking.options);
        Self {
            handle: SchedulerHandle {
                shared: Arc::new(Shared {
                    blocking,
                    cleanup,
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
    /// Queued `spawn_blocking` jobs are discarded. Started blocking jobs finish independently; await their handles first for graceful shutdown. Accepted file-close jobs survive destruction and are drained by this method.
    pub async fn shutdown(self) {
        assert_outside_scheduler(&self.handle.shared);
        self.handle.shared.close();
        self.handle.shared.wait().await;
        self.handle.shared.cleanup.wait_submitted().await;
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

impl<O: crate::scheduler::native_socket_option::NativeSocketOption> crate::SocketOptions<O>
    for SchedulerHandle
{
    async fn socket_get_option(
        &self,
        socket: &Self::Socket,
        option: O,
        cancellation: &Cancellation,
    ) -> OperationResult<O::Value> {
        self.check_runtime(socket.runtime())?;
        cancellation.check()?;
        Ok(option.get(socket.get_ref())?)
    }

    async fn socket_set_option(
        &self,
        socket: &Self::Socket,
        option: O,
        value: O::Value,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.check_runtime(socket.runtime())?;
        cancellation.check()?;
        Ok(option.set(socket.get_ref(), value)?)
    }
}

impl SocketRegistration for SchedulerHandle {
    fn register_socket(&self, socket: TcpStream) -> io::Result<SocketDescriptor> {
        self.shared.check_open()?;
        SocketDescriptor::new(&self.shared.runtime, socket.into())
    }

    fn register_listener(&self, listener: TcpListener) -> io::Result<SocketDescriptor> {
        self.shared.check_open()?;
        SocketDescriptor::new(&self.shared.runtime, listener.into())
    }
}
impl SocketOperations for SchedulerHandle {
    type Socket = SocketDescriptor;

    async fn socket_close(&self, socket: Self::Socket) -> io::Result<()> {
        self.check_runtime(socket.runtime())?;
        crate::scheduler::cleanup::run(&self.shared.cleanup, socket.into_close()).await
    }

    async fn socket_close_submit(&self, socket: Self::Socket) -> io::Result<()> {
        self.check_runtime(socket.runtime())?;
        crate::scheduler::cleanup::submit_only(&self.shared.cleanup, socket.into_close()).await
    }

    async fn socket_shutdown(
        &self,
        socket: &Self::Socket,
        how: std::net::Shutdown,
    ) -> io::Result<()> {
        self.check_runtime(socket.runtime())?;
        let socket = socket.retain();
        crate::scheduler::cleanup::run(&self.shared.cleanup, move || socket.get_ref().shutdown(how))
            .await
    }

    async fn socket_shutdown_submit(
        &self,
        socket: &Self::Socket,
        how: std::net::Shutdown,
    ) -> io::Result<()> {
        self.check_runtime(socket.runtime())?;
        let socket = socket.retain();
        crate::scheduler::cleanup::submit_only(&self.shared.cleanup, move || {
            socket.get_ref().shutdown(how)
        })
        .await
    }

    async fn socket_open(
        &self,
        domain: crate::SocketDomain,
        kind: crate::SocketType,
        protocol: Option<crate::SocketProtocol>,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Socket> {
        cancellation.check()?;
        self.shared.check_open()?;
        crate::scheduler::socket_io::open_descriptor(
            domain,
            kind,
            protocol,
            crate::scheduler::socket_io::open,
            |socket| SocketDescriptor::new(&self.shared.runtime, socket),
        )
    }

    async fn socket_bind(
        &self,
        socket: &Self::Socket,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.check_runtime(socket.runtime())?;
        cancellation.check()?;
        socket.get_ref().bind(&address.into())?;
        Ok(())
    }

    async fn socket_listen(
        &self,
        socket: &Self::Socket,
        backlog: i32,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.check_runtime(socket.runtime())?;
        cancellation.check()?;
        socket.get_ref().listen(backlog)?;
        Ok(())
    }

    fn socket_local_addr(&self, socket: &Self::Socket) -> io::Result<SocketAddr> {
        self.check_runtime(socket.runtime())?;
        socket.local_addr()
    }

    fn socket_peer_addr(&self, socket: &Self::Socket) -> io::Result<SocketAddr> {
        self.check_runtime(socket.runtime())?;
        socket.peer_addr()
    }

    async fn socket_connect(
        &self,
        socket: &Self::Socket,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.check_runtime(socket.runtime())?;
        cancellation.check()?;
        let result = socket.get_ref().connect(&address.into());
        crate::scheduler::socket_io::connect_result(
            result,
            cancellable(
                cancellation,
                socket.io(Interest::Writable, crate::scheduler::socket_io::connected),
            ),
        )
        .await
    }

    async fn socket_accept(
        &self,
        socket: &Self::Socket,
        cancellation: &Cancellation,
    ) -> OperationResult<(Self::Socket, SocketAddr)> {
        self.check_runtime(socket.runtime())?;
        accept_with(
            socket,
            &self.shared.runtime,
            cancellation,
            |socket| socket.accept(),
            Self::Socket::new,
        )
        .await
    }

    async fn socket_read<B: IoBufMut>(
        &self,
        socket: &Self::Socket,
        mut buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        if let Err(error) = self.check_runtime(socket.runtime()) {
            return (Err(error.into()), buffer);
        }
        let result = cancellable(cancellation, async {
            socket
                .io(Interest::Readable, |socket| {
                    crate::scheduler::socket_io::recv(socket, buffer.as_mut())
                })
                .await
        })
        .await;
        (result, buffer)
    }

    async fn socket_write<B: IoBuf>(
        &self,
        socket: &Self::Socket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        if let Err(error) = self.check_runtime(socket.runtime()) {
            return (Err(error.into()), buffer);
        }
        let send_buffer = crate::scheduler::socket_io::SendBuffer::new(&buffer);
        let result = cancellable(
            cancellation,
            socket.io(Interest::Writable, move |socket| send_buffer.send(socket)),
        )
        .await;
        (result, buffer)
    }

    async fn socket_wait(
        &self,
        socket: &Self::Socket,
        interest: Interest,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.check_runtime(socket.runtime())?;
        cancellable(cancellation, socket.wait(interest)).await?;
        Ok(())
    }
}

#[cfg(test)]
mod socket_accept_tests {
    use super::*;

    #[test]
    fn accepted_socket_registration_errors_are_returned() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let accepted =
            socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap();
        let address = socket2::SockAddr::from("127.0.0.1:1".parse::<SocketAddr>().unwrap());

        assert_eq!(
            register_accepted(runtime.handle(), accepted, address, |_, _| {
                Err(io::Error::from(io::ErrorKind::PermissionDenied))
            })
            .err()
            .unwrap()
            .kind(),
            io::ErrorKind::PermissionDenied
        );
    }

    #[test]
    fn accept_errors_are_returned() {
        use std::io::Write;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (socket, _) = listener.accept().unwrap();
        let mut peer = peer;
        peer.write_all(b"ready").unwrap();
        let socket = SocketDescriptor::new(runtime.handle(), socket.into()).unwrap();

        let result = runtime.block_on(accept_with(
            &socket,
            runtime.handle(),
            &Cancellation::never(),
            |_| Err(io::Error::from(io::ErrorKind::PermissionDenied)),
            |_, _| unreachable!("accept errors before descriptor registration"),
        ));

        assert_eq!(
            result.err().unwrap().kind(),
            io::ErrorKind::PermissionDenied
        );
    }
}

#[cfg(target_os = "linux")]
impl FileAllocate for SchedulerHandle {
    async fn file_allocate(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        mode: AllocationMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        self.shared.check_open()?;
        let file = file.retain();
        let cancellation = cancellation.clone();
        file_operation_result(
            self.shared
                .runtime
                .spawn_blocking(move || {
                    super::blocking::run(&cancellation, || {
                        super::file_io::allocate(&file, offset, length, mode)
                    })
                })
                .await,
        )
    }
}

impl File for SchedulerHandle {
    type File = FileDescriptor;
    async fn file_close(&self, file: Self::File) -> io::Result<()> {
        self.shared.check_open()?;
        super::file_close::close(&self.shared.cleanup, file).await
    }

    async fn file_close_submit(&self, file: Self::File) -> io::Result<()> {
        self.shared.check_open()?;
        super::file_close::submit_only(&self.shared.cleanup, file).await
    }

    async fn file_truncate(
        &self,
        file: &Self::File,
        length: u64,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        self.shared.check_open()?;
        let file = file.retain();
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
        let file = file.retain();
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
        let file = file.retain();
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
                    super::blocking::run(&cancellation, || {
                        options.open(&path).map(FileDescriptor::from)
                    })
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
        let file = file.retain();
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
        let file = file.retain();
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
        let file = file.retain();
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
        let file = file.retain();
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

impl<O: crate::SocketOption> crate::SocketOptions<O> for Scheduler
where
    SchedulerHandle: crate::SocketOptions<O>,
{
    async fn socket_get_option(
        &self,
        socket: &Self::Socket,
        option: O,
        cancellation: &Cancellation,
    ) -> OperationResult<O::Value> {
        self.handle
            .socket_get_option(socket, option, cancellation)
            .await
    }

    async fn socket_set_option(
        &self,
        socket: &Self::Socket,
        option: O,
        value: O::Value,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.handle
            .socket_set_option(socket, option, value, cancellation)
            .await
    }
}

impl SocketRegistration for Scheduler {
    fn register_socket(&self, socket: TcpStream) -> io::Result<Self::Socket> {
        self.handle.register_socket(socket)
    }
    fn register_listener(&self, listener: TcpListener) -> io::Result<Self::Socket> {
        self.handle.register_listener(listener)
    }
}
impl SocketOperations for Scheduler {
    type Socket = <SchedulerHandle as SocketOperations>::Socket;

    async fn socket_close(&self, socket: Self::Socket) -> io::Result<()> {
        self.handle.socket_close(socket).await
    }

    async fn socket_close_submit(&self, socket: Self::Socket) -> io::Result<()> {
        self.handle.socket_close_submit(socket).await
    }

    async fn socket_shutdown(
        &self,
        socket: &Self::Socket,
        how: std::net::Shutdown,
    ) -> io::Result<()> {
        self.handle.socket_shutdown(socket, how).await
    }

    async fn socket_shutdown_submit(
        &self,
        socket: &Self::Socket,
        how: std::net::Shutdown,
    ) -> io::Result<()> {
        self.handle.socket_shutdown_submit(socket, how).await
    }

    async fn socket_open(
        &self,
        domain: crate::SocketDomain,
        kind: crate::SocketType,
        protocol: Option<crate::SocketProtocol>,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Socket> {
        self.handle
            .socket_open(domain, kind, protocol, cancellation)
            .await
    }
    async fn socket_bind(
        &self,
        socket: &Self::Socket,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.handle.socket_bind(socket, address, cancellation).await
    }
    async fn socket_listen(
        &self,
        socket: &Self::Socket,
        backlog: i32,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.handle
            .socket_listen(socket, backlog, cancellation)
            .await
    }
    fn socket_local_addr(&self, socket: &Self::Socket) -> io::Result<SocketAddr> {
        self.handle.socket_local_addr(socket)
    }
    fn socket_peer_addr(&self, socket: &Self::Socket) -> io::Result<SocketAddr> {
        self.handle.socket_peer_addr(socket)
    }
    async fn socket_connect(
        &self,
        socket: &Self::Socket,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.handle
            .socket_connect(socket, address, cancellation)
            .await
    }
    async fn socket_accept(
        &self,
        socket: &Self::Socket,
        cancellation: &Cancellation,
    ) -> OperationResult<(Self::Socket, SocketAddr)> {
        self.handle.socket_accept(socket, cancellation).await
    }
    async fn socket_read<B: IoBufMut>(
        &self,
        socket: &Self::Socket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.handle.socket_read(socket, buffer, cancellation).await
    }
    async fn socket_write<B: IoBuf>(
        &self,
        socket: &Self::Socket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.handle.socket_write(socket, buffer, cancellation).await
    }
    async fn socket_wait(
        &self,
        socket: &Self::Socket,
        interest: Interest,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.handle
            .socket_wait(socket, interest, cancellation)
            .await
    }
}
#[cfg(target_os = "linux")]
impl FileAllocate for Scheduler {
    async fn file_allocate(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        mode: AllocationMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.handle
            .file_allocate(file, offset, length, mode, cancellation)
            .await
    }
}

impl File for Scheduler {
    type File = <SchedulerHandle as File>::File;
    async fn file_close(&self, file: Self::File) -> io::Result<()> {
        self.handle.file_close(file).await
    }

    async fn file_close_submit(&self, file: Self::File) -> io::Result<()> {
        self.handle.file_close_submit(file).await
    }

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

impl SocketMessages for SchedulerHandle {
    async fn socket_sendmsg<B: IoBuf>(
        &self,
        socket: &Self::Socket,
        message: SocketMessage<B>,
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<usize>, SocketMessage<B>) {
        if let Err(error) = self.check_runtime(socket.runtime()) {
            return (Err(error.into()), message);
        }
        if let Err(error) = cancellation
            .check()
            .map_err(crate::OperationError::from)
            .and_then(|()| {
                crate::scheduler::message_io::validate(&message.buffers).map_err(Into::into)
            })
        {
            return (Err(error), message);
        }
        let address = message.address;
        let mut message = crate::scheduler::message_io::Message::sending(message.buffers, address);
        let result = cancellable(
            cancellation,
            socket.io(Interest::Writable, |socket| message.send(socket, flags)),
        )
        .await;
        (
            result,
            SocketMessage {
                buffers: message.into_buffers(),
                address,
            },
        )
    }
    async fn socket_recvmsg<B: IoBufMut>(
        &self,
        socket: &Self::Socket,
        buffers: Vec<B>,
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<SocketRecvResult>, Vec<B>) {
        if let Err(error) = self.check_runtime(socket.runtime()) {
            return (Err(error.into()), buffers);
        }
        if let Err(error) = cancellation
            .check()
            .map_err(crate::OperationError::from)
            .and_then(|()| crate::scheduler::message_io::validate(&buffers).map_err(Into::into))
        {
            return (Err(error), buffers);
        }
        let mut message = crate::scheduler::message_io::Message::receiving(buffers);
        let result = cancellable(
            cancellation,
            socket.io(Interest::Readable, |socket| message.recv(socket, flags)),
        )
        .await
        .map(|bytes| message.received(bytes));
        (result, message.into_buffers())
    }
}

impl SocketMessages for Scheduler {
    async fn socket_sendmsg<B: IoBuf>(
        &self,
        socket: &Self::Socket,
        message: SocketMessage<B>,
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<usize>, SocketMessage<B>) {
        self.handle
            .socket_sendmsg(socket, message, flags, cancellation)
            .await
    }
    async fn socket_recvmsg<B: IoBufMut>(
        &self,
        socket: &Self::Socket,
        buffers: Vec<B>,
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<SocketRecvResult>, Vec<B>) {
        self.handle
            .socket_recvmsg(socket, buffers, flags, cancellation)
            .await
    }
}

impl crate::AddressResolution for SchedulerHandle {
    async fn address_resolve(
        &self,
        name: Option<&str>,
        service: crate::AddressService<'_>,
        hints: crate::AddressHints,
        cancellation: &Cancellation,
    ) -> OperationResult<Vec<crate::AddressInfo>> {
        crate::scheduler::address_resolution::system::resolve(
            &self.shared.blocking,
            name,
            service,
            hints,
            cancellation,
        )
        .await
    }
}

impl crate::AddressResolution for Scheduler {
    async fn address_resolve(
        &self,
        name: Option<&str>,
        service: crate::AddressService<'_>,
        hints: crate::AddressHints,
        cancellation: &Cancellation,
    ) -> OperationResult<Vec<crate::AddressInfo>> {
        self.handle
            .address_resolve(name, service, hints, cancellation)
            .await
    }
}
