// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Adapter for an existing, running Tokio runtime with I/O and time enabled.
//!
//! This adapter owns the tasks it spawns, but does not own or drive the Tokio
//! runtime. Dropping it closes admission and requests cancellation. Await
//! shutdown to join task destruction. Socketry's Task::current and
//! Scheduler::current describe Socketry execution, not Tokio tasks.
use super::{BufferResult, Clock, File, Interest, Socket as SocketOperations};
use crate::{Spawn, SpawnError, TaskError};
use ::tokio::runtime::Handle;
use ::tokio::task::{AbortHandle, JoinHandle};
use event_listener::Event;
use pin_project_lite::pin_project;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File as StdFile;
use std::future::Future;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

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

fn file_operation_result<Output>(
    result: Result<io::Result<Output>, ::tokio::task::JoinError>,
) -> io::Result<Output> {
    match result {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => Err(io::Error::new(io::ErrorKind::Interrupted, error)),
    }
}

fn with_file_buffer<Output>(
    buffer: &Mutex<Vec<u8>>,
    operation: impl FnOnce(&mut Vec<u8>) -> io::Result<Output>,
) -> io::Result<Output> {
    let mut buffer = buffer.lock().unwrap_or_else(|error| error.into_inner());
    operation(&mut buffer)
}

fn take_file_buffer(buffer: &Mutex<Vec<u8>>) -> Vec<u8> {
    std::mem::take(&mut *buffer.lock().unwrap_or_else(|error| error.into_inner()))
}

/// An awaitable Tokio task result. Drop abandons the result without cancelling
/// the task; its scheduler remains the owner.
#[must_use = "await the task to observe its result, or explicitly drop the handle"]
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
    /// The runtime must remain alive and driven, with I/O and time enabled.
    pub fn new(runtime: Handle) -> Self {
        Self {
            handle: SchedulerHandle {
                shared: Arc::new(Shared {
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

    /// Close admission, cancel all owned tasks,
    /// and wait for their futures to be destroyed.
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

impl SocketOperations for SchedulerHandle {
    type Socket = Socket;
    type Listener = Listener;

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

    async fn connect(&self, address: SocketAddr) -> io::Result<Socket> {
        self.shared.check_open()?;
        connect_with(
            self.shared.runtime.clone(),
            self.shared.runtime.id(),
            ::tokio::net::TcpStream::connect(address),
        )
        .await
    }

    async fn accept(&self, listener: &Listener) -> io::Result<(Socket, SocketAddr)> {
        self.check_runtime(listener.runtime)?;
        // Accept registers a new stream with Tokio's current runtime. Enter
        // our runtime for each poll, even when another executor polls us.
        accept_with(
            self.shared.runtime.clone(),
            listener.runtime,
            listener.inner.accept(),
        )
        .await
    }

    async fn io_read(&self, socket: &Socket, mut buffer: Vec<u8>) -> BufferResult {
        if let Err(error) = self.check_runtime(socket.runtime) {
            return (Err(error), buffer);
        }
        if buffer.is_empty() {
            return (Ok(0), buffer);
        }
        let result = read_with(
            &mut buffer,
            |buffer| socket.inner.try_read(buffer),
            || socket.inner.readable(),
        )
        .await;
        (result, buffer)
    }

    async fn io_write(&self, socket: &Socket, buffer: Vec<u8>) -> BufferResult {
        if let Err(error) = self.check_runtime(socket.runtime) {
            return (Err(error), buffer);
        }
        if buffer.is_empty() {
            return (Ok(0), buffer);
        }
        let result = write_with(
            &buffer,
            |buffer| socket.inner.try_write(buffer),
            || socket.inner.writable(),
        )
        .await;
        (result, buffer)
    }

    async fn io_wait(&self, socket: &Socket, interest: Interest) -> io::Result<()> {
        self.check_runtime(socket.runtime)?;
        match interest {
            Interest::Readable => socket.inner.readable().await,
            Interest::Writable => socket.inner.writable().await,
        }
    }
}

impl File for SchedulerHandle {
    async fn file_read_at(&self, file: Arc<StdFile>, buffer: Vec<u8>, offset: u64) -> BufferResult {
        self.file_operation(file, buffer, offset, false).await
    }

    async fn file_write_at(
        &self,
        file: Arc<StdFile>,
        buffer: Vec<u8>,
        offset: u64,
    ) -> BufferResult {
        self.file_operation(file, buffer, offset, true).await
    }
}

impl SchedulerHandle {
    async fn file_operation(
        &self,
        file: Arc<StdFile>,
        buffer: Vec<u8>,
        offset: u64,
        write: bool,
    ) -> BufferResult {
        if let Err(error) = self.shared.check_open() {
            return (Err(error), buffer);
        }
        // Retain a second owner so runtime shutdown before the blocking job
        // starts can still return the caller's buffer. No byte copy is made.
        let buffer = Arc::new(Mutex::new(buffer));
        let operation_buffer = Arc::clone(&buffer);
        let result = self
            .shared
            .runtime
            .spawn_blocking(move || {
                with_file_buffer(&operation_buffer, |buffer| {
                    if write {
                        super::positioned_file::write_at(&file, buffer, offset)
                    } else {
                        super::positioned_file::read_at(&file, buffer, offset)
                    }
                })
            })
            .await;
        let result = file_operation_result(result);
        let buffer = take_file_buffer(&buffer);
        (result, buffer)
    }
}

impl Clock for SchedulerHandle {
    async fn sleep(&self, duration: Duration) {
        let future = {
            let _scope = self.shared.runtime.enter();
            ::tokio::time::sleep(duration)
        };
        future.await;
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

impl SocketOperations for Scheduler {
    type Socket = Socket;
    type Listener = Listener;
    fn register_socket(&self, socket: TcpStream) -> io::Result<Socket> {
        self.handle.register_socket(socket)
    }
    fn register_listener(&self, listener: TcpListener) -> io::Result<Listener> {
        self.handle.register_listener(listener)
    }
    async fn connect(&self, address: SocketAddr) -> io::Result<Socket> {
        self.handle.connect(address).await
    }
    async fn accept(&self, listener: &Listener) -> io::Result<(Socket, SocketAddr)> {
        self.handle.accept(listener).await
    }
    async fn io_read(&self, socket: &Socket, buffer: Vec<u8>) -> BufferResult {
        self.handle.io_read(socket, buffer).await
    }
    async fn io_write(&self, socket: &Socket, buffer: Vec<u8>) -> BufferResult {
        self.handle.io_write(socket, buffer).await
    }
    async fn io_wait(&self, socket: &Socket, interest: Interest) -> io::Result<()> {
        self.handle.io_wait(socket, interest).await
    }
}
impl File for Scheduler {
    async fn file_read_at(&self, file: Arc<StdFile>, buffer: Vec<u8>, offset: u64) -> BufferResult {
        self.handle.file_read_at(file, buffer, offset).await
    }
    async fn file_write_at(
        &self,
        file: Arc<StdFile>,
        buffer: Vec<u8>,
        offset: u64,
    ) -> BufferResult {
        self.handle.file_write_at(file, buffer, offset).await
    }
}
impl Clock for Scheduler {
    async fn sleep(&self, duration: Duration) {
        self.handle.sleep(duration).await;
    }
}

#[cfg(test)]
mod tests;
