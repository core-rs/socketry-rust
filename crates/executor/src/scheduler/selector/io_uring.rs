// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Linux completion reads, writes, metadata, allocation, synchronization, and truncation using a ring owned by a dedicated selector thread. Tasks communicate through owned requests and standard wakers, so operation futures remain Send and can migrate between executor workers.
//!
//! Socket setup, accept, readiness waits and timers currently reuse async-io.
//! Initialization probes the required opcodes and returns an error rather than
//! silently selecting another backend. Buffers are retained until the original
//! operation's completion, including after cancellation. A cancellation result
//! alone never releases an original operation.
//!
//! This initial implementation uses one channel request and one completion
//! channel per operation. It does not yet pool operation records or register
//! buffers with the kernel.
//!
//! Allocation and truncation use native requests when their opcodes are available, otherwise the blocking file fallback preserves the same public contract. File advice uses bounded cleanup workers so accepted jobs survive ring shutdown; native advice SQEs remain future work.
use super::readiness::{self, SocketDescriptor};
use crate::scheduler::{BufferResult, Clock, Interest, Socket as SocketOperations};
use crate::{AllocationMode, FileAllocate, FileDescriptor, SyncMode};
use crate::{Cancellation, FileOptions, IoBuf, IoBufMut, OperationResult, SocketRegistration};
use crate::{
    FileClose, FileOpen, FileRead, FileReadAt, FileSeek, FileStat, FileSync, FileTruncate,
    FileWrite, FileWriteAt,
};
use event_listener::{Event as CompletionEvent, Listener as _};
use futures_channel::oneshot;
use io_uring::{IoUring, opcode, squeue, types};
use polling::{Event, Events, Poller};
use std::cell::Cell;
use std::collections::{HashMap, VecDeque};
use std::fs::File as StdFile;
use std::io::{self, Read, SeekFrom, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};
use std::{
    future::{Future, poll_fn},
    path::Path,
    pin::pin,
    task::Poll,
};

mod message;
mod stat;

const CANCEL_TAG: u64 = 1 << 63;
const COMMAND_EVENT: usize = 0;
const RING_EVENT: usize = 1;

// Keep syscall failures deterministic in unit tests while production calls the OS directly.
fn syscall<T>(name: &'static str, operation: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
    #[cfg(test)]
    {
        tests::syscall(name, operation)
    }
    #[cfg(not(test))]
    {
        let _ = name;
        operation()
    }
}

fn validate_completion_support(nodrop: bool) -> io::Result<()> {
    if nodrop {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "io_uring requires reliable completion overflow handling",
        ))
    }
}

fn validate_operation_support(supported: impl Fn(u8) -> bool) -> io::Result<()> {
    for operation in [
        opcode::Read::CODE,
        opcode::Write::CODE,
        opcode::Recv::CODE,
        opcode::Send::CODE,
        opcode::SendMsg::CODE,
        opcode::RecvMsg::CODE,
        opcode::AsyncCancel::CODE,
        opcode::Fsync::CODE,
        opcode::Statx::CODE,
    ] {
        if !supported(operation) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "required io_uring operation is unavailable",
            ));
        }
    }
    Ok(())
}

type RawBufferResult<B = Vec<u8>> = (io::Result<usize>, B);

async fn retry_operation<B, O, F, W, G>(
    mut buffer: B,
    mut operation: O,
    mut wait: W,
    cancellation: &Cancellation,
) -> RawBufferResult<B>
where
    O: FnMut(B) -> F,
    F: std::future::Future<Output = RawBufferResult<B>>,
    W: FnMut() -> G,
    G: std::future::Future<Output = io::Result<()>>,
{
    loop {
        if cancellation.is_cancelled() {
            return (
                Err(io::Error::new(io::ErrorKind::Interrupted, crate::Cancelled)),
                buffer,
            );
        }
        let (result, returned) = operation(buffer).await;
        buffer = returned;
        match result {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if let Err(error) = wait().await {
                    return (Err(error), buffer);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return (result, buffer),
        }
    }
}

enum Resource {
    Socket(SocketDescriptor),
    // None uses the shared OS cursor; Some is a validated explicit offset.
    File(Arc<StdFile>, Option<u64>),
}

struct Request<B: IoBuf = Vec<u8>> {
    identifier: u64,
    resource: Option<Resource>,
    buffer: Option<B>,
    pointer: fn(&mut B) -> *mut u8,
    write: bool,
    reply: Cell<Option<oneshot::Sender<RawBufferResult<B>>>>,
    cancelling: Cell<bool>,
}

impl<B: IoBuf> Request<B> {
    fn entry(&mut self) -> squeue::Entry {
        let buffer = self.buffer.as_mut().expect("request retains its buffer");
        let length = buffer.as_ref().len().min(i32::MAX as usize) as u32;
        let pointer = (self.pointer)(buffer);
        let entry = match self
            .resource
            .as_ref()
            .expect("request retains its resource")
        {
            Resource::Socket(socket) => {
                let descriptor = types::Fd(socket.get_ref().as_raw_fd());
                if self.write {
                    opcode::Send::new(descriptor, pointer, length)
                        .flags(libc::MSG_NOSIGNAL)
                        .build()
                } else {
                    opcode::Recv::new(descriptor, pointer, length).build()
                }
            }
            Resource::File(file, offset) => {
                let descriptor = types::Fd(file.as_raw_fd());
                if self.write {
                    opcode::Write::new(descriptor, pointer, length)
                        .offset(offset.unwrap_or(u64::MAX))
                        .build()
                } else {
                    opcode::Read::new(descriptor, pointer, length)
                        .offset(offset.unwrap_or(u64::MAX))
                        .build()
                }
            }
        };
        entry.user_data(self.identifier)
    }

    fn finish(mut self, result: io::Result<usize>) {
        let reply = self
            .reply
            .take()
            .expect("unfinished requests retain their completion sender");
        let buffer = self.buffer.take().expect("request returns its buffer once");
        // Release the retained descriptor before waking a caller that may immediately close it.
        drop(self);
        let _ = reply.send((result, buffer));
    }
}

impl<B: IoBuf> Drop for Request<B> {
    fn drop(&mut self) {
        drop(self.resource.take());
        // A submission can race Shutdown and remain in the incoming queue as
        // the selector exits. Such requests have never reached the kernel and
        // can return their buffers normally. InFlight deliberately suppresses
        // this destructor for requests whose completion is unknown.
        if let Some(reply) = self.reply.take() {
            let _ = reply.send((
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "selector closed before submission",
                )),
                self.buffer.take().expect("request returns its buffer once"),
            ));
        }
    }
}

// One erased request allocation lets a selector queue heterogeneous owned buffer types. Its typed completion returns the exact original owner.
// Only entry preparation (before publication) and original completion may access the request exclusively. In-flight bookkeeping uses shared access so inline UnsafeCell output can remain kernel-accessible without creating an aliasing mutable reference.
trait PendingRequest: Send {
    fn identifier(&self) -> u64;
    fn entry(&mut self) -> squeue::Entry;
    fn finish(self: Box<Self>, result: io::Result<usize>);
    fn cancelling(&self) -> bool;
    fn set_cancelling(&self);
    fn take_reply(&self) -> Option<Box<dyn Send>>;
}
impl<B: IoBuf> PendingRequest for Request<B> {
    fn identifier(&self) -> u64 {
        self.identifier
    }
    fn entry(&mut self) -> squeue::Entry {
        Request::entry(self)
    }
    fn finish(self: Box<Self>, result: io::Result<usize>) {
        Request::finish(*self, result);
    }
    fn cancelling(&self) -> bool {
        self.cancelling.get()
    }
    fn set_cancelling(&self) {
        self.cancelling.set(true);
    }
    fn take_reply(&self) -> Option<Box<dyn Send>> {
        self.reply
            .take()
            .map(|reply| Box::new(reply) as Box<dyn Send>)
    }
}

#[derive(Clone, Copy)]
enum FileOperation {
    Sync(SyncMode),
    Truncate(u64),
    Allocate {
        offset: u64,
        length: u64,
        mode: AllocationMode,
    },
}

// Buffer-free file operations retain the file until the original completion exactly like transfer requests.
struct FileRequest {
    identifier: u64,
    file: Option<Arc<StdFile>>,
    operation: FileOperation,
    reply: Cell<Option<oneshot::Sender<io::Result<()>>>>,
    cancelling: Cell<bool>,
}

impl PendingRequest for FileRequest {
    fn identifier(&self) -> u64 {
        self.identifier
    }

    fn entry(&mut self) -> squeue::Entry {
        let descriptor = types::Fd(
            self.file
                .as_ref()
                .expect("request retains its file")
                .as_raw_fd(),
        );
        let entry = match self.operation {
            FileOperation::Sync(mode) => {
                let flags = match mode {
                    SyncMode::All => types::FsyncFlags::empty(),
                    SyncMode::Data => types::FsyncFlags::DATASYNC,
                };
                opcode::Fsync::new(descriptor).flags(flags).build()
            }
            FileOperation::Truncate(length) => opcode::Ftruncate::new(descriptor, length).build(),
            FileOperation::Allocate {
                offset,
                length,
                mode,
            } => opcode::Fallocate::new(descriptor, length)
                .offset(offset)
                .mode(crate::scheduler::file_io::allocation_flags(mode))
                .build(),
        };
        entry.user_data(self.identifier)
    }

    fn finish(self: Box<Self>, result: io::Result<usize>) {
        let reply = self
            .reply
            .take()
            .expect("unfinished requests retain their completion sender");
        // Completion must also establish exclusive application ownership before notifying the waiter.
        drop(self);
        let _ = reply.send(result.map(|_| ()));
    }

    fn cancelling(&self) -> bool {
        self.cancelling.get()
    }

    fn set_cancelling(&self) {
        self.cancelling.set(true);
    }

    fn take_reply(&self) -> Option<Box<dyn Send>> {
        self.reply
            .take()
            .map(|reply| Box::new(reply) as Box<dyn Send>)
    }
}

impl Drop for FileRequest {
    fn drop(&mut self) {
        drop(self.file.take());
        // Queued requests can be destroyed before submission. InFlight retains requests whose kernel completion is unknown.
        if let Some(reply) = self.reply.take() {
            let _ = reply.send(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "selector closed before submission",
            )));
        }
    }
}

/// Only terminal original completions remove requests from this map.
/// On an unexpected selector failure, leak kernel-accessible resources rather
/// than freeing memory whose completion cannot be established.
#[derive(Default)]
struct InFlight(HashMap<u64, Box<dyn PendingRequest>>);

impl Drop for InFlight {
    fn drop(&mut self) {
        let mut replies = Vec::new();
        for (_, request) in self.0.drain() {
            replies.extend(request.take_reply());
            std::mem::forget(request);
        }
        // Wake abandoned callers only after every in-flight buffer is retained.
        drop(replies);
    }
}

enum Command {
    Submit(Box<dyn PendingRequest>),
    Cancel(u64),
    Shutdown,
}

struct Shutdown {
    finished: AtomicBool,
    event: CompletionEvent,
}

struct SignalCompletion(Arc<Shutdown>);
impl Drop for SignalCompletion {
    fn drop(&mut self) {
        self.0.finished.store(true, Ordering::Release);
        self.0.event.notify(usize::MAX);
    }
}

struct Controller {
    cleanup: crate::blocking_pool::BlockingPool,
    commands: mpsc::Sender<Command>,
    notification: UnixStream,
    next_identifier: AtomicU64,
    closed: AtomicBool,
    shutdown: Arc<Shutdown>,
}

impl Controller {
    fn notify(&self) {
        // A full pipe already contains a wakeup. Other errors mean the selector
        // has exited; dropping its receivers wakes pending operation futures.
        loop {
            match syscall("notify", || (&self.notification).write(&[1])) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                _ => break,
            }
        }
    }

    fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.cleanup.close();
            let _ = self.commands.send(Command::Shutdown);
            self.notify();
        }
    }
}

impl Drop for Controller {
    fn drop(&mut self) {
        self.close();
    }
}

struct CancelOnDrop {
    controller: Arc<Controller>,
    identifier: u64,
    armed: bool,
}
impl CancelOnDrop {
    fn request(&mut self) {
        if self.armed {
            let _ = self
                .controller
                .commands
                .send(Command::Cancel(self.identifier));
            self.controller.notify();
            self.armed = false;
        }
    }
}
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.request();
    }
}

/// A clonable reference to one io_uring selector thread.
#[derive(Clone)]
pub struct Selector {
    controller: Arc<Controller>,
    supports_file_truncate: bool,
    supports_file_allocate: bool,
}

impl Selector {
    pub fn new() -> io::Result<Self> {
        let ring = syscall("ring", || IoUring::new(256))?;
        syscall("completion-support", || {
            validate_completion_support(ring.params().is_feature_nodrop())
        })?;
        let mut probe = io_uring::Probe::new();
        syscall("probe", || ring.submitter().register_probe(&mut probe))?;
        syscall("operation-support", || {
            validate_operation_support(|operation| probe.is_supported(operation))
        })?;
        let (notification, receiver) = syscall("notification-pair", UnixStream::pair)?;
        syscall("notification-mode", || notification.set_nonblocking(true))?;
        syscall("receiver-mode", || receiver.set_nonblocking(true))?;
        let (commands, incoming) = mpsc::channel();
        let shutdown = Arc::new(Shutdown {
            finished: AtomicBool::new(false),
            event: CompletionEvent::new(),
        });
        let reactor = Reactor::new(ring, receiver, incoming)?;
        let completion = SignalCompletion(Arc::clone(&shutdown));
        syscall("spawn", || {
            thread::Builder::new()
                .name("socketry-io-uring".into())
                .spawn(move || {
                    let _completion = completion;
                    // An unexpected system error closes completion channels. InFlight's
                    // destructor retains any memory still accessible to the kernel.
                    let mut reactor = reactor;
                    let _ = reactor.run();
                })
        })?;
        Ok(Self {
            supports_file_truncate: probe.is_supported(opcode::Ftruncate::CODE),
            supports_file_allocate: probe.is_supported(opcode::Fallocate::CODE),
            controller: Arc::new(Controller {
                cleanup: crate::blocking_pool::BlockingPool::new(crate::BlockingOptions::default()),
                commands,
                notification,
                next_identifier: AtomicU64::new(0),
                closed: AtomicBool::new(false),
                shutdown,
            }),
        })
    }

    pub(crate) fn close(&self) {
        self.controller.close();
    }

    pub(crate) fn wait_closed(&self) {
        self.close();
        self.controller.cleanup.wait_submitted_blocking();
        loop {
            let listener = self.controller.shutdown.event.listen();
            if self.controller.shutdown.finished.load(Ordering::Acquire) {
                return;
            }
            listener.wait();
        }
    }

    /// Close admission, request cancellation, and await completion processing.
    /// Other clones also become closed. A kernel operation which cannot yet be
    /// cancelled may delay shutdown.
    pub async fn shutdown(&self) {
        self.close();
        self.controller.cleanup.wait_submitted().await;
        loop {
            let listener = self.controller.shutdown.event.listen();
            if self.controller.shutdown.finished.load(Ordering::Acquire) {
                return;
            }
            listener.await;
        }
    }

    fn check_open(&self) -> io::Result<()> {
        if self.controller.closed.load(Ordering::Acquire)
            || self.controller.shutdown.finished.load(Ordering::Acquire)
        {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "io_uring selector is closed",
            ))
        } else {
            Ok(())
        }
    }

    async fn operation<B: IoBuf>(
        &self,
        resource: Resource,
        buffer: B,
        write: bool,
        pointer: fn(&mut B) -> *mut u8,
        cancellation: &Cancellation,
    ) -> RawBufferResult<B> {
        if cancellation.is_cancelled() {
            return (
                Err(io::Error::new(io::ErrorKind::Interrupted, crate::Cancelled)),
                buffer,
            );
        }
        // Some kernels retry EAGAIN internally. Tests can deterministically
        // exercise the public retry contract before submitting real I/O.
        #[cfg(test)]
        if let Err(error) = tests::syscall("operation", || Ok(())) {
            return (Err(error), buffer);
        }
        if let Err(error) = self.check_open() {
            return (Err(error), buffer);
        }
        if matches!(&resource, Resource::File(_, Some(offset)) if *offset > i64::MAX as u64) {
            return (
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "file offset exceeds i64::MAX",
                )),
                buffer,
            );
        }
        if matches!(&resource, Resource::File(_, _)) && buffer.as_ref().is_empty() {
            return (Ok(0), buffer);
        }
        let identifier = match self.next_identifier() {
            Ok(identifier) => identifier,
            Err(error) => return (Err(error), buffer),
        };
        let (reply, result) = oneshot::channel();
        let request = Request {
            identifier,
            resource: Some(resource),
            buffer: Some(buffer),
            pointer,
            write,
            reply: Cell::new(Some(reply)),
            cancelling: Cell::new(false),
        };
        self.submit(Box::new(request), result, cancellation).await
    }

    async fn file_operation(
        &self,
        file: &FileDescriptor,
        operation: FileOperation,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        self.check_open()?;
        let identifier = self.next_identifier()?;
        let (reply, result) = oneshot::channel();
        let request = FileRequest {
            identifier,
            file: Some(file.retain()),
            operation,
            reply: Cell::new(Some(reply)),
            cancelling: Cell::new(false),
        };
        operation_result(
            self.submit(Box::new(request), result, cancellation).await,
            cancellation,
        )
    }

    fn next_identifier(&self) -> io::Result<u64> {
        self.controller
            .next_identifier
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |identifier| {
                (identifier < CANCEL_TAG - 1).then_some(identifier + 1)
            })
            .map_err(|_| io::Error::other("io_uring operation identifiers exhausted"))
    }

    async fn submit<T>(
        &self,
        request: Box<dyn PendingRequest>,
        result: oneshot::Receiver<T>,
        cancellation: &Cancellation,
    ) -> T {
        let identifier = request.identifier();
        if let Err(mpsc::SendError(Command::Submit(request))) =
            self.controller.commands.send(Command::Submit(request))
        {
            request.finish(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "io_uring selector has exited",
            )));
        }

        let mut cancel_request = CancelOnDrop {
            controller: Arc::clone(&self.controller),
            identifier,
            armed: true,
        };
        self.controller.notify();
        // A disconnected reply is an unexpected selector failure. Resources whose kernel completion cannot be established are deliberately retained.
        let mut result = pin!(result);
        let mut cancelled = pin!(cancellation.cancelled());
        let result = poll_fn(|context| {
            // Original completion wins the race. Cancellation never releases resources; continue waiting for the original CQE.
            if let Poll::Ready(result) = result.as_mut().poll(context) {
                return Poll::Ready(result);
            }
            if cancel_request.armed && cancelled.as_mut().poll(context).is_ready() {
                cancel_request.request();
            }
            Poll::Pending
        })
        .await
        .expect("io_uring selector failed with an operation in flight");
        cancel_request.armed = false;
        result
    }
}

fn operation_result<T>(result: io::Result<T>, cancellation: &Cancellation) -> OperationResult<T> {
    match result {
        Err(error)
            if cancellation.is_cancelled()
                && (error.kind() == io::ErrorKind::Interrupted
                    || error.raw_os_error() == Some(libc::ECANCELED)) =>
        {
            Err(crate::Cancelled.into())
        }
        result => result.map_err(Into::into),
    }
}
impl<O: crate::scheduler::native_socket_option::NativeSocketOption> crate::SocketOptions<O>
    for Selector
{
    async fn socket_get_option(
        &self,
        socket: &Self::Socket,
        option: O,
        cancellation: &Cancellation,
    ) -> OperationResult<O::Value> {
        self.check_open()?;
        readiness::Selector
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
        self.check_open()?;
        readiness::Selector
            .socket_set_option(socket, option, value, cancellation)
            .await
    }
}

impl SocketRegistration for Selector {
    fn register_socket(&self, socket: TcpStream) -> io::Result<SocketDescriptor> {
        self.check_open()?;
        readiness::Selector.register_socket(socket)
    }
    fn register_listener(&self, listener: TcpListener) -> io::Result<SocketDescriptor> {
        self.check_open()?;
        readiness::Selector.register_listener(listener)
    }
}
impl SocketOperations for Selector {
    type Socket = SocketDescriptor;

    async fn socket_close(&self, socket: Self::Socket) -> io::Result<()> {
        self.check_open()?;
        crate::scheduler::cleanup::run(&self.controller.cleanup, socket.into_close()).await
    }

    async fn socket_close_submit(&self, socket: Self::Socket) -> io::Result<()> {
        self.check_open()?;
        crate::scheduler::cleanup::submit_only(&self.controller.cleanup, socket.into_close()).await
    }

    async fn socket_shutdown(
        &self,
        socket: &Self::Socket,
        how: std::net::Shutdown,
    ) -> io::Result<()> {
        self.check_open()?;
        let socket = socket.retain();
        crate::scheduler::cleanup::run(&self.controller.cleanup, move || {
            socket.get_ref().shutdown(how)
        })
        .await
    }

    async fn socket_shutdown_submit(
        &self,
        socket: &Self::Socket,
        how: std::net::Shutdown,
    ) -> io::Result<()> {
        self.check_open()?;
        let socket = socket.retain();
        crate::scheduler::cleanup::submit_only(&self.controller.cleanup, move || {
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
        self.check_open()?;
        readiness::Selector
            .socket_open(domain, kind, protocol, cancellation)
            .await
    }
    async fn socket_bind(
        &self,
        socket: &Self::Socket,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.check_open()?;
        readiness::Selector
            .socket_bind(socket, address, cancellation)
            .await
    }
    async fn socket_listen(
        &self,
        socket: &Self::Socket,
        backlog: i32,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.check_open()?;
        readiness::Selector
            .socket_listen(socket, backlog, cancellation)
            .await
    }
    fn socket_local_addr(&self, socket: &Self::Socket) -> io::Result<SocketAddr> {
        self.check_open()?;
        readiness::Selector.socket_local_addr(socket)
    }
    fn socket_peer_addr(&self, socket: &Self::Socket) -> io::Result<SocketAddr> {
        self.check_open()?;
        readiness::Selector.socket_peer_addr(socket)
    }
    async fn socket_connect(
        &self,
        socket: &Self::Socket,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.check_open()?;
        readiness::Selector
            .socket_connect(socket, address, cancellation)
            .await
    }
    async fn socket_accept(
        &self,
        socket: &Self::Socket,
        cancellation: &Cancellation,
    ) -> OperationResult<(Self::Socket, SocketAddr)> {
        self.check_open()?;
        readiness::Selector
            .socket_accept(socket, cancellation)
            .await
    }
    async fn socket_read<B: IoBufMut>(
        &self,
        socket: &SocketDescriptor,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let (result, buffer) = retry_operation(
            buffer,
            |buffer| {
                self.operation(
                    Resource::Socket(socket.retain()),
                    buffer,
                    false,
                    |buffer| buffer.as_mut().as_mut_ptr(),
                    cancellation,
                )
            },
            || async {
                readiness::Selector
                    .socket_wait(socket, Interest::Readable, cancellation)
                    .await
                    .map_err(Into::into)
            },
            cancellation,
        )
        .await;
        (operation_result(result, cancellation), buffer)
    }
    async fn socket_write<B: IoBuf>(
        &self,
        socket: &SocketDescriptor,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let (result, buffer) = retry_operation(
            buffer,
            |buffer| {
                self.operation(
                    Resource::Socket(socket.retain()),
                    buffer,
                    true,
                    |buffer| buffer.as_ref().as_ptr().cast_mut(),
                    cancellation,
                )
            },
            || async {
                readiness::Selector
                    .socket_wait(socket, Interest::Writable, cancellation)
                    .await
                    .map_err(Into::into)
            },
            cancellation,
        )
        .await;
        (operation_result(result, cancellation), buffer)
    }
    async fn socket_wait(
        &self,
        socket: &SocketDescriptor,
        interest: Interest,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.check_open()?;
        readiness::Selector
            .socket_wait(socket, interest, cancellation)
            .await
    }
}
impl FileAllocate for Selector {
    async fn file_allocate(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        mode: AllocationMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        self.check_open()?;
        crate::scheduler::file_io::validate_allocation(offset, length)?;
        if self.supports_file_allocate {
            self.file_operation(
                file,
                FileOperation::Allocate {
                    offset,
                    length,
                    mode,
                },
                cancellation,
            )
            .await
        } else {
            readiness::Selector
                .file_allocate(file, offset, length, mode, cancellation)
                .await
        }
    }
}

impl crate::FileAdvice for Selector {
    async fn file_advise(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        advice: crate::AdviceMode,
    ) -> io::Result<()> {
        self.check_open()?;
        crate::scheduler::file_advice_io::run(
            &self.controller.cleanup,
            file,
            offset,
            length,
            advice,
        )
        .await
    }

    async fn file_advise_submit(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        advice: crate::AdviceMode,
    ) -> io::Result<()> {
        self.check_open()?;
        crate::scheduler::file_advice_io::submit_only(
            &self.controller.cleanup,
            file,
            offset,
            length,
            advice,
        )
        .await
    }
}

impl crate::FileOperation for Selector {
    type File = FileDescriptor;
}

impl FileClose for Selector {
    async fn file_close(&self, file: Self::File) -> io::Result<()> {
        self.check_open()?;
        crate::scheduler::file_close_io::close(&self.controller.cleanup, file).await
    }

    async fn file_close_submit(&self, file: Self::File) -> io::Result<()> {
        self.check_open()?;
        crate::scheduler::file_close_io::submit_only(&self.controller.cleanup, file).await
    }
}

impl FileTruncate for Selector {
    async fn file_truncate(
        &self,
        file: &Self::File,
        length: u64,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        self.check_open()?;
        crate::scheduler::file_io::validate_length(length)?;
        if self.supports_file_truncate {
            self.file_operation(file, FileOperation::Truncate(length), cancellation)
                .await
        } else {
            readiness::Selector
                .file_truncate(file, length, cancellation)
                .await
        }
    }
}

impl FileStat for Selector {
    type Metadata = crate::StatxMetadata;

    async fn file_stat(
        &self,
        file: &Self::File,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Metadata> {
        self.stat(file, cancellation).await
    }
}

impl FileSync for Selector {
    async fn file_sync(
        &self,
        file: &Self::File,
        mode: SyncMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.file_operation(file, FileOperation::Sync(mode), cancellation)
            .await
    }
}

impl FileSeek for Selector {
    async fn file_seek(
        &self,
        file: &Self::File,
        position: SeekFrom,
        cancellation: &Cancellation,
    ) -> OperationResult<u64> {
        self.check_open()?;
        // Seek uses the blocking fallback and shares the OS cursor with ring operations. Callers await cursor operations in order when ordering matters.
        readiness::Selector
            .file_seek(file, position, cancellation)
            .await
    }
}

impl FileOpen for Selector {
    async fn file_open(
        &self,
        path: &Path,
        options: FileOptions,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::File> {
        self.check_open()?;
        readiness::Selector
            .file_open(path, options, cancellation)
            .await
    }
}

impl FileRead for Selector {
    async fn file_read<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let (result, buffer) = self
            .operation(
                Resource::File(file.retain(), None),
                buffer,
                false,
                |buffer| buffer.as_mut().as_mut_ptr(),
                cancellation,
            )
            .await;
        (operation_result(result, cancellation), buffer)
    }
}

impl FileWrite for Selector {
    async fn file_write<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let (result, buffer) = self
            .operation(
                Resource::File(file.retain(), None),
                buffer,
                true,
                |buffer| buffer.as_ref().as_ptr().cast_mut(),
                cancellation,
            )
            .await;
        (operation_result(result, cancellation), buffer)
    }
}

impl FileReadAt for Selector {
    async fn file_read_at<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let (result, buffer) = self
            .operation(
                Resource::File(file.retain(), Some(offset)),
                buffer,
                false,
                |buffer| buffer.as_mut().as_mut_ptr(),
                cancellation,
            )
            .await;
        (operation_result(result, cancellation), buffer)
    }
}

impl FileWriteAt for Selector {
    async fn file_write_at<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let (result, buffer) = self
            .operation(
                Resource::File(file.retain(), Some(offset)),
                buffer,
                true,
                |buffer| buffer.as_ref().as_ptr().cast_mut(),
                cancellation,
            )
            .await;
        (operation_result(result, cancellation), buffer)
    }
}
impl Clock for Selector {
    fn now(&self) -> Instant {
        readiness::Selector.now()
    }
    async fn sleep_until(
        &self,
        deadline: Instant,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        readiness::Selector
            .sleep_until(deadline, cancellation)
            .await
    }
}

struct Reactor {
    // The poller is destroyed before its registered sources on error/unwind.
    poller: Poller,
    ring: IoUring,
    notification: UnixStream,
    incoming: mpsc::Receiver<Command>,
    pending: VecDeque<Box<dyn PendingRequest>>,
    in_flight: InFlight,
    cancellations: VecDeque<u64>,
    closing: bool,
}

impl Reactor {
    fn new(
        ring: IoUring,
        notification: UnixStream,
        incoming: mpsc::Receiver<Command>,
    ) -> io::Result<Self> {
        let reactor = Self {
            poller: syscall("poller", Poller::new)?,
            ring,
            notification,
            incoming,
            pending: VecDeque::new(),
            in_flight: InFlight::default(),
            cancellations: VecDeque::new(),
            closing: false,
        };
        // SAFETY: Reactor owns both sources and deletes their registrations in
        // Drop, before the poller and sources are destroyed.
        unsafe {
            syscall("add-notification", || {
                reactor
                    .poller
                    .add(&reactor.notification, Event::readable(COMMAND_EVENT))
            })?;
            syscall("add-ring", || {
                reactor
                    .poller
                    .add(&reactor.ring, Event::readable(RING_EVENT))
            })?;
        }
        Ok(reactor)
    }

    fn cancel(&mut self, identifier: u64) {
        if let Some(position) = self
            .pending
            .iter()
            .position(|request| request.identifier() == identifier)
        {
            let request = self
                .pending
                .remove(position)
                .expect("pending queue is unchanged since finding this position");
            request.finish(Err(io::ErrorKind::Interrupted.into()));
        } else if let Some(request) = self.in_flight.0.get(&identifier)
            && !request.cancelling()
        {
            request.set_cancelling();
            self.cancellations.push_back(identifier);
        }
    }

    fn run(&mut self) -> io::Result<()> {
        let mut events = Events::new();
        loop {
            // Bound command processing so a busy producer cannot starve completions.
            let mut command_batch_full = true;
            for _ in 0..256 {
                match self.incoming.try_recv() {
                    Ok(Command::Submit(request)) if !self.closing => {
                        self.pending.push_back(request)
                    }
                    Ok(Command::Submit(request)) => {
                        request.finish(Err(io::ErrorKind::Interrupted.into()))
                    }
                    Ok(Command::Cancel(identifier)) => self.cancel(identifier),
                    Ok(Command::Shutdown) => {
                        self.closing = true;
                        for request in self.pending.drain(..) {
                            request.finish(Err(io::ErrorKind::Interrupted.into()));
                        }
                        for request in self.in_flight.0.values() {
                            if !request.cancelling() {
                                request.set_cancelling();
                                self.cancellations.push_back(request.identifier());
                            }
                        }
                    }
                    Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => {
                        command_batch_full = false;
                        break;
                    }
                }
            }

            for completion in self.ring.completion() {
                let identifier = completion.user_data();
                if identifier & CANCEL_TAG != 0 {
                    continue;
                }
                // Every untagged CQE is the single original completion of a
                // published request. Only this path removes it from InFlight.
                let request = self
                    .in_flight
                    .0
                    .remove(&identifier)
                    .expect("original completion retains its in-flight request");
                let result = completion.result();
                request.finish(if result < 0 {
                    Err(io::Error::from_raw_os_error(-result))
                } else {
                    Ok(result as usize)
                });
            }

            {
                let mut submissions = self.ring.submission();
                while let Some(&identifier) = self.cancellations.front() {
                    let entry = opcode::AsyncCancel::new(identifier)
                        .build()
                        .user_data(identifier | CANCEL_TAG);
                    // SAFETY: cancellation entries reference only a never-reused
                    // numeric operation identifier, not userspace memory.
                    if unsafe { submissions.push(&entry) }.is_err() {
                        break;
                    }
                    self.cancellations.pop_front();
                }
                while !submissions.is_full() {
                    let Some(request) = self.pending.pop_front() else {
                        break;
                    };
                    let identifier = request.identifier();
                    let request = self.in_flight.0.entry(identifier).or_insert(request);
                    let entry = request.entry();
                    // SAFETY: InFlight owns the buffer allocation and descriptor
                    // before publication. Neither is released or accessed until
                    // the original CQE, even when cancellation completes first.
                    // A failed push does not publish the entry.
                    unsafe { submissions.push(&entry) }.expect(
                        "submission capacity was checked and the local queue has not changed",
                    );
                }
            }

            match syscall("submit", || self.ring.submit()) {
                Err(error)
                    if error.kind() == io::ErrorKind::Interrupted
                        || matches!(error.raw_os_error(), Some(libc::EBUSY | libc::EAGAIN)) =>
                {
                    continue;
                }
                result => {
                    result?;
                }
            }

            if self.closing && self.in_flight.0.is_empty() && self.pending.is_empty() {
                return Ok(());
            }

            // Drain the notification pipe before checking the channel again on
            // the next iteration. A racing send either leaves a byte or is seen
            // by try_recv. Use a nonblocking wait when commands may remain.
            let mut notified = false;
            let mut bytes = [0; 256];
            loop {
                match syscall("notification-read", || self.notification.read(&mut bytes)) {
                    Ok(0) => break,
                    Ok(_) => notified = true,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) => return Err(error),
                }
            }
            syscall("modify-notification", || {
                self.poller
                    .modify(&self.notification, Event::readable(COMMAND_EVENT))
            })?;
            syscall("modify-ring", || {
                self.poller.modify(&self.ring, Event::readable(RING_EVENT))
            })?;
            events.clear();
            let result = syscall("wait", || {
                self.poller.wait(
                    &mut events,
                    if notified
                        || command_batch_full
                        || !self.pending.is_empty()
                        || !self.ring.submission().is_empty()
                    {
                        Some(Duration::ZERO)
                    } else {
                        None
                    },
                )
            });
            if let Err(error) = result
                && error.kind() != io::ErrorKind::Interrupted
            {
                return Err(error);
            }
        }
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {
        let _ = self.poller.delete(&self.notification);
        let _ = self.poller.delete(&self.ring);
    }
}

#[cfg(test)]
mod tests;
