// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Linux completion reads/writes using a ring owned by a dedicated selector
//! thread. Tasks communicate through owned requests and standard wakers, so
//! operation futures remain Send and can migrate between executor workers.
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
use super::readiness::{self, Listener, Socket};
use crate::scheduler::{BufferResult, Clock, File, Interest, Socket as SocketOperations};
use crate::{Cancellation, FileOptions, IoBuf, IoBufMut, OperationResult, SocketRegistration};
use event_listener::{Event as CompletionEvent, Listener as _};
use futures_channel::oneshot;
use io_uring::{IoUring, opcode, squeue, types};
use polling::{Event, Events, Poller};
use std::collections::{HashMap, VecDeque};
use std::fs::File as StdFile;
use std::io::{self, Read, Write};
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
        opcode::AsyncCancel::CODE,
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
    Socket(Socket),
    // None uses the shared OS cursor; Some is a validated explicit offset.
    File(Arc<StdFile>, Option<u64>),
}

struct Request<B: IoBuf = Vec<u8>> {
    identifier: u64,
    resource: Resource,
    buffer: Option<B>,
    pointer: fn(&mut B) -> *mut u8,
    write: bool,
    reply: Option<oneshot::Sender<RawBufferResult<B>>>,
    cancelling: bool,
}

impl<B: IoBuf> Request<B> {
    fn entry(&mut self) -> squeue::Entry {
        let buffer = self.buffer.as_mut().expect("request retains its buffer");
        let length = buffer.as_ref().len().min(i32::MAX as usize) as u32;
        let pointer = (self.pointer)(buffer);
        let entry = match &self.resource {
            Resource::Socket(socket) => {
                let descriptor = types::Fd(socket.0.get_ref().as_raw_fd());
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
        let _ = reply.send((
            result,
            self.buffer.take().expect("request returns its buffer once"),
        ));
    }
}

impl<B: IoBuf> Drop for Request<B> {
    fn drop(&mut self) {
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

// One erased request allocation lets a selector queue heterogeneous owned
// buffer types. Its typed completion returns the exact original owner.
trait PendingRequest: Send {
    fn identifier(&self) -> u64;
    fn entry(&mut self) -> squeue::Entry;
    fn finish(self: Box<Self>, result: io::Result<usize>);
    fn cancelling(&self) -> bool;
    fn set_cancelling(&mut self);
    fn take_reply(&mut self) -> Option<Box<dyn Send>>;
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
        self.cancelling
    }
    fn set_cancelling(&mut self) {
        self.cancelling = true;
    }
    fn take_reply(&mut self) -> Option<Box<dyn Send>> {
        self.reply
            .take()
            .map(|reply| Box::new(reply) as Box<dyn Send>)
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
        for (_, mut request) in self.0.drain() {
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
            controller: Arc::new(Controller {
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
        if buffer.as_ref().is_empty() {
            return (Ok(0), buffer);
        }
        let identifier = match self.controller.next_identifier.try_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |identifier| (identifier < CANCEL_TAG - 1).then_some(identifier + 1),
        ) {
            Ok(identifier) => identifier,
            Err(_) => {
                return (
                    Err(io::Error::other("io_uring operation identifiers exhausted")),
                    buffer,
                );
            }
        };
        let (reply, result) = oneshot::channel();
        let request = Request {
            identifier,
            resource,
            buffer: Some(buffer),
            pointer,
            write,
            reply: Some(reply),
            cancelling: false,
        };
        if let Err(mpsc::SendError(Command::Submit(request))) = self
            .controller
            .commands
            .send(Command::Submit(Box::new(request)))
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
        // A disconnected reply is an unexpected selector failure. Its original
        // buffer is deliberately retained; it cannot safely be returned.
        let mut result = pin!(result);
        let mut cancelled = pin!(cancellation.cancelled());
        let result = poll_fn(|context| {
            // Original completion wins the race. A cancellation request never
            // releases its buffer; continue waiting for the original CQE.
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
impl SocketRegistration for Selector {
    fn register_socket(&self, socket: TcpStream) -> io::Result<Socket> {
        self.check_open()?;
        readiness::Selector.register_socket(socket)
    }
    fn register_listener(&self, listener: TcpListener) -> io::Result<Listener> {
        self.check_open()?;
        readiness::Selector.register_listener(listener)
    }
}
impl SocketOperations for Selector {
    type Socket = Socket;
    type Listener = Listener;
    async fn socket_listen(
        &self,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<Listener> {
        self.check_open()?;
        readiness::Selector
            .socket_listen(address, cancellation)
            .await
    }
    fn listener_addr(&self, listener: &Listener) -> io::Result<SocketAddr> {
        self.check_open()?;
        listener.local_addr()
    }
    fn socket_local_addr(&self, socket: &Socket) -> io::Result<SocketAddr> {
        self.check_open()?;
        socket.local_addr()
    }
    fn socket_peer_addr(&self, socket: &Socket) -> io::Result<SocketAddr> {
        self.check_open()?;
        socket.peer_addr()
    }
    async fn connect(
        &self,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<Socket> {
        self.check_open()?;
        readiness::Selector.connect(address, cancellation).await
    }
    async fn accept(
        &self,
        listener: &Listener,
        cancellation: &Cancellation,
    ) -> OperationResult<(Socket, SocketAddr)> {
        self.check_open()?;
        readiness::Selector.accept(listener, cancellation).await
    }
    async fn io_read<B: IoBufMut>(
        &self,
        socket: &Socket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let (result, buffer) = retry_operation(
            buffer,
            |buffer| {
                self.operation(
                    Resource::Socket(socket.clone()),
                    buffer,
                    false,
                    |buffer| buffer.as_mut().as_mut_ptr(),
                    cancellation,
                )
            },
            || async {
                readiness::Selector
                    .io_wait(socket, Interest::Readable, cancellation)
                    .await
                    .map_err(Into::into)
            },
            cancellation,
        )
        .await;
        (operation_result(result, cancellation), buffer)
    }
    async fn io_write<B: IoBuf>(
        &self,
        socket: &Socket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let (result, buffer) = retry_operation(
            buffer,
            |buffer| {
                self.operation(
                    Resource::Socket(socket.clone()),
                    buffer,
                    true,
                    |buffer| buffer.as_ref().as_ptr().cast_mut(),
                    cancellation,
                )
            },
            || async {
                readiness::Selector
                    .io_wait(socket, Interest::Writable, cancellation)
                    .await
                    .map_err(Into::into)
            },
            cancellation,
        )
        .await;
        (operation_result(result, cancellation), buffer)
    }
    async fn io_wait(
        &self,
        socket: &Socket,
        interest: Interest,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.check_open()?;
        readiness::Selector
            .io_wait(socket, interest, cancellation)
            .await
    }
}
impl File for Selector {
    type File = Arc<StdFile>;
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
    async fn file_read<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let (result, buffer) = self
            .operation(
                Resource::File(Arc::clone(file), None),
                buffer,
                false,
                |buffer| buffer.as_mut().as_mut_ptr(),
                cancellation,
            )
            .await;
        (operation_result(result, cancellation), buffer)
    }
    async fn file_write<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let (result, buffer) = self
            .operation(
                Resource::File(Arc::clone(file), None),
                buffer,
                true,
                |buffer| buffer.as_ref().as_ptr().cast_mut(),
                cancellation,
            )
            .await;
        (operation_result(result, cancellation), buffer)
    }
    async fn file_read_at<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let (result, buffer) = self
            .operation(
                Resource::File(Arc::clone(file), Some(offset)),
                buffer,
                false,
                |buffer| buffer.as_mut().as_mut_ptr(),
                cancellation,
            )
            .await;
        (operation_result(result, cancellation), buffer)
    }
    async fn file_write_at<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let (result, buffer) = self
            .operation(
                Resource::File(Arc::clone(file), Some(offset)),
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
        } else if let Some(request) = self.in_flight.0.get_mut(&identifier)
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
                        for request in self.in_flight.0.values_mut() {
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
