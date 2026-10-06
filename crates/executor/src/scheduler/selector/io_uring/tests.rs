// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;

#[test]
fn queued_request_returns_its_buffer_if_the_selector_exits() {
    let (reply, mut result) = oneshot::channel();
    let buffer = vec![7; 32];
    let allocation = buffer.as_ptr();
    let request = Request {
        identifier: 0,
        resource: Resource::File(
            Arc::new(StdFile::open(std::env::current_exe().unwrap()).unwrap()),
            0,
        ),
        buffer,
        write: false,
        reply: Some(reply),
        cancelling: false,
    };
    drop(request);
    let (result, buffer) = result.try_recv().unwrap().unwrap();
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(buffer, vec![7; 32]);
    assert_eq!(buffer.as_ptr(), allocation);
}

use std::cell::{Cell, RefCell};
thread_local! {
    static FAILURES: RefCell<VecDeque<(&'static str, io::Error)>> = const { RefCell::new(VecDeque::new()) };
}

pub(super) fn syscall<T>(
    name: &'static str,
    operation: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    let failure = FAILURES.with(|failures| {
        let mut failures = failures.borrow_mut();
        if failures
            .front()
            .is_some_and(|(expected, _)| *expected == name)
        {
            failures.pop_front().map(|(_, error)| error)
        } else {
            None
        }
    });
    match failure {
        Some(error) => Err(error),
        None => operation(),
    }
}

struct Failures;
impl Failures {
    fn new(failures: impl IntoIterator<Item = (&'static str, io::Error)>) -> Self {
        FAILURES.with(|state| *state.borrow_mut() = failures.into_iter().collect());
        Self
    }
}
impl Drop for Failures {
    fn drop(&mut self) {
        FAILURES.with(|failures| failures.borrow_mut().clear());
    }
}

fn request(identifier: u64) -> (Request, oneshot::Receiver<BufferResult>) {
    let (reply, result) = oneshot::channel();
    (
        Request {
            identifier,
            resource: Resource::File(Arc::new(tempfile::tempfile().unwrap()), 0),
            buffer: vec![1],
            write: false,
            reply: Some(reply),
            cancelling: false,
        },
        result,
    )
}

fn disconnected_selector() -> (Selector, UnixStream) {
    let (commands, incoming) = mpsc::channel();
    drop(incoming);
    let (notification, peer) = UnixStream::pair().unwrap();
    (
        Selector {
            controller: Arc::new(Controller {
                commands,
                notification,
                next_identifier: AtomicU64::new(0),
                closed: AtomicBool::new(false),
                shutdown: Arc::new(Shutdown {
                    finished: AtomicBool::new(false),
                    event: CompletionEvent::new(),
                }),
            }),
        },
        peer,
    )
}

fn new_reactor() -> (Reactor, mpsc::Sender<Command>, UnixStream) {
    let (notification, receiver) = UnixStream::pair().unwrap();
    receiver.set_nonblocking(true).unwrap();
    let (commands, incoming) = mpsc::channel();
    (
        Reactor::new(IoUring::new(2).unwrap(), receiver, incoming).unwrap(),
        commands,
        notification,
    )
}

#[test]
fn capability_validation_rejects_each_unsupported_requirement() {
    assert_eq!(
        validate_completion_support(false).unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
    validate_completion_support(true).unwrap();
    for missing in [
        opcode::Read::CODE,
        opcode::Write::CODE,
        opcode::Recv::CODE,
        opcode::Send::CODE,
        opcode::AsyncCancel::CODE,
    ] {
        assert_eq!(
            validate_operation_support(|operation| operation != missing)
                .unwrap_err()
                .kind(),
            io::ErrorKind::Unsupported
        );
    }
    validate_operation_support(|_| true).unwrap();
}

#[test]
fn initialization_propagates_each_system_failure() {
    for name in [
        "ring",
        "completion-support",
        "probe",
        "operation-support",
        "notification-pair",
        "notification-mode",
        "receiver-mode",
        "poller",
        "add-notification",
        "add-ring",
        "spawn",
    ] {
        let _failures = Failures::new([(name, io::Error::other(name))]);
        let error = Selector::new().err().unwrap();
        assert_eq!(error.to_string(), name);
    }
}

#[test]
fn rejected_operations_preserve_buffer_allocations() {
    async_io::block_on(async {
        let (selector, _peer) = disconnected_selector();
        let file = Arc::new(tempfile::tempfile().unwrap());
        let (_, buffer) = selector
            .file_read_at(Arc::clone(&file), Vec::new(), 0)
            .await;
        assert!(buffer.is_empty());
        let buffer = vec![7; 8];
        let allocation = buffer.as_ptr();
        let (result, returned) = selector.file_read_at(Arc::clone(&file), buffer, 0).await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(returned.as_ptr(), allocation);
        selector
            .controller
            .next_identifier
            .store(CANCEL_TAG - 1, Ordering::Relaxed);
        let buffer = vec![9; 8];
        let allocation = buffer.as_ptr();
        let (result, returned) = selector.file_write_at(file, buffer, 0).await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("identifiers exhausted")
        );
        assert_eq!(returned.as_ptr(), allocation);
    });
}

#[test]
fn failure_retains_in_flight_resources_before_notifying_callers() {
    let (request, mut receiver) = request(0);
    let Resource::File(file, _) = &request.resource else {
        panic!("file request")
    };
    let retained = Arc::downgrade(file);
    let mut in_flight = InFlight::default();
    in_flight.0.insert(0, request);
    drop(in_flight);
    assert!(receiver.try_recv().is_err());
    assert_eq!(retained.strong_count(), 1);
}

#[test]
fn notification_retries_interruption() {
    let (selector, mut peer) = disconnected_selector();
    let _failures = Failures::new([("notify", io::ErrorKind::Interrupted.into())]);
    selector.controller.notify();
    let mut byte = [0];
    peer.read_exact(&mut byte).unwrap();
    assert_eq!(byte, [1]);
}

#[test]
fn socket_retries_keep_the_owned_buffer_and_propagate_wait_errors() {
    async_io::block_on(async {
        let buffer = vec![4; 4];
        let allocation = buffer.as_ptr();
        let waits = Cell::new(0);
        let mut results: VecDeque<io::Result<usize>> = [
            Err(io::ErrorKind::Interrupted.into()),
            Err(io::ErrorKind::WouldBlock.into()),
            Ok(4),
        ]
        .into();
        let (result, buffer) = retry_operation(
            buffer,
            |buffer| {
                assert_eq!(buffer.as_ptr(), allocation);
                std::future::ready((results.pop_front().unwrap(), buffer))
            },
            || {
                waits.set(waits.get() + 1);
                std::future::ready(Ok(()))
            },
        )
        .await;
        assert_eq!(result.unwrap(), 4);
        assert_eq!(buffer.as_ptr(), allocation);
        assert_eq!(waits.get(), 1);
        let (result, buffer) = retry_operation(
            buffer,
            |buffer| std::future::ready((Err(io::ErrorKind::WouldBlock.into()), buffer)),
            || std::future::ready(Err(io::ErrorKind::BrokenPipe.into())),
        )
        .await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(buffer.as_ptr(), allocation);
    });
}

#[test]
fn closed_selector_rejects_its_direct_network_operations() {
    async_io::block_on(async {
        let selector = Selector::new().unwrap();
        let raw_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = raw_listener.local_addr().unwrap();
        let raw_socket = TcpStream::connect(address).unwrap();
        let socket = readiness::Selector
            .register_socket(raw_socket.try_clone().unwrap())
            .unwrap();
        let listener = readiness::Selector
            .register_listener(raw_listener.try_clone().unwrap())
            .unwrap();
        selector.wait_closed();
        assert!(selector.register_socket(raw_socket).is_err());
        assert!(selector.register_listener(raw_listener).is_err());
        assert!(selector.connect(address).await.is_err());
        assert!(selector.accept(&listener).await.is_err());
        assert!(selector.io_wait(&socket, Interest::Readable).await.is_err());
    });
}

#[test]
fn reactor_propagates_terminal_system_errors_and_retries_submission() {
    for name in [
        "submit",
        "notification-read",
        "modify-notification",
        "modify-ring",
        "wait",
    ] {
        let (mut reactor, _commands, _notification) = new_reactor();
        let _failures = Failures::new([(name, io::Error::other(name))]);
        assert_eq!(reactor.run().unwrap_err().to_string(), name);
    }
    let (mut reactor, commands, _notification) = new_reactor();
    commands.send(Command::Shutdown).unwrap();
    let _failures = Failures::new([
        ("submit", io::ErrorKind::Interrupted.into()),
        ("submit", io::Error::from_raw_os_error(libc::EBUSY)),
        ("submit", io::Error::from_raw_os_error(libc::EAGAIN)),
    ]);
    reactor.run().unwrap();
}

#[test]
fn reactor_drains_notifications_and_retries_interrupted_waits() {
    let (mut reactor, _commands, mut notification) = new_reactor();
    notification.write_all(&[1]).unwrap();
    let _failures = Failures::new([
        ("notification-read", io::ErrorKind::Interrupted.into()),
        ("wait", io::ErrorKind::Interrupted.into()),
        ("notification-read", io::Error::other("stop")),
    ]);
    assert_eq!(reactor.run().unwrap_err().to_string(), "stop");
    let (mut reactor, _commands, notification) = new_reactor();
    drop(notification);
    let _failures = Failures::new([("wait", io::Error::other("closed pipe"))]);
    assert_eq!(reactor.run().unwrap_err().to_string(), "closed pipe");
}

#[test]
fn pending_cancellation_returns_buffers_and_late_cancellation_is_harmless() {
    let (mut reactor, _commands, _notification) = new_reactor();
    let (request, mut result) = request(7);
    reactor.pending.push_back(request);
    reactor.cancel(7);
    let (result, buffer) = result.try_recv().unwrap().unwrap();
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Interrupted);
    assert_eq!(buffer, [1]);
    reactor.cancel(7);
}

#[test]
fn closing_reactor_rejects_new_requests_after_draining_pending_requests() {
    let (mut reactor, commands, _notification) = new_reactor();
    let (pending, mut pending_result) = request(1);
    let (late, mut late_result) = request(2);
    commands.send(Command::Submit(pending)).unwrap();
    commands.send(Command::Shutdown).unwrap();
    commands.send(Command::Submit(late)).unwrap();
    reactor.run().unwrap();
    for result in [&mut pending_result, &mut late_result] {
        assert_eq!(
            result.try_recv().unwrap().unwrap().0.unwrap_err().kind(),
            io::ErrorKind::Interrupted
        );
    }
}

#[test]
fn command_batches_do_not_starve_shutdown_or_completions() {
    let (mut reactor, commands, _notification) = new_reactor();
    let mut results = Vec::new();
    for identifier in 0..257 {
        let (request, result) = request(identifier);
        commands.send(Command::Submit(request)).unwrap();
        results.push(result);
    }
    commands.send(Command::Shutdown).unwrap();
    reactor.run().unwrap();
    for mut result in results {
        let (result, buffer) = result.try_recv().unwrap().unwrap();
        assert!(result.is_ok() || result.unwrap_err().kind() == io::ErrorKind::Interrupted);
        assert_eq!(buffer, [1]);
    }
}

#[test]
fn repeated_cancellation_does_not_duplicate_submission_and_full_queues_defer_it() {
    let (mut reactor, commands, _notification) = new_reactor();
    let mut results = Vec::new();
    for identifier in 0..3 {
        let (request, result) = request(identifier);
        reactor.in_flight.0.insert(identifier, request);
        let entry = reactor.in_flight.0.get_mut(&identifier).unwrap().entry();
        // SAFETY: the map owns the stable allocation before publication and
        // run() retains it until the original completion is received.
        unsafe {
            reactor.ring.submission().push(&entry).unwrap();
        }
        reactor.ring.submit().unwrap();
        reactor.cancel(identifier);
        reactor.cancel(identifier);
        results.push(result);
    }
    assert_eq!(reactor.cancellations.len(), 3);
    commands.send(Command::Shutdown).unwrap();
    reactor.run().unwrap();
    for mut result in results {
        let (_, buffer) = result.try_recv().unwrap().unwrap();
        assert_eq!(buffer, [1]);
    }
}

#[test]
fn socket_readiness_retries_preserve_allocations() {
    async_io::block_on(async {
        let selector = Selector::new().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let raw_socket = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (mut peer, _) = listener.accept().unwrap();
        let socket = selector.register_socket(raw_socket).unwrap();
        peer.write_all(b"ready").unwrap();
        let buffer = vec![0; 5];
        let allocation = buffer.as_ptr();
        let failures = Failures::new([("operation", io::ErrorKind::WouldBlock.into())]);
        let (result, buffer) = selector.io_read(&socket, buffer).await;
        let count = result.unwrap();
        assert!(count > 0);
        assert_eq!(&buffer[..count], &b"ready"[..count]);
        assert_eq!(buffer.as_ptr(), allocation);
        assert!(FAILURES.with(|failures| failures.borrow().is_empty()));
        drop(failures);
        let buffer = b"write".to_vec();
        let allocation = buffer.as_ptr();
        let _failures = Failures::new([("operation", io::ErrorKind::WouldBlock.into())]);
        let (result, buffer) = selector.io_write(&socket, buffer).await;
        let count = result.unwrap();
        assert!(count > 0);
        assert_eq!(buffer.as_ptr(), allocation);
        let mut received = vec![0; count];
        peer.read_exact(&mut received).unwrap();
        assert_eq!(received, &b"write"[..count]);
        assert!(FAILURES.with(|failures| failures.borrow().is_empty()));
        selector.wait_closed();
    });
}
