// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

static NEVER: crate::Cancellation = crate::Cancellation::never();

use super::*;

struct ExclusiveOnWake {
    file: std::sync::Weak<StdFile>,
    wakes: std::sync::atomic::AtomicUsize,
}

impl std::task::Wake for ExclusiveOnWake {
    fn wake(self: Arc<Self>) {
        assert_eq!(
            self.file.strong_count(),
            1,
            "completion woke the caller before releasing backend ownership"
        );
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn completion_releases_file_ownership_before_waking_close_callers() {
    use std::task::{Context, Waker};
    for transfer in [false, true] {
        for queued_drop in [false, true] {
            let file = Arc::new(tempfile::tempfile().unwrap());
            let observer = Arc::new(ExclusiveOnWake {
                file: Arc::downgrade(&file),
                wakes: std::sync::atomic::AtomicUsize::new(0),
            });
            let waker = Waker::from(Arc::clone(&observer));
            let mut context = Context::from_waker(&waker);
            if transfer {
                let (mut request, mut result) = request(1);
                request.resource = Some(Resource::File(Arc::clone(&file), Some(0)));
                assert!(
                    std::pin::Pin::new(&mut result)
                        .poll(&mut context)
                        .is_pending()
                );
                if queued_drop {
                    drop(request);
                } else {
                    request.finish(Ok(0));
                }
            } else {
                let (mut request, mut result) = sync_request(1);
                request.file = Some(Arc::clone(&file));
                assert!(
                    std::pin::Pin::new(&mut result)
                        .poll(&mut context)
                        .is_pending()
                );
                if queued_drop {
                    drop(request);
                } else {
                    Box::new(request).finish(Ok(0));
                }
            }
            assert_eq!(observer.wakes.load(Ordering::SeqCst), 1);
        }
    }
}

#[test]
fn close_rejects_a_stopped_selector() {
    async_io::block_on(async {
        let selector = Selector::new().unwrap();
        let socket = || {
            SocketDescriptor::new(
                socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap(),
            )
            .unwrap()
        };
        let first = socket();
        let second = socket();
        let third = socket();
        selector.shutdown().await;
        assert_eq!(
            selector
                .socket_shutdown(&first, std::net::Shutdown::Write)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            selector
                .socket_shutdown_submit(&first, std::net::Shutdown::Write)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            selector.socket_close(second).await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            selector
                .socket_close_submit(third)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            selector
                .file_close(tempfile::tempfile().unwrap().into())
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            selector
                .file_close_submit(tempfile::tempfile().unwrap().into())
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
    });
}

#[test]
fn queued_request_returns_its_buffer_if_the_selector_exits() {
    let (reply, mut result) = oneshot::channel();
    let buffer = vec![7; 32];
    let allocation = buffer.as_ptr();
    let request = Request {
        identifier: 0,
        resource: Some(Resource::File(
            Arc::new(StdFile::open(std::env::current_exe().unwrap()).unwrap()),
            Some(0),
        )),
        buffer: Some(buffer),
        pointer: |buffer| buffer.as_mut_ptr(),
        write: false,
        reply: Cell::new(Some(reply)),
        cancelling: Cell::new(false),
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

fn request(identifier: u64) -> (Request, oneshot::Receiver<RawBufferResult>) {
    let (reply, result) = oneshot::channel();
    (
        Request {
            identifier,
            resource: Some(Resource::File(
                Arc::new(tempfile::tempfile().unwrap()),
                Some(0),
            )),
            buffer: Some(vec![1]),
            pointer: |buffer| buffer.as_mut_ptr(),
            write: false,
            reply: Cell::new(Some(reply)),
            cancelling: Cell::new(false),
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
            supports_file_truncate: true,
            supports_file_allocate: true,
            controller: Arc::new(Controller {
                cleanup: crate::blocking_pool::BlockingPool::new(crate::BlockingOptions::default()),
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
        opcode::SendMsg::CODE,
        opcode::RecvMsg::CODE,
        opcode::AsyncCancel::CODE,
        opcode::Fsync::CODE,
        opcode::Statx::CODE,
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
        let file = crate::FileDescriptor::from(tempfile::tempfile().unwrap());
        let (_, buffer) = selector.file_read_at(&file, Vec::new(), 0, &NEVER).await;
        assert!(buffer.is_empty());
        let buffer = vec![7; 8];
        let allocation = buffer.as_ptr();
        let (result, returned) = selector.file_read_at(&file, buffer, 0, &NEVER).await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(returned.as_ptr(), allocation);
        selector
            .controller
            .next_identifier
            .store(CANCEL_TAG - 1, Ordering::Relaxed);
        let buffer = vec![9; 8];
        let allocation = buffer.as_ptr();
        let (result, returned) = selector.file_write_at(&file, buffer, 0, &NEVER).await;
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
fn empty_file_buffers_complete_without_selector_admission() {
    async_io::block_on(async {
        let (selector, _peer) = disconnected_selector();
        let file = Arc::new(tempfile::tempfile().unwrap());

        let (result, buffer) = selector
            .operation(
                Resource::File(file, Some(0)),
                Vec::new(),
                false,
                |buffer| buffer.as_mut_ptr(),
                &NEVER,
            )
            .await;

        assert_eq!(result.unwrap(), 0);
        assert!(buffer.is_empty());
    });
}

#[test]
fn failure_retains_in_flight_resources_before_notifying_callers() {
    let (request, mut receiver) = request(0);
    let Some(Resource::File(file, _)) = &request.resource else {
        panic!("file request")
    };
    let retained = Arc::downgrade(file);
    let mut in_flight = InFlight::default();
    in_flight.0.insert(0, Box::new(request));
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
            &NEVER,
        )
        .await;
        assert_eq!(result.unwrap(), 4);
        assert_eq!(buffer.as_ptr(), allocation);
        assert_eq!(waits.get(), 1);
        let (result, buffer) = retry_operation(
            buffer,
            |buffer| std::future::ready((Err(io::ErrorKind::WouldBlock.into()), buffer)),
            || std::future::ready(Err(io::ErrorKind::BrokenPipe.into())),
            &NEVER,
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
        assert!(
            selector
                .socket_open(
                    crate::SocketDomain::Ipv4,
                    crate::SocketType::Stream,
                    Some(crate::SocketProtocol::Tcp),
                    &NEVER,
                )
                .await
                .is_err()
        );
        assert!(
            selector
                .socket_bind(&socket, address, &NEVER)
                .await
                .is_err()
        );
        assert!(
            selector
                .socket_listen(&listener, 128, &NEVER)
                .await
                .is_err()
        );
        assert!(selector.socket_local_address(&listener).is_err());
        assert!(selector.socket_local_address(&socket).is_err());
        assert!(selector.socket_peer_address(&socket).is_err());
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("closed");
        assert!(
            selector
                .file_open(&path, FileOptions::CREATE, &NEVER)
                .await
                .is_err()
        );
        assert!(!path.exists());
        let file = crate::FileDescriptor::from(tempfile::tempfile().unwrap());
        assert_eq!(
            selector
                .file_seek(&file, SeekFrom::Start(1), &NEVER)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            selector
                .file_sync(&file, SyncMode::All, &NEVER)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        for supports_file_truncate in [false, true] {
            let mut selector = selector.clone();
            selector.supports_file_truncate = supports_file_truncate;
            assert_eq!(
                selector
                    .file_truncate(&file, 13, &NEVER)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
        }
        assert_eq!(file.as_ref().metadata().unwrap().len(), 0);
        assert!(
            selector
                .socket_connect(&socket, address, &NEVER)
                .await
                .is_err()
        );
        assert!(selector.socket_accept(&listener, &NEVER).await.is_err());
        assert!(
            selector
                .socket_wait(&socket, Interest::Readable, &NEVER)
                .await
                .is_err()
        );
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
    reactor.pending.push_back(Box::new(request));
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
    commands.send(Command::Submit(Box::new(pending))).unwrap();
    commands.send(Command::Shutdown).unwrap();
    commands.send(Command::Submit(Box::new(late))).unwrap();
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
        commands.send(Command::Submit(Box::new(request))).unwrap();
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
        reactor.in_flight.0.insert(identifier, Box::new(request));
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
        let (result, buffer) = selector.socket_read(&socket, buffer, &NEVER).await;
        let count = result.unwrap();
        assert!(count > 0);
        assert_eq!(&buffer[..count], &b"ready"[..count]);
        assert_eq!(buffer.as_ptr(), allocation);
        assert!(FAILURES.with(|failures| failures.borrow().is_empty()));
        drop(failures);
        let buffer = b"write".to_vec();
        let allocation = buffer.as_ptr();
        let _failures = Failures::new([("operation", io::ErrorKind::WouldBlock.into())]);
        let (result, buffer) = selector.socket_write(&socket, buffer, &NEVER).await;
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

#[test]
fn cooperative_cancellation_waits_for_original_completion_and_preserves_progress() {
    use std::sync::atomic::AtomicUsize;
    struct CountedBuffer {
        bytes: Vec<u8>,
        dropped: Arc<AtomicUsize>,
    }
    impl AsRef<[u8]> for CountedBuffer {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl AsMut<[u8]> for CountedBuffer {
        fn as_mut(&mut self) -> &mut [u8] {
            &mut self.bytes
        }
    }
    // SAFETY: The Vec retains its initialized heap storage through owner moves.
    unsafe impl IoBuf for CountedBuffer {}
    unsafe impl IoBufMut for CountedBuffer {}
    impl Drop for CountedBuffer {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    async_io::block_on(async {
        for successful in [false, true] {
            let (commands, incoming) = mpsc::channel();
            let (notification, _peer) = UnixStream::pair().unwrap();
            let selector = Selector {
                supports_file_truncate: true,
                supports_file_allocate: true,
                controller: Arc::new(Controller {
                    cleanup: crate::blocking_pool::BlockingPool::new(
                        crate::BlockingOptions::default(),
                    ),
                    commands,
                    notification,
                    next_identifier: AtomicU64::new(0),
                    closed: AtomicBool::new(false),
                    shutdown: Arc::new(Shutdown {
                        finished: AtomicBool::new(false),
                        event: CompletionEvent::new(),
                    }),
                }),
            };
            let file = crate::FileDescriptor::from(tempfile::tempfile().unwrap());
            let cancelled = Cancellation::new();
            let dropped = Arc::new(AtomicUsize::new(0));
            let buffer = CountedBuffer {
                bytes: vec![7; 4],
                dropped: dropped.clone(),
            };
            let pointer = buffer.bytes.as_ptr();
            let mut read = Box::pin(selector.file_read_at(&file, buffer, 0, &cancelled));
            assert!(futures_lite::future::poll_once(&mut read).await.is_none());
            let Command::Submit(request) = incoming.try_recv().unwrap() else {
                panic!("missing original submission")
            };
            let identifier = request.identifier();
            cancelled.cancel();
            assert!(futures_lite::future::poll_once(&mut read).await.is_none());
            assert!(
                matches!(incoming.try_recv().unwrap(), Command::Cancel(value) if value == identifier)
            );
            assert!(futures_lite::future::poll_once(&mut read).await.is_none());
            assert!(
                incoming.try_recv().is_err(),
                "cancellation is submitted only once"
            );
            assert_eq!(dropped.load(Ordering::SeqCst), 0);
            // No SQE was published in this controlled selector. Simulate its
            // terminal original CQE only after confirming cancellation retained
            // the typed buffer, regardless of the cancellation request result.
            request.finish(if successful {
                Ok(3)
            } else {
                Err(io::Error::from_raw_os_error(libc::ECANCELED))
            });
            let (result, buffer) = read.await;
            if successful {
                assert_eq!(result.unwrap(), 3);
            } else {
                assert!(matches!(result, Err(crate::OperationError::Cancelled(_))));
            }
            assert_eq!(buffer.bytes.as_ptr(), pointer);
            assert_eq!(dropped.load(Ordering::SeqCst), 0);
            drop(buffer);
            assert_eq!(dropped.load(Ordering::SeqCst), 1);
        }
    });
}

#[test]
fn cancellation_before_submission_and_retry_returns_owned_buffers() {
    async_io::block_on(async {
        let cancellation = Cancellation::new();
        cancellation.cancel();
        let (selector, _peer) = disconnected_selector();
        let file = crate::FileDescriptor::from(tempfile::tempfile().unwrap());
        let buffer = vec![5; 3];
        let allocation = buffer.as_ptr();
        let (result, buffer) = selector
            .file_write_at(&file, buffer, 0, &cancellation)
            .await;
        assert!(matches!(result, Err(crate::OperationError::Cancelled(_))));
        let (result, buffer) = retry_operation(
            buffer,
            |_| std::future::ready((Ok(0), Vec::new())),
            || std::future::ready(Ok(())),
            &cancellation,
        )
        .await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Interrupted);
        assert_eq!(buffer.as_ptr(), allocation);
        assert!(matches!(
            operation_result::<()>(Err(io::ErrorKind::Interrupted.into()), &cancellation),
            Err(crate::OperationError::Cancelled(_))
        ));
    });
}

fn sync_request(identifier: u64) -> (FileRequest, oneshot::Receiver<io::Result<()>>) {
    let (reply, result) = oneshot::channel();
    (
        FileRequest {
            identifier,
            file: Some(Arc::new(tempfile::tempfile().unwrap())),
            operation: FileOperation::Sync(SyncMode::All),
            reply: Cell::new(Some(reply)),
            cancelling: Cell::new(false),
        },
        result,
    )
}

#[test]
fn queued_sync_releases_its_file_and_reports_selector_exit() {
    let (request, mut result) = sync_request(0);
    let retained = Arc::downgrade(request.file.as_ref().unwrap());
    drop(request);
    assert_eq!(retained.strong_count(), 0);
    assert_eq!(
        result.try_recv().unwrap().unwrap().unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[test]
fn failed_selector_retains_in_flight_sync_file_before_notifying_caller() {
    let (request, mut result) = sync_request(0);
    let retained = Arc::downgrade(request.file.as_ref().unwrap());
    let mut in_flight = InFlight::default();
    in_flight.0.insert(0, Box::new(request));
    drop(in_flight);
    assert!(result.try_recv().is_err());
    assert_eq!(retained.strong_count(), 1);
}

#[test]
fn sync_rejects_disconnected_and_exhausted_selectors() {
    async_io::block_on(async {
        let (selector, _peer) = disconnected_selector();
        let file = crate::FileDescriptor::from(tempfile::tempfile().unwrap());
        assert_eq!(
            selector
                .file_sync(&file, SyncMode::All, &NEVER)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        selector
            .controller
            .next_identifier
            .store(CANCEL_TAG - 1, Ordering::Relaxed);
        assert!(
            selector
                .file_sync(&file, SyncMode::Data, &NEVER)
                .await
                .unwrap_err()
                .to_string()
                .contains("identifiers exhausted")
        );
    });
}

#[test]
fn pending_and_in_flight_sync_cancellation_preserve_retirement() {
    let (mut reactor, commands, _notification) = new_reactor();
    let (pending, mut pending_result) = sync_request(1);
    let pending_file = Arc::downgrade(pending.file.as_ref().unwrap());
    reactor.pending.push_back(Box::new(pending));
    reactor.cancel(1);
    assert_eq!(
        pending_result
            .try_recv()
            .unwrap()
            .unwrap()
            .unwrap_err()
            .kind(),
        io::ErrorKind::Interrupted
    );
    assert_eq!(pending_file.strong_count(), 0);

    let (mut request, mut result) = sync_request(2);
    let retained = Arc::downgrade(request.file.as_ref().unwrap());
    let entry = request.entry();
    reactor.in_flight.0.insert(2, Box::new(request));
    // SAFETY: The in-flight map owns the file until the original completion is consumed.
    unsafe {
        reactor.ring.submission().push(&entry).unwrap();
    }
    reactor.ring.submit().unwrap();
    reactor.cancel(2);
    reactor.cancel(2);
    assert_eq!(reactor.cancellations.len(), 1);
    assert_eq!(retained.strong_count(), 1);
    commands.send(Command::Shutdown).unwrap();
    reactor.run().unwrap();
    let outcome = result.try_recv().unwrap().unwrap();
    assert!(
        outcome.is_ok() || matches!(outcome.unwrap_err().raw_os_error(), Some(libc::ECANCELED))
    );
    assert_eq!(retained.strong_count(), 0);
}

#[test]
fn buffer_free_file_operations_are_lazy_and_retain_the_file_until_original_completion() {
    async_io::block_on(async {
        for operation in [
            FileOperation::Sync(SyncMode::All),
            FileOperation::Sync(SyncMode::Data),
            FileOperation::Truncate(7),
            FileOperation::Allocate {
                offset: 3,
                length: 7,
                mode: AllocationMode::Extend,
            },
            FileOperation::Allocate {
                offset: 3,
                length: 7,
                mode: AllocationMode::KeepSize,
            },
        ] {
            for abandon in [false, true] {
                for complete in [false, true] {
                    let (commands, incoming) = mpsc::channel();
                    let (notification, _peer) = UnixStream::pair().unwrap();
                    let selector = Selector {
                        supports_file_truncate: true,
                        supports_file_allocate: true,
                        controller: Arc::new(Controller {
                            cleanup: crate::blocking_pool::BlockingPool::new(
                                crate::BlockingOptions::default(),
                            ),
                            commands,
                            notification,
                            next_identifier: AtomicU64::new(0),
                            closed: AtomicBool::new(false),
                            shutdown: Arc::new(Shutdown {
                                finished: AtomicBool::new(false),
                                event: CompletionEvent::new(),
                            }),
                        }),
                    };
                    let file = crate::FileDescriptor::from(tempfile::tempfile().unwrap());
                    let cancellation = Cancellation::new();
                    drop(run_file_operation(
                        &selector,
                        &file,
                        operation,
                        &cancellation,
                    ));
                    assert!(incoming.try_recv().is_err());
                    assert_eq!(Arc::strong_count(&file.0), 1);
                    let mut sync = Box::pin(run_file_operation(
                        &selector,
                        &file,
                        operation,
                        &cancellation,
                    ));
                    assert!(futures_lite::future::poll_once(&mut sync).await.is_none());
                    let Command::Submit(request) = incoming.try_recv().unwrap() else {
                        panic!("expected sync submission")
                    };
                    let identifier = request.identifier();
                    cancellation.cancel();
                    assert!(futures_lite::future::poll_once(&mut sync).await.is_none());
                    assert!(
                        matches!(incoming.try_recv().unwrap(), Command::Cancel(value) if value == identifier)
                    );
                    assert_eq!(Arc::strong_count(&file.0), 2);
                    let completion = if complete {
                        Ok(0)
                    } else {
                        Err(io::Error::from_raw_os_error(libc::ECANCELED))
                    };
                    if abandon {
                        drop(sync);
                        assert_eq!(Arc::strong_count(&file.0), 2);
                        request.finish(completion);
                    } else {
                        request.finish(completion);
                        let result = sync.await;
                        if complete {
                            result.unwrap();
                        } else {
                            assert!(matches!(result, Err(crate::OperationError::Cancelled(_))));
                        }
                    }
                    assert_eq!(Arc::strong_count(&file.0), 1);
                }
            }
        }
    });
}

async fn run_file_operation(
    selector: &Selector,
    file: &FileDescriptor,
    operation: FileOperation,
    cancellation: &Cancellation,
) -> OperationResult<()> {
    match operation {
        FileOperation::Sync(mode) => selector.file_sync(file, mode, cancellation).await,
        FileOperation::Truncate(length) => selector.file_truncate(file, length, cancellation).await,
        FileOperation::Allocate {
            offset,
            length,
            mode,
        } => {
            selector
                .file_allocate(file, offset, length, mode, cancellation)
                .await
        }
    }
}

#[test]
fn truncate_capability_fallback_preserves_length_cursor_and_errors() {
    async_io::block_on(async {
        let selector = Selector::new().unwrap();
        let mut fallback = selector.clone();
        fallback.supports_file_truncate = false;
        for backend in [&selector, &fallback] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("truncate");
            let file = backend
                .file_open(
                    &path,
                    FileOptions {
                        read: true,
                        ..FileOptions::CREATE
                    },
                    &NEVER,
                )
                .await
                .unwrap();
            backend
                .file_seek(&file, SeekFrom::Start(3), &NEVER)
                .await
                .unwrap();
            backend.file_truncate(&file, 5, &NEVER).await.unwrap();
            assert_eq!(file.as_ref().metadata().unwrap().len(), 5);
            assert_eq!(
                backend
                    .file_seek(&file, SeekFrom::Current(0), &NEVER)
                    .await
                    .unwrap(),
                3
            );
            let (result, bytes) = backend.file_read(&file, vec![255; 2], &NEVER).await;
            assert_eq!(result.unwrap(), 2);
            assert_eq!(bytes, [0, 0]);
            backend.file_truncate(&file, 1, &NEVER).await.unwrap();
            assert_eq!(file.as_ref().metadata().unwrap().len(), 1);
            assert_eq!(
                backend
                    .file_seek(&file, SeekFrom::Current(0), &NEVER)
                    .await
                    .unwrap(),
                5
            );
            let cancelled = Cancellation::new();
            cancelled.cancel();
            assert!(matches!(
                backend.file_truncate(&file, 0, &cancelled).await,
                Err(crate::OperationError::Cancelled(_))
            ));
            assert_eq!(
                backend
                    .file_truncate(&file, u64::MAX, &NEVER)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
            let reader = backend
                .file_open(&path, FileOptions::READ, &NEVER)
                .await
                .unwrap();
            assert!(matches!(
                backend.file_truncate(&reader, 0, &NEVER).await,
                Err(crate::OperationError::Io(_))
            ));
            assert_eq!(file.as_ref().metadata().unwrap().len(), 1);
        }
        selector.shutdown().await;
    });
}

#[test]
fn truncate_preparation_is_inert_and_failed_admission_preserves_file() {
    async_io::block_on(async {
        let (mut request, _result) = sync_request(123);
        request.operation = FileOperation::Truncate(7);
        // Building a request is inert, including on kernels where the selector would choose the fallback.
        let entry = request.entry();
        assert_eq!(entry.get_user_data(), 123);
        assert_eq!(entry.get_opcode(), u32::from(opcode::Ftruncate::CODE));
        assert_eq!(request.file.as_ref().unwrap().metadata().unwrap().len(), 0);
        let (selector, _peer) = disconnected_selector();
        let file = crate::FileDescriptor::from(tempfile::tempfile().unwrap());
        assert_eq!(
            selector
                .file_truncate(&file, 13, &NEVER)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        selector
            .controller
            .next_identifier
            .store(CANCEL_TAG - 1, Ordering::Relaxed);
        assert!(
            selector
                .file_truncate(&file, 13, &NEVER)
                .await
                .unwrap_err()
                .to_string()
                .contains("identifiers exhausted")
        );
        assert_eq!(file.as_ref().metadata().unwrap().len(), 0);
    });
}

mod allocation_contract {
    use crate as socketry_executor;
    include!("../../../../examples/interface_probe/tests/file_allocate.rs");
}

#[test]
fn allocation_native_and_fallback_preserve_contract_and_reserve_blocks() {
    use std::os::unix::fs::MetadataExt;
    async_io::block_on(async {
        let selector = Selector::new().unwrap();
        // CI must exercise native allocation, rather than silently testing only the fallback.
        assert!(selector.supports_file_allocate);
        let mut fallback = selector.clone();
        fallback.supports_file_allocate = false;
        for backend in [&selector, &fallback] {
            let directory = tempfile::tempdir().unwrap();
            allocation_contract::check(backend, directory.path()).await;
            let file = crate::FileDescriptor::from(tempfile::tempfile().unwrap());
            let before = file.as_ref().metadata().unwrap().blocks();
            backend
                .file_allocate(&file, 4096, 128 * 1024, AllocationMode::KeepSize, &NEVER)
                .await
                .unwrap();
            assert_eq!(file.as_ref().metadata().unwrap().len(), 0);
            assert!(file.as_ref().metadata().unwrap().blocks() > before);
        }
        selector.shutdown().await;
        for backend in [&selector, &fallback] {
            let file = crate::FileDescriptor::from(tempfile::tempfile().unwrap());
            assert_eq!(
                backend
                    .file_allocate(&file, 0, 4096, AllocationMode::Extend, &NEVER)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(file.as_ref().metadata().unwrap().len(), 0);
        }
    });
}

#[test]
fn allocation_preparation_is_inert_and_failed_admission_preserves_file() {
    async_io::block_on(async {
        for mode in [AllocationMode::Extend, AllocationMode::KeepSize] {
            let (mut request, _result) = sync_request(123);
            request.operation = FileOperation::Allocate {
                offset: 3,
                length: 7,
                mode,
            };
            let entry = request.entry();
            assert_eq!(entry.get_user_data(), 123);
            assert_eq!(entry.get_opcode(), u32::from(opcode::Fallocate::CODE));
            assert_eq!(request.file.as_ref().unwrap().metadata().unwrap().len(), 0);
        }
        let (selector, _peer) = disconnected_selector();
        let file = crate::FileDescriptor::from(tempfile::tempfile().unwrap());
        assert_eq!(
            selector
                .file_allocate(&file, 0, 4096, AllocationMode::Extend, &NEVER)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        selector
            .controller
            .next_identifier
            .store(CANCEL_TAG - 1, Ordering::Relaxed);
        assert!(
            selector
                .file_allocate(&file, 0, 4096, AllocationMode::KeepSize, &NEVER)
                .await
                .unwrap_err()
                .to_string()
                .contains("identifiers exhausted")
        );
        assert_eq!(file.as_ref().metadata().unwrap().len(), 0);
    });
}

#[test]
fn advice_uses_retained_cleanup_and_rejects_closed_selector() {
    use crate::scheduler::file_advice_io::tests::{accepted, closed, saturated};
    futures_lite::future::block_on(async {
        for mode in 0..3 {
            let capacity = if mode == 2 { 1 } else { 2 };
            let mut selector = Selector::new().unwrap();
            // The reactor does not retain Controller; customize its initially idle cleanup pool for deterministic admission tests.
            Arc::get_mut(&mut selector.controller).unwrap().cleanup =
                crate::blocking_pool::BlockingPool::new(crate::BlockingOptions {
                    threads: 1,
                    capacity,
                });
            let pool = &selector.controller.cleanup;
            if mode < 2 {
                accepted(&selector, pool, mode == 1).await;
            } else {
                saturated(&selector, pool).await;
            }
            selector.shutdown().await;
            closed(&selector).await;
        }
    });
}
