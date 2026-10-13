// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

static NEVER: crate::Cancellation = crate::Cancellation::never();

use super::*;
use std::future::ready;
use std::io::Read;
use std::sync::mpsc;
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;

fn runtime() -> ::tokio::runtime::Runtime {
    ::tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn socket_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (server, _) = listener.accept().unwrap();
    (client, server)
}

fn expect_error<ResultType>(result: io::Result<ResultType>) -> io::Error {
    match result {
        Ok(_) => panic!("operation unexpectedly succeeded"),
        Err(error) => error,
    }
}

fn shared(runtime: &::tokio::runtime::Runtime) -> Arc<Shared> {
    Arc::new(Shared {
        runtime: runtime.handle().clone(),
        closed: AtomicBool::new(false),
        registry: Mutex::new(Registry {
            closed: false,
            next_identifier: 0,
            tasks: HashMap::new(),
        }),
        remaining: AtomicUsize::new(0),
        completion: Event::new(),
    })
}

fn task_state(cancelled: bool) -> Arc<TaskState> {
    Arc::new(TaskState {
        cancelled: AtomicBool::new(cancelled),
    })
}

#[test]
fn completion_removes_registration_and_notifies_scheduler() {
    let runtime = runtime();
    let shared = shared(&runtime);
    shared.remaining.store(1, Ordering::Release);
    let state = task_state(false);
    shared.registry.lock().unwrap().tasks.insert(
        7,
        Registration {
            state: Arc::clone(&state),
            abort: None,
        },
    );

    drop(Completion {
        scheduler: Arc::downgrade(&shared),
        identifier: 7,
        state,
    });

    assert!(shared.registry.lock().unwrap().tasks.is_empty());
    assert_eq!(shared.remaining.load(Ordering::Acquire), 0);
}

#[test]
fn completion_is_safe_after_scheduler_is_gone() {
    let runtime = runtime();
    let shared = shared(&runtime);
    shared.remaining.store(1, Ordering::Release);
    let completion = Completion {
        scheduler: Arc::downgrade(&shared),
        identifier: 7,
        state: task_state(false),
    };
    drop(shared);

    drop(completion);
}

#[test]
fn tracked_future_does_not_poll_after_cancellation() {
    let runtime = runtime();
    let shared = shared(&runtime);
    shared.remaining.store(1, Ordering::Release);
    let state = task_state(true);
    let mut context = Context::from_waker(Waker::noop());

    {
        let mut future = Box::pin(TrackedFuture {
            future: ready(42),
            completion: Completion {
                scheduler: Arc::downgrade(&shared),
                identifier: 0,
                state,
            },
        });
        assert!(matches!(
            future.as_mut().poll(&mut context),
            Poll::Ready(Err(TaskError::Cancelled))
        ));
    }
    assert_eq!(shared.remaining.load(Ordering::Acquire), 0);
}

#[test]
fn delayed_abort_respects_an_earlier_cancellation_request() {
    let runtime = runtime();
    let task = runtime.spawn(std::future::pending::<()>());
    abort_if_cancelled(false, || panic!("an active task should not be aborted"));
    abort_if_cancelled(true, || task.abort());

    assert!(runtime.block_on(task).is_err());
}

#[test]
fn poisoned_registry_locks_do_not_break_spawn_or_shutdown() {
    let runtime = runtime();
    let scheduler = Scheduler::new(runtime.handle().clone());
    let shared = Arc::clone(&scheduler.handle.shared);
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _registry = shared.registry.lock().unwrap();
        panic!("poison registry mutex");
    }));
    assert!(poisoned.is_err());

    let task = scheduler.spawn(async { 42 }).unwrap();
    assert_eq!(runtime.block_on(task).unwrap(), 42);
    runtime.block_on(scheduler.shutdown());
    assert!(shared.closed.load(Ordering::Acquire));
}

#[test]
fn poisoned_registry_is_recovered_when_a_task_attempts_self_shutdown() {
    let runtime = runtime();
    let scheduler = Scheduler::new(runtime.handle().clone());
    let shared = Arc::clone(&scheduler.handle.shared);
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _registry = shared.registry.lock().unwrap();
        panic!("poison registry mutex");
    }));
    assert!(poisoned.is_err());

    let task = scheduler
        .handle()
        .spawn(async move { scheduler.shutdown().await })
        .unwrap();
    assert!(matches!(
        runtime.block_on(task),
        Err(TaskError::Panicked(_))
    ));
}

#[test]
fn task_identifier_exhaustion_is_reported() {
    let runtime = runtime();
    let scheduler = Scheduler::new(runtime.handle().clone());
    scheduler
        .handle
        .shared
        .registry
        .lock()
        .unwrap()
        .next_identifier = u64::MAX;

    assert!(matches!(
        scheduler.spawn(async {}),
        Err(SpawnError::IdentifiersExhausted)
    ));
}

#[test]
fn shutdown_cancels_a_task_registered_before_tokio_spawn() {
    let runtime = runtime();
    let scheduler = Scheduler::new(runtime.handle().clone());
    let shared = Arc::clone(&scheduler.handle.shared);
    let (registered, wait_for_registration) = mpsc::sync_channel(0);
    let (resume, wait_to_resume) = mpsc::sync_channel(0);

    let spawning = {
        let shared = Arc::clone(&shared);
        thread::spawn(move || {
            shared.spawn_with_registration_hook(std::future::pending::<()>(), || {
                registered.send(()).unwrap();
                wait_to_resume.recv().unwrap();
            })
        })
    };

    wait_for_registration.recv().unwrap();
    {
        let registry = shared.registry.lock().unwrap();
        let registration = registry.tasks.values().next().unwrap();
        assert!(registration.abort.is_none());
        assert_eq!(shared.remaining.load(Ordering::Acquire), 1);
    }

    let mut shutdown = Box::pin(scheduler.shutdown());
    let mut context = Context::from_waker(Waker::noop());
    assert!(shutdown.as_mut().poll(&mut context).is_pending());

    assert!(shared.closed.load(Ordering::Acquire));
    assert!(
        shared
            .registry
            .lock()
            .unwrap()
            .tasks
            .values()
            .next()
            .unwrap()
            .state
            .cancelled
            .load(Ordering::Acquire)
    );

    resume.send(()).unwrap();
    let task = match spawning.join().unwrap() {
        Ok(task) => task,
        Err(error) => panic!("task registration failed: {error}"),
    };

    runtime.block_on(shutdown);
    assert!(matches!(runtime.block_on(task), Err(TaskError::Cancelled)));
    assert_eq!(shared.remaining.load(Ordering::Acquire), 0);
}

#[test]
fn socket_registration_preserves_configuration_and_conversion_errors() {
    let runtime = runtime();
    let (socket, _peer) = socket_pair();
    let error = expect_error(register_socket_with(
        runtime.handle(),
        socket,
        |_| Err(io::Error::other("socket mode failed")),
        |_| panic!("conversion should not run after configuration fails"),
    ));
    assert_eq!(error.to_string(), "socket mode failed");

    let (socket, _peer) = socket_pair();
    let error = expect_error(register_socket_with(
        runtime.handle(),
        socket,
        |_| Ok(()),
        |_| Err(io::Error::other("socket conversion failed")),
    ));
    assert_eq!(error.to_string(), "socket conversion failed");
}

#[test]
fn listener_registration_preserves_configuration_and_conversion_errors() {
    let runtime = runtime();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let error = expect_error(register_listener_with(
        runtime.handle(),
        listener,
        |_| Err(io::Error::other("listener mode failed")),
        |_| panic!("conversion should not run after configuration fails"),
    ));
    assert_eq!(error.to_string(), "listener mode failed");

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let error = expect_error(register_listener_with(
        runtime.handle(),
        listener,
        |_| Ok(()),
        |_| Err(io::Error::other("listener conversion failed")),
    ));
    assert_eq!(error.to_string(), "listener conversion failed");
}

#[test]
fn connection_and_accept_errors_are_propagated() {
    let runtime = runtime();
    let error = runtime.block_on(connect_with(
        runtime.handle().clone(),
        runtime.handle().id(),
        ready(Err(io::Error::other("connect failed"))),
    ));
    assert_eq!(expect_error(error).to_string(), "connect failed");

    let error = runtime.block_on(accept_with(
        runtime.handle().clone(),
        runtime.handle().id(),
        ready(Err(io::Error::other("accept failed"))),
    ));
    assert_eq!(expect_error(error).to_string(), "accept failed");
}

#[test]
fn poisoned_file_buffers_are_recovered_for_operations_and_return() {
    let buffer = Mutex::new(vec![1]);
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _buffer = buffer.lock().unwrap();
        panic!("poison file buffer");
    }));
    assert!(poisoned.is_err());

    assert_eq!(
        with_file_buffer(&buffer, |buffer| {
            buffer.push(2);
            Ok::<_, io::Error>(buffer.len())
        })
        .unwrap(),
        2
    );
    assert_eq!(take_file_buffer(&buffer), vec![1, 2]);
}

#[test]
fn read_retries_interrupted_and_would_block_operations() {
    let runtime = runtime();
    runtime.block_on(async {
        let mut attempts = 0;
        let mut readiness_waits = 0;
        let mut buffer = [0; 1];
        let result = read_with(
            &mut buffer,
            |buffer| {
                attempts += 1;
                match attempts {
                    1 => Err(io::ErrorKind::WouldBlock.into()),
                    2 => Err(io::ErrorKind::Interrupted.into()),
                    _ => {
                        buffer[0] = 42;
                        Ok(1)
                    }
                }
            },
            || {
                readiness_waits += 1;
                ready(Ok(()))
            },
        )
        .await;

        assert_eq!(result.unwrap(), 1);
        assert_eq!(buffer, [42]);
        assert_eq!(attempts, 3);
        assert_eq!(readiness_waits, 1);
    });
}

#[test]
fn read_returns_readiness_errors_and_non_retryable_errors() {
    let runtime = runtime();
    runtime.block_on(async {
        let mut buffer = [0; 1];
        let result = read_with(
            &mut buffer,
            |_| Err(io::ErrorKind::WouldBlock.into()),
            || ready(Err(io::Error::other("readiness failed"))),
        )
        .await;
        assert_eq!(result.unwrap_err().to_string(), "readiness failed");

        let result = read_with(
            &mut buffer,
            |_| Err(io::Error::other("read failed")),
            || ready(Ok(())),
        )
        .await;
        assert_eq!(result.unwrap_err().to_string(), "read failed");
    });
}

#[test]
fn write_retries_interrupted_and_would_block_operations() {
    let runtime = runtime();
    runtime.block_on(async {
        let mut attempts = 0;
        let mut readiness_waits = 0;
        let result = write_with(
            &[42],
            |_| {
                attempts += 1;
                match attempts {
                    1 => Err(io::ErrorKind::WouldBlock.into()),
                    2 => Err(io::ErrorKind::Interrupted.into()),
                    _ => Ok(1),
                }
            },
            || {
                readiness_waits += 1;
                ready(Ok(()))
            },
        )
        .await;

        assert_eq!(result.unwrap(), 1);
        assert_eq!(attempts, 3);
        assert_eq!(readiness_waits, 1);
    });
}

#[test]
fn socket_write_waits_until_the_send_buffer_is_writable() {
    let runtime = runtime();
    let scheduler = Scheduler::new(runtime.handle().clone());
    let handle = scheduler.handle();
    let (client, mut server) = socket_pair();
    server.set_nonblocking(true).unwrap();
    let socket = handle.register_socket(client).unwrap();
    let fill = vec![0; 64 * 1024];

    runtime.block_on(async {
        socket.inner.writable().await.unwrap();
        let mut written = 0;
        loop {
            match socket.inner.try_write(&fill) {
                Ok(0) => panic!("socket write made no progress"),
                Ok(count) => written += count,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("failed to fill socket send buffer: {error}"),
            }
        }
        assert!(written > 0);
    });

    let (start_reader, wait_for_start) = mpsc::sync_channel(0);
    let (finish_reader, wait_for_finish) = mpsc::sync_channel(0);
    let reader = thread::spawn(move || {
        wait_for_start.recv().unwrap();
        let mut buffer = [0; 64 * 1024];
        let mut total = 0;
        loop {
            match wait_for_finish.try_recv() {
                Ok(()) | Err(mpsc::TryRecvError::Disconnected) => break,
                Err(mpsc::TryRecvError::Empty) => {}
            }

            match server.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => total += count,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(error) => panic!("failed to drain socket receive buffer: {error}"),
            }
        }
        total
    });

    let mut write = Box::pin(handle.io_write(&socket, vec![42], &NEVER));
    runtime.block_on(async {
        std::future::poll_fn(|context| {
            assert!(write.as_mut().poll(context).is_pending());
            start_reader.send(()).unwrap();
            Poll::Ready(())
        })
        .await;

        let (result, buffer) = write.await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(buffer, [42]);
    });

    finish_reader.send(()).unwrap();
    assert!(reader.join().unwrap() > 0);
}

#[test]
fn write_returns_readiness_errors_and_non_retryable_errors() {
    let runtime = runtime();
    runtime.block_on(async {
        let result = write_with(
            &[42],
            |_| Err(io::ErrorKind::WouldBlock.into()),
            || ready(Err(io::Error::other("readiness failed"))),
        )
        .await;
        assert_eq!(result.unwrap_err().to_string(), "readiness failed");

        let result = write_with(
            &[42],
            |_| Err(io::Error::other("write failed")),
            || ready(Ok(())),
        )
        .await;
        assert_eq!(result.unwrap_err().to_string(), "write failed");
    });
}

#[test]
fn file_operation_results_preserve_errors_and_resume_panics() {
    let runtime = runtime();
    assert_eq!(
        file_operation_result::<_, io::Error>(Ok(Ok(42))).unwrap(),
        42
    );
    assert_eq!(
        file_operation_result::<usize, io::Error>(Ok(Err(io::Error::other("file failed"))))
            .unwrap_err()
            .to_string(),
        "file failed"
    );

    let cancelled = runtime.spawn(std::future::pending::<()>());
    cancelled.abort();
    let cancelled = runtime.block_on(cancelled).unwrap_err();
    assert_eq!(
        file_operation_result::<usize, io::Error>(Err(cancelled))
            .unwrap_err()
            .kind(),
        io::ErrorKind::Interrupted
    );

    let panicked = runtime.spawn(async { panic!("blocking task panicked") });
    let panicked = runtime.block_on(panicked).unwrap_err();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            file_operation_result::<usize, io::Error>(Err(panicked))
        }))
        .is_err()
    );
}

#[test]
fn polling_restores_the_previous_scheduler_context() {
    let runtime = runtime();
    let previous = shared(&runtime);
    let current = shared(&runtime);
    current.remaining.store(1, Ordering::Release);
    let _scope =
        SchedulerScope(CURRENT_SCHEDULER.with(|value| value.replace(Arc::downgrade(&previous))));
    let mut future = Box::pin(TrackedFuture {
        future: std::future::poll_fn(|_| {
            assert!(
                CURRENT_SCHEDULER
                    .with(|value| Weak::ptr_eq(&value.borrow(), &Arc::downgrade(&current)))
            );
            Poll::Ready(42)
        }),
        completion: Completion {
            scheduler: Arc::downgrade(&current),
            identifier: 0,
            state: task_state(false),
        },
    });
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Ok(42))
    ));
    assert!(
        CURRENT_SCHEDULER.with(|value| Weak::ptr_eq(&value.borrow(), &Arc::downgrade(&previous)))
    );
}

#[test]
fn shutdown_waits_for_all_task_destructors_even_without_result_handles() {
    struct CountDrop(Arc<AtomicUsize>);
    impl Drop for CountDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Release);
        }
    }
    let runtime = runtime();
    let scheduler = Scheduler::new(runtime.handle().clone());
    let dropped = Arc::new(AtomicUsize::new(0));
    for _ in 0..100 {
        let guard = CountDrop(Arc::clone(&dropped));
        drop(
            scheduler
                .spawn(async move {
                    let _guard = guard;
                    std::future::pending::<()>().await;
                })
                .unwrap(),
        );
    }
    runtime.block_on(scheduler.shutdown());
    assert_eq!(dropped.load(Ordering::Acquire), 100);
}

#[test]
fn a_task_can_shut_down_another_scheduler_on_the_same_runtime() {
    let runtime = runtime();
    let first = Scheduler::new(runtime.handle().clone());
    let second = Scheduler::new(runtime.handle().clone());
    let task = first.spawn(async move { second.shutdown().await }).unwrap();
    runtime.block_on(task).unwrap();
    runtime.block_on(first.shutdown());
}

#[test]
fn cancelling_queued_blocking_work_preserves_buffers_and_prevents_creation() {
    let runtime = ::tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    let scheduler = Scheduler::new(runtime.handle().clone());
    let handle = scheduler.handle();
    let (started, waiting) = std::sync::mpsc::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let blocker = runtime.spawn_blocking(move || {
        started.send(()).unwrap();
        blocked
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
    });
    waiting
        .recv_timeout(std::time::Duration::from_secs(10))
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cancelled-open");
    let file = Arc::new(tempfile::tempfile().unwrap());
    let cancellation = Cancellation::new();
    runtime.block_on(async {
        let buffer = vec![42; 4];
        let allocation = buffer.as_ptr();
        let mut write = Box::pin(handle.file_write_at(&file, buffer, 0, &cancellation));
        let mut open = Box::pin(handle.file_open(&path, FileOptions::CREATE, &cancellation));
        std::future::poll_fn(|context| {
            assert!(write.as_mut().poll(context).is_pending());
            assert!(open.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        cancellation.cancel();
        release.send(()).unwrap();
        let (result, buffer) = write.await;
        assert!(matches!(result, Err(crate::OperationError::Cancelled(_))));
        assert_eq!(buffer.as_ptr(), allocation);
        assert_eq!(buffer, vec![42; 4]);
        assert!(matches!(
            open.await,
            Err(crate::OperationError::Cancelled(_))
        ));
        blocker.await.unwrap();
    });
    assert!(!path.exists());
    assert_eq!(file.metadata().unwrap().len(), 0);
    runtime.block_on(scheduler.shutdown());
}
