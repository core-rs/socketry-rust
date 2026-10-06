#![cfg(any(feature = "native", feature = "tokio"))]

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

mod support;

use socketry_executor::{Clock, File, Interest, Socket, Spawn};
use std::fs::{File as StdFile, OpenOptions};
use std::future::{Future, poll_fn};
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Poll;

async fn within<ClockType: Clock, FutureType: Future>(
    clock: &ClockType,
    future: FutureType,
) -> FutureType::Output {
    let mut future = pin!(future);
    let mut timeout = pin!(clock.sleep(support::TIMEOUT));
    poll_fn(|context| {
        if let Poll::Ready(output) = future.as_mut().poll(context) {
            return Poll::Ready(output);
        }
        assert!(
            timeout.as_mut().poll(context).is_pending(),
            "I/O operation timed out"
        );
        Poll::Pending
    })
    .await
}

fn listening_socket() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    (listener, address)
}

fn socket_pair() -> (TcpStream, TcpStream) {
    let (listener, address) = listening_socket();
    let client = TcpStream::connect(address).unwrap();
    let (server, _) = listener.accept().unwrap();
    (client, server)
}

#[cfg(feature = "tokio")]
fn error_kind<ResultType>(result: io::Result<ResultType>) -> io::ErrorKind {
    match result {
        Ok(_) => panic!("operation unexpectedly succeeded"),
        Err(error) => error.kind(),
    }
}

async fn round_trip<SchedulerType>(scheduler: SchedulerType)
where
    SchedulerType: Socket + Spawn + Clock + Clone + 'static,
{
    let (listener, address) = listening_socket();
    let listener = scheduler.register_listener(listener).unwrap();
    let server = scheduler.clone();
    let server = scheduler
        .spawn(async move {
            let (socket, _) = server.accept(&listener).await.unwrap();
            let mut buffer = vec![0; 1];
            let allocation = buffer.as_ptr() as usize;
            for expected in 0..64u8 {
                let (result, returned) = server.io_read(&socket, buffer).await;
                buffer = returned;
                assert_eq!(result.unwrap(), 1);
                assert_eq!(buffer[0], expected);
                assert_eq!(buffer.as_ptr() as usize, allocation);
                let (result, returned) = server.io_write(&socket, buffer).await;
                buffer = returned;
                assert_eq!(result.unwrap(), 1);
                assert_eq!(buffer.as_ptr() as usize, allocation);
            }
        })
        .unwrap();

    let socket = scheduler.connect(address).await.unwrap();
    let mut buffer = vec![0; 1];
    let allocation = buffer.as_ptr() as usize;
    for value in 0..64u8 {
        buffer[0] = value;
        let (result, returned) = scheduler.io_write(&socket, buffer).await;
        buffer = returned;
        assert_eq!(result.unwrap(), 1);
        scheduler
            .io_wait(&socket, Interest::Readable)
            .await
            .unwrap();
        let (result, returned) = scheduler.io_read(&socket, buffer).await;
        buffer = returned;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(buffer[0], value);
        assert_eq!(buffer.as_ptr() as usize, allocation);
    }
    server.await.unwrap();
    let (result, _) = scheduler.io_read(&socket, vec![0; 1]).await;
    assert_eq!(result.unwrap(), 0, "peer closure must report EOF");
}

async fn cancel_pending_read<SchedulerType>(scheduler: &SchedulerType)
where
    SchedulerType: Socket + Clock,
{
    let (client, server) = socket_pair();
    let socket = scheduler.register_socket(client).unwrap();
    {
        let mut read = pin!(scheduler.io_read(&socket, vec![0; 1024]));
        poll_fn(|context| {
            assert!(read.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    // Cancellation must not prevent later use of the same registration.
    // Close the peer: both the abandoned read and any later read can finish.
    drop(server);
    let (result, buffer) = scheduler.io_read(&socket, vec![0; 1]).await;
    assert_eq!(result.unwrap(), 0);
    assert_eq!(buffer.len(), 1);
}

async fn concurrent_reads<SchedulerType>(scheduler: SchedulerType)
where
    SchedulerType: Socket + Spawn + Clone + 'static,
{
    let (client, mut server) = socket_pair();
    let socket = Arc::new(scheduler.register_socket(client).unwrap());
    let mut handles = Vec::new();
    for _ in 0..16 {
        let scheduler = scheduler.clone();
        let socket = Arc::clone(&socket);
        let handle = scheduler
            .clone()
            .spawn(async move {
                let (result, buffer) = scheduler.io_read(&socket, vec![0; 1]).await;
                assert_eq!(result.unwrap(), 1);
                buffer[0]
            })
            .unwrap();
        handles.push(handle);
    }
    // A dedicated writer supplies all bytes while readers race for readiness.
    let writer = std::thread::spawn(move || {
        use std::io::Write;
        server.write_all(&(0..16u8).collect::<Vec<_>>()).unwrap();
    });
    let mut values = Vec::new();
    for handle in handles {
        values.push(handle.await.unwrap());
    }
    values.sort_unstable();
    assert_eq!(values, (0..16u8).collect::<Vec<_>>());
    writer.join().unwrap();
}

struct TemporaryFile(std::path::PathBuf);
impl Drop for TemporaryFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

async fn positioned_files<SchedulerType: File>(scheduler: &SchedulerType) {
    static NEXT_FILE: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "socketry-io-{}-{}",
        std::process::id(),
        NEXT_FILE.fetch_add(1, Ordering::Relaxed)
    ));
    let _cleanup = TemporaryFile(path.clone());
    let file = Arc::new(
        OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap(),
    );
    let buffer = b"hello".to_vec();
    let allocation = buffer.as_ptr();
    let (result, buffer) = scheduler.file_write_at(Arc::clone(&file), buffer, 7).await;
    assert_eq!(result.unwrap(), 5);
    assert_eq!(buffer.as_ptr(), allocation);
    let (result, buffer) = scheduler.file_read_at(Arc::clone(&file), buffer, 7).await;
    assert_eq!(result.unwrap(), 5);
    assert_eq!(&buffer, b"hello");
    assert_eq!(buffer.as_ptr(), allocation);
    let (result, buffer) = scheduler.file_read_at(Arc::clone(&file), buffer, 100).await;
    assert_eq!(result.unwrap(), 0);
    let (result, buffer) = scheduler
        .file_read_at(Arc::clone(&file), buffer, u64::MAX)
        .await;
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
    assert_eq!(buffer.as_ptr(), allocation);
    let read_only = Arc::new(StdFile::open(&path).unwrap());
    let (result, returned) = scheduler.file_write_at(read_only, buffer, 0).await;
    assert!(result.is_err());
    assert_eq!(returned.as_ptr(), allocation);
}

#[cfg(feature = "native")]
mod native {
    use super::*;
    use socketry_executor::{Scheduler, TaskError};

    #[test]
    fn sockets_reuse_registration_and_buffers() {
        let scheduler = Scheduler::with_workers(2).unwrap();
        let handle = scheduler.handle();
        scheduler.block_on(within(&handle, round_trip(handle.clone())));
    }

    #[test]
    fn socket_read_can_be_abandoned() {
        let scheduler = Scheduler::with_workers(2).unwrap();
        let handle = scheduler.handle();
        scheduler.block_on(within(&handle, cancel_pending_read(&handle)));
    }

    #[test]
    fn multiple_readers_share_one_registration() {
        let scheduler = Scheduler::with_workers(4).unwrap();
        let handle = scheduler.handle();
        scheduler.block_on(within(&handle, concurrent_reads(handle.clone())));
    }

    #[test]
    fn files_use_owned_buffers_and_explicit_offsets() {
        let scheduler = Scheduler::with_workers(2).unwrap();
        let handle = scheduler.handle();
        scheduler.block_on(within(&handle, positioned_files(&handle)));
    }

    #[test]
    fn current_scheduler_supports_io() {
        let scheduler = Scheduler::with_workers(2).unwrap();
        let task = scheduler
            .spawn(async {
                let current = Scheduler::current().unwrap();
                round_trip(current).await;
            })
            .unwrap();
        scheduler
            .block_on(within(&scheduler.handle(), task))
            .unwrap();
    }

    #[test]
    fn shutdown_cancels_tasks_waiting_for_io() {
        let scheduler = Scheduler::with_workers(1).unwrap();
        let handle = scheduler.handle();
        let (client, _server) = socket_pair();
        let socket = handle.register_socket(client).unwrap();
        let (started, waiting) = std::sync::mpsc::channel();
        let task = scheduler
            .spawn(async move {
                let mut read = pin!(handle.io_read(&socket, vec![0; 1024]));
                let mut started = Some(started);
                poll_fn(|context| -> Poll<()> {
                    assert!(read.as_mut().poll(context).is_pending());
                    if let Some(started) = started.take() {
                        started.send(()).unwrap();
                    }
                    Poll::Pending
                })
                .await
            })
            .unwrap();
        support::receive(&waiting);
        scheduler.shutdown();
        let executor = Scheduler::with_workers(1).unwrap();
        assert!(matches!(executor.block_on(task), Err(TaskError::Cancelled)));
    }

    #[test]
    fn handles_reject_io_after_shutdown() {
        let scheduler = Scheduler::with_workers(1).unwrap();
        let handle = scheduler.handle();
        scheduler.shutdown();
        let (client, _server) = socket_pair();
        assert!(
            matches!(handle.register_socket(client), Err(error) if error.kind() == io::ErrorKind::BrokenPipe)
        );
    }

    #[test]
    fn scheduler_forwards_network_file_and_clock_operations() {
        let scheduler = Scheduler::with_workers(2).unwrap();
        let (listener, address) = listening_socket();
        let listener = scheduler.register_listener(listener).unwrap();
        assert_eq!(listener.local_addr().unwrap(), address);

        let (accepted, client) = std::thread::scope(|scope| {
            let connect = scope.spawn(|| scheduler.block_on(scheduler.connect(address)));
            let accepted = scheduler
                .block_on(scheduler.accept(&listener))
                .expect("accept connected socket");
            let client = connect.join().unwrap().expect("connect to listener");
            (accepted, client)
        });
        let (server, peer_address) = accepted;
        assert_eq!(client.peer_addr().unwrap(), address);
        assert_eq!(server.local_addr().unwrap(), address);
        assert_eq!(server.peer_addr().unwrap(), peer_address);

        scheduler.block_on(async {
            let (result, buffer) = scheduler.io_write(&client, vec![42]).await;
            assert_eq!(result.unwrap(), 1);
            assert_eq!(buffer, vec![42]);
            scheduler
                .io_wait(&server, Interest::Readable)
                .await
                .unwrap();
            let (result, buffer) = scheduler.io_read(&server, vec![0]).await;
            assert_eq!(result.unwrap(), 1);
            assert_eq!(buffer, vec![42]);
            scheduler
                .io_wait(&client, Interest::Writable)
                .await
                .unwrap();
            scheduler.sleep(std::time::Duration::ZERO).await;
        });

        let (registered_socket, _peer) = socket_pair();
        let registered_socket = scheduler.register_socket(registered_socket).unwrap();
        assert!(registered_socket.local_addr().is_ok());

        let handle = scheduler.handle();
        let selector = handle.selector().unwrap();
        scheduler.block_on(selector.sleep(std::time::Duration::ZERO));
        scheduler.block_on(within(&scheduler, positioned_files(&scheduler)));
    }

    #[test]
    fn closed_scheduler_io_returns_errors_and_owned_buffers() {
        let scheduler = Scheduler::with_workers(1).unwrap();
        let handle = scheduler.handle();
        let (client, _peer) = socket_pair();
        let socket = handle.register_socket(client).unwrap();
        let (raw_listener, address) = listening_socket();
        let registered_listener = handle.register_listener(raw_listener).unwrap();
        let file = Arc::new(StdFile::open(std::env::current_exe().unwrap()).unwrap());
        scheduler.shutdown();

        let (client, _peer) = socket_pair();
        assert!(matches!(
            handle.register_socket(client),
            Err(error) if error.kind() == io::ErrorKind::BrokenPipe
        ));
        let (raw_listener, _) = listening_socket();
        assert!(matches!(
            handle.register_listener(raw_listener),
            Err(error) if error.kind() == io::ErrorKind::BrokenPipe
        ));

        let executor = Scheduler::with_workers(1).unwrap();
        executor.block_on(async {
            let error = match handle.connect(address).await {
                Ok(_) => panic!("closed scheduler accepted a connection"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);

            let error = match handle.accept(&registered_listener).await {
                Ok(_) => panic!("closed scheduler accepted a listener"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(
                handle
                    .io_wait(&socket, Interest::Readable)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );

            let (result, buffer) = handle.io_read(&socket, vec![1, 2]).await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(buffer, vec![1, 2]);
            let (result, buffer) = handle.io_write(&socket, vec![3, 4]).await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(buffer, vec![3, 4]);
            let (result, buffer) = handle.file_read_at(Arc::clone(&file), vec![5], 0).await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(buffer, vec![5]);
            let (result, buffer) = handle.file_write_at(Arc::clone(&file), vec![6], 0).await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(buffer, vec![6]);
        });
    }
}

#[cfg(feature = "tokio")]
mod tokio_adapter {
    use super::*;
    use socketry_executor::scheduler::tokio::{Scheduler, SchedulerHandle};
    use socketry_executor::{SpawnError, TaskError};

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn the_same_network_program_runs_on_tokio() {
        let runtime = runtime();
        let scheduler = Scheduler::new(runtime.handle().clone());
        let handle = scheduler.handle();
        runtime.block_on(within(&handle, round_trip(handle.clone())));
        runtime.block_on(scheduler.shutdown());
    }

    #[test]
    fn socketry_workers_can_poll_io_from_a_driven_tokio_runtime() {
        let runtime = runtime();
        let scheduler = Scheduler::new(runtime.handle().clone());
        let executor = socketry_executor::Scheduler::with_workers(2).unwrap();
        let handle = scheduler.handle();
        let (listener, address) = listening_socket();
        let listener = handle.register_listener(listener).unwrap();

        let server_scheduler = handle.clone();
        let server = executor
            .spawn(async move {
                assert!(tokio::runtime::Handle::try_current().is_err());
                let (socket, _) = server_scheduler.accept(&listener).await.unwrap();
                let (result, _) = server_scheduler.io_write(&socket, vec![42]).await;
                assert_eq!(result.unwrap(), 1);
                assert!(tokio::runtime::Handle::try_current().is_err());
            })
            .unwrap();

        let client_scheduler = handle.clone();
        let client = executor
            .spawn(async move {
                assert!(tokio::runtime::Handle::try_current().is_err());
                let socket = client_scheduler.connect(address).await.unwrap();
                client_scheduler
                    .io_wait(&socket, Interest::Readable)
                    .await
                    .unwrap();
                let (result, buffer) = client_scheduler.io_read(&socket, vec![0; 1]).await;
                assert_eq!(result.unwrap(), 1);
                assert_eq!(buffer, vec![42]);
                client_scheduler.sleep(std::time::Duration::ZERO).await;
                assert!(tokio::runtime::Handle::try_current().is_err());
            })
            .unwrap();

        // The Tokio runtime drives events; Socketry workers poll every I/O
        // operation without an ambient Tokio context.
        runtime.block_on(within(&handle, async {
            server.await.unwrap();
            client.await.unwrap();
        }));
        runtime.block_on(scheduler.shutdown());
    }

    #[test]
    fn files_use_owned_buffers_and_explicit_offsets() {
        let runtime = runtime();
        let scheduler = Scheduler::new(runtime.handle().clone());
        let handle = scheduler.handle();
        runtime.block_on(within(&handle, positioned_files(&handle)));
    }

    #[test]
    fn socket_read_can_be_abandoned() {
        let runtime = runtime();
        let scheduler = Scheduler::new(runtime.handle().clone());
        let handle = scheduler.handle();
        runtime.block_on(within(&handle, cancel_pending_read(&handle)));
    }

    #[test]
    fn multiple_readers_share_one_registration() {
        let runtime = runtime();
        let scheduler = Scheduler::new(runtime.handle().clone());
        let handle = scheduler.handle();
        runtime.block_on(within(&handle, concurrent_reads(handle.clone())));
    }

    #[test]
    fn task_ownership_cancellation_and_panics() {
        let runtime = runtime();
        let scheduler = Scheduler::new(runtime.handle().clone());
        runtime.block_on(async {
            let barrier = scheduler.barrier();
            let completed = barrier.spawn(async { 42 }).unwrap();
            assert_eq!(completed.await.unwrap(), 42);
            let cancelled = barrier.spawn(std::future::pending::<()>()).unwrap();
            barrier.stop().await;
            assert!(matches!(cancelled.await, Err(TaskError::Cancelled)));
            assert!(barrier.is_empty());
            assert!(matches!(
                barrier.spawn(async {}),
                Err(SpawnError::OwnerClosed)
            ));
            let panicked = scheduler.spawn(async { panic!("task panic") }).unwrap();
            assert!(matches!(panicked.await, Err(TaskError::Panicked(_))));

            let pending = scheduler.spawn(std::future::pending::<()>()).unwrap();
            drop(pending);
        });
        let handle = scheduler.handle();
        runtime.block_on(scheduler.shutdown());
        assert!(matches!(
            handle.spawn(async {}),
            Err(SpawnError::SchedulerClosed)
        ));
    }

    #[test]
    fn task_handles_report_completion_and_cancel_tasks() {
        let runtime = runtime();
        let scheduler = Scheduler::new(runtime.handle().clone());
        let completed = scheduler.spawn(async { 42 }).unwrap();
        assert!(!completed.is_finished());
        runtime.block_on(async {
            tokio::task::yield_now().await;
            assert!(completed.is_finished());
            assert_eq!(completed.await.unwrap(), 42);
        });

        let cancelled = scheduler.spawn(std::future::pending::<()>()).unwrap();
        assert!(matches!(
            runtime.block_on(cancelled.cancel()),
            Err(TaskError::Cancelled)
        ));
        runtime.block_on(scheduler.shutdown());
    }

    #[test]
    fn scheduler_methods_and_spawn_traits_forward_to_tokio() {
        use socketry_executor::Spawn;

        let runtime = runtime();
        let scheduler = Arc::new(Scheduler::new(runtime.handle().clone()));
        let (listener, address) = listening_socket();
        let listener = scheduler.register_listener(listener).unwrap();
        assert_eq!(listener.local_addr().unwrap(), address);

        let server_scheduler = Arc::clone(&scheduler);
        let server = scheduler
            .spawn(async move {
                let (socket, _) = server_scheduler.accept(&listener).await.unwrap();
                assert_eq!(socket.local_addr().unwrap(), address);
                assert!(socket.peer_addr().is_ok());
                let (result, buffer) = server_scheduler.io_read(&socket, vec![0]).await;
                assert_eq!(result.unwrap(), 1);
                assert_eq!(buffer, vec![42]);
                let (result, _) = server_scheduler.io_write(&socket, vec![24]).await;
                assert_eq!(result.unwrap(), 1);
            })
            .unwrap();

        runtime.block_on(async {
            let client = scheduler.connect(address).await.unwrap();
            assert!(client.local_addr().is_ok());
            assert_eq!(client.peer_addr().unwrap(), address);
            let (result, buffer) = scheduler.io_write(&client, vec![42]).await;
            assert_eq!(result.unwrap(), 1);
            assert_eq!(buffer, vec![42]);
            scheduler
                .io_wait(&client, Interest::Readable)
                .await
                .unwrap();
            let (result, buffer) = scheduler.io_read(&client, vec![0]).await;
            assert_eq!(result.unwrap(), 1);
            assert_eq!(buffer, vec![24]);
            scheduler
                .io_wait(&client, Interest::Writable)
                .await
                .unwrap();
            scheduler.sleep(std::time::Duration::ZERO).await;

            let (result, buffer) = scheduler.io_read(&client, Vec::new()).await;
            assert_eq!(result.unwrap(), 0);
            assert!(buffer.is_empty());
            let (result, buffer) = scheduler.io_write(&client, Vec::new()).await;
            assert_eq!(result.unwrap(), 0);
            assert!(buffer.is_empty());

            let ordinary_task = Spawn::spawn(&*scheduler, async { 9 }).unwrap();
            assert_eq!(ordinary_task.await.unwrap(), 9);

            let barrier = scheduler.barrier();
            let child = Spawn::spawn(&barrier, async { 7 }).unwrap();
            barrier.close();
            assert!(matches!(
                barrier.spawn(async {}),
                Err(SpawnError::OwnerClosed)
            ));
            assert_eq!(child.await.unwrap(), 7);
            barrier.wait().await;
        });

        runtime.block_on(server).unwrap();
        runtime.block_on(within(&*scheduler, positioned_files(&*scheduler)));
        let scheduler = match Arc::try_unwrap(scheduler) {
            Ok(scheduler) => scheduler,
            Err(_) => panic!("scheduler still has outstanding references"),
        };
        runtime.block_on(scheduler.shutdown());
    }

    #[test]
    fn shutdown_waits_for_owned_tasks_and_rejects_self_shutdown() {
        let runtime = runtime();
        let scheduler = Scheduler::new(runtime.handle().clone());
        let handle = scheduler.handle();
        let task = handle
            .spawn(async move { scheduler.shutdown().await })
            .unwrap();
        assert!(matches!(
            runtime.block_on(task),
            Err(TaskError::Panicked(_))
        ));
    }

    #[test]
    fn closed_scheduler_returns_errors_and_preserves_buffers() {
        let runtime = runtime();
        let scheduler = Scheduler::new(runtime.handle().clone());
        let handle = scheduler.handle();
        let (client, _) = socket_pair();
        let socket = handle.register_socket(client).unwrap();
        let (listener, address) = listening_socket();
        let listener = handle.register_listener(listener).unwrap();
        let file = Arc::new(StdFile::open(std::env::current_exe().unwrap()).unwrap());
        runtime.block_on(scheduler.shutdown());

        let (new_socket, _) = socket_pair();
        assert_eq!(
            error_kind(handle.register_socket(new_socket)),
            io::ErrorKind::BrokenPipe
        );
        let (new_listener, _) = listening_socket();
        assert_eq!(
            error_kind(handle.register_listener(new_listener)),
            io::ErrorKind::BrokenPipe
        );

        runtime.block_on(async {
            assert_eq!(
                error_kind(handle.connect(address).await),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(
                error_kind(handle.accept(&listener).await),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(
                handle
                    .io_wait(&socket, Interest::Readable)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
            let (result, buffer) = handle.io_read(&socket, vec![1]).await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(buffer, vec![1]);
            let (result, buffer) = handle.io_write(&socket, vec![2]).await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(buffer, vec![2]);
            let (result, buffer) = handle.file_read_at(Arc::clone(&file), vec![3], 0).await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(buffer, vec![3]);
            let (result, buffer) = handle.file_write_at(Arc::clone(&file), vec![4], 0).await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(buffer, vec![4]);
        });
    }

    #[test]
    fn dropping_result_does_not_cancel_owner_task() {
        let runtime = runtime();
        let scheduler = Scheduler::new(runtime.handle().clone());
        let barrier = scheduler.barrier();
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task_completed = Arc::clone(&completed);
        drop(
            barrier
                .spawn(async move {
                    task_completed.store(true, Ordering::Release);
                })
                .unwrap(),
        );
        runtime.block_on(barrier.wait());
        assert!(completed.load(Ordering::Acquire));
    }

    #[test]
    fn barrier_rejects_a_child_joining_itself() {
        let runtime = runtime();
        let scheduler = Scheduler::new(runtime.handle().clone());
        let barrier = Arc::new(scheduler.barrier());
        let child_barrier = Arc::clone(&barrier);
        let child = barrier
            .spawn(async move { child_barrier.wait().await })
            .unwrap();
        assert!(matches!(
            runtime.block_on(child),
            Err(TaskError::Panicked(_))
        ));
    }

    #[test]
    fn spawning_on_stopped_runtime_does_not_deadlock_registry() {
        let runtime = runtime();
        let scheduler = Scheduler::new(runtime.handle().clone());
        drop(runtime);
        let task = scheduler.spawn(async { 42 }).unwrap();
        let waiting_runtime = self::runtime();
        assert!(matches!(
            waiting_runtime.block_on(task),
            Err(TaskError::Cancelled)
        ));
        waiting_runtime.block_on(scheduler.shutdown());
    }

    #[test]
    fn another_runtime_cannot_use_the_registration() {
        let first_runtime = runtime();
        let second_runtime = runtime();
        let first = Scheduler::new(first_runtime.handle().clone());
        let second = Scheduler::new(second_runtime.handle().clone());
        let (client, _server) = socket_pair();
        let socket = first.register_socket(client).unwrap();
        let (result, buffer) = second_runtime.block_on(second.io_read(&socket, vec![0; 7]));
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        assert_eq!(buffer.len(), 7);
    }

    #[test]
    fn handles_and_resources_are_send_and_sync() {
        fn assert_send_sync<Value: Send + Sync>() {}
        assert_send_sync::<SchedulerHandle>();
        assert_send_sync::<socketry_executor::scheduler::tokio::Socket>();
    }
}

#[cfg(all(feature = "io-uring", target_os = "linux"))]
#[test]
fn ring_drains_more_than_one_submission_batch_after_cancellation() {
    use socketry_executor::scheduler::selector::io_uring::Selector;
    let executor = socketry_executor::Scheduler::with_workers(2).unwrap();
    let selector = Selector::new().unwrap();
    executor.block_on(within(&executor.handle(), async {
        let (client, _server) = socket_pair();
        let socket = selector.register_socket(client).unwrap();
        let mut reads: Vec<_> = (0..1024)
            .map(|_| Box::pin(selector.io_read(&socket, vec![0; 16])))
            .collect();
        poll_fn(|context| {
            for read in &mut reads {
                assert!(read.as_mut().poll(context).is_pending());
            }
            Poll::Ready(())
        })
        .await;
        drop(reads);
        selector.shutdown().await;
    }));
}

#[cfg(all(feature = "io-uring", target_os = "linux"))]
#[test]
fn ring_shutdown_racing_submissions_returns_owned_buffers() {
    use socketry_executor::scheduler::selector::io_uring::Selector;
    let executor = socketry_executor::Scheduler::with_workers(4).unwrap();
    executor.block_on(within(&executor.handle(), async {
        for _ in 0..16 {
            let selector = Selector::new().unwrap();
            let (client, _server) = socket_pair();
            let socket = selector.register_socket(client).unwrap();
            let mut tasks = Vec::new();
            for _ in 0..64 {
                let selector = selector.clone();
                let socket = socket.clone();
                tasks.push(
                    executor
                        .spawn(async move {
                            let (result, buffer) = selector.io_read(&socket, vec![42; 32]).await;
                            assert!(result.is_err());
                            assert_eq!(buffer, vec![42; 32]);
                        })
                        .unwrap(),
                );
            }
            selector.shutdown().await;
            for task in tasks {
                task.await.unwrap();
            }
        }
    }));
}
