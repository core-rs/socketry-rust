#![cfg(any(feature = "native", feature = "tokio"))]

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

mod support;

static NEVER: socketry_executor::Cancellation = socketry_executor::Cancellation::never();

use socketry_executor::{
    Clock, File, Interest, OperationResult, Socket, SocketDomain, SocketProtocol, SocketType, Spawn,
};
use socketry_executor::{SocketRegistration, SyncMode};
use std::fs::File as StdFile;
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
    let mut timeout = pin!(clock.sleep(support::TIMEOUT, &NEVER));
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

async fn connect<S: Socket>(
    scheduler: &S,
    address: SocketAddr,
    cancellation: &socketry_executor::Cancellation,
) -> OperationResult<S::Socket> {
    let socket = scheduler
        .socket_open(
            SocketDomain::from(address),
            SocketType::Stream,
            Some(SocketProtocol::Tcp),
            cancellation,
        )
        .await?;
    scheduler
        .socket_connect(&socket, address, cancellation)
        .await?;
    Ok(socket)
}

async fn socket_variants<S: Socket>(scheduler: &S) {
    let ipv4 = scheduler
        .socket_open(SocketDomain::Ipv4, SocketType::Stream, None, &NEVER)
        .await
        .unwrap();
    let ipv6 = scheduler
        .socket_open(SocketDomain::Ipv6, SocketType::Stream, None, &NEVER)
        .await
        .unwrap();
    scheduler
        .socket_bind(&ipv6, "[::1]:0".parse().unwrap(), &NEVER)
        .await
        .unwrap();
    assert!(scheduler.socket_local_addr(&ipv6).unwrap().is_ipv6());

    let datagram = scheduler
        .socket_open(
            SocketDomain::Ipv4,
            SocketType::Datagram,
            Some(SocketProtocol::Udp),
            &NEVER,
        )
        .await
        .unwrap();
    scheduler
        .socket_connect(&datagram, "127.0.0.1:9".parse().unwrap(), &NEVER)
        .await
        .unwrap();
    assert!(scheduler.socket_peer_addr(&datagram).is_ok());
    assert!(scheduler.socket_listen(&datagram, 1, &NEVER).await.is_err());
    assert!(
        scheduler
            .socket_connect(&ipv4, "[::1]:9".parse().unwrap(), &NEVER)
            .await
            .is_err()
    );
}

#[cfg(feature = "tokio")]
fn error_kind<ResultType, Error: Into<io::Error>>(
    result: Result<ResultType, Error>,
) -> io::ErrorKind {
    match result {
        Ok(_) => panic!("operation unexpectedly succeeded"),
        Err(error) => error.into().kind(),
    }
}

async fn round_trip<SchedulerType>(scheduler: SchedulerType)
where
    SchedulerType: Socket + SocketRegistration + Spawn + Clock + Clone + 'static,
{
    let (listener, address) = listening_socket();
    let listener = scheduler.register_listener(listener).unwrap();
    let server = scheduler.clone();
    let server = scheduler
        .spawn(async move {
            let (socket, _) = server.socket_accept(&listener, &NEVER).await.unwrap();
            let mut buffer = vec![0; 1];
            let allocation = buffer.as_ptr() as usize;
            for expected in 0..64u8 {
                let (result, returned) = server.socket_read(&socket, buffer, &NEVER).await;
                buffer = returned;
                assert_eq!(result.unwrap(), 1);
                assert_eq!(buffer[0], expected);
                assert_eq!(buffer.as_ptr() as usize, allocation);
                let (result, returned) = server.socket_write(&socket, buffer, &NEVER).await;
                buffer = returned;
                assert_eq!(result.unwrap(), 1);
                assert_eq!(buffer.as_ptr() as usize, allocation);
            }
        })
        .unwrap();

    let socket = connect(&scheduler, address, &NEVER).await.unwrap();
    let mut buffer = vec![0; 1];
    let allocation = buffer.as_ptr() as usize;
    for value in 0..64u8 {
        buffer[0] = value;
        let (result, returned) = scheduler.socket_write(&socket, buffer, &NEVER).await;
        buffer = returned;
        assert_eq!(result.unwrap(), 1);
        scheduler
            .socket_wait(&socket, Interest::Readable, &NEVER)
            .await
            .unwrap();
        let (result, returned) = scheduler.socket_read(&socket, buffer, &NEVER).await;
        buffer = returned;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(buffer[0], value);
        assert_eq!(buffer.as_ptr() as usize, allocation);
    }
    server.await.unwrap();
    let (result, _) = scheduler.socket_read(&socket, vec![0; 1], &NEVER).await;
    assert_eq!(result.unwrap(), 0, "peer closure must report EOF");
}

async fn cancel_pending_read<SchedulerType>(scheduler: &SchedulerType)
where
    SchedulerType: Socket + SocketRegistration + Clock,
{
    let (client, server) = socket_pair();
    let socket = scheduler.register_socket(client).unwrap();
    {
        let mut read = pin!(scheduler.socket_read(&socket, vec![0; 1024], &NEVER));
        poll_fn(|context| {
            assert!(read.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    // Cancellation must not prevent later use of the same registration.
    // Close the peer: both the abandoned read and any later read can finish.
    drop(server);
    let (result, buffer) = scheduler.socket_read(&socket, vec![0; 1], &NEVER).await;
    assert_eq!(result.unwrap(), 0);
    assert_eq!(buffer.len(), 1);
}

async fn concurrent_reads<SchedulerType>(scheduler: SchedulerType)
where
    SchedulerType: Socket + SocketRegistration + Spawn + Clone + 'static,
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
                let (result, buffer) = scheduler.socket_read(&socket, vec![0; 1], &NEVER).await;
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
    let cancellation = socketry_executor::Cancellation::never();
    let file = scheduler
        .file_open(
            &path,
            socketry_executor::FileOptions {
                read: true,
                write: true,
                create_new: true,
                ..Default::default()
            },
            &cancellation,
        )
        .await
        .unwrap();
    let buffer = b"hello".to_vec();
    let allocation = buffer.as_ptr();
    let (result, buffer) = scheduler.file_write_at(&file, buffer, 7, &NEVER).await;
    assert_eq!(result.unwrap(), 5);
    assert_eq!(buffer.as_ptr(), allocation);
    let (result, buffer) = scheduler.file_read_at(&file, buffer, 7, &NEVER).await;
    assert_eq!(result.unwrap(), 5);
    assert_eq!(&buffer, b"hello");
    assert_eq!(buffer.as_ptr(), allocation);
    let (result, buffer) = scheduler.file_read_at(&file, buffer, 100, &NEVER).await;
    assert_eq!(result.unwrap(), 0);
    let (result, buffer) = scheduler
        .file_read_at(&file, buffer, u64::MAX, &NEVER)
        .await;
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
    assert_eq!(buffer.as_ptr(), allocation);
    for write in [false, true] {
        let (result, empty) = if write {
            scheduler
                .file_write_at(&file, Vec::new(), u64::MAX, &NEVER)
                .await
        } else {
            scheduler
                .file_read_at(&file, Vec::new(), u64::MAX, &NEVER)
                .await
        };
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        assert!(empty.is_empty());
    }
    let read_only = scheduler
        .file_open(&path, socketry_executor::FileOptions::READ, &cancellation)
        .await
        .unwrap();
    let (result, returned) = scheduler.file_write_at(&read_only, buffer, 0, &NEVER).await;
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
        scheduler.block_on(socket_variants(&handle));

        let readiness = socketry_executor::scheduler::selector::readiness::Selector::new().unwrap();
        scheduler.block_on(socket_variants(&readiness));
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
                let mut read = pin!(handle.socket_read(&socket, vec![0; 1024], &NEVER));
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
            let connect = scope.spawn(|| scheduler.block_on(connect(&scheduler, address, &NEVER)));
            let accepted = scheduler
                .block_on(scheduler.socket_accept(&listener, &NEVER))
                .expect("accept connected socket");
            let client = connect.join().unwrap().expect("connect to listener");
            (accepted, client)
        });
        let (server, peer_address) = accepted;
        assert_eq!(client.peer_addr().unwrap(), address);
        assert_eq!(server.local_addr().unwrap(), address);
        assert_eq!(server.peer_addr().unwrap(), peer_address);

        scheduler.block_on(async {
            let (result, buffer) = scheduler.socket_write(&client, vec![42], &NEVER).await;
            assert_eq!(result.unwrap(), 1);
            assert_eq!(buffer, vec![42]);
            scheduler
                .socket_wait(&server, Interest::Readable, &NEVER)
                .await
                .unwrap();
            let (result, buffer) = scheduler.socket_read(&server, vec![0], &NEVER).await;
            assert_eq!(result.unwrap(), 1);
            assert_eq!(buffer, vec![42]);
            scheduler
                .socket_wait(&client, Interest::Writable, &NEVER)
                .await
                .unwrap();
            scheduler
                .sleep(std::time::Duration::ZERO, &NEVER)
                .await
                .unwrap();
        });

        let (registered_socket, _peer) = socket_pair();
        let registered_socket = scheduler.register_socket(registered_socket).unwrap();
        assert!(registered_socket.local_addr().is_ok());

        let handle = scheduler.handle();
        let selector = handle.selector().unwrap();
        scheduler
            .block_on(selector.sleep(std::time::Duration::ZERO, &NEVER))
            .unwrap();
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
        let file = socketry_executor::FileDescriptor::from(
            StdFile::open(std::env::current_exe().unwrap()).unwrap(),
        );
        scheduler.shutdown();
        futures_lite::future::block_on(async {
            let close_file = socketry_executor::FileDescriptor::from(tempfile::tempfile().unwrap());
            assert_eq!(
                handle.file_close(close_file).await.unwrap_err().kind(),
                io::ErrorKind::BrokenPipe
            );
            let close_file = socketry_executor::FileDescriptor::from(tempfile::tempfile().unwrap());
            assert_eq!(
                handle
                    .file_close_submit(close_file)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
        });

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

        assert_eq!(
            handle
                .socket_local_addr(&registered_listener)
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            handle.socket_local_addr(&socket).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            handle.socket_peer_addr(&socket).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        let executor = Scheduler::with_workers(1).unwrap();
        executor.block_on(async {
            assert_eq!(
                handle
                    .socket_open(
                        SocketDomain::from(address),
                        SocketType::Stream,
                        Some(SocketProtocol::Tcp),
                        &NEVER,
                    )
                    .await
                    .err()
                    .unwrap()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(
                handle
                    .file_open(
                        std::path::Path::new("closed"),
                        socketry_executor::FileOptions::READ,
                        &NEVER
                    )
                    .await
                    .err()
                    .unwrap()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
            let error = match connect(&handle, address, &NEVER).await {
                Ok(_) => panic!("closed scheduler accepted a connection"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);

            assert_eq!(
                handle
                    .socket_bind(&socket, address, &NEVER)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(
                handle
                    .socket_listen(&registered_listener, 128, &NEVER)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(
                handle
                    .socket_connect(&socket, address, &NEVER)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );

            let error = match handle.socket_accept(&registered_listener, &NEVER).await {
                Ok(_) => panic!("closed scheduler accepted a listener"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(
                handle
                    .socket_wait(&socket, Interest::Readable, &NEVER)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );

            let (result, buffer) = handle.socket_read(&socket, vec![1, 2], &NEVER).await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(buffer, vec![1, 2]);
            let (result, buffer) = handle.socket_write(&socket, vec![3, 4], &NEVER).await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(buffer, vec![3, 4]);
            let (result, buffer) = handle.file_read_at(&file, vec![5], 0, &NEVER).await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(buffer, vec![5]);
            let (result, buffer) = handle.file_write_at(&file, vec![6], 0, &NEVER).await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(buffer, vec![6]);

            assert_eq!(
                handle
                    .file_seek(&file, std::io::SeekFrom::Start(1), &NEVER)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );

            let resize_file =
                socketry_executor::FileDescriptor::from(tempfile::tempfile().unwrap());
            assert_eq!(
                handle
                    .file_truncate(&resize_file, 13, &NEVER)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(resize_file.as_ref().metadata().unwrap().len(), 0);
            #[cfg(target_os = "linux")]
            {
                use socketry_executor::{AllocationMode, FileAllocate};
                assert_eq!(
                    handle
                        .file_allocate(&resize_file, 0, 4096, AllocationMode::Extend, &NEVER)
                        .await
                        .unwrap_err()
                        .kind(),
                    io::ErrorKind::BrokenPipe
                );
                assert_eq!(resize_file.as_ref().metadata().unwrap().len(), 0);
            }

            for mode in [SyncMode::All, SyncMode::Data] {
                assert_eq!(
                    handle
                        .file_sync(&file, mode, &NEVER)
                        .await
                        .unwrap_err()
                        .kind(),
                    io::ErrorKind::BrokenPipe
                );
            }

            for write in [false, true] {
                let buffer = vec![11];
                let allocation = buffer.as_ptr();
                let (result, buffer) = if write {
                    handle.file_write(&file, buffer, &NEVER).await
                } else {
                    handle.file_read(&file, buffer, &NEVER).await
                };
                assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
                assert_eq!(buffer, [11]);
                assert_eq!(buffer.as_ptr(), allocation);
            }
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
        runtime.block_on(socket_variants(&handle));
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
                let (socket, _) = server_scheduler
                    .socket_accept(&listener, &NEVER)
                    .await
                    .unwrap();
                let (result, _) = server_scheduler
                    .socket_write(&socket, vec![42], &NEVER)
                    .await;
                assert_eq!(result.unwrap(), 1);
                assert!(tokio::runtime::Handle::try_current().is_err());
            })
            .unwrap();

        let client_scheduler = handle.clone();
        let client = executor
            .spawn(async move {
                assert!(tokio::runtime::Handle::try_current().is_err());
                let socket = connect(&client_scheduler, address, &NEVER).await.unwrap();
                client_scheduler
                    .socket_wait(&socket, Interest::Readable, &NEVER)
                    .await
                    .unwrap();
                let (result, buffer) = client_scheduler
                    .socket_read(&socket, vec![0; 1], &NEVER)
                    .await;
                assert_eq!(result.unwrap(), 1);
                assert_eq!(buffer, vec![42]);
                client_scheduler
                    .sleep(std::time::Duration::ZERO, &NEVER)
                    .await
                    .unwrap();
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
                let (socket, _) = server_scheduler
                    .socket_accept(&listener, &NEVER)
                    .await
                    .unwrap();
                assert_eq!(socket.local_addr().unwrap(), address);
                assert!(socket.peer_addr().is_ok());
                let (result, buffer) = server_scheduler.socket_read(&socket, vec![0], &NEVER).await;
                assert_eq!(result.unwrap(), 1);
                assert_eq!(buffer, vec![42]);
                let (result, _) = server_scheduler
                    .socket_write(&socket, vec![24], &NEVER)
                    .await;
                assert_eq!(result.unwrap(), 1);
                let (result, _) = server_scheduler
                    .socket_write(&socket, vec![99], &NEVER)
                    .await;
                assert_eq!(result.unwrap(), 1);
            })
            .unwrap();

        runtime.block_on(async {
            let client = connect(scheduler.as_ref(), address, &NEVER).await.unwrap();
            assert!(client.local_addr().is_ok());
            assert_eq!(client.peer_addr().unwrap(), address);
            let (result, buffer) = scheduler.socket_write(&client, vec![42], &NEVER).await;
            assert_eq!(result.unwrap(), 1);
            assert_eq!(buffer, vec![42]);
            scheduler
                .socket_wait(&client, Interest::Readable, &NEVER)
                .await
                .unwrap();
            let (result, buffer) = scheduler.socket_read(&client, vec![0], &NEVER).await;
            assert_eq!(result.unwrap(), 1);
            assert_eq!(buffer, vec![24]);
            scheduler
                .socket_wait(&client, Interest::Readable, &NEVER)
                .await
                .unwrap();

            let (result, buffer) = scheduler.socket_read(&client, Vec::new(), &NEVER).await;
            #[cfg(windows)]
            assert!(result.is_err());
            #[cfg(not(windows))]
            assert_eq!(result.unwrap(), 0);
            assert!(buffer.is_empty());
            let (result, buffer) = scheduler.socket_read(&client, vec![0], &NEVER).await;
            assert_eq!(result.unwrap(), 1);
            assert_eq!(buffer, vec![99]);
            let (result, buffer) = scheduler.socket_write(&client, Vec::new(), &NEVER).await;
            assert_eq!(result.unwrap(), 0);
            assert!(buffer.is_empty());

            let ordinary_task = Spawn::spawn(&*scheduler, async { 9 }).unwrap();
            assert_eq!(ordinary_task.await.unwrap(), 9);
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
        let file = socketry_executor::FileDescriptor::from(
            StdFile::open(std::env::current_exe().unwrap()).unwrap(),
        );
        runtime.block_on(scheduler.shutdown());
        runtime.block_on(async {
            assert_eq!(
                handle
                    .file_close(tempfile::tempfile().unwrap().into())
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(
                handle
                    .file_close_submit(tempfile::tempfile().unwrap().into())
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
        });

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

        assert_eq!(
            handle.socket_local_addr(&listener).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            handle.socket_local_addr(&socket).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            handle.socket_peer_addr(&socket).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        runtime.block_on(async {
            assert_eq!(
                handle
                    .socket_open(
                        SocketDomain::from(address),
                        SocketType::Stream,
                        Some(SocketProtocol::Tcp),
                        &NEVER,
                    )
                    .await
                    .err()
                    .unwrap()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(
                handle
                    .file_open(
                        std::path::Path::new("closed"),
                        socketry_executor::FileOptions::READ,
                        &NEVER
                    )
                    .await
                    .err()
                    .unwrap()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(
                error_kind(connect(&handle, address, &NEVER).await),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(
                error_kind(handle.socket_bind(&socket, address, &NEVER).await),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(
                error_kind(handle.socket_listen(&listener, 128, &NEVER).await),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(
                error_kind(handle.socket_connect(&socket, address, &NEVER).await),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(
                error_kind(handle.socket_accept(&listener, &NEVER).await),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(
                handle
                    .socket_wait(&socket, Interest::Readable, &NEVER)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
            let (result, buffer) = handle.socket_read(&socket, vec![1], &NEVER).await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(buffer, vec![1]);
            let (result, buffer) = handle.socket_write(&socket, vec![2], &NEVER).await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(buffer, vec![2]);
            let (result, buffer) = handle.file_read_at(&file, vec![3], 0, &NEVER).await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(buffer, vec![3]);
            let (result, buffer) = handle.file_write_at(&file, vec![4], 0, &NEVER).await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(buffer, vec![4]);

            assert_eq!(
                handle
                    .file_seek(&file, std::io::SeekFrom::Start(1), &NEVER)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );

            let resize_file =
                socketry_executor::FileDescriptor::from(tempfile::tempfile().unwrap());
            assert_eq!(
                handle
                    .file_truncate(&resize_file, 13, &NEVER)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(resize_file.as_ref().metadata().unwrap().len(), 0);
            #[cfg(target_os = "linux")]
            {
                use socketry_executor::{AllocationMode, FileAllocate};
                assert_eq!(
                    handle
                        .file_allocate(&resize_file, 0, 4096, AllocationMode::Extend, &NEVER)
                        .await
                        .unwrap_err()
                        .kind(),
                    io::ErrorKind::BrokenPipe
                );
                assert_eq!(resize_file.as_ref().metadata().unwrap().len(), 0);
            }

            for mode in [SyncMode::All, SyncMode::Data] {
                assert_eq!(
                    handle
                        .file_sync(&file, mode, &NEVER)
                        .await
                        .unwrap_err()
                        .kind(),
                    io::ErrorKind::BrokenPipe
                );
            }

            for write in [false, true] {
                let buffer = vec![11];
                let allocation = buffer.as_ptr();
                let (result, buffer) = if write {
                    handle.file_write(&file, buffer, &NEVER).await
                } else {
                    handle.file_read(&file, buffer, &NEVER).await
                };
                assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
                assert_eq!(buffer, [11]);
                assert_eq!(buffer.as_ptr(), allocation);
            }
        });
    }

    #[test]
    fn dropping_handle_abandons_tokio_task() {
        let runtime = runtime();
        let scheduler = Scheduler::new(runtime.handle().clone());
        let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let guard = support::CountDrop(dropped.clone());
        let task = scheduler
            .spawn(async move {
                let _guard = guard;
                std::future::pending::<()>().await;
            })
            .unwrap();
        drop(task);
        runtime.block_on(scheduler.shutdown());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
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
        let (raw_listener, _) = listening_socket();
        let listener = first.register_listener(raw_listener).unwrap();
        second_runtime.block_on(async {
            let (result, buffer) = second.socket_read(&socket, vec![0; 7], &NEVER).await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
            assert_eq!(buffer.len(), 7);
            let (result, buffer) = second.socket_write(&socket, vec![1; 7], &NEVER).await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
            assert_eq!(buffer, vec![1; 7]);
            assert_eq!(
                second
                    .socket_bind(&socket, "127.0.0.1:0".parse().unwrap(), &NEVER)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
            assert_eq!(
                second
                    .socket_listen(&listener, 128, &NEVER)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
            assert_eq!(
                error_kind(
                    second
                        .socket_connect(&socket, "127.0.0.1:9".parse().unwrap(), &NEVER)
                        .await
                ),
                io::ErrorKind::InvalidInput
            );
            assert_eq!(
                error_kind(second.socket_accept(&listener, &NEVER).await),
                io::ErrorKind::InvalidInput
            );
            assert_eq!(
                second
                    .socket_wait(&socket, Interest::Readable, &NEVER)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
            assert_eq!(
                second.socket_local_addr(&socket).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
            assert_eq!(
                second.socket_peer_addr(&socket).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        });
    }

    #[test]
    fn handles_and_resources_are_send_and_sync() {
        fn assert_send_sync<Value: Send + Sync>() {}
        assert_send_sync::<SchedulerHandle>();
        assert_send_sync::<socketry_executor::scheduler::tokio::SocketDescriptor>();
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
            .map(|_| Box::pin(selector.socket_read(&socket, vec![0; 16], &NEVER)))
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
            let socket = Arc::new(selector.register_socket(client).unwrap());
            let mut tasks = Vec::new();
            for _ in 0..64 {
                let selector = selector.clone();
                let socket = Arc::clone(&socket);
                tasks.push(
                    executor
                        .spawn(async move {
                            let (result, buffer) =
                                selector.socket_read(&socket, vec![42; 32], &NEVER).await;
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

#[cfg(unix)]
async fn sync_errors<S: File<File = socketry_executor::FileDescriptor>>(scheduler: &S) {
    use std::os::{fd::OwnedFd, unix::net::UnixStream};
    let (socket, _peer) = UnixStream::pair().unwrap();
    let file = socketry_executor::FileDescriptor::from(StdFile::from(OwnedFd::from(socket)));
    for mode in [SyncMode::All, SyncMode::Data] {
        let expected = match mode {
            SyncMode::All => file.as_ref().sync_all(),
            SyncMode::Data => file.as_ref().sync_data(),
        }
        .unwrap_err();
        let actual = scheduler.file_sync(&file, mode, &NEVER).await.unwrap_err();
        assert!(matches!(actual, socketry_executor::OperationError::Io(_)));
        assert_eq!(actual.kind(), expected.kind());
        assert_eq!(actual.raw_os_error(), expected.raw_os_error());
    }
}

#[cfg(all(unix, feature = "native"))]
#[test]
fn native_file_sync_preserves_os_errors() {
    let scheduler = socketry_executor::Scheduler::with_workers(2).unwrap();
    scheduler.block_on(sync_errors(&scheduler));
    scheduler.block_on(sync_errors(
        &socketry_executor::scheduler::selector::readiness::Selector,
    ));
}

#[cfg(all(unix, feature = "tokio"))]
#[test]
fn tokio_file_sync_preserves_os_errors() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let scheduler = socketry_executor::scheduler::tokio::Scheduler::new(runtime.handle().clone());
    runtime.block_on(sync_errors(&scheduler));
    runtime.block_on(scheduler.shutdown());
}
