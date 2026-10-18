// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{FileReadAt, FileWriteAt};
static NEVER: crate::Cancellation = crate::Cancellation::never();

use super::{DefaultSelector, Scheduler, check_initialized_open, check_open};
use crate::SocketRegistration;
use crate::scheduler::{Clock, Interest, Socket};
use crate::{OperationResult, SocketDomain, SocketProtocol, SocketType};
use std::fs::OpenOptions;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

fn listening_socket() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    (listener, address)
}

fn socket_pair() -> (TcpStream, TcpStream) {
    let (listener, address) = listening_socket();
    let client = TcpStream::connect(address).unwrap();
    let (server, _) = listener.accept().unwrap();
    (client, server)
}

async fn connect<S: Socket>(scheduler: &S, address: SocketAddr) -> OperationResult<S::Socket> {
    let socket = scheduler
        .socket_open(
            SocketDomain::from(address),
            SocketType::Stream,
            Some(SocketProtocol::Tcp),
            &NEVER,
        )
        .await?;
    scheduler.socket_connect(&socket, address, &NEVER).await?;
    Ok(socket)
}

struct Cleanup(std::path::PathBuf);

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[test]
fn selector_open_check_reports_scheduler_shutdown() {
    assert!(check_open(false).is_ok());
    assert_eq!(
        check_open(true).unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );

    let selector = DefaultSelector::new().unwrap();
    assert_eq!(
        check_initialized_open(true, &selector).unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[test]
fn selector_initialization_returns_the_selected_backend() {
    let scheduler = Scheduler::with_workers(1).unwrap();
    assert!(scheduler.handle().selector().is_ok());
}

#[test]
fn selector_rejects_a_scheduler_closed_before_initialization() {
    let scheduler = Scheduler::with_workers(1).unwrap();
    let handle = scheduler.handle();
    scheduler.shutdown();

    assert!(matches!(
        handle.selector(),
        Err(error) if error.kind() == io::ErrorKind::BrokenPipe
    ));
}

#[test]
fn selector_initialization_preserves_backend_errors() {
    let scheduler = Scheduler::with_workers(1).unwrap();
    let handle = scheduler.handle();

    let result = handle.selector_with(|| {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "injected selector initialization failure",
        ))
    });

    let error = result.err().expect("selector initialization should fail");
    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    assert_eq!(
        error.to_string(),
        "injected selector initialization failure"
    );
}

#[test]
fn selector_closes_if_shutdown_races_lazy_initialization() {
    let scheduler = Scheduler::with_workers(1).unwrap();
    let handle = scheduler.handle();
    let shared = std::sync::Arc::clone(&handle.shared);

    let result = handle.selector_with(|| {
        shared.closed.store(true, Ordering::Release);
        DefaultSelector::new()
    });

    assert!(matches!(
        result,
        Err(error) if error.kind() == io::ErrorKind::BrokenPipe
    ));
}

#[test]
fn scheduler_and_handle_forward_network_file_and_clock_operations() {
    static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

    let scheduler = Scheduler::with_workers(2).unwrap();
    let handle = scheduler.handle();

    let (raw_listener, address) = listening_socket();
    let listener = scheduler.register_listener(raw_listener).unwrap();
    let (_first_client, _first_server) = std::thread::scope(|scope| {
        let handle = handle.clone();
        let scheduler = &scheduler;
        let connect = scope.spawn(move || scheduler.block_on(connect(&handle, address)));
        let (server, _) = scheduler
            .block_on(scheduler.socket_accept(&listener, &NEVER))
            .unwrap();
        let client = connect.join().unwrap().unwrap();
        (client, server)
    });

    let (raw_listener, address) = listening_socket();
    let listener = handle.register_listener(raw_listener).unwrap();
    let (client, server) = std::thread::scope(|scope| {
        let scheduler = &scheduler;
        let connect = scope.spawn(move || scheduler.block_on(connect(scheduler, address)));
        let (server, _) = scheduler
            .block_on(handle.socket_accept(&listener, &NEVER))
            .unwrap();
        let client = connect.join().unwrap().unwrap();
        (client, server)
    });

    let (raw_socket, _peer) = socket_pair();
    let _socket = scheduler.register_socket(raw_socket).unwrap();
    let (raw_socket, _peer) = socket_pair();
    let _socket = handle.register_socket(raw_socket).unwrap();

    let path = std::env::temp_dir().join(format!(
        "socketry-operations-{}-{}",
        std::process::id(),
        NEXT_FILE.fetch_add(1, Ordering::Relaxed)
    ));
    let _cleanup = Cleanup(path.clone());
    let file = crate::FileDescriptor::from(
        OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)
            .unwrap(),
    );

    scheduler.block_on(async {
        let (result, buffer) = handle.socket_write(&client, vec![42], &NEVER).await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(buffer, vec![42]);
        handle
            .socket_wait(&server, Interest::Readable, &NEVER)
            .await
            .unwrap();
        let (result, buffer) = scheduler.socket_read(&server, vec![0], &NEVER).await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(buffer, vec![42]);

        let (result, buffer) = scheduler.socket_write(&client, vec![43], &NEVER).await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(buffer, vec![43]);
        scheduler
            .socket_wait(&server, Interest::Readable, &NEVER)
            .await
            .unwrap();
        let (result, buffer) = handle.socket_read(&server, vec![0], &NEVER).await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(buffer, vec![43]);

        let (result, buffer) = scheduler
            .file_write_at(&file, b"data".to_vec(), 0, &NEVER)
            .await;
        assert_eq!(result.unwrap(), 4);
        assert_eq!(buffer, b"data".to_vec());
        let (result, buffer) = handle.file_read_at(&file, vec![0; 4], 0, &NEVER).await;
        assert_eq!(result.unwrap(), 4);
        assert_eq!(buffer, b"data".to_vec());

        let (result, buffer) = handle
            .file_write_at(&file, b"test".to_vec(), 4, &NEVER)
            .await;
        assert_eq!(result.unwrap(), 4);
        assert_eq!(buffer, b"test".to_vec());
        let (result, buffer) = scheduler.file_read_at(&file, vec![0; 4], 4, &NEVER).await;
        assert_eq!(result.unwrap(), 4);
        assert_eq!(buffer, b"test".to_vec());

        handle.sleep(Duration::ZERO, &NEVER).await.unwrap();
        scheduler.sleep(Duration::ZERO, &NEVER).await.unwrap();
    });
}

#[test]
fn socket_cleanup_preserves_admitted_jobs_and_bounds_admission() {
    for mode in 0..6 {
        let capacity = if mode == 4 { 1 } else { 2 };
        let scheduler = Scheduler::with_workers_and_blocking(
            1,
            crate::BlockingOptions {
                threads: 1,
                capacity,
            },
        )
        .unwrap();
        let handle = scheduler.handle();
        scheduler.block_on(async {
            let pool = &handle.shared.cleanup;
            match mode {
                0..=3 => {
                    crate::scheduler::cleanup::tests::accepted_socket_jobs(&handle, pool, mode)
                        .await
                }
                4 => {
                    crate::scheduler::cleanup::tests::saturated_socket_admission(&handle, pool)
                        .await
                }
                _ => crate::scheduler::cleanup::tests::closed_socket_admission(&handle, pool).await,
            }
        });
    }
}

#[cfg(target_os = "linux")]
#[test]
fn advice_preserves_accepted_jobs_and_bounds_admission() {
    use crate::scheduler::file_advice_io::tests::{accepted, closed, saturated};
    use futures_lite::future::block_on;
    for mode in 0..3 {
        let capacity = if mode == 2 { 1 } else { 2 };
        let scheduler = Scheduler::with_workers_and_blocking(
            1,
            crate::BlockingOptions {
                threads: 1,
                capacity,
            },
        )
        .unwrap();
        let handle = scheduler.handle();
        block_on(async {
            let pool = &handle.shared.cleanup;
            if mode < 2 {
                accepted(&handle, pool, mode == 1).await;
            } else {
                saturated(&handle, pool).await;
            }
        });
        block_on(closed(&scheduler));
        scheduler.shutdown();
        block_on(closed(&handle));
    }
}
