// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::{DefaultSelector, Scheduler, check_initialized_open, check_open};
use crate::scheduler::{Clock, File, Interest, Socket};
use std::fs::OpenOptions;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::Ordering;
use std::sync::{Arc, atomic::AtomicU64};
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
        let connect = scope.spawn(move || scheduler.block_on(handle.connect(address)));
        let (server, _) = scheduler.block_on(scheduler.accept(&listener)).unwrap();
        let client = connect.join().unwrap().unwrap();
        (client, server)
    });

    let (raw_listener, address) = listening_socket();
    let listener = handle.register_listener(raw_listener).unwrap();
    let (client, server) = std::thread::scope(|scope| {
        let scheduler = &scheduler;
        let connect = scope.spawn(move || scheduler.block_on(scheduler.connect(address)));
        let (server, _) = scheduler.block_on(handle.accept(&listener)).unwrap();
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
    let file = Arc::new(
        OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)
            .unwrap(),
    );

    scheduler.block_on(async {
        let (result, buffer) = handle.io_write(&client, vec![42]).await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(buffer, vec![42]);
        handle.io_wait(&server, Interest::Readable).await.unwrap();
        let (result, buffer) = scheduler.io_read(&server, vec![0]).await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(buffer, vec![42]);

        let (result, buffer) = scheduler.io_write(&client, vec![43]).await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(buffer, vec![43]);
        scheduler
            .io_wait(&server, Interest::Readable)
            .await
            .unwrap();
        let (result, buffer) = handle.io_read(&server, vec![0]).await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(buffer, vec![43]);

        let (result, buffer) = scheduler
            .file_write_at(Arc::clone(&file), b"data".to_vec(), 0)
            .await;
        assert_eq!(result.unwrap(), 4);
        assert_eq!(buffer, b"data".to_vec());
        let (result, buffer) = handle.file_read_at(Arc::clone(&file), vec![0; 4], 0).await;
        assert_eq!(result.unwrap(), 4);
        assert_eq!(buffer, b"data".to_vec());

        let (result, buffer) = handle
            .file_write_at(Arc::clone(&file), b"test".to_vec(), 4)
            .await;
        assert_eq!(result.unwrap(), 4);
        assert_eq!(buffer, b"test".to_vec());
        let (result, buffer) = scheduler
            .file_read_at(Arc::clone(&file), vec![0; 4], 4)
            .await;
        assert_eq!(result.unwrap(), 4);
        assert_eq!(buffer, b"test".to_vec());

        handle.sleep(Duration::ZERO).await;
        scheduler.sleep(Duration::ZERO).await;
    });
}
