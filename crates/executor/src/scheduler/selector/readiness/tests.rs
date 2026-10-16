// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

static NEVER: crate::Cancellation = crate::Cancellation::never();

use super::{wrap_accepted, wrap_listener, wrap_socket};
use std::io;

fn injected_error<T>(result: io::Result<T>) {
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("injected error was not preserved"),
    };

    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert_eq!(error.to_string(), "injected selector failure");
}

fn selector_error<T>() -> io::Result<T> {
    Err(io::Error::other("injected selector failure"))
}

#[test]
fn registration_and_network_errors_are_preserved() {
    injected_error(wrap_socket(selector_error()));
    injected_error(wrap_listener(selector_error()));
    injected_error(wrap_accepted(selector_error()));
}

#[test]
fn readiness_operations_remain_available_with_other_backends_selected() {
    use super::*;
    async_io::block_on(async {
        let selector = Selector::new().unwrap();
        let listener = selector
            .socket_listen("127.0.0.1:0".parse().unwrap(), &NEVER)
            .await
            .unwrap();
        let address = selector.listener_addr(&listener).unwrap();
        assert_eq!(
            selector
                .socket_listen(address, &NEVER)
                .await
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::AddrInUse
        );
        let socket = selector.connect(address, &NEVER).await.unwrap();
        let (peer, peer_address) = selector.accept(&listener, &NEVER).await.unwrap();
        assert_eq!(selector.socket_local_addr(&socket).unwrap(), peer_address);
        assert_eq!(selector.socket_peer_addr(&socket).unwrap(), address);
        let mut written = 0;
        while written < 5 {
            let (result, buffer) = selector
                .io_write(&socket, b"hello"[written..].to_vec(), &NEVER)
                .await;
            let count = result.unwrap();
            assert!(count > 0);
            assert_eq!(buffer, &b"hello"[written..]);
            written += count;
        }
        let mut received = Vec::new();
        while received.len() < 5 {
            let (result, buffer) = selector
                .io_read(&peer, vec![0; 5 - received.len()], &NEVER)
                .await;
            let count = result.unwrap();
            assert!(count > 0);
            received.extend_from_slice(&buffer[..count]);
        }
        assert_eq!(received, b"hello");
        let file = Arc::new(tempfile::tempfile().unwrap());
        let (result, buffer) = selector
            .file_write_at(&Arc::clone(&file), b"file".to_vec(), 3, &NEVER)
            .await;
        assert_eq!(result.unwrap(), 4);
        assert_eq!(buffer, b"file");
        let (result, buffer) = selector.file_read_at(&file, vec![0; 4], 3, &NEVER).await;
        assert_eq!(result.unwrap(), 4);
        assert_eq!(buffer, b"file");
    });
}

#[test]
fn direct_readiness_cancellation_returns_buffers_before_submitting_work() {
    use super::*;
    async_io::block_on(async {
        let cancellation = Cancellation::new();
        cancellation.cancel();
        let selector = Selector;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cancelled");
        assert!(matches!(
            selector
                .file_open(&path, FileOptions::CREATE, &cancellation)
                .await,
            Err(crate::OperationError::Cancelled(_))
        ));
        let file = Arc::new(tempfile::tempfile().unwrap());
        let buffer = vec![1; 4];
        let allocation = buffer.as_ptr();
        let (result, buffer) = selector
            .file_write_at(&file, buffer, 0, &cancellation)
            .await;
        assert!(matches!(result, Err(crate::OperationError::Cancelled(_))));
        let (result, buffer) = selector.file_read_at(&file, buffer, 0, &cancellation).await;
        assert!(matches!(result, Err(crate::OperationError::Cancelled(_))));
        assert_eq!(buffer.as_ptr(), allocation);
        assert!(!path.exists());
    });
}
