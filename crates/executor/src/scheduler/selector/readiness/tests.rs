// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

static NEVER: crate::Cancellation = crate::Cancellation::never();

#[test]
fn readiness_operations_remain_available_with_other_backends_selected() {
    use super::*;
    async_io::block_on(async {
        let selector = Selector::new().unwrap();
        let listener = selector
            .socket_open(
                crate::SocketDomain::Ipv4,
                crate::SocketType::Stream,
                Some(crate::SocketProtocol::Tcp),
                &NEVER,
            )
            .await
            .unwrap();
        selector
            .socket_bind(&listener, "127.0.0.1:0".parse().unwrap(), &NEVER)
            .await
            .unwrap();
        selector
            .socket_listen(&listener, 128, &NEVER)
            .await
            .unwrap();
        let address = selector.socket_local_address(&listener).unwrap();
        let duplicate = selector
            .socket_open(
                crate::SocketDomain::Ipv4,
                crate::SocketType::Stream,
                Some(crate::SocketProtocol::Tcp),
                &NEVER,
            )
            .await
            .unwrap();
        assert_eq!(
            selector
                .socket_bind(&duplicate, address, &NEVER)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::AddrInUse
        );
        let socket = selector
            .socket_open(
                crate::SocketDomain::Ipv4,
                crate::SocketType::Stream,
                Some(crate::SocketProtocol::Tcp),
                &NEVER,
            )
            .await
            .unwrap();
        selector
            .socket_connect(&socket, address, &NEVER)
            .await
            .unwrap();
        let (peer, peer_address) = selector.socket_accept(&listener, &NEVER).await.unwrap();
        assert_eq!(
            selector.socket_local_address(&socket).unwrap(),
            peer_address
        );
        assert_eq!(selector.socket_peer_address(&socket).unwrap(), address);
        let mut written = 0;
        while written < 5 {
            let (result, buffer) = selector
                .socket_write(&socket, b"hello"[written..].to_vec(), &NEVER)
                .await;
            let count = result.unwrap();
            assert!(count > 0);
            assert_eq!(buffer, &b"hello"[written..]);
            written += count;
        }
        let mut received = Vec::new();
        while received.len() < 5 {
            let (result, buffer) = selector
                .socket_read(&peer, vec![0; 5 - received.len()], &NEVER)
                .await;
            let count = result.unwrap();
            assert!(count > 0);
            received.extend_from_slice(&buffer[..count]);
        }
        assert_eq!(received, b"hello");
        let (result, _) = selector.socket_write(&peer, vec![99], &NEVER).await;
        assert_eq!(result.unwrap(), 1);
        selector
            .socket_wait(&socket, Interest::Readable, &NEVER)
            .await
            .unwrap();
        let (result, buffer) = selector.socket_read(&socket, Vec::new(), &NEVER).await;
        assert_eq!(result.unwrap(), 0);
        assert!(buffer.is_empty());
        let (result, buffer) = selector.socket_read(&socket, vec![0], &NEVER).await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(buffer, vec![99]);
        let (result, buffer) = selector.socket_write(&socket, Vec::new(), &NEVER).await;
        assert_eq!(result.unwrap(), 0);
        assert!(buffer.is_empty());

        let file = crate::FileDescriptor::from(tempfile::tempfile().unwrap());
        let (result, buffer) = selector
            .file_write_at(&file, b"file".to_vec(), 3, &NEVER)
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
        let file = crate::FileDescriptor::from(tempfile::tempfile().unwrap());
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
