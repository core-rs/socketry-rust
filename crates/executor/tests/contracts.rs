// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use socketry_executor::{
    Cancellation, File, FileOptions, Interest, IoBuf, OperationError, OperationResult, Socket,
    SocketDomain, SocketProtocol, SocketType, Spawn,
};
use std::{
    future::{Future, poll_fn},
    io,
    path::Path,
    pin::pin,
    task::Poll,
};

#[path = "../examples/interface_probe/simulated.rs"]
mod simulated;

#[path = "../examples/interface_probe/tests/stateful_file.rs"]
mod stateful_file;

#[path = "../examples/interface_probe/tests/file_sync.rs"]
mod file_sync;

#[path = "../examples/interface_probe/tests/file_truncate.rs"]
mod file_truncate;

#[path = "../examples/interface_probe/tests/file_allocate.rs"]
mod file_allocate;

#[path = "../examples/interface_probe/tests/file_close.rs"]
mod file_close;

#[path = "../examples/interface_probe/tests/socket_lifecycle.rs"]
mod socket_lifecycle;

async fn resources<S: File + Socket>(scheduler: &S, root: &Path) {
    socket_lifecycle::check(scheduler).await;
    file_close::check(scheduler, root).await;
    stateful_file::check(scheduler, root).await;
    file_sync::check(scheduler, root).await;
    file_truncate::check(scheduler, root).await;
    let never = Cancellation::never();
    let cancelled = Cancellation::new();
    cancelled.cancel();
    let path = root.join("contracts");
    let options = FileOptions {
        read: true,
        write: true,
        create_new: true,
        ..Default::default()
    };
    assert!(matches!(
        scheduler.file_open(&path, options, &cancelled).await,
        Err(OperationError::Cancelled(_))
    ));
    let file = scheduler.file_open(&path, options, &never).await.unwrap();
    assert_eq!(
        scheduler
            .file_open(&path, options, &never)
            .await
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    let source = b"--data--".to_vec();
    let allocation = source.as_ptr();
    let (result, source) = scheduler
        .file_write_at(&file, source.slice(2..6), 3, &never)
        .await;
    assert_eq!(result.unwrap(), 4);
    let source = source.into_inner();
    assert_eq!(source.as_ptr(), allocation);
    let destination = vec![255; 8].into_boxed_slice();
    let allocation = destination.as_ptr();
    let (result, destination) = scheduler
        .file_read_at(&file, destination.slice(2..6), 3, &never)
        .await;
    assert_eq!(result.unwrap(), 4);
    let destination = destination.into_inner();
    assert_eq!(destination.as_ptr(), allocation);
    assert_eq!(&*destination, b"\xff\xffdata\xff\xff");
    for write in [false, true] {
        let buffer = vec![17; 4];
        let allocation = buffer.as_ptr();
        let (result, buffer) = if write {
            scheduler.file_write_at(&file, buffer, 0, &cancelled).await
        } else {
            scheduler.file_read_at(&file, buffer, 0, &cancelled).await
        };
        assert!(matches!(result, Err(OperationError::Cancelled(_))));
        assert_eq!(buffer, vec![17; 4]);
        assert_eq!(buffer.as_ptr(), allocation);
    }
    let missing = root.join("missing").join("file");
    assert!(
        scheduler
            .file_open(&missing, FileOptions::READ, &never)
            .await
            .is_err()
    );
    drop(file);

    let address = "127.0.0.1:0".parse().unwrap();
    assert!(matches!(
        scheduler
            .socket_open(
                SocketDomain::Ipv4,
                SocketType::Stream,
                Some(SocketProtocol::Tcp),
                &cancelled
            )
            .await,
        Err(OperationError::Cancelled(_))
    ));
    let listener = scheduler
        .socket_open(
            SocketDomain::Ipv4,
            SocketType::Stream,
            Some(SocketProtocol::Tcp),
            &never,
        )
        .await
        .unwrap();
    let unbound = scheduler
        .socket_open(SocketDomain::Ipv4, SocketType::Stream, None, &never)
        .await
        .unwrap();
    assert!(matches!(
        scheduler.socket_bind(&unbound, address, &cancelled).await,
        Err(OperationError::Cancelled(_))
    ));
    assert!(matches!(
        scheduler.socket_listen(&unbound, 128, &cancelled).await,
        Err(OperationError::Cancelled(_))
    ));
    let reserved = scheduler
        .socket_open(SocketDomain::Ipv4, SocketType::Stream, None, &never)
        .await
        .unwrap();
    scheduler
        .socket_bind(&reserved, address, &never)
        .await
        .unwrap();
    let reserved_address = scheduler.socket_local_addr(&reserved).unwrap();
    let another_reserved = scheduler
        .socket_open(SocketDomain::Ipv4, SocketType::Stream, None, &never)
        .await
        .unwrap();
    scheduler
        .socket_bind(&another_reserved, address, &never)
        .await
        .unwrap();
    assert_ne!(
        scheduler.socket_local_addr(&another_reserved).unwrap(),
        reserved_address
    );
    let duplicate = scheduler
        .socket_open(SocketDomain::Ipv4, SocketType::Stream, None, &never)
        .await
        .unwrap();
    assert_eq!(
        scheduler
            .socket_bind(&duplicate, reserved_address, &never)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::AddrInUse
    );
    drop(reserved);
    scheduler
        .socket_bind(&duplicate, reserved_address, &never)
        .await
        .unwrap();
    let ipv6 = scheduler
        .socket_open(
            SocketDomain::from("[::1]:0".parse::<std::net::SocketAddr>().unwrap()),
            SocketType::Stream,
            None,
            &never,
        )
        .await
        .unwrap();
    scheduler
        .socket_bind(&ipv6, "[::1]:0".parse().unwrap(), &never)
        .await
        .unwrap();
    assert!(scheduler.socket_local_addr(&ipv6).unwrap().is_ipv6());
    scheduler
        .socket_bind(&listener, address, &never)
        .await
        .unwrap();
    scheduler
        .socket_listen(&listener, 128, &never)
        .await
        .unwrap();
    let address = scheduler.socket_local_addr(&listener).unwrap();
    assert_ne!(address.port(), 0);
    let duplicate = scheduler
        .socket_open(
            SocketDomain::Ipv4,
            SocketType::Stream,
            Some(SocketProtocol::Tcp),
            &never,
        )
        .await
        .unwrap();
    assert_eq!(
        scheduler
            .socket_bind(&duplicate, address, &never)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::AddrInUse
    );
    let cancelled_client = scheduler
        .socket_open(
            SocketDomain::Ipv4,
            SocketType::Stream,
            Some(SocketProtocol::Tcp),
            &never,
        )
        .await
        .unwrap();
    assert!(matches!(
        scheduler
            .socket_connect(&cancelled_client, address, &cancelled)
            .await,
        Err(OperationError::Cancelled(_))
    ));
    let stopping = Cancellation::new();
    let mut accept = pin!(scheduler.socket_accept(&listener, &stopping));
    poll_fn(|context| {
        assert!(accept.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    stopping.cancel();
    assert!(matches!(accept.await, Err(OperationError::Cancelled(_))));

    let client = scheduler
        .socket_open(
            SocketDomain::Ipv4,
            SocketType::Stream,
            Some(SocketProtocol::Tcp),
            &never,
        )
        .await
        .unwrap();
    scheduler
        .socket_connect(&client, address, &never)
        .await
        .unwrap();
    let (server, peer) = scheduler.socket_accept(&listener, &never).await.unwrap();
    assert_eq!(scheduler.socket_local_addr(&server).unwrap(), address);
    assert_eq!(scheduler.socket_peer_addr(&client).unwrap(), address);
    assert_eq!(scheduler.socket_local_addr(&client).unwrap(), peer);
    assert_eq!(scheduler.socket_peer_addr(&server).unwrap(), peer);
    scheduler
        .socket_wait(&client, Interest::Writable, &never)
        .await
        .unwrap();
    assert!(matches!(
        scheduler
            .socket_wait(&server, Interest::Readable, &cancelled)
            .await,
        Err(OperationError::Cancelled(_))
    ));
    let buffer = vec![9; 4];
    let allocation = buffer.as_ptr();
    let stopping = Cancellation::new();
    let mut read = pin!(scheduler.socket_read(&server, buffer, &stopping));
    poll_fn(|context| {
        assert!(read.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    stopping.cancel();
    let (result, buffer) = read.await;
    assert!(matches!(result, Err(OperationError::Cancelled(_))));
    assert_eq!(buffer.as_ptr(), allocation);
    assert_eq!(buffer, vec![9; 4]);
    let (result, _) = scheduler.socket_write(&client, vec![0], &cancelled).await;
    assert!(matches!(result, Err(OperationError::Cancelled(_))));
    let (result, _) = scheduler
        .socket_write(&client, b"-ok-".to_vec().slice(1..3), &never)
        .await;
    assert_eq!(result.unwrap(), 2);
    scheduler
        .socket_wait(&server, Interest::Readable, &never)
        .await
        .unwrap();
    let (result, buffer) = scheduler
        .socket_read(&server, buffer.slice(1..3), &never)
        .await;
    assert_eq!(result.unwrap(), 2);
    assert_eq!(buffer.into_inner(), vec![9, b'o', b'k', 9]);

    let datagram = scheduler
        .socket_open(
            SocketDomain::Ipv4,
            SocketType::Datagram,
            Some(SocketProtocol::Udp),
            &never,
        )
        .await;
    match datagram {
        Ok(datagram) => {
            let receiver = scheduler
                .socket_open(
                    SocketDomain::Ipv4,
                    SocketType::Datagram,
                    Some(SocketProtocol::Udp),
                    &never,
                )
                .await
                .unwrap();
            scheduler
                .socket_bind(&receiver, "127.0.0.1:0".parse().unwrap(), &never)
                .await
                .unwrap();
            let address = scheduler.socket_local_addr(&receiver).unwrap();
            scheduler
                .socket_connect(&datagram, address, &never)
                .await
                .unwrap();
            assert!(scheduler.socket_listen(&datagram, 1, &never).await.is_err());

            let (result, buffer) = scheduler.socket_write(&datagram, Vec::new(), &never).await;
            assert_eq!(result.unwrap(), 0);
            assert!(buffer.is_empty());
            let (result, _) = scheduler
                .socket_write(&datagram, b"later".to_vec(), &never)
                .await;
            assert_eq!(result.unwrap(), 5);
            let (result, buffer) = scheduler.socket_read(&receiver, vec![0; 8], &never).await;
            assert_eq!(result.unwrap(), 0);
            assert_eq!(buffer, vec![0; 8]);
            let (result, buffer) = scheduler.socket_read(&receiver, vec![0; 8], &never).await;
            assert_eq!(result.unwrap(), 5);
            assert_eq!(&buffer[..5], b"later");

            let zero_receiver = scheduler
                .socket_open(
                    SocketDomain::Ipv4,
                    SocketType::Datagram,
                    Some(SocketProtocol::Udp),
                    &never,
                )
                .await
                .unwrap();
            scheduler
                .socket_bind(&zero_receiver, "127.0.0.1:0".parse().unwrap(), &never)
                .await
                .unwrap();
            let zero_sender = scheduler
                .socket_open(
                    SocketDomain::Ipv4,
                    SocketType::Datagram,
                    Some(SocketProtocol::Udp),
                    &never,
                )
                .await
                .unwrap();
            scheduler
                .socket_connect(
                    &zero_sender,
                    scheduler.socket_local_addr(&zero_receiver).unwrap(),
                    &never,
                )
                .await
                .unwrap();
            let (result, _) = scheduler
                .socket_write(&zero_sender, b"drop".to_vec(), &never)
                .await;
            assert_eq!(result.unwrap(), 4);
            scheduler
                .socket_wait(&zero_receiver, Interest::Readable, &never)
                .await
                .unwrap();
            let (result, buffer) = scheduler
                .socket_read(&zero_receiver, Vec::new(), &never)
                .await;
            #[cfg(windows)]
            assert!(matches!(result, Err(OperationError::Io(_))));
            #[cfg(not(windows))]
            assert_eq!(result.unwrap(), 0);
            assert!(buffer.is_empty());

            // The readiness wait confirms a datagram is queued before the empty receive.
            let stopping = Cancellation::new();
            let buffer = vec![0; 8];
            let allocation = buffer.as_ptr();
            let mut read = pin!(scheduler.socket_read(&zero_receiver, buffer, &stopping));
            poll_fn(|context| match read.as_mut().poll(context) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(result) => {
                    panic!("unexpected datagram after empty receive: {result:?}")
                }
            })
            .await;
            stopping.cancel();
            let (result, buffer) = read.await;
            assert!(
                matches!(result, Err(OperationError::Cancelled(_))),
                "unexpected datagram read result: {result:?}"
            );
            assert_eq!(buffer.as_ptr(), allocation);
            assert_eq!(buffer, vec![0; 8]);
        }
        Err(error) => assert_eq!(error.kind(), io::ErrorKind::Unsupported),
    }

    assert_eq!(
        SocketDomain::from("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap()),
        SocketDomain::Ipv4
    );
    assert_eq!(
        SocketDomain::from("[::1]:0".parse::<std::net::SocketAddr>().unwrap()),
        SocketDomain::Ipv6
    );
}

struct ReportDrop(Option<futures_channel::oneshot::Sender<()>>);
impl Drop for ReportDrop {
    fn drop(&mut self) {
        let _ = self.0.take().unwrap().send(());
    }
}

async fn ownership<S: Spawn + Clone + Send + 'static>(scheduler: S) {
    let (dropped, destroyed) = futures_channel::oneshot::channel();
    let guard = ReportDrop(Some(dropped));
    let child = scheduler
        .spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        })
        .unwrap();
    let (retained, ready) = futures_channel::oneshot::channel();
    let owner = scheduler
        .spawn(async move {
            let _child = child;
            retained.send(()).unwrap();
            std::future::pending::<()>().await;
        })
        .unwrap();
    ready.await.unwrap();
    drop(owner);
    destroyed.await.unwrap();

    let finish = Cancellation::new();
    let release = finish.clone();
    let child = scheduler
        .spawn(async move {
            release.cancelled().await;
            42
        })
        .unwrap();
    let (transfer, transferred) = futures_channel::oneshot::channel();
    let original = scheduler
        .spawn(async move {
            assert!(transfer.send(child).is_ok());
            std::future::pending::<()>().await;
        })
        .unwrap();
    let child = transferred.await.unwrap();
    drop(original);
    let waiter = scheduler.spawn(child).unwrap();
    finish.cancel();
    assert_eq!(waiter.await.unwrap().unwrap(), 42);
}

#[test]
fn public_contracts_work_without_native_resources() {
    let scheduler = simulated::Scheduler::new(1024, 1024);
    futures_lite::future::block_on(resources(&scheduler, Path::new("fixture")));
    assert_eq!(scheduler.open_files(), 0);
    scheduler.insert(Path::new("fixture/imported"), b"fixture");
    assert_eq!(
        scheduler.contents(Path::new("fixture/imported")),
        b"fixture"
    );
}

#[test]
fn simulated_stateful_files_preserve_short_transfer_and_cursor_semantics() {
    let scheduler = simulated::Scheduler::new(2, 1);
    futures_lite::future::block_on(stateful_file::check(&scheduler, Path::new("fixture")));
    assert_eq!(scheduler.open_files(), 0);
}

#[cfg(feature = "native")]
#[test]
fn native_resources() {
    let scheduler = socketry_executor::Scheduler::with_workers(2).unwrap();
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    scheduler.block_on(resources(&scheduler, first.path()));
    scheduler.block_on(resources(&scheduler.handle(), second.path()));
    // Exercise the readiness fallback even when the native scheduler selects io_uring.
    let third = tempfile::tempdir().unwrap();
    let readiness = socketry_executor::scheduler::selector::readiness::Selector::new().unwrap();
    scheduler.block_on(resources(&readiness, third.path()));
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    {
        let fourth = tempfile::tempdir().unwrap();
        let handle = scheduler.handle();
        scheduler.block_on(file_close::check(handle.selector().unwrap(), fourth.path()));
        scheduler.block_on(socket_lifecycle::check(handle.selector().unwrap()));
    }
}

#[test]
fn native_transferable_task_ownership() {
    let scheduler = socketry_executor::Scheduler::with_workers(2).unwrap();
    scheduler.block_on(ownership(scheduler.handle()));
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_resources_and_transferable_task_ownership() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let scheduler = socketry_executor::scheduler::tokio::Scheduler::new(runtime.handle().clone());
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    runtime.block_on(resources(&scheduler, first.path()));
    runtime.block_on(resources(&scheduler.handle(), second.path()));
    runtime.block_on(ownership(scheduler.handle()));
    runtime.block_on(scheduler.shutdown());
}

#[test]
fn simulated_allocation_contract() {
    let scheduler = simulated::Scheduler::new(2, 1);
    futures_lite::future::block_on(file_allocate::check(&scheduler, Path::new("fixture")));
}

#[cfg(all(target_os = "linux", feature = "native"))]
#[test]
fn native_allocation_contract() {
    let scheduler = socketry_executor::Scheduler::with_workers(2).unwrap();
    let directory = tempfile::tempdir().unwrap();
    scheduler.block_on(file_allocate::check(&scheduler, directory.path()));
    scheduler.block_on(file_allocate::check(&scheduler.handle(), directory.path()));
    let readiness = socketry_executor::scheduler::selector::readiness::Selector::new().unwrap();
    scheduler.block_on(file_allocate::check(&readiness, directory.path()));
}

#[cfg(all(target_os = "linux", feature = "tokio"))]
#[test]
fn tokio_allocation_contract() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let scheduler = socketry_executor::scheduler::tokio::Scheduler::new(runtime.handle().clone());
    let directory = tempfile::tempdir().unwrap();
    runtime.block_on(file_allocate::check(&scheduler, directory.path()));
    runtime.block_on(file_allocate::check(&scheduler.handle(), directory.path()));
}
