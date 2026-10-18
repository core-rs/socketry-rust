// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![cfg(any(feature = "native", feature = "tokio"))]

use socketry_executor::{
    Cancellation, OperationError, SocketDomain, SocketOptions, SocketType, TcpNoDelay,
};
use socketry_executor::{SocketClose, SocketOpen};
use std::io;

async fn open<S: SocketOpen>(scheduler: &S, kind: SocketType) -> S::Socket {
    scheduler
        .socket_open(SocketDomain::Ipv4, kind, None, &Cancellation::never())
        .await
        .unwrap()
}

async fn rejected<S: SocketOptions<TcpNoDelay>>(
    scheduler: &S,
    socket: &S::Socket,
    expected: io::ErrorKind,
) {
    let never = Cancellation::never();
    assert_eq!(
        scheduler
            .socket_get_option(socket, TcpNoDelay, &never)
            .await
            .unwrap_err()
            .kind(),
        expected
    );
    assert_eq!(
        scheduler
            .socket_set_option(socket, TcpNoDelay, true, &never)
            .await
            .unwrap_err()
            .kind(),
        expected
    );
}

async fn invalid_protocol<S: SocketOptions<TcpNoDelay> + SocketOpen + SocketClose>(scheduler: &S) {
    let socket = open(scheduler, SocketType::Datagram).await;
    let never = Cancellation::never();
    for error in [
        scheduler
            .socket_get_option(&socket, TcpNoDelay, &never)
            .await
            .unwrap_err(),
        scheduler
            .socket_set_option(&socket, TcpNoDelay, true, &never)
            .await
            .unwrap_err(),
    ] {
        // The error must come from the OS rather than a scheduler-specific Unsupported stub.
        assert!(matches!(error, OperationError::Io(error) if error.raw_os_error().is_some()));
    }
    scheduler.socket_close(socket).await.unwrap();
}

#[cfg(feature = "native")]
#[test]
fn native_options_preserve_os_errors_and_reject_closed_schedulers() {
    let scheduler = socketry_executor::Scheduler::with_workers(1).unwrap();
    let handle = scheduler.handle();
    let socket = scheduler.block_on(open(&scheduler, SocketType::Stream));
    scheduler.block_on(invalid_protocol(&scheduler));
    scheduler.block_on(invalid_protocol(
        &socketry_executor::scheduler::selector::readiness::Selector,
    ));
    scheduler.shutdown();
    futures_lite::future::block_on(rejected(&handle, &socket, io::ErrorKind::BrokenPipe));
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_options_preserve_os_errors_and_validate_runtime_and_shutdown() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let scheduler = socketry_executor::scheduler::tokio::Scheduler::new(runtime.handle().clone());
    let other_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let other = socketry_executor::scheduler::tokio::Scheduler::new(other_runtime.handle().clone());
    let socket = runtime.block_on(open(&scheduler, SocketType::Stream));
    runtime.block_on(invalid_protocol(&scheduler));
    other_runtime.block_on(rejected(&other, &socket, io::ErrorKind::InvalidInput));
    other_runtime.block_on(rejected(
        &other.handle(),
        &socket,
        io::ErrorKind::InvalidInput,
    ));
    let handle = scheduler.handle();
    runtime.block_on(scheduler.shutdown());
    runtime.block_on(rejected(&handle, &socket, io::ErrorKind::BrokenPipe));
    other_runtime.block_on(other.shutdown());
}

#[cfg(all(feature = "io-uring", target_os = "linux"))]
#[test]
fn stopped_io_uring_rejects_options() {
    futures_lite::future::block_on(async {
        let selector = socketry_executor::scheduler::selector::io_uring::Selector::new().unwrap();
        let socket = open(&selector, SocketType::Stream).await;
        selector.shutdown().await;
        rejected(&selector, &socket, io::ErrorKind::BrokenPipe).await;
    });
}
