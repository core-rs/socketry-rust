// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![cfg(any(feature = "native", feature = "tokio"))]

#[path = "../examples/interface_probe/tests/socket_messages.rs"]
mod contracts;
mod support;
use socketry_executor::{
    Cancellation, SocketDomain, SocketMessage, SocketMessages, SocketRecvFlags, SocketScheduler,
    SocketSendFlags, SocketType,
};
use std::io;

async fn errors<S: SocketMessages>(scheduler: &S) {
    let never = Cancellation::never();
    let socket = scheduler
        .socket_open(SocketDomain::Ipv4, SocketType::Stream, None, &never)
        .await
        .unwrap();
    rejected(scheduler, &socket, None).await;
    scheduler.socket_close(socket).await.unwrap();
}

async fn rejected<S: SocketMessages>(
    scheduler: &S,
    socket: &S::Socket,
    kind: Option<io::ErrorKind>,
) {
    let never = Cancellation::never();
    let buffer = vec![42; 4];
    let allocation = buffer.as_ptr();
    let (sent, message) = scheduler
        .socket_sendmsg(
            socket,
            SocketMessage {
                buffers: vec![buffer],
                address: None,
            },
            SocketSendFlags::default(),
            &never,
        )
        .await;
    assert!(sent.is_err());
    if let Some(kind) = kind {
        assert_eq!(sent.unwrap_err().kind(), kind);
    }
    assert_eq!(message.buffers[0].as_ptr(), allocation);
    let (received, buffers) = scheduler
        .socket_recvmsg(socket, message.buffers, SocketRecvFlags::default(), &never)
        .await;
    assert!(received.is_err());
    if let Some(kind) = kind {
        assert_eq!(received.unwrap_err().kind(), kind);
    }
    assert_eq!(buffers[0].as_ptr(), allocation);
    assert_eq!(buffers, vec![vec![42; 4]]);
}

#[cfg(feature = "native")]
#[test]
fn native_messages_preserve_contracts_and_shutdown() {
    let scheduler = socketry_executor::Scheduler::with_workers(2).unwrap();
    let handle = scheduler.handle();
    let socket = scheduler.block_on(async {
        futures_lite::future::race(
            async {
                contracts::check(&scheduler).await;
                contracts::check(&handle).await;
                contracts::check(&socketry_executor::scheduler::selector::readiness::Selector)
                    .await;
                errors(&scheduler).await;
                errors(&socketry_executor::scheduler::selector::readiness::Selector).await;
                scheduler
                    .socket_open(
                        SocketDomain::Ipv4,
                        SocketType::Datagram,
                        None,
                        &Cancellation::never(),
                    )
                    .await
                    .unwrap()
            },
            async {
                async_io::Timer::after(support::TIMEOUT).await;
                panic!("message contracts timed out")
            },
        )
        .await
    });
    scheduler.shutdown();
    futures_lite::future::block_on(rejected(&handle, &socket, Some(io::ErrorKind::BrokenPipe)));
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_messages_preserve_contracts_and_runtime_validation() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let scheduler = socketry_executor::scheduler::tokio::Scheduler::new(runtime.handle().clone());
    let socket = runtime.block_on(async {
        tokio::time::timeout(support::TIMEOUT, async {
            contracts::check(&scheduler).await;
            contracts::check(&scheduler.handle()).await;
            errors(&scheduler).await;
            scheduler
                .socket_open(
                    SocketDomain::Ipv4,
                    SocketType::Datagram,
                    None,
                    &Cancellation::never(),
                )
                .await
                .unwrap()
        })
        .await
        .expect("message contracts timed out")
    });
    let other_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let other = socketry_executor::scheduler::tokio::Scheduler::new(other_runtime.handle().clone());
    other_runtime.block_on(rejected(&other, &socket, Some(io::ErrorKind::InvalidInput)));
    let handle = scheduler.handle();
    runtime.block_on(scheduler.shutdown());
    runtime.block_on(rejected(&handle, &socket, Some(io::ErrorKind::BrokenPipe)));
    other_runtime.block_on(other.shutdown());
}

#[cfg(all(feature = "io-uring", target_os = "linux"))]
#[test]
fn io_uring_messages_reject_closed_selectors() {
    futures_lite::future::block_on(async {
        let selector = socketry_executor::scheduler::selector::io_uring::Selector::new().unwrap();
        let socket = selector
            .socket_open(
                SocketDomain::Ipv4,
                SocketType::Datagram,
                None,
                &Cancellation::never(),
            )
            .await
            .unwrap();
        selector.shutdown().await;
        rejected(&selector, &socket, Some(io::ErrorKind::BrokenPipe)).await;
    });
}
