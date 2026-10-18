// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![cfg(any(feature = "native", feature = "tokio"))]

use socketry_executor::{Cancellation, SocketDomain, SocketScheduler, SocketType};

use std::{io, net::Shutdown};

async fn sockets<S: SocketScheduler>(scheduler: &S) -> [S::Socket; 3] {
    async fn open<S: SocketScheduler>(scheduler: &S) -> S::Socket {
        scheduler
            .socket_open(
                SocketDomain::Ipv4,
                SocketType::Stream,
                None,
                &Cancellation::never(),
            )
            .await
            .unwrap()
    }
    [
        open(scheduler).await,
        open(scheduler).await,
        open(scheduler).await,
    ]
}

async fn rejected<S: SocketScheduler>(
    scheduler: &S,
    [first, second, third]: [S::Socket; 3],
    expected: io::ErrorKind,
) {
    assert_eq!(
        scheduler
            .socket_shutdown(&first, Shutdown::Write)
            .await
            .unwrap_err()
            .kind(),
        expected
    );
    assert_eq!(
        scheduler
            .socket_shutdown_submit(&first, Shutdown::Write)
            .await
            .unwrap_err()
            .kind(),
        expected
    );
    assert_eq!(
        scheduler.socket_close(second).await.unwrap_err().kind(),
        expected
    );
    assert_eq!(
        scheduler
            .socket_close_submit(third)
            .await
            .unwrap_err()
            .kind(),
        expected
    );
}

#[cfg(feature = "native")]
#[test]
fn closed_native_scheduler_rejects_socket_lifecycle_operations() {
    let scheduler = socketry_executor::Scheduler::with_workers(1).unwrap();
    let handle = scheduler.handle();
    let sockets = scheduler.block_on(sockets(&scheduler));
    scheduler.shutdown();
    futures_lite::future::block_on(rejected(&handle, sockets, io::ErrorKind::BrokenPipe));
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_lifecycle_rejects_closed_schedulers_and_foreign_resources() {
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
    let foreign = runtime.block_on(sockets(&scheduler));
    other_runtime.block_on(rejected(&other, foreign, io::ErrorKind::InvalidInput));
    let sockets = runtime.block_on(sockets(&scheduler));
    let handle = scheduler.handle();
    runtime.block_on(scheduler.shutdown());
    runtime.block_on(rejected(&handle, sockets, io::ErrorKind::BrokenPipe));
    other_runtime.block_on(other.shutdown());
}
