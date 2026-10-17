// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::SocketRegistration;
use futures_lite::future::poll_once;
use std::{
    io::Read,
    net::{Shutdown, TcpListener, TcpStream},
    sync::mpsc,
    time::Duration,
};

const TIMEOUT: Duration = Duration::from_secs(10);

fn pair<S: SocketRegistration>(scheduler: &S) -> (S::Socket, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (peer, _) = listener.accept().unwrap();
    peer.set_read_timeout(Some(TIMEOUT)).unwrap();
    (scheduler.register_socket(client).unwrap(), peer)
}

// A blocked worker makes admission and execution observably different. Each caller supplies a pool with one worker and capacity two.
pub(crate) async fn accepted_socket_jobs<S: SocketRegistration>(
    scheduler: &S,
    pool: &BlockingPool,
    mode: u8,
) {
    let (socket, mut peer) = pair(scheduler);
    let (started, starting) = mpsc::channel();
    let (release, released) = mpsc::channel();
    drop(
        pool.submit(move || {
            started.send(()).unwrap();
            released.recv_timeout(TIMEOUT).unwrap();
        })
        .await
        .unwrap(),
    );
    starting.recv_timeout(TIMEOUT).unwrap();

    match mode {
        0 => {
            let mut close = Box::pin(scheduler.socket_close(socket));
            assert!(poll_once(&mut close).await.is_none());
            drop(close);
        }
        1 => scheduler.socket_close_submit(socket).await.unwrap(),
        2 => {
            let mut shutdown = Box::pin(scheduler.socket_shutdown(&socket, Shutdown::Write));
            assert!(poll_once(&mut shutdown).await.is_none());
            drop(shutdown);
            drop(socket);
        }
        3 => {
            scheduler
                .socket_shutdown_submit(&socket, Shutdown::Write)
                .await
                .unwrap();
            drop(socket);
        }
        _ => unreachable!(),
    }
    peer.set_nonblocking(true).unwrap();
    assert_eq!(
        peer.peek(&mut [0]).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    peer.set_nonblocking(false).unwrap();
    // Closing admission must retain every accepted job, including ones whose waiter or original descriptor has disappeared.
    pool.close();
    let mut draining = Box::pin(pool.wait_submitted());
    assert!(poll_once(&mut draining).await.is_none());
    release.send(()).unwrap();
    draining.await;
    assert_eq!(peer.read(&mut [0]).unwrap(), 0);
}

pub(crate) async fn saturated_socket_admission<S: SocketRegistration>(
    scheduler: &S,
    pool: &BlockingPool,
) {
    let (socket, mut peer) = pair(scheduler);
    let (started, starting) = mpsc::channel();
    let (release, released) = mpsc::channel();
    drop(
        pool.submit(move || {
            started.send(()).unwrap();
            released.recv_timeout(TIMEOUT).unwrap();
        })
        .await
        .unwrap(),
    );
    starting.recv_timeout(TIMEOUT).unwrap();
    let mut admission = Box::pin(scheduler.socket_shutdown_submit(&socket, Shutdown::Both));
    assert!(poll_once(&mut admission).await.is_none());
    drop(admission);
    release.send(()).unwrap();
    pool.wait_submitted().await;
    // Abandonment before admission must not execute shutdown.
    let (result, _) = scheduler
        .socket_write(&socket, vec![5], &crate::Cancellation::never())
        .await;
    assert_eq!(result.unwrap(), 1);
    let mut byte = [0];
    peer.read_exact(&mut byte).unwrap();
    assert_eq!(byte, [5]);
    scheduler.socket_close(socket).await.unwrap();
}

pub(crate) async fn closed_socket_admission<S: SocketRegistration>(
    scheduler: &S,
    pool: &BlockingPool,
) {
    let (a, _peer_a) = pair(scheduler);
    let (b, _peer_b) = pair(scheduler);
    let (c, _peer_c) = pair(scheduler);
    pool.close();
    assert_eq!(
        scheduler
            .socket_shutdown(&a, Shutdown::Write)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        scheduler
            .socket_shutdown_submit(&a, Shutdown::Write)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        scheduler.socket_close(b).await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        scheduler.socket_close_submit(c).await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}
