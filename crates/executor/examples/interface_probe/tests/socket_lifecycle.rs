// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use socketry_executor::{Cancellation, OperationError, SocketDomain, SocketScheduler, SocketType};
use std::net::Shutdown;

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

async fn pair<S: SocketScheduler>(scheduler: &S) -> (S::Socket, S::Socket) {
    let never = Cancellation::never();
    let listener = open(scheduler).await;
    scheduler
        .socket_bind(&listener, "127.0.0.1:0".parse().unwrap(), &never)
        .await
        .unwrap();
    scheduler
        .socket_listen(&listener, 16, &never)
        .await
        .unwrap();
    let client = open(scheduler).await;
    scheduler
        .socket_connect(
            &client,
            scheduler.socket_local_addr(&listener).unwrap(),
            &never,
        )
        .await
        .unwrap();
    let (server, _) = scheduler.socket_accept(&listener, &never).await.unwrap();
    scheduler.socket_close(listener).await.unwrap();
    (client, server)
}

pub async fn check<S: SocketScheduler>(scheduler: &S) {
    let never = Cancellation::never();
    let (client, server) = pair(scheduler).await;
    // An unpolled shutdown must have no effect; the following write still succeeds.
    drop(scheduler.socket_shutdown(&client, Shutdown::Both));
    drop(scheduler.socket_shutdown_submit(&client, Shutdown::Both));
    let (result, _) = scheduler.socket_write(&client, vec![42], &never).await;
    assert_eq!(result.unwrap(), 1);
    scheduler
        .socket_shutdown(&client, Shutdown::Write)
        .await
        .unwrap();
    let (result, buffer) = scheduler.socket_read(&server, vec![0], &never).await;
    assert_eq!(result.unwrap(), 1);
    assert_eq!(buffer, vec![42]);
    let (result, _) = scheduler.socket_read(&server, vec![0], &never).await;
    assert_eq!(result.unwrap(), 0);
    let (result, _) = scheduler.socket_write(&client, vec![1], &never).await;
    assert!(matches!(result, Err(OperationError::Io(_))));
    // EOF in one direction does not prevent the response in the other direction.
    let (result, _) = scheduler.socket_write(&server, vec![24], &never).await;
    assert_eq!(result.unwrap(), 1);
    scheduler
        .socket_shutdown(&server, Shutdown::Write)
        .await
        .unwrap();
    let (result, buffer) = scheduler.socket_read(&client, vec![0], &never).await;
    assert_eq!(result.unwrap(), 1);
    assert_eq!(buffer, vec![24]);
    let (result, _) = scheduler.socket_read(&client, vec![0], &never).await;
    assert_eq!(result.unwrap(), 0);
    scheduler.socket_close(client).await.unwrap();
    scheduler.socket_close(server).await.unwrap();

    for how in [Shutdown::Read, Shutdown::Both] {
        let (client, server) = pair(scheduler).await;
        let mut read = Box::pin(scheduler.socket_read(&server, vec![0], &never));
        assert!(futures_lite::future::poll_once(&mut read).await.is_none());
        scheduler.socket_shutdown(&server, how).await.unwrap();
        // Directional shutdown must also wake a receive that was already waiting.
        let (result, _) = read.await;
        // Some platforms return EOF; Winsock can report WSAESHUTDOWN.
        assert!(matches!(result, Ok(0) | Err(OperationError::Io(_))));
        if how == Shutdown::Read {
            let (result, _) = scheduler.socket_write(&server, vec![17], &never).await;
            assert_eq!(result.unwrap(), 1);
            let (result, buffer) = scheduler.socket_read(&client, vec![0], &never).await;
            assert_eq!(result.unwrap(), 1);
            assert_eq!(buffer, vec![17]);
        }
        scheduler.socket_close(client).await.unwrap();
        scheduler.socket_close(server).await.unwrap();
    }

    let (client, server) = pair(scheduler).await;
    scheduler
        .socket_shutdown_submit(&server, Shutdown::Write)
        .await
        .unwrap();
    // Admission alone cannot authorize explicit close of the same socket. Ordinary Drop lets the accepted job retain it until completion.
    drop(server);
    let (result, _) = scheduler.socket_read(&client, vec![0], &never).await;
    assert_eq!(result.unwrap(), 0);
    scheduler.socket_close(client).await.unwrap();

    let (client, server) = pair(scheduler).await;
    scheduler.socket_close_submit(server).await.unwrap();
    let (result, _) = scheduler.socket_read(&client, vec![0], &never).await;
    assert_eq!(result.unwrap(), 0);
    scheduler.socket_close(client).await.unwrap();

    let (client, server) = pair(scheduler).await;
    let stopping = Cancellation::new();
    let mut read = Box::pin(scheduler.socket_read(&client, vec![0; 8], &stopping));
    assert!(futures_lite::future::poll_once(&mut read).await.is_none());
    stopping.cancel();
    let (result, _) = read.await;
    assert!(matches!(result, Err(OperationError::Cancelled(_))));
    // Returning cancellation must release backend ownership before waking this close caller.
    scheduler.socket_close(client).await.unwrap();
    scheduler.socket_close(server).await.unwrap();

    let socket = open(scheduler).await;
    assert!(
        scheduler
            .socket_shutdown(&socket, Shutdown::Write)
            .await
            .is_err()
    );
    // Submission reports admission, so the same OS error is not returned here.
    scheduler
        .socket_shutdown_submit(&socket, Shutdown::Write)
        .await
        .unwrap();
    drop(socket);
}
