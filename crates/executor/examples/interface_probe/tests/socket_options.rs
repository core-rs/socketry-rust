// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use socketry_executor::{
    Cancellation, OperationError, SocketDomain, SocketKeepalive, SocketOption, SocketOptions,
    SocketReuseAddress, SocketScheduler, SocketType, TcpNoDelay,
};

async fn round_trip<S, O>(scheduler: &S, socket: &S::Socket, option: O)
where
    S: SocketOptions<O>,
    O: SocketOption<Value = bool> + Copy,
{
    let never = Cancellation::never();
    for value in [false, true, false] {
        scheduler
            .socket_set_option(socket, option, value, &never)
            .await
            .unwrap();
        assert_eq!(
            scheduler
                .socket_get_option(socket, option, &never)
                .await
                .unwrap(),
            value
        );
    }

    // Creating and dropping a setter must not execute the syscall.
    drop(scheduler.socket_set_option(socket, option, true, &never));
    assert!(
        !scheduler
            .socket_get_option(socket, option, &never)
            .await
            .unwrap()
    );

    // Cancellation is checked when the future is polled, including cancellation after construction.
    let cancelled = Cancellation::new();
    let setting = scheduler.socket_set_option(socket, option, true, &cancelled);
    let getting = scheduler.socket_get_option(socket, option, &cancelled);
    cancelled.cancel();
    assert!(matches!(setting.await, Err(OperationError::Cancelled(_))));
    assert!(matches!(getting.await, Err(OperationError::Cancelled(_))));
    assert!(
        !scheduler
            .socket_get_option(socket, option, &never)
            .await
            .unwrap()
    );
}

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

pub async fn check<S>(scheduler: &S)
where
    S: SocketOptions<SocketReuseAddress>
        + SocketOptions<SocketKeepalive>
        + SocketOptions<TcpNoDelay>,
{
    let never = Cancellation::never();
    let listener = open(scheduler).await;
    round_trip(scheduler, &listener, SocketReuseAddress).await;
    round_trip(scheduler, &listener, SocketKeepalive).await;
    round_trip(scheduler, &listener, TcpNoDelay).await;
    scheduler
        .socket_set_option(&listener, SocketReuseAddress, true, &never)
        .await
        .unwrap();
    scheduler
        .socket_bind(&listener, "127.0.0.1:0".parse().unwrap(), &never)
        .await
        .unwrap();
    scheduler
        .socket_listen(&listener, 16, &never)
        .await
        .unwrap();
    assert!(
        scheduler
            .socket_get_option(&listener, SocketReuseAddress, &never)
            .await
            .unwrap()
    );

    let client = open(scheduler).await;
    scheduler
        .socket_set_option(&client, TcpNoDelay, true, &never)
        .await
        .unwrap();
    scheduler
        .socket_set_option(&client, SocketKeepalive, true, &never)
        .await
        .unwrap();
    scheduler
        .socket_connect(
            &client,
            scheduler.socket_local_addr(&listener).unwrap(),
            &never,
        )
        .await
        .unwrap();
    let (server, _) = scheduler.socket_accept(&listener, &never).await.unwrap();
    assert!(
        scheduler
            .socket_get_option(&client, TcpNoDelay, &never)
            .await
            .unwrap()
    );
    assert!(
        scheduler
            .socket_get_option(&client, SocketKeepalive, &never)
            .await
            .unwrap()
    );

    // Configure accepted sockets explicitly: inheritance is OS-specific. Updating one endpoint must not change the other.
    round_trip(scheduler, &server, TcpNoDelay).await;
    round_trip(scheduler, &server, SocketKeepalive).await;
    assert!(
        scheduler
            .socket_get_option(&client, TcpNoDelay, &never)
            .await
            .unwrap()
    );
    assert!(
        scheduler
            .socket_get_option(&client, SocketKeepalive, &never)
            .await
            .unwrap()
    );
    scheduler.socket_close(server).await.unwrap();
    scheduler.socket_close(client).await.unwrap();
    scheduler.socket_close(listener).await.unwrap();
}
