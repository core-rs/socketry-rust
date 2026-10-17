// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use socketry_executor::{
    AddressResolution, AddressResolveOptions, Cancellation, OperationError, SocketDomain,
};
use std::{io, net::SocketAddr};

pub async fn check(scheduler: &impl AddressResolution) {
    let never = Cancellation::never();
    for name in ["127.0.0.1", "::1"] {
        let expected = SocketAddr::new(name.parse().unwrap(), 443);
        for domain in [None, Some(SocketDomain::Ipv4), Some(SocketDomain::Ipv6)] {
            let addresses = scheduler
                .address_resolve(name, AddressResolveOptions { port: 443, domain }, &never)
                .await
                .unwrap();
            if domain.is_none_or(|domain| domain == SocketDomain::from(expected)) {
                assert_eq!(addresses, vec![expected]);
            } else {
                assert!(addresses.is_empty());
            }
        }
    }
    let options = AddressResolveOptions::default();
    assert_eq!(options.port, 0);
    assert_eq!(options.domain, None);
    let copy = options;
    assert_eq!(copy, options);
    assert!(format!("{options:?}").contains("port"));
    assert_eq!(
        scheduler
            .address_resolve("127.0.0.1", options, &never)
            .await
            .unwrap(),
        vec!["127.0.0.1:0".parse::<SocketAddr>().unwrap()]
    );
    for name in ["", "local\0host"] {
        assert_eq!(
            scheduler
                .address_resolve(name, options, &never)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
    let cancelled = Cancellation::new();
    cancelled.cancel();
    for name in ["", "localhost", "127.0.0.1"] {
        assert!(matches!(
            scheduler.address_resolve(name, options, &cancelled).await,
            Err(OperationError::Cancelled(_))
        ));
    }
}

/// Exercise the boundary between resolution and associated socket resources through public traits only.
pub async fn connection(
    scheduler: &(impl AddressResolution + socketry_executor::Socket),
    name: &str,
) {
    use socketry_executor::SocketType;
    let never = Cancellation::never();
    let listener = scheduler
        .socket_open(SocketDomain::Ipv4, SocketType::Stream, None, &never)
        .await
        .unwrap();
    scheduler
        .socket_bind(&listener, "127.0.0.1:0".parse().unwrap(), &never)
        .await
        .unwrap();
    scheduler.socket_listen(&listener, 8, &never).await.unwrap();
    let port = scheduler.socket_local_addr(&listener).unwrap().port();
    let addresses = scheduler
        .address_resolve(
            name,
            AddressResolveOptions {
                port,
                domain: Some(SocketDomain::Ipv4),
            },
            &never,
        )
        .await
        .unwrap();
    assert!(!addresses.is_empty());
    let socket = scheduler
        .socket_open(SocketDomain::Ipv4, SocketType::Stream, None, &never)
        .await
        .unwrap();
    let ((), (accepted, _)) = futures_lite::future::try_zip(
        scheduler.socket_connect(&socket, addresses[0], &never),
        scheduler.socket_accept(&listener, &never),
    )
    .await
    .unwrap();
    let (sent, _) = scheduler.socket_write(&socket, vec![42], &never).await;
    assert_eq!(sent.unwrap(), 1);
    let (received, buffer) = scheduler.socket_read(&accepted, vec![0], &never).await;
    assert_eq!(received.unwrap(), 1);
    assert_eq!(buffer, vec![42]);
    scheduler.socket_close(socket).await.unwrap();
    scheduler.socket_close(accepted).await.unwrap();
    scheduler.socket_close(listener).await.unwrap();
}
