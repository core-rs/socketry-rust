// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use socketry_executor::{
    AddressHints, AddressInfo, AddressResolution, AddressService, Cancellation, OperationError,
    SocketDomain, SocketProtocol, SocketType,
};

use std::{io, net::SocketAddr};

pub async fn check(scheduler: &impl AddressResolution) {
    let never = Cancellation::never();
    for name in ["127.0.0.1", "::1"] {
        let expected = SocketAddr::new(name.parse().unwrap(), 443);
        for domain in [None, Some(SocketDomain::Ipv4), Some(SocketDomain::Ipv6)] {
            let hints = AddressHints {
                domain,
                socket_type: Some(SocketType::Stream),
                ..AddressHints::default()
            };
            let result = scheduler
                .address_resolve(Some(name), AddressService::Port(443), hints, &never)
                .await;
            if domain.is_none_or(|domain| domain == SocketDomain::from(expected)) {
                let addresses = result.unwrap();
                assert_eq!(addresses.len(), 1);
                assert_eq!(addresses[0].address, expected);
                assert_eq!(addresses[0].socket_type, SocketType::Stream);
                // Some native resolvers retain the unspecified protocol as zero/default.
                assert!(matches!(
                    addresses[0].protocol,
                    None | Some(SocketProtocol::Tcp)
                ));
            } else if let Ok(addresses) = result {
                // Some system resolvers synthesize mapped IPv6 literals even without an explicit V4MAPPED hint. Preserve native policy while checking the requested family and service.
                assert!(
                    addresses
                        .iter()
                        .all(|info| Some(SocketDomain::from(info.address)) == domain
                            && info.address.port() == 443)
                );
            }
        }
    }
    let hints = AddressHints::default();
    assert_eq!(hints.domain, None);
    assert_eq!(hints.socket_type, None);
    assert_eq!(hints.protocol, None);
    assert!(!hints.passive && !hints.numeric_host && !hints.address_configured);
    let copy = hints;
    assert_eq!(copy, hints);
    assert!(format!("{hints:?}").contains("domain"));
    let addresses = scheduler
        .address_resolve(Some("127.0.0.1"), AddressService::Port(0), hints, &never)
        .await
        .unwrap();
    assert!(!addresses.is_empty());
    assert!(
        addresses
            .iter()
            .all(|candidate| candidate.address == "127.0.0.1:0".parse::<SocketAddr>().unwrap())
    );
    for (socket_type, protocol) in [
        (SocketType::Stream, SocketProtocol::Tcp),
        (SocketType::Datagram, SocketProtocol::Udp),
    ] {
        let hints = AddressHints {
            socket_type: Some(socket_type),
            protocol: Some(protocol),
            ..hints
        };
        let addresses = scheduler
            .address_resolve(Some("127.0.0.1"), AddressService::Name("53"), hints, &never)
            .await
            .unwrap();
        assert_eq!(
            addresses,
            vec![AddressInfo {
                address: "127.0.0.1:53".parse().unwrap(),
                socket_type,
                protocol: Some(protocol)
            }]
        );
        for passive in [false, true] {
            let addresses = scheduler
                .address_resolve(
                    None,
                    AddressService::Port(1234),
                    AddressHints { passive, ..hints },
                    &never,
                )
                .await
                .unwrap();
            assert!(!addresses.is_empty());
            for candidate in addresses {
                assert_eq!(candidate.address.port(), 1234);
                assert!(if passive {
                    candidate.address.ip().is_unspecified()
                } else {
                    candidate.address.ip().is_loopback()
                });
            }
        }
    }
    for name in ["", "local\0host"] {
        assert_eq!(
            scheduler
                .address_resolve(Some(name), AddressService::Port(0), hints, &never)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
    for service in ["", "ht\0tp"] {
        assert_eq!(
            scheduler
                .address_resolve(
                    Some("127.0.0.1"),
                    AddressService::Name(service),
                    hints,
                    &never
                )
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
    assert!(
        scheduler
            .address_resolve(
                Some("not-a-numeric-host.invalid"),
                AddressService::Port(0),
                AddressHints {
                    numeric_host: true,
                    ..hints
                },
                &never
            )
            .await
            .is_err()
    );
    let cancelled = Cancellation::new();
    cancelled.cancel();
    for name in [None, Some(""), Some("localhost"), Some("127.0.0.1")] {
        assert!(matches!(
            scheduler
                .address_resolve(name, AddressService::Name(""), hints, &cancelled)
                .await,
            Err(OperationError::Cancelled(_))
        ));
    }
}

/// Exercise the boundary between resolution and associated socket resources through public traits only.
pub async fn connection(
    scheduler: &(impl AddressResolution + socketry_executor::SocketScheduler),
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
            Some(name),
            AddressService::Port(port),
            AddressHints {
                socket_type: Some(SocketType::Stream),
                domain: Some(SocketDomain::Ipv4),
                ..AddressHints::default()
            },
            &never,
        )
        .await
        .unwrap();
    assert!(!addresses.is_empty());
    let socket = scheduler
        .socket_open(
            SocketDomain::from(addresses[0].address),
            addresses[0].socket_type,
            addresses[0].protocol,
            &never,
        )
        .await
        .unwrap();
    let ((), (accepted, _)) = futures_lite::future::try_zip(
        scheduler.socket_connect(&socket, addresses[0].address, &never),
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
