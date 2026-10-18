// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::Scheduler;
use socketry_executor::{
    AddressHints, AddressInfo, AddressResolution, AddressResolveError,
    AddressResolveErrorKind as Kind, AddressService, Cancellation, OperationResult, SocketDomain,
    SocketProtocol, SocketType,
};
use std::{
    io,
    net::{IpAddr, SocketAddr},
};

impl Scheduler {
    /// Configure exact-name results, retaining their order and duplicates. This table intentionally does not model DNS search paths, TTLs, or hostname normalization.
    pub fn insert_addresses(&self, name: &str, addresses: Vec<IpAddr>) {
        self.names
            .lock()
            .unwrap()
            .insert(name.to_owned(), addresses);
    }
}

fn failure(kind: Kind) -> io::Error {
    AddressResolveError {
        kind,
        code: None,
        message: format!("simulated resolver: {kind:?}"),
        system_error: None,
    }
    .into()
}

impl AddressResolution for Scheduler {
    // This deterministic model has both IP families configured, a fixed service table (http/TCP and domain/TCP+UDP), and three numeric-service transports. It does not consult the host's resolver, interfaces, or service database. Its socket implementation still only executes stream operations.
    async fn address_resolve(
        &self,
        name: Option<&str>,
        service: AddressService<'_>,
        hints: AddressHints,
        cancellation: &Cancellation,
    ) -> OperationResult<Vec<AddressInfo>> {
        cancellation.check()?;
        if name.is_some_and(|name| name.is_empty() || name.contains('\0')) {
            return Err(io::Error::from(io::ErrorKind::InvalidInput).into());
        }
        let service = match service {
            AddressService::Port(port) => port.to_string(),
            AddressService::Name(name) => {
                if name.is_empty() || name.contains('\0') {
                    return Err(io::Error::from(io::ErrorKind::InvalidInput).into());
                }
                name.to_owned()
            }
        };
        let transports = [
            (SocketType::Stream, Some(SocketProtocol::Tcp)),
            (SocketType::Datagram, Some(SocketProtocol::Udp)),
            (SocketType::Raw, None),
        ];
        let transports: Vec<_> = transports
            .into_iter()
            .filter(|(kind, protocol)| {
                hints.socket_type.is_none_or(|requested| requested == *kind)
                    && hints
                        .protocol
                        .is_none_or(|requested| Some(requested) == *protocol)
            })
            .collect();
        if transports.is_empty() {
            return Err(failure(Kind::InvalidHints).into());
        }
        let transports: Vec<_> = transports
            .into_iter()
            .filter_map(|(kind, protocol)| {
                let port = match service.parse::<u16>() {
                    Ok(port) => Some(port),
                    Err(_) => match (service.as_str(), kind) {
                        ("http", SocketType::Stream) => Some(80),
                        ("domain", SocketType::Stream | SocketType::Datagram) => Some(53),
                        _ => None,
                    },
                };
                port.map(|port| (kind, protocol, port))
            })
            .collect();
        if transports.is_empty() {
            return Err(failure(Kind::ServiceUnavailable).into());
        }
        let addresses = match name {
            None => {
                if hints.passive {
                    vec![
                        IpAddr::from([0, 0, 0, 0]),
                        IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
                    ]
                } else {
                    vec![
                        IpAddr::from([127, 0, 0, 1]),
                        IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
                    ]
                }
            }
            Some(name) => match name.parse::<IpAddr>() {
                Ok(address) => vec![address],
                Err(_) if hints.numeric_host => return Err(failure(Kind::NameNotFound).into()),
                Err(_) => self
                    .names
                    .lock()
                    .unwrap()
                    .get(name)
                    .cloned()
                    .ok_or_else(|| failure(Kind::NameNotFound))?,
            },
        };
        let addresses: Vec<_> = addresses
            .into_iter()
            .filter(|address| {
                hints
                    .domain
                    .is_none_or(|domain| SocketDomain::from(SocketAddr::new(*address, 0)) == domain)
            })
            .collect();
        if addresses.is_empty() {
            return Err(failure(Kind::NameNotFound).into());
        }
        Ok(addresses
            .into_iter()
            .flat_map(|address| {
                transports
                    .iter()
                    .map(move |&(socket_type, protocol, port)| AddressInfo {
                        address: SocketAddr::new(address, port),
                        socket_type,
                        protocol,
                    })
            })
            .collect())
    }
}
