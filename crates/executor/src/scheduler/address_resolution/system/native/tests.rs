// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::{SocketProtocol, SocketType};
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};

#[test]
fn flags_and_numeric_resolution() {
    let hints = AddressHints {
        socket_type: Some(SocketType::Stream),
        numeric_host: true,
        ..AddressHints::default()
    };
    assert_eq!(flags(hints), sys::AI_NUMERICHOST as i32);
    assert_eq!(
        flags(AddressHints {
            passive: true,
            address_configured: true,
            ..hints
        }),
        (sys::AI_PASSIVE | sys::AI_ADDRCONFIG | sys::AI_NUMERICHOST) as i32
    );
    let addresses = lookup(Some("127.0.0.1"), "80", hints).unwrap();
    assert_eq!(
        addresses[0].address,
        "127.0.0.1:80".parse::<SocketAddr>().unwrap()
    );
    assert_eq!(addresses[0].socket_type, SocketType::Stream);
    assert_eq!(addresses[0].protocol, Some(SocketProtocol::Tcp));
    assert!(lookup(Some("not-numeric.invalid"), "80", hints).is_err());
    // AI_ADDRCONFIG depends on the machine's interfaces: either a result or a native resolver error is valid.
    let _ = lookup(
        Some("127.0.0.1"),
        "80",
        AddressHints {
            address_configured: true,
            ..hints
        },
    );
}

#[test]
fn address_copy_preserves_scope_and_flow() {
    let expected = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 1234, 42, 7));
    let storage = socket2::SockAddr::from(expected);
    let mut entry: AddrInfo = unsafe { std::mem::zeroed() };
    entry.ai_family = sys::AF_INET6 as i32;
    entry.ai_addrlen = storage.len() as _;
    entry.ai_addr = storage.as_ptr().cast_mut().cast();
    assert_eq!(unsafe { address(&entry) }, expected);
}

#[test]
fn named_service_and_protocol_hints() {
    let hints = AddressHints {
        socket_type: Some(SocketType::Stream),
        protocol: Some(SocketProtocol::Tcp),
        ..AddressHints::default()
    };
    let addresses = lookup(Some("127.0.0.1"), "http", hints).unwrap();
    assert!(
        addresses
            .iter()
            .all(|entry| entry.address.port() == 80 && entry.socket_type == SocketType::Stream)
    );
    assert!(lookup(Some("127.0.0.1"), "socketry-no-such-service", hints).is_err());
}

#[cfg(unix)]
#[test]
fn unix_resolver_error_categories_and_errno() {
    use crate::AddressResolveErrorKind as Kind;
    for (code, kind) in [
        (sys::EAI_AGAIN, Kind::TemporaryFailure),
        (sys::EAI_NONAME, Kind::NameNotFound),
        (sys::EAI_SERVICE, Kind::ServiceUnavailable),
        (sys::EAI_BADFLAGS, Kind::InvalidHints),
        (sys::EAI_FAMILY, Kind::InvalidHints),
        (sys::EAI_SOCKTYPE, Kind::InvalidHints),
        (sys::EAI_FAIL, Kind::PermanentFailure),
        (sys::EAI_MEMORY, Kind::OutOfMemory),
        (12345, Kind::Other),
    ] {
        let error = platform::error(code);
        assert_eq!(error.kind, kind);
        assert_eq!(error.code, Some(code));
        assert!(!error.message.is_empty());
        assert!(error.system_error.is_none());
    }
    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_vendor = "apple",
        target_os = "netbsd",
        target_os = "openbsd"
    ))]
    assert_eq!(platform::error(sys::EAI_NODATA).kind, Kind::NameNotFound);
    // A real failing syscall establishes errno without platform-specific errno setters.
    assert_eq!(unsafe { libc::close(-1) }, -1);
    let error = platform::error(sys::EAI_SYSTEM);
    assert_eq!(error.kind, Kind::System);
    assert_eq!(
        error.system_error.unwrap().raw_os_error(),
        Some(libc::EBADF)
    );
}

#[cfg(windows)]
#[test]
fn windows_resolver_error_categories() {
    use crate::AddressResolveErrorKind as Kind;
    for (code, kind) in [
        (sys::WSATRY_AGAIN, Kind::TemporaryFailure),
        (sys::WSAHOST_NOT_FOUND, Kind::NameNotFound),
        (sys::WSANO_DATA, Kind::NameNotFound),
        (sys::WSATYPE_NOT_FOUND, Kind::ServiceUnavailable),
        (sys::WSAEINVAL, Kind::InvalidHints),
        (sys::WSAEAFNOSUPPORT, Kind::InvalidHints),
        (sys::WSAESOCKTNOSUPPORT, Kind::InvalidHints),
        (sys::WSANO_RECOVERY, Kind::PermanentFailure),
        (sys::WSA_NOT_ENOUGH_MEMORY, Kind::OutOfMemory),
        (12345, Kind::Other),
    ] {
        let error = platform::error(code);
        assert_eq!(error.kind, kind);
        assert_eq!(error.code, Some(code));
        assert!(!error.message.is_empty());
        assert!(error.system_error.is_none());
    }
}
