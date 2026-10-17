// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{AddressResolveError, AddressResolveErrorKind as Kind};
use std::io;
pub(super) use sys::{
    ADDRINFOW as AddrInfo, FreeAddrInfoW as freeaddrinfo, GetAddrInfoW as getaddrinfo,
};
pub(super) use windows_sys::Win32::Networking::WinSock as sys;

pub(super) fn string(value: &str) -> Vec<u16> {
    value.encode_utf16().chain(Some(0)).collect()
}

pub(super) fn error(code: i32) -> AddressResolveError {
    let kind = match code {
        sys::WSATRY_AGAIN => Kind::TemporaryFailure,
        sys::WSAHOST_NOT_FOUND | sys::WSANO_DATA => Kind::NameNotFound,
        sys::WSATYPE_NOT_FOUND => Kind::ServiceUnavailable,
        sys::WSAEINVAL | sys::WSAEAFNOSUPPORT | sys::WSAESOCKTNOSUPPORT => Kind::InvalidHints,
        sys::WSANO_RECOVERY => Kind::PermanentFailure,
        sys::WSA_NOT_ENOUGH_MEMORY => Kind::OutOfMemory,
        _ => Kind::Other,
    };
    // FormatMessage through io::Error avoids Winsock's non-thread-safe gai_strerror buffer.
    AddressResolveError {
        kind,
        code: Some(code),
        message: io::Error::from_raw_os_error(code).to_string(),
        system_error: None,
    }
}

pub(super) struct Winsock;
impl Winsock {
    pub(super) fn new() -> io::Result<Self> {
        Self::start_with(|data| {
            // SAFETY: data points to writable WSADATA; version 2.2 is requested. Each successful startup is paired with this guard's cleanup.
            unsafe { sys::WSAStartup(0x0202, data) }
        })
    }

    fn start_with(start: impl FnOnce(*mut sys::WSADATA) -> i32) -> io::Result<Self> {
        let mut data = std::mem::MaybeUninit::uninit();
        let result = start(data.as_mut_ptr());
        if result != 0 {
            return Err(io::Error::from_raw_os_error(result));
        }
        Ok(Self)
    }
}
impl Drop for Winsock {
    fn drop(&mut self) {
        // SAFETY: The guard owns one successful WSAStartup reference. It is dropped after the address list is freed.
        unsafe {
            sys::WSACleanup();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn startup_failure_is_an_io_error() {
        assert_eq!(
            Winsock::start_with(|_| sys::WSASYSNOTREADY)
                .err()
                .unwrap()
                .raw_os_error(),
            Some(sys::WSASYSNOTREADY)
        );
    }
}
