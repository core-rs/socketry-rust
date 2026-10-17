// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

// Native constant and length types differ between libc and Winsock.
#![allow(clippy::unnecessary_cast)]

use crate::scheduler::socket_parameters;
use crate::{AddressHints, AddressInfo, SocketDomain};
use std::{io, ptr};

#[cfg(unix)]
#[path = "native/unix.rs"]
mod platform;
#[cfg(windows)]
#[path = "native/windows.rs"]
mod platform;
use platform::{AddrInfo, sys};

pub(super) fn lookup(
    name: Option<&str>,
    service: &str,
    hints: AddressHints,
) -> io::Result<Vec<AddressInfo>> {
    #[cfg(windows)]
    let _winsock = platform::Winsock::new()?;
    let name = name.map(platform::string);
    let service = platform::string(service);
    // SAFETY: Zeroed addrinfo has null output-only fields; all populated hints below are numeric values.
    let mut native: AddrInfo = unsafe { std::mem::zeroed() };
    native.ai_family = match hints.domain {
        None => sys::AF_UNSPEC as i32,
        Some(SocketDomain::Ipv4) => sys::AF_INET as i32,
        Some(SocketDomain::Ipv6) => sys::AF_INET6 as i32,
    };
    native.ai_socktype = hints.socket_type.map_or(0, socket_parameters::socket_type);
    native.ai_protocol = hints.protocol.map_or(0, socket_parameters::protocol);
    native.ai_flags = flags(hints);
    let mut result = ptr::null_mut();
    // SAFETY: Strings are terminated and retained through the call. Hints and the output pointer are valid; the successful list is immediately owned by Results.
    let code = unsafe {
        platform::getaddrinfo(
            name.as_ref().map_or(ptr::null(), |name| name.as_ptr()),
            service.as_ptr(),
            &native,
            &mut result,
        )
    };
    if code != 0 {
        // Capture errno before any other native call or allocation on platforms supporting EAI_SYSTEM.
        return Err(platform::error(code).into());
    }
    let results = Results(result);
    // SAFETY: Results owns the successful native list for the entire copy.
    unsafe { copy_results(results.0) }
}

// SAFETY: current must be a live native result list; each recognized IP entry must contain a valid matching socket address. Other families are rejected without reading their address data.
unsafe fn copy_results(mut current: *mut AddrInfo) -> io::Result<Vec<AddressInfo>> {
    let mut addresses = Vec::new();
    while !current.is_null() {
        // SAFETY: getaddrinfo owns a linked list valid until Results is dropped. All address data is copied before freeing the list.
        let entry = unsafe { &*current };
        addresses.push(AddressInfo {
            // SAFETY: The entry and address belong to the live resolver list.
            address: unsafe { address(entry) }?,
            socket_type: socket_parameters::decode_socket_type(entry.ai_socktype),
            protocol: socket_parameters::decode_protocol(entry.ai_protocol),
        });
        current = entry.ai_next;
    }
    Ok(addresses)
}

fn flags(hints: AddressHints) -> i32 {
    (i32::from(hints.passive) * sys::AI_PASSIVE as i32)
        | (i32::from(hints.numeric_host) * sys::AI_NUMERICHOST as i32)
        | (i32::from(hints.address_configured) * sys::AI_ADDRCONFIG as i32)
}

// SAFETY: For an IP family, entry must contain a valid matching socket address and ai_addrlen, as provided by native getaddrinfo. Other families are rejected before dereferencing ai_addr.
unsafe fn address(entry: &AddrInfo) -> io::Result<std::net::SocketAddr> {
    // AF_UNSPEC can include AF_LOCAL on FreeBSD. This capability exposes only IP endpoints and must reject other families without panicking.
    if entry.ai_family != sys::AF_INET as i32 && entry.ai_family != sys::AF_INET6 as i32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "resolver returned a non-IP socket address",
        ));
    }
    let mut storage = socket2::SockAddrStorage::zeroed();
    let length = entry.ai_addrlen as usize;
    if length > std::mem::size_of::<socket2::SockAddrStorage>() {
        unreachable!("getaddrinfo IP addresses fit sockaddr_storage");
    }
    // SAFETY: The caller provides the valid native record, including its readable address and matching length/family. Storage is aligned, zeroed, and large enough. Copying retains IPv6 flow and scope values without a fallible conversion.
    let address = unsafe {
        ptr::copy_nonoverlapping(
            entry.ai_addr.cast::<u8>(),
            (&mut storage as *mut socket2::SockAddrStorage).cast::<u8>(),
            length,
        );
        socket2::SockAddr::new(storage, length as _)
    };
    match address.as_socket() {
        Some(address) => Ok(address),
        None => {
            unreachable!("getaddrinfo records have matching ai_family and socket address families")
        }
    }
}

struct Results(*mut AddrInfo);
impl Drop for Results {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: This is the unchanged successful list returned by the corresponding resolver, owned only by this guard.
            unsafe { platform::freeaddrinfo(self.0) };
        }
    }
}

#[cfg(test)]
mod tests;
