// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Shared nonblocking socket syscalls. Readiness and runtime registration belong to the selected backend.

use crate::{IoBuf, SocketDomain, SocketProtocol, SocketType};
use socket2::{Domain, Protocol, Socket, Type};
use std::{io, marker::PhantomData, net::SocketAddr, slice};

pub(super) struct SendBuffer<'buffer> {
    pointer: *const u8,
    length: usize,
    owner: PhantomData<&'buffer [u8]>,
}

// SAFETY: IoBuf keeps the pointed-to allocation stable, and the owning operation retains its buffer while this value is used.
unsafe impl Send for SendBuffer<'_> {}

impl<'buffer> SendBuffer<'buffer> {
    pub(super) fn new<B: IoBuf>(buffer: &'buffer B) -> Self {
        Self {
            pointer: buffer.as_ref().as_ptr(),
            length: buffer.as_ref().len(),
            owner: PhantomData,
        }
    }

    pub(super) fn send(&self, socket: &Socket) -> io::Result<usize> {
        // SAFETY: `owner` borrows the IoBuf owner, which remains alive until this synchronous syscall returns.
        let bytes = unsafe { slice::from_raw_parts(self.pointer, self.length) };
        socket.send(bytes)
    }
}

pub(super) fn open(
    domain: SocketDomain,
    kind: SocketType,
    protocol: Option<SocketProtocol>,
) -> io::Result<Socket> {
    let domain = match domain {
        SocketDomain::Ipv4 => Domain::IPV4,
        SocketDomain::Ipv6 => Domain::IPV6,
    };
    let kind = match kind {
        SocketType::Stream => Type::STREAM,
        SocketType::Datagram => Type::DGRAM,
    };
    let protocol = protocol.map(|protocol| match protocol {
        SocketProtocol::Tcp => Protocol::TCP,
        SocketProtocol::Udp => Protocol::UDP,
    });
    let socket = Socket::new(domain, kind, protocol)?;
    socket.set_nonblocking(true)?;
    Ok(socket)
}

pub(super) fn address(address: socket2::SockAddr) -> SocketAddr {
    address.as_socket().expect("IP socket address")
}

pub(super) fn recv(socket: &Socket, buffer: &mut [u8]) -> io::Result<usize> {
    // `socket2::Socket::recv` accepts uninitialized storage. The buffer contract keeps this allocation stable and exclusively borrowed while the syscall writes into it.
    let buffer = unsafe {
        slice::from_raw_parts_mut(
            buffer.as_mut_ptr().cast::<std::mem::MaybeUninit<u8>>(),
            buffer.len(),
        )
    };
    socket.recv(buffer)
}

pub(super) fn connect_pending(error: &io::Error) -> bool {
    #[cfg(unix)]
    let pending = error.raw_os_error() == Some(libc::EINPROGRESS);
    #[cfg(windows)]
    let pending =
        error.raw_os_error() == Some(windows_sys::Win32::Networking::WinSock::WSAEWOULDBLOCK);
    pending || error.kind() == io::ErrorKind::WouldBlock
}

/// A readiness notification can predate `connect`, so check both the pending error and peer association before reporting success.
pub(super) fn connected(socket: &Socket) -> io::Result<()> {
    if let Some(error) = socket.take_error()? {
        return Err(error);
    }
    match socket.peer_addr() {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotConnected => {
            Err(io::ErrorKind::WouldBlock.into())
        }
        Err(error) => Err(error),
    }
}
