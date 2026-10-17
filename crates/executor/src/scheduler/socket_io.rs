// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Shared nonblocking socket syscalls. Readiness and runtime registration belong to the selected backend.

use crate::{IoBuf, OperationResult, SocketDomain, SocketProtocol, SocketType};
use socket2::{Domain, Protocol, Socket, Type};
use std::future::Future;
use std::{io, marker::PhantomData, net::SocketAddr, slice};

// Called only on the cleanup pool after exclusive ownership and deregistration. Close is never retried on Unix: the descriptor may already have been released on error.
pub(crate) fn close(socket: Socket) -> io::Result<()> {
    #[cfg(unix)]
    let result = {
        use std::os::fd::IntoRawFd;
        // SAFETY: The socket is exclusively owned and deregistered, and transferring the raw descriptor prevents a second close in Drop.
        unsafe { libc::close(socket.into_raw_fd()) }
    };
    #[cfg(unix)]
    return close_result(result);

    #[cfg(windows)]
    close_with(socket, |socket| socket.set_nonblocking(false))
}

#[cfg(windows)]
fn close_with(
    socket: Socket,
    set_blocking: impl FnOnce(&Socket) -> io::Result<()>,
) -> io::Result<()> {
    use std::os::windows::io::IntoRawSocket;
    // Winsock cannot close a nonblocking socket with positive linger reliably. The cleanup worker may block, and no operation or readiness registration still uses this socket.
    set_blocking(&socket)?;
    // SAFETY: Ownership is transferred out of Socket; no other backend operation retains this socket, and Drop must not close it again.
    let result = unsafe {
        windows_sys::Win32::Networking::WinSock::closesocket(socket.into_raw_socket() as _)
    };
    close_result(result)
}

fn close_result(result: i32) -> io::Result<()> {
    if result == 0 {
        Ok(())
    } else {
        #[cfg(unix)]
        let error = io::Error::last_os_error();
        #[cfg(windows)]
        // SAFETY: WSAGetLastError reads the calling thread's most recent Winsock error.
        let error = io::Error::from_raw_os_error(unsafe {
            windows_sys::Win32::Networking::WinSock::WSAGetLastError()
        });
        Err(error)
    }
}

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
        #[cfg(unix)]
        return socket.send_with_flags(bytes, libc::MSG_NOSIGNAL);

        #[cfg(not(unix))]
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
    open_with(domain, kind, protocol, Socket::new, set_nonblocking)
}

fn set_nonblocking(socket: &Socket) -> io::Result<()> {
    socket.set_nonblocking(true)
}

fn open_with(
    domain: Domain,
    kind: Type,
    protocol: Option<Protocol>,
    create: impl FnOnce(Domain, Type, Option<Protocol>) -> io::Result<Socket>,
    set_nonblocking: impl FnOnce(&Socket) -> io::Result<()>,
) -> io::Result<Socket> {
    let socket = create(domain, kind, protocol)?;
    set_nonblocking(&socket)?;
    Ok(socket)
}

pub(super) fn open_descriptor<Descriptor>(
    domain: SocketDomain,
    kind: SocketType,
    protocol: Option<SocketProtocol>,
    open: impl FnOnce(SocketDomain, SocketType, Option<SocketProtocol>) -> io::Result<Socket>,
    register: impl FnOnce(Socket) -> io::Result<Descriptor>,
) -> OperationResult<Descriptor> {
    let socket = open(domain, kind, protocol)?;
    Ok(register(socket)?)
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

pub(super) async fn connect_result(
    result: io::Result<()>,
    pending: impl Future<Output = OperationResult<()>>,
) -> OperationResult<()> {
    match result {
        Ok(()) => Ok(()),
        Err(error) if connect_pending(&error) => pending.await,
        Err(error) => Err(error.into()),
    }
}

/// A readiness notification can predate `connect`, so check both the pending error and peer association before reporting success.
pub(super) fn connected(socket: &Socket) -> io::Result<()> {
    connected_with(socket, Socket::take_error, Socket::peer_addr)
}

fn connected_with(
    socket: &Socket,
    take_error: impl FnOnce(&Socket) -> io::Result<Option<io::Error>>,
    peer_addr: impl FnOnce(&Socket) -> io::Result<socket2::SockAddr>,
) -> io::Result<()> {
    if let Some(error) = take_error(socket)? {
        return Err(error);
    }
    match peer_addr(socket) {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotConnected => {
            Err(io::ErrorKind::WouldBlock.into())
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_close_consumes_socket_and_preserves_error_results() {
        close(Socket::new(Domain::IPV4, Type::STREAM, None).unwrap()).unwrap();
        assert!(close_result(-1).is_err());
    }

    #[test]
    fn explicit_close_preserves_positive_linger_and_queued_data() {
        use std::{io::Read, net::TcpListener, thread, time::Duration};

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).unwrap();
        socket
            .connect(&listener.local_addr().unwrap().into())
            .unwrap();
        let (mut peer, _) = listener.accept().unwrap();
        peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        socket2::SockRef::from(&peer)
            .set_recv_buffer_size(8192)
            .unwrap();
        socket.set_send_buffer_size(4096).unwrap();
        socket.set_linger(Some(Duration::from_secs(5))).unwrap();
        socket.set_nonblocking(true).unwrap();

        // Fill the send queue before the peer starts reading. Bound the payload as socket buffer limits vary by platform.
        let mut sent = 0;
        while sent < 1024 * 1024 {
            match socket.send(&[0x5a; 4096]) {
                Ok(length) => {
                    assert_ne!(length, 0);
                    sent += length;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("send failed: {error}"),
            }
        }
        assert!(sent > 0);
        let reader = thread::spawn(move || {
            let mut received = Vec::new();
            peer.read_to_end(&mut received).unwrap();
            received
        });

        close(socket).unwrap();
        assert_eq!(reader.join().unwrap(), vec![0x5a; sent]);
    }

    #[cfg(windows)]
    #[test]
    fn close_configuration_errors_are_returned_and_ownership_is_released() {
        use std::{
            io::Read,
            net::{TcpListener, TcpStream},
            time::Duration,
        };

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let socket = Socket::from(TcpStream::connect(listener.local_addr().unwrap()).unwrap());
        let (mut peer, _) = listener.accept().unwrap();
        peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        socket.set_nonblocking(true).unwrap();

        let error =
            close_with(socket, |_| Err(io::ErrorKind::PermissionDenied.into())).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(peer.read(&mut [0]).unwrap(), 0);
    }

    #[test]
    fn socket_creation_errors_are_returned() {
        let error = io::Error::from(io::ErrorKind::PermissionDenied);
        let returned = open_with(
            Domain::IPV4,
            Type::STREAM,
            Some(Protocol::TCP),
            |_, _, _| Err(error),
            set_nonblocking,
        )
        .unwrap_err();

        assert_eq!(returned.kind(), io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn nonblocking_configuration_errors_are_returned() {
        let error = io::Error::from(io::ErrorKind::PermissionDenied);
        let returned = open_with(
            Domain::IPV4,
            Type::STREAM,
            Some(Protocol::TCP),
            Socket::new,
            |_| Err(error),
        )
        .unwrap_err();

        assert_eq!(returned.kind(), io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn descriptor_open_propagates_socket_creation_errors() {
        let result = open_descriptor::<()>(
            SocketDomain::Ipv4,
            SocketType::Stream,
            Some(SocketProtocol::Tcp),
            |_, _, _| Err(io::Error::from(io::ErrorKind::PermissionDenied)),
            |_| unreachable!("registration follows successful socket creation"),
        );

        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn descriptor_open_propagates_registration_errors() {
        let result = open_descriptor::<()>(
            SocketDomain::Ipv4,
            SocketType::Stream,
            Some(SocketProtocol::Tcp),
            open,
            |_| Err(io::Error::from(io::ErrorKind::PermissionDenied)),
        );

        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn connect_pending_recognizes_platform_and_standard_errors() {
        #[cfg(unix)]
        assert!(connect_pending(&io::Error::from_raw_os_error(
            libc::EINPROGRESS
        )));

        #[cfg(windows)]
        assert!(connect_pending(&io::Error::from_raw_os_error(
            windows_sys::Win32::Networking::WinSock::WSAEWOULDBLOCK as i32
        )));

        assert!(connect_pending(&io::Error::from(io::ErrorKind::WouldBlock)));
        assert!(!connect_pending(&io::Error::from(io::ErrorKind::Other)));
    }

    #[test]
    fn connection_result_preserves_immediate_errors_and_waits_for_pending_results() {
        let immediate_success =
            futures_lite::future::block_on(connect_result(Ok(()), std::future::pending()));
        assert!(immediate_success.is_ok());

        let direct_error = futures_lite::future::block_on(connect_result(
            Err(io::Error::from(io::ErrorKind::ConnectionRefused)),
            std::future::pending(),
        ));
        assert_eq!(
            direct_error.unwrap_err().kind(),
            io::ErrorKind::ConnectionRefused
        );

        let pending_result = futures_lite::future::block_on(connect_result(
            Err(io::Error::from(io::ErrorKind::WouldBlock)),
            std::future::ready(Ok(())),
        ));
        assert!(pending_result.is_ok());
    }

    #[test]
    fn connection_check_propagates_socket_errors_and_pending_state() {
        let socket = open(
            SocketDomain::Ipv4,
            SocketType::Stream,
            Some(SocketProtocol::Tcp),
        )
        .unwrap();
        let error = io::Error::from(io::ErrorKind::ConnectionRefused);

        assert_eq!(
            connected_with(
                &socket,
                |_| Err(io::Error::from(io::ErrorKind::PermissionDenied)),
                |_| unreachable!("peer address is not checked after an SO_ERROR failure"),
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(
            connected_with(
                &socket,
                |_| Ok(Some(error)),
                |_| unreachable!("peer address is not checked after SO_ERROR"),
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::ConnectionRefused
        );
        assert_eq!(
            connected_with(
                &socket,
                |_| Ok(None),
                |_| { Err(io::ErrorKind::NotConnected.into()) }
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(
            connected_with(
                &socket,
                |_| Ok(None),
                |_| { Err(io::ErrorKind::ConnectionRefused.into()) }
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::ConnectionRefused
        );
        let socket = open(
            SocketDomain::Ipv4,
            SocketType::Datagram,
            Some(SocketProtocol::Udp),
        )
        .unwrap();
        socket
            .connect(&"127.0.0.1:9".parse::<SocketAddr>().unwrap().into())
            .unwrap();
        assert!(connected(&socket).is_ok());
    }
}
