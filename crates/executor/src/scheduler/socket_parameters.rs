// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{SocketProtocol, SocketType};
#[cfg(unix)]
use libc as sys;
#[cfg(windows)]
use windows_sys::Win32::Networking::WinSock as sys;

pub(crate) fn socket_type(value: SocketType) -> i32 {
    match value {
        SocketType::Stream => sys::SOCK_STREAM,
        SocketType::Datagram => sys::SOCK_DGRAM,
        SocketType::Raw => sys::SOCK_RAW,
        SocketType::Other(value) => value,
    }
}

pub(crate) fn decode_socket_type(value: i32) -> SocketType {
    match value {
        sys::SOCK_STREAM => SocketType::Stream,
        sys::SOCK_DGRAM => SocketType::Datagram,
        sys::SOCK_RAW => SocketType::Raw,
        value => SocketType::Other(value),
    }
}

pub(crate) fn protocol(value: SocketProtocol) -> i32 {
    match value {
        SocketProtocol::Tcp => sys::IPPROTO_TCP,
        SocketProtocol::Udp => sys::IPPROTO_UDP,
        SocketProtocol::Other(value) => value,
    }
}

pub(crate) fn decode_protocol(value: i32) -> Option<SocketProtocol> {
    match value {
        0 => None,
        sys::IPPROTO_TCP => Some(SocketProtocol::Tcp),
        sys::IPPROTO_UDP => Some(SocketProtocol::Udp),
        value => Some(SocketProtocol::Other(value)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn socket_creation_parameters_preserve_native_values() {
        for value in [
            SocketType::Stream,
            SocketType::Datagram,
            SocketType::Raw,
            SocketType::Other(12345),
        ] {
            assert_eq!(decode_socket_type(socket_type(value)), value);
        }
        for value in [
            SocketProtocol::Tcp,
            SocketProtocol::Udp,
            SocketProtocol::Other(12345),
        ] {
            assert_eq!(decode_protocol(protocol(value)), Some(value));
        }
        assert_eq!(decode_protocol(0), None);
    }
}
