// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Owned payload and native metadata shared by readiness, Tokio, and completion operations.

use crate::{IoBuf, IoBufMut, SocketRecvFlags, SocketRecvResult, SocketSendFlags};
use std::{io, net::SocketAddr};

pub(super) fn validate<B: IoBuf>(buffers: &[B]) -> io::Result<()> {
    validate_lengths(
        buffers.len(),
        buffers.iter().map(|buffer| buffer.as_ref().len()),
    )
}

fn validate_lengths(count: usize, mut lengths: impl Iterator<Item = usize>) -> io::Result<()> {
    // CQEs carry a signed 32-bit result. Reject oversized payloads rather than silently changing a datagram; native vector counts must also fit every supported ABI.
    let total = lengths.try_fold(0_usize, usize::checked_add);
    if count > i32::MAX as usize || total.is_none_or(|total| total > i32::MAX as usize) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "message exceeds native length limits",
        ));
    }
    Ok(())
}

pub(super) fn send_flags(flags: SocketSendFlags) -> i32 {
    #[cfg(unix)]
    let (base, dont_route) = (libc::MSG_NOSIGNAL, libc::MSG_DONTROUTE);
    #[cfg(windows)]
    let (base, dont_route) = (0, windows_sys::Win32::Networking::WinSock::MSG_DONTROUTE);
    base | if flags.dont_route { dont_route } else { 0 }
}

pub(super) fn recv_flags(flags: SocketRecvFlags) -> i32 {
    #[cfg(unix)]
    let peek = libc::MSG_PEEK;
    #[cfg(windows)]
    let peek = windows_sys::Win32::Networking::WinSock::MSG_PEEK;
    if flags.peek { peek } else { 0 }
}

pub(super) struct Message<B> {
    buffers: Vec<B>,
    #[cfg(unix)]
    native: Box<NativeMessage>,
    #[cfg(windows)]
    address: Option<SocketAddr>,
    #[cfg(windows)]
    received: Option<SocketRecvResult>,
}

#[cfg(unix)]
struct NativeMessage {
    header: libc::msghdr,
    address: libc::sockaddr_storage,
    vectors: Vec<libc::iovec>,
}

// SAFETY: All pointers refer to this boxed metadata or the stable allocations owned by Message's IoBuf values. Metadata and payload move together between threads and are accessed exclusively. Completion requests retain the entire Message until the original terminal CQE; readiness calls use it only synchronously.
#[cfg(unix)]
unsafe impl Send for NativeMessage {}

impl<B: IoBuf> Message<B> {
    pub fn into_buffers(self) -> Vec<B> {
        self.buffers
    }

    pub fn new(
        mut buffers: Vec<B>,
        address: Option<SocketAddr>,
        pointer: fn(&mut B) -> *mut u8,
    ) -> Self {
        #[cfg(unix)]
        {
            let mut vectors = buffers
                .iter_mut()
                .map(|buffer| libc::iovec {
                    iov_base: pointer(buffer).cast(),
                    iov_len: buffer.as_ref().len(),
                })
                .collect::<Vec<_>>();
            // Some kernels reject zero iovec entries. A single zero-length entry still sends or consumes an empty datagram without changing the caller's buffer list.
            if vectors.is_empty() {
                vectors.push(libc::iovec {
                    iov_base: std::ptr::NonNull::<u8>::dangling().as_ptr().cast(),
                    iov_len: 0,
                });
            }
            // SAFETY: Zero is valid for the native header and address storage. No syscall observes these until their pointers and lengths are set below.
            let mut native = Box::new(NativeMessage {
                header: unsafe { std::mem::zeroed() },
                address: unsafe { std::mem::zeroed() },
                vectors,
            });
            native.header.msg_iov = native.vectors.as_mut_ptr();
            native.header.msg_iovlen = native.vectors.len() as _;
            if let Some(address) = address {
                let address = socket2::SockAddr::from(address);
                // SAFETY: SockAddr contains an initialized IP address that fits sockaddr_storage. The destination is distinct, aligned, and owned by this boxed message.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        address.as_ptr().cast::<u8>(),
                        (&mut native.address as *mut libc::sockaddr_storage).cast(),
                        address.len() as usize,
                    );
                }
                native.header.msg_name =
                    (&mut native.address as *mut libc::sockaddr_storage).cast();
                native.header.msg_namelen = address.len();
            }
            Self { buffers, native }
        }
        #[cfg(windows)]
        {
            let _ = (&mut buffers, pointer);
            Self {
                buffers,
                address,
                received: None,
            }
        }
    }

    #[cfg(unix)]
    pub fn send_header(&mut self) -> *const libc::msghdr {
        &self.native.header
    }

    #[cfg(unix)]
    pub fn recv_header(&mut self) -> *mut libc::msghdr {
        // Restore output lengths before each retry. A failed syscall may have changed metadata.
        self.native.header.msg_name =
            (&mut self.native.address as *mut libc::sockaddr_storage).cast();
        self.native.header.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as _;
        self.native.header.msg_flags = 0;
        &mut self.native.header
    }

    pub fn send(&mut self, socket: &socket2::Socket, flags: SocketSendFlags) -> io::Result<usize> {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            // SAFETY: The message owns all initialized payload, address, vector, and header storage through this synchronous nonblocking syscall. No control data or zero-copy lifetime extensions are requested.
            syscall_result(unsafe {
                libc::sendmsg(socket.as_raw_fd(), self.send_header(), send_flags(flags))
            })
        }
        #[cfg(windows)]
        {
            let mut buffers = self
                .buffers
                .iter()
                .map(|buffer| io::IoSlice::new(buffer.as_ref()))
                .collect::<Vec<_>>();
            if buffers.is_empty() {
                buffers.push(io::IoSlice::new(&[]));
            }
            match self.address {
                Some(address) => {
                    socket.send_to_vectored_with_flags(&buffers, &address.into(), send_flags(flags))
                }
                None => socket.send_vectored_with_flags(&buffers, send_flags(flags)),
            }
        }
    }

    pub fn received(&self, bytes: usize) -> SocketRecvResult {
        #[cfg(unix)]
        {
            let address = if self.native.header.msg_namelen == 0 {
                None
            } else {
                // SAFETY: The OS supplied a nonempty address in the zero-initialized storage. Copying it preserves the returned family and length; only supported IP addresses are decoded.
                let (_, address) = unsafe {
                    socket2::SockAddr::try_init(|storage, length| {
                        storage
                            .cast::<libc::sockaddr_storage>()
                            .write(self.native.address);
                        *length = self.native.header.msg_namelen;
                        Ok::<(), io::Error>(())
                    })
                }
                .expect("copying completed address metadata is infallible");
                address.as_socket()
            };
            SocketRecvResult {
                bytes,
                address,
                truncated: self.native.header.msg_flags & libc::MSG_TRUNC != 0,
                control_truncated: self.native.header.msg_flags & libc::MSG_CTRUNC != 0,
            }
        }
        #[cfg(windows)]
        {
            let mut result = self
                .received
                .expect("successful receive records its metadata");
            result.bytes = bytes;
            result
        }
    }
}

impl<B: IoBufMut> Message<B> {
    pub fn recv(&mut self, socket: &socket2::Socket, flags: SocketRecvFlags) -> io::Result<usize> {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            // SAFETY: IoBufMut provides stable, exclusive initialized storage. All metadata remains alive through this synchronous nonblocking syscall, which writes at most the supplied capacity.
            syscall_result(unsafe {
                libc::recvmsg(socket.as_raw_fd(), self.recv_header(), recv_flags(flags))
            })
        }
        #[cfg(windows)]
        {
            let mut buffers = self
                .buffers
                .iter_mut()
                .map(|buffer| {
                    let buffer = buffer.as_mut();
                    // SAFETY: Reinterpreting initialized bytes as MaybeUninit preserves their allocation and exclusive borrow; Winsock cannot write past their length.
                    socket2::MaybeUninitSlice::new(unsafe {
                        std::slice::from_raw_parts_mut(buffer.as_mut_ptr().cast(), buffer.len())
                    })
                })
                .collect::<Vec<_>>();
            if buffers.is_empty() {
                buffers.push(socket2::MaybeUninitSlice::new(&mut []));
            }
            let (bytes, flags, address) =
                socket.recv_from_vectored_with_flags(&mut buffers, recv_flags(flags))?;
            self.received = Some(SocketRecvResult {
                bytes,
                address: address.as_socket(),
                truncated: flags.is_truncated(),
                control_truncated: false,
            });
            Ok(bytes)
        }
    }
}

#[cfg(unix)]
fn syscall_result(result: libc::ssize_t) -> io::Result<usize> {
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn missing_control_storage_reports_discarded_packet_metadata() {
        use std::os::fd::AsRawFd;
        let receiver =
            socket2::Socket::new(socket2::Domain::IPV6, socket2::Type::DGRAM, None).unwrap();
        receiver
            .bind(&"[::1]:0".parse::<SocketAddr>().unwrap().into())
            .unwrap();
        receiver
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let enabled: libc::c_int = 1;
        // SAFETY: setsockopt reads one initialized integer and does not retain its address.
        let result = unsafe {
            libc::setsockopt(
                receiver.as_raw_fd(),
                libc::IPPROTO_IPV6,
                libc::IPV6_RECVPKTINFO,
                (&enabled as *const libc::c_int).cast(),
                std::mem::size_of_val(&enabled) as _,
            )
        };
        assert_eq!(result, 0, "{}", io::Error::last_os_error());
        let sender =
            socket2::Socket::new(socket2::Domain::IPV6, socket2::Type::DGRAM, None).unwrap();
        sender
            .send_to(b"x", &receiver.local_addr().unwrap())
            .unwrap();
        let mut message = Message::new(vec![vec![0; 8]], None, |buffer| buffer.as_mut_ptr());
        let bytes = message.recv(&receiver, SocketRecvFlags::default()).unwrap();
        let received = message.received(bytes);
        assert_eq!(received.bytes, 1);
        assert!(!received.truncated);
        assert!(received.control_truncated);
        assert_eq!(&message.into_buffers()[0][..1], b"x");
    }

    #[test]
    fn message_lengths_reject_overflow_without_shortening_payloads() {
        for (count, lengths) in [
            (i32::MAX as usize + 1, vec![]),
            (1, vec![i32::MAX as usize + 1]),
            (2, vec![usize::MAX, 1]),
        ] {
            assert_eq!(
                validate_lengths(count, lengths.into_iter())
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
        }
        validate_lengths(1, [i32::MAX as usize].into_iter()).unwrap();
        validate_lengths(0, [].into_iter()).unwrap();
    }
}
