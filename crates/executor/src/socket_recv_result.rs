// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::SocketAddress;

/// Metadata from one receive. Only `bytes` bytes, spread across the buffers in order, were written; their initialized lengths remain unchanged.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SocketRecvResult {
    pub bytes: usize,
    /// The sender when supplied by the OS, usually for datagrams. Stream receives may return `None`; use `socket_peer_address` for the connected peer.
    pub address: Option<SocketAddress>,
    /// A datagram exceeded the total buffer length. With nonzero capacity, remaining bytes were discarded unless this was a peek. Zero-capacity receive behavior is OS-specific. `bytes` is the copied length, not the original datagram length.
    pub truncated: bool,
    /// The OS reported discarded ancillary data. This initial interface supplies no control buffer; ancillary data requires a separate capability.
    pub control_truncated: bool,
}
