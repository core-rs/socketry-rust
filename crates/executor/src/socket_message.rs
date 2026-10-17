// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::net::SocketAddr;

/// Owned scatter/gather payload and optional destination for one send. `None` uses the connected peer. The operation returns this value on success, error, and cancellation without copying its payload.
#[derive(Debug)]
pub struct SocketMessage<B = Vec<u8>> {
    pub buffers: Vec<B>,
    pub address: Option<SocketAddr>,
}
