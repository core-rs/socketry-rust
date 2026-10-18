// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{SocketAddress, SocketProtocol, SocketType};

/// An owned IP endpoint and its socket creation parameters. Obtain its domain with `SocketDomain::from(info.address)`; pass its type and protocol directly to `socket_open`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AddressInfo {
    pub address: SocketAddress,
    pub socket_type: SocketType,
    /// `None` represents the resolver's zero/default protocol.
    pub protocol: Option<SocketProtocol>,
}
