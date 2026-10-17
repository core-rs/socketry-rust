// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Boolean `SO_REUSEADDR`. Set before binding when required by the application.
///
/// This preserves OS-specific address-sharing rules, including Windows allowing an already-used address to be rebound. It does not provide portable exclusive binding or `SO_REUSEPORT` semantics.
#[derive(Clone, Copy, Debug)]
pub struct SocketReuseAddress;

impl crate::SocketOption for SocketReuseAddress {
    type Value = bool;
}
