// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Boolean `SO_KEEPALIVE`: enable or disable OS keepalive probes, using the system's timing defaults. Probe intervals, counts, and application-level liveness policies are separate concerns.
#[derive(Clone, Copy, Debug)]
pub struct SocketKeepalive;

impl crate::SocketOption for SocketKeepalive {
    type Value = bool;
}
