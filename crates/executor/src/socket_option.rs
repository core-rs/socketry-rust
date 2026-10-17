// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// A typed socket-option identifier. This describes its value, not how an operating system represents or accesses it.
///
/// Backends implement `SocketOptions<Self>` separately for each supported option. Additional option types can be defined without changing the core `Socket` trait or introducing a runtime option enum.
pub trait SocketOption: Send {
    type Value: Send;
}
