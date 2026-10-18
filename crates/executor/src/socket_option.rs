// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// A typed socket-option identifier. This describes its value, not how an operating system represents or accesses it.
///
/// Backends implement `SocketGetOption<Self>` and/or `SocketSetOption<Self>` for each supported operation and option; `SocketOptions<Self>` automatically aggregates both. Additional option types can be defined without changing the core `SocketScheduler` trait or introducing a runtime option enum.
pub trait SocketOption: Send {
    type Value: Send;
}
