// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Windows socket readiness selector, using `polling`'s IOCP/AFD backend through
//! `async-io`. File operations use the blocking pool. This does not yet submit
//! native overlapped file reads or writes.
pub use super::readiness::{Selector, SocketDescriptor};
