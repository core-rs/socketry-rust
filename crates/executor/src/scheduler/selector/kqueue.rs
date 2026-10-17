// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Apple/BSD readiness selector. `async-io` uses `polling`'s kqueue backend.
//! Registrations are reused across operations and task migration.
pub use super::readiness::{Selector, SocketDescriptor};
