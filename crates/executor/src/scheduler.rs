// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Scheduler implementations and their I/O selectors.
use std::io;

pub mod selector;
pub mod socketry;

#[cfg(any(feature = "native", feature = "tokio"))]
mod positioned_file;

#[cfg(feature = "tokio")]
pub mod tokio;

pub use socketry::{Scheduler, SchedulerHandle};
pub(crate) use socketry::{Shared, enter};

/// An operation result together with its reusable, owned buffer.
///
/// Reads fill the existing buffer length and leave its length unchanged. Only
/// the first `result?` bytes contain newly read data. Writes may be partial.
/// The buffer is returned on both success and failure.
pub type BufferResult = (io::Result<usize>, Vec<u8>);

mod interest;
pub use interest::Interest;

mod socket;
pub use socket::Socket;

mod file;
pub use file::File;

mod clock;
pub use clock::Clock;
