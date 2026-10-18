// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Scheduler implementations and their I/O selectors.

pub mod selector;
pub mod socketry;

#[cfg(any(feature = "native", feature = "tokio"))]
mod cleanup;
#[cfg(any(feature = "native", feature = "tokio"))]
mod file_close;
#[cfg(any(feature = "native", feature = "tokio"))]
mod file_io;

#[cfg(feature = "tokio")]
pub mod tokio;

pub use socketry::{Scheduler, SchedulerHandle};
pub(crate) use socketry::{Shared, enter};

mod address_resolution;
pub use address_resolution::AddressResolution;

/// An operation result together with its reusable, owned buffer.
///
/// Reads fill the existing buffer length and leave its length unchanged. Only
/// the first `result?` bytes contain newly read data. Writes may be partial.
/// The buffer is returned on both success and failure.
pub type BufferResult<B = Vec<u8>> = (crate::OperationResult<usize>, B);

#[cfg(any(feature = "native", feature = "tokio"))]
mod blocking;
#[cfg(any(feature = "native", feature = "tokio"))]
mod operation;

mod socket_registration;
pub use socket_registration::SocketRegistration;

mod interest;
pub use interest::Interest;

mod socket;
pub use socket::Socket;

mod socket_options;
pub use socket_options::SocketOptions;

#[cfg(any(feature = "native", feature = "tokio"))]
mod native_socket_option;

mod file;
pub use file::File;

mod file_advice;
pub use file_advice::FileAdvice;

#[cfg(all(target_os = "linux", any(feature = "native", feature = "tokio")))]
mod file_advice_io;

mod file_allocate;
pub use file_allocate::FileAllocate;

mod clock;
pub use clock::Clock;

mod signals;
pub use signals::Signals;

#[cfg(any(feature = "native", feature = "tokio"))]
mod socket_io;

mod socket_messages;
pub use socket_messages::SocketMessages;

#[cfg(any(feature = "native", feature = "tokio"))]
mod message_io;

#[cfg(any(feature = "native", feature = "tokio"))]
mod socket_parameters;
