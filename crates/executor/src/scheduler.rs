// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Scheduler implementations and their I/O selectors.

pub mod selector;
pub mod socketry;

#[cfg(any(feature = "native", feature = "tokio"))]
mod cleanup;
#[cfg(any(feature = "native", feature = "tokio"))]
mod file_close_io;
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

mod socket_operation;
pub use socket_operation::SocketOperation;

mod socket_close;
pub use socket_close::SocketClose;

mod socket_shutdown;
pub use socket_shutdown::SocketShutdown;

mod socket_open;
pub use socket_open::SocketOpen;

mod socket_bind;
pub use socket_bind::SocketBind;

mod socket_listen;
pub use socket_listen::SocketListen;

mod socket_local_addr;
pub use socket_local_addr::SocketLocalAddr;

mod socket_peer_addr;
pub use socket_peer_addr::SocketPeerAddr;

mod socket_connect;
pub use socket_connect::SocketConnect;

mod socket_accept;
pub use socket_accept::SocketAccept;

mod socket_read;
pub use socket_read::SocketRead;

mod socket_write;
pub use socket_write::SocketWrite;

mod socket_wait;
pub use socket_wait::SocketWait;

mod socket_send_msg;
pub use socket_send_msg::SocketSendMsg;

mod socket_recv_msg;
pub use socket_recv_msg::SocketRecvMsg;

mod socket_get_option;
pub use socket_get_option::SocketGetOption;

mod socket_set_option;
pub use socket_set_option::SocketSetOption;

mod socket_scheduler;
pub use socket_scheduler::SocketScheduler;

mod socket_options;
pub use socket_options::SocketOptions;

#[cfg(any(feature = "native", feature = "tokio"))]
mod native_socket_option;

mod file_scheduler;
pub use file_scheduler::FileScheduler;

mod file_operation;
pub use file_operation::FileOperation;

mod file_open;
pub use file_open::FileOpen;

mod file_close;
pub use file_close::FileClose;

mod file_stat;
pub use file_stat::FileStat;

mod file_truncate;
pub use file_truncate::FileTruncate;

mod file_sync;
pub use file_sync::FileSync;

mod file_seek;
pub use file_seek::FileSeek;

mod file_read;
pub use file_read::FileRead;

mod file_write;
pub use file_write::FileWrite;

mod file_read_at;
pub use file_read_at::FileReadAt;

mod file_write_at;
pub use file_write_at::FileWriteAt;

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
