// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Owned tasks and cooperative future polling on work-stealing workers.
//!
//! Tasks require `Send + 'static` and use the ordinary worker thread stack.
//! Task handles own execution lifetimes; dropping one abandons its task.
#![doc = include_str!("../readme.md")]

mod address_hints;
mod address_info;
mod address_resolve_error;
mod address_resolve_error_kind;
mod address_service;
mod advice_mode;
mod allocation_mode;
mod blocking;
mod blocking_error;
mod blocking_options;
mod blocking_pool;
mod blocking_task;
mod buffer_slice;
mod cancellation;
mod cancelled;
mod defer_cancel;
mod file_descriptor;
mod file_metadata;
mod file_options;
mod file_type;
mod io_buf;
mod io_buf_mut;
mod notify;
mod operation_error;
pub mod scheduler;
#[cfg(all(unix, feature = "signals", any(feature = "native", feature = "tokio")))]
mod signal_registration;
mod socket_domain;
mod socket_keepalive;
mod socket_message;
mod socket_option;
mod socket_protocol;
mod socket_recv_flags;
mod socket_recv_result;
mod socket_reuse_address;
mod socket_send_flags;
mod socket_type;
mod spawn;
#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod statx_metadata;
mod sync_mode;
mod system_file_metadata;
mod task;
mod tcp_no_delay;
mod worker;

pub use address_hints::AddressHints;
pub use address_info::AddressInfo;
pub use address_resolve_error::AddressResolveError;
pub use address_resolve_error_kind::AddressResolveErrorKind;
pub use address_service::AddressService;
pub use advice_mode::AdviceMode;
pub use allocation_mode::AllocationMode;
pub use blocking::Blocking;
pub use blocking_error::BlockingError;
pub use blocking_options::BlockingOptions;
pub use blocking_task::BlockingTask;
pub use buffer_slice::BufferSlice;
pub use cancellation::Cancellation;
pub use cancelled::Cancelled;
pub use defer_cancel::defer_cancel;
pub use file_descriptor::FileDescriptor;
pub use file_metadata::FileMetadata;
pub use file_options::FileOptions;
pub use file_type::FileType;
pub use io_buf::IoBuf;
pub use io_buf_mut::IoBufMut;
pub use notify::Notify;
pub use operation_error::{OperationError, OperationResult};
pub use scheduler::{
    AddressResolution, BufferResult, Clock, File, FileAdvice, FileAllocate, Interest, Scheduler,
    SchedulerHandle, Signals, Socket, SocketMessages, SocketOptions, SocketRegistration,
};
#[cfg(all(unix, feature = "signals"))]
pub use signal_hook::consts::signal;
#[cfg(all(unix, feature = "signals", any(feature = "native", feature = "tokio")))]
pub use signal_registration::SignalRegistration;
pub use socket_domain::SocketDomain;
pub use socket_keepalive::SocketKeepalive;
pub use socket_message::SocketMessage;
pub use socket_option::SocketOption;
pub use socket_protocol::SocketProtocol;
pub use socket_recv_flags::SocketRecvFlags;
pub use socket_recv_result::SocketRecvResult;
pub use socket_reuse_address::SocketReuseAddress;
pub use socket_send_flags::SocketSendFlags;
pub use socket_type::SocketType;
pub use spawn::{Spawn, SpawnError};
#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use statx_metadata::StatxMetadata;
pub use sync_mode::SyncMode;
pub use system_file_metadata::SystemFileMetadata;
pub use task::{Task, TaskError, TaskHandle, yield_now};
pub use tcp_no_delay::TcpNoDelay;

#[cfg(feature = "native")]
pub use scheduler::selector::readiness::SocketDescriptor;
