// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Owned tasks and cooperative future polling on work-stealing workers.
//!
//! Tasks require `Send + 'static` and use the ordinary worker thread stack.
//! Task handles own execution lifetimes; dropping one abandons its task.
#![doc = include_str!("../readme.md")]

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
mod file_options;
mod io_buf;
mod io_buf_mut;
mod notify;
mod operation_error;
pub mod scheduler;
#[cfg(all(unix, feature = "signals", any(feature = "native", feature = "tokio")))]
mod signal_registration;
mod spawn;
mod sync_mode;
mod task;
mod worker;

pub use allocation_mode::AllocationMode;
pub use blocking::Blocking;
pub use blocking_error::BlockingError;
pub use blocking_options::BlockingOptions;
pub use blocking_task::BlockingTask;
pub use buffer_slice::BufferSlice;
pub use cancellation::Cancellation;
pub use cancelled::Cancelled;
pub use defer_cancel::defer_cancel;
pub use file_options::FileOptions;
pub use io_buf::IoBuf;
pub use io_buf_mut::IoBufMut;
pub use notify::Notify;
pub use operation_error::{OperationError, OperationResult};
pub use scheduler::{
    BufferResult, Clock, File, FileAllocate, Interest, Scheduler, SchedulerHandle, Signals, Socket,
    SocketRegistration,
};
#[cfg(all(unix, feature = "signals"))]
pub use signal_hook::consts::signal;
#[cfg(all(unix, feature = "signals", any(feature = "native", feature = "tokio")))]
pub use signal_registration::SignalRegistration;
pub use spawn::{Spawn, SpawnError};
pub use sync_mode::SyncMode;
pub use task::{Task, TaskError, TaskHandle, yield_now};
