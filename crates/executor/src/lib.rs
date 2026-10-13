// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Owned tasks and cooperative future polling on work-stealing workers.
//!
//! Tasks require `Send + 'static` and use the ordinary worker thread stack.
//! The scheduler owns spawned tasks even when their join handles are dropped.
#![doc = include_str!("../readme.md")]

mod cancellation;
mod cancelled;
mod defer_cancel;
mod notify;
pub mod scheduler;
#[cfg(all(unix, feature = "signals", any(feature = "native", feature = "tokio")))]
mod signal_registration;
mod spawn;
mod task;
mod worker;

pub use cancellation::Cancellation;
pub use cancelled::Cancelled;
pub use defer_cancel::defer_cancel;
pub use notify::Notify;
pub use scheduler::{
    BufferResult, Clock, File, Interest, Scheduler, SchedulerHandle, Signals, Socket,
};
#[cfg(all(unix, feature = "signals"))]
pub use signal_hook::consts::signal;
#[cfg(all(unix, feature = "signals", any(feature = "native", feature = "tokio")))]
pub use signal_registration::SignalRegistration;
pub use spawn::{Spawn, SpawnError};
pub use task::{Task, TaskError, TaskHandle, yield_now};
