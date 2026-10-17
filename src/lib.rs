// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Foundational concurrency APIs for Socketry.
#![doc = include_str!("../readme.md")]

pub use socketry_executor as executor;
pub use socketry_executor::{
    Blocking, BlockingError, BlockingOptions, BlockingTask, BufferResult, BufferSlice,
    Cancellation, Cancelled, Clock, File, FileOptions, Interest, IoBuf, IoBufMut, Notify,
    OperationError, OperationResult, Scheduler, SchedulerHandle, Signals, Socket,
    SocketRegistration, Spawn, SpawnError, SyncMode, Task, TaskError, TaskHandle, defer_cancel,
    yield_now,
};

#[cfg(all(unix, feature = "signals", any(feature = "native", feature = "tokio")))]
pub use socketry_executor::SignalRegistration;
#[cfg(all(unix, feature = "signals"))]
pub use socketry_executor::signal;

pub use socketry_timers as timers;

pub use socketry_executor::scheduler;
