// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Foundational concurrency APIs for Socketry.
#![doc = include_str!("../readme.md")]

pub use socketry_executor as executor;
pub use socketry_executor::{
    Barrier, BufferResult, Cancellation, Cancelled, Clock, File, Interest, Scheduler,
    SchedulerHandle, Socket, Spawn, SpawnError, Task, TaskError, TaskHandle, defer_cancel,
    yield_now,
};

pub use socketry_timers as timers;

pub use socketry_executor::scheduler;
