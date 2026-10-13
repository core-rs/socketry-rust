// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![doc = include_str!("../readme.md")]

mod timer_handle;
mod timers;

pub use timer_handle::TimerHandle;
pub use timers::Timers;

#[cfg(feature = "driver")]
pub mod driver;

#[cfg(feature = "sharded")]
pub mod sharded;

#[cfg(feature = "driver")]
pub use timers::SharedTimerProducer;
