// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::TaskError;
use std::fmt;
use std::future::Future;

/// The common spawning contract for schedulers and their handles.
pub trait Spawn {
    /// The runtime's awaitable task handle. Runtime adapters may use their own
    /// representation while preserving the ownership and result contract.
    type Handle<Output>: Future<Output = Result<Output, TaskError>> + Send
    where
        Output: Send + 'static;

    /// Register a task with this scheduler before making it runnable.
    ///
    /// Dropping the returned join handle does not cancel the owned task.
    /// If the scheduler is closed, the future is dropped without being polled.
    fn spawn<FutureType>(
        &self,
        future: FutureType,
    ) -> Result<Self::Handle<FutureType::Output>, SpawnError>
    where
        FutureType: Future + Send + 'static,
        FutureType::Output: Send + 'static;
}

/// A task could not be registered with its scheduler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpawnError {
    /// The scheduler has begun shutting down.
    SchedulerClosed,
    /// All task identifiers have been used.
    IdentifiersExhausted,
}

impl fmt::Display for SpawnError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::SchedulerClosed => "the scheduler is closed",
            Self::IdentifiersExhausted => "task identifiers are exhausted",
        })
    }
}

impl std::error::Error for SpawnError {}

#[cfg(test)]
mod tests;
