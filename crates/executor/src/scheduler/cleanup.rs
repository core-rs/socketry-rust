// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{BlockingError, TaskError, blocking_pool::BlockingPool};
use futures_channel::oneshot;
use std::io;

pub(crate) type Completion = oneshot::Receiver<Result<io::Result<()>, TaskError>>;

pub(crate) async fn run(
    pool: &BlockingPool,
    operation: impl FnOnce() -> io::Result<()> + Send + 'static,
) -> io::Result<()> {
    wait(submit(pool, operation).await?).await
}

pub(crate) async fn submit_only(
    pool: &BlockingPool,
    operation: impl FnOnce() -> io::Result<()> + Send + 'static,
) -> io::Result<()> {
    submit(pool, operation).await.map(drop)
}

pub(crate) async fn submit(
    pool: &BlockingPool,
    operation: impl FnOnce() -> io::Result<()> + Send + 'static,
) -> io::Result<Completion> {
    pool.submit(operation).await.map_err(|error| match error {
        BlockingError::Io(error) => error,
        BlockingError::SchedulerClosed => {
            io::Error::new(io::ErrorKind::BrokenPipe, "scheduler is closed")
        }
        BlockingError::Cancelled => unreachable!("cleanup admission has no cancellation input"),
    })
}

pub(crate) async fn wait(completion: Completion) -> io::Result<()> {
    completion
        .await
        .map_err(io::Error::other)?
        .map_err(|error| io::Error::other(error.to_string()))?
}

// Direct readiness selector consumers use a process-wide fallback, like their existing file operations. Scheduler consumers instead use their own bounded pool and shutdown accounting.
#[cfg(feature = "native")]
pub(crate) fn standalone_pool() -> &'static BlockingPool {
    static POOL: std::sync::OnceLock<BlockingPool> = std::sync::OnceLock::new();
    POOL.get_or_init(|| BlockingPool::new(crate::BlockingOptions::default()))
}

#[cfg(test)]
pub(crate) mod tests;
