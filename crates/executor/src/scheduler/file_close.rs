// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{BlockingError, FileHandle, TaskError, blocking_pool::BlockingPool};
use futures_channel::oneshot;
use std::io;

type Completion = oneshot::Receiver<Result<io::Result<()>, TaskError>>;

pub(crate) async fn close(pool: &BlockingPool, file: FileHandle) -> io::Result<()> {
    wait(submit(pool, file).await?).await
}

pub(crate) async fn submit_only(pool: &BlockingPool, file: FileHandle) -> io::Result<()> {
    submit(pool, file).await.map(drop)
}

async fn submit(pool: &BlockingPool, file: FileHandle) -> io::Result<Completion> {
    // Establish exclusivity before admission. Never wait for or interrupt other uses of this handle.
    let file = file.into_file();
    pool.submit(move || super::file_io::close(file))
        .await
        .map_err(|error| match error {
            BlockingError::Io(error) => error,
            BlockingError::SchedulerClosed => {
                io::Error::new(io::ErrorKind::BrokenPipe, "scheduler is closed")
            }
            BlockingError::Cancelled => unreachable!("close admission has no cancellation input"),
        })
}

async fn wait(completion: Completion) -> io::Result<()> {
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
mod tests;
