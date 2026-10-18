// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::cleanup::{self, Completion, wait};
use crate::{FileDescriptor, blocking_pool::BlockingPool};
use std::io;

pub(crate) async fn close(pool: &BlockingPool, file: FileDescriptor) -> io::Result<()> {
    wait(submit(pool, file).await?).await
}

pub(crate) async fn submit_only(pool: &BlockingPool, file: FileDescriptor) -> io::Result<()> {
    submit(pool, file).await.map(drop)
}

async fn submit(pool: &BlockingPool, file: FileDescriptor) -> io::Result<Completion> {
    // Establish exclusivity before admission. Never wait for or interrupt other uses of this handle.
    let file = file.into_file();
    cleanup::submit(pool, move || super::file_io::close(file)).await
}

#[cfg(test)]
mod tests;
