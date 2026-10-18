// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::cleanup::{self, Completion};
use crate::{AdviceMode, FileDescriptor, blocking_pool::BlockingPool};
use std::{io, os::fd::AsRawFd};

pub(crate) async fn run(
    pool: &BlockingPool,
    file: &FileDescriptor,
    offset: u64,
    length: u64,
    advice: AdviceMode,
) -> io::Result<()> {
    cleanup::wait(submit(pool, file, offset, length, advice).await?).await
}

pub(crate) async fn submit_only(
    pool: &BlockingPool,
    file: &FileDescriptor,
    offset: u64,
    length: u64,
    advice: AdviceMode,
) -> io::Result<()> {
    submit(pool, file, offset, length, advice).await.map(drop)
}

async fn submit(
    pool: &BlockingPool,
    file: &FileDescriptor,
    offset: u64,
    length: u64,
    advice: AdviceMode,
) -> io::Result<Completion> {
    if offset
        .checked_add(length)
        .is_none_or(|end| end > i64::MAX as u64)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "advice offset and range end must fit in i64",
        ));
    }
    let file = file.retain();
    cleanup::submit(pool, move || {
        let advice = match advice {
            AdviceMode::Normal => libc::POSIX_FADV_NORMAL,
            AdviceMode::Sequential => libc::POSIX_FADV_SEQUENTIAL,
            AdviceMode::Random => libc::POSIX_FADV_RANDOM,
            AdviceMode::WillNeed => libc::POSIX_FADV_WILLNEED,
            AdviceMode::DontNeed => libc::POSIX_FADV_DONTNEED,
            AdviceMode::NoReuse => libc::POSIX_FADV_NOREUSE,
        };
        // SAFETY: The retained file keeps the descriptor valid until this call returns. Range validation permits signed 64-bit offsets, including on 32-bit Linux.
        let result = unsafe {
            libc::posix_fadvise64(file.as_raw_fd(), offset as i64, length as i64, advice)
        };
        // Release backend ownership before completion wakes a caller that may immediately close the descriptor.
        drop(file);
        if result == 0 {
            Ok(())
        } else {
            // posix_fadvise returns an error number directly; errno is unrelated.
            Err(io::Error::from_raw_os_error(result))
        }
    })
    .await
}

#[cfg(test)]
pub(crate) mod tests;
