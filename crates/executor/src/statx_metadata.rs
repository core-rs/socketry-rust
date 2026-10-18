// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{FileMetadata, FileType};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Linux status retained in its native `statx` layout. io_uring writes this value inside its existing request allocation and returns it by value after original completion. No separate output allocation or eager field conversion is required; return-value copy elimination is an optimization, not a guarantee.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct StatxMetadata(pub(crate) libc::statx);

impl StatxMetadata {
    /// Borrow all native fields. Consult `stx_mask` before using fields whose availability it describes; unprovided fields can contain dummy values.
    #[inline]
    pub fn as_statx(&self) -> &libc::statx {
        &self.0
    }

    pub(crate) fn empty() -> Self {
        // SAFETY: `statx` contains only integer fields and reserved integer storage, all of which admit zero. The zero validity mask marks every optional field absent before kernel completion.
        Self(unsafe { std::mem::zeroed() })
    }

    fn time(&self, mask: u32, value: libc::statx_timestamp) -> Option<SystemTime> {
        if self.0.stx_mask & mask == 0 {
            return None;
        }
        timestamp(value.tv_sec, value.tv_nsec)
    }
}

impl std::fmt::Debug for StatxMetadata {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StatxMetadata")
            .field("mask", &self.0.stx_mask)
            .field("file_type", &self.file_type())
            .field("size", &self.size())
            .field("accessed", &self.accessed())
            .field("modified", &self.modified())
            .field("created", &self.created())
            .finish_non_exhaustive()
    }
}

impl FileMetadata for StatxMetadata {
    #[inline]
    fn file_type(&self) -> Option<FileType> {
        if self.0.stx_mask & libc::STATX_TYPE == 0 {
            return None;
        }
        Some(match u32::from(self.0.stx_mode) & libc::S_IFMT {
            libc::S_IFREG => FileType::File,
            libc::S_IFDIR => FileType::Directory,
            libc::S_IFLNK => FileType::Symlink,
            _ => FileType::Other,
        })
    }

    #[inline]
    fn size(&self) -> Option<u64> {
        (self.0.stx_mask & libc::STATX_SIZE != 0).then_some(self.0.stx_size)
    }

    #[inline]
    fn accessed(&self) -> Option<SystemTime> {
        self.time(libc::STATX_ATIME, self.0.stx_atime)
    }

    #[inline]
    fn modified(&self) -> Option<SystemTime> {
        self.time(libc::STATX_MTIME, self.0.stx_mtime)
    }

    #[inline]
    fn created(&self) -> Option<SystemTime> {
        self.time(libc::STATX_BTIME, self.0.stx_btime)
    }
}

fn timestamp(seconds: i64, nanoseconds: u32) -> Option<SystemTime> {
    if nanoseconds >= 1_000_000_000 {
        return None;
    }
    // Negative timestamps use floor seconds: (-1, 500_000_000) is half a second before the epoch. Combine the magnitude before checking the platform's representable time range.
    let fraction = Duration::from_nanos(u64::from(nanoseconds));
    if seconds < 0 {
        UNIX_EPOCH.checked_sub(Duration::from_secs(seconds.unsigned_abs()) - fraction)
    } else {
        UNIX_EPOCH.checked_add(Duration::from_secs(seconds as u64) + fraction)
    }
}

#[cfg(test)]
mod tests;
