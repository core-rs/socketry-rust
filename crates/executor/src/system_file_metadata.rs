// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{FileMetadata, FileType};
use std::{fs::Metadata, time::SystemTime};

/// File status retained in the standard library's platform-specific representation. Readiness and Tokio return this type; common accessors convert only the requested field.
#[repr(transparent)]
#[derive(Clone, Debug)]
pub struct SystemFileMetadata(Metadata);

impl SystemFileMetadata {
    /// Borrow the original metadata, including platform-specific extension methods such as Unix `MetadataExt::ino` or Windows `MetadataExt::file_attributes`.
    #[inline]
    pub fn as_std(&self) -> &Metadata {
        &self.0
    }
}

impl From<Metadata> for SystemFileMetadata {
    #[inline]
    fn from(metadata: Metadata) -> Self {
        Self(metadata)
    }
}

impl FileMetadata for SystemFileMetadata {
    #[inline]
    fn file_type(&self) -> Option<FileType> {
        let kind = self.0.file_type();
        Some(classify(kind.is_file(), kind.is_dir(), kind.is_symlink()))
    }

    #[inline]
    fn size(&self) -> Option<u64> {
        Some(self.0.len())
    }

    #[inline]
    fn accessed(&self) -> Option<SystemTime> {
        self.0.accessed().ok()
    }

    #[inline]
    fn modified(&self) -> Option<SystemTime> {
        self.0.modified().ok()
    }

    #[inline]
    fn created(&self) -> Option<SystemTime> {
        self.0.created().ok()
    }
}

#[inline]
fn classify(file: bool, directory: bool, symlink: bool) -> FileType {
    match (file, directory, symlink) {
        (true, _, _) => FileType::File,
        (_, true, _) => FileType::Directory,
        (_, _, true) => FileType::Symlink,
        _ => FileType::Other,
    }
}

#[cfg(test)]
mod tests;
