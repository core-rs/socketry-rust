// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::FileType;
use std::time::SystemTime;

/// Portable accessors for owned file status returned by [`crate::FileStat::file_stat`]. Each backend chooses its representation, so native results need no eager field conversion and simulations need no OS resources.
///
/// `None` means a field was not supplied or cannot be represented. Fields can reflect different instants during concurrent modification; metadata is an observation, not an atomic snapshot or synchronization operation. Native fields remain available through the concrete result type.
pub trait FileMetadata: Send + Sync + 'static {
    fn file_type(&self) -> Option<FileType>;

    /// Logical length in bytes for regular files. Other file types follow the platform's meaning; this is not allocated disk space.
    fn size(&self) -> Option<u64>;

    /// Last access time. Filesystem policies can suppress or defer updates.
    fn accessed(&self) -> Option<SystemTime>;

    /// Last content modification time.
    fn modified(&self) -> Option<SystemTime>;

    /// Creation time. Never substituted with Unix status-change time.
    fn created(&self) -> Option<SystemTime>;
}
