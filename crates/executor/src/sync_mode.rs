// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// The file data and metadata requested by `FileSync::file_sync`. A backend may provide stronger synchronization than requested.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncMode {
    /// Synchronize file contents and metadata, like `std::fs::File::sync_all`.
    All,
    /// Synchronize file contents and metadata needed to retrieve them. Other metadata may be omitted, like `std::fs::File::sync_data`.
    Data,
}
