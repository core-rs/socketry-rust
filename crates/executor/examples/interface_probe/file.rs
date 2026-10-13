// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use socketry_executor::BufferResult;
use std::{future::Future, io, path::Path};

/// Deliberately small opening modes for this experiment, not portable flags.
#[derive(Clone, Copy)]
pub enum Open {
    Read,
    Create,
}

/// Provisional subset: creation and associated resources are the variables
/// being tested. Cancellation and general IoBuf contracts remain design work.
pub trait File: Send + Sync {
    type File: Send + Sync;

    fn file_open(
        &self,
        path: &Path,
        mode: Open,
    ) -> impl Future<Output = io::Result<Self::File>> + Send;
    fn file_read_at(
        &self,
        file: &Self::File,
        buffer: Vec<u8>,
        offset: u64,
    ) -> impl Future<Output = BufferResult> + Send;
    fn file_write_at(
        &self,
        file: &Self::File,
        buffer: Vec<u8>,
        offset: u64,
    ) -> impl Future<Output = BufferResult> + Send;
}
