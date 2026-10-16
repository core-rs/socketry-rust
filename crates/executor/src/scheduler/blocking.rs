// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Cancellation, OperationResult};
use std::io;

/// Called on the selected pool worker, after queueing. A request can prevent
/// execution here; once the closure starts, its actual result is preserved.
pub(crate) fn run<T>(
    cancellation: &Cancellation,
    operation: impl FnOnce() -> io::Result<T>,
) -> OperationResult<T> {
    cancellation.check()?;
    operation().map_err(Into::into)
}

#[cfg(test)]
mod tests;
