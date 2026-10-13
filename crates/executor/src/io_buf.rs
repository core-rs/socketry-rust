// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::BufferSlice;
use std::ops::RangeBounds;

/// Owned, initialized storage suitable for completion-based I/O.
///
/// # Safety
/// `as_ref()` must always expose the same initialized allocation and length
/// while an operation owns this value, even if the value moves. The allocation
/// must remain alive until this value is dropped, and must not be mutated
/// through another owner during an operation. Implementations must not expose
/// inline storage whose address changes when the owner moves.
pub unsafe trait IoBuf: AsRef<[u8]> + Send + 'static {
    /// Limit an operation to an owned range without copying its bytes.
    /// Panics if the range is outside the initialized buffer.
    fn slice(self, range: impl RangeBounds<usize>) -> BufferSlice<Self>
    where
        Self: Sized,
    {
        BufferSlice::new(self, range)
    }
}

// SAFETY: Both owners retain a stable heap allocation as they move. Exclusive
// ownership prevents mutation by another owner while an operation holds them.
unsafe impl IoBuf for Vec<u8> {}
unsafe impl IoBuf for Box<[u8]> {}
