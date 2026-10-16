// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::IoBuf;

/// Owned writable storage. Operations fill its existing initialized length;
/// they never expose uninitialized capacity or change the buffer length.
///
/// # Safety
/// In addition to `IoBuf`, `as_mut()` must expose the same allocation and range
/// as `as_ref()`, with exclusive access throughout an operation. No other owner
/// may read or write that range while the operation owns the buffer.
pub unsafe trait IoBufMut: IoBuf + AsMut<[u8]> {}

// SAFETY: Vec and Box provide exclusive access to their stable allocation.
unsafe impl IoBufMut for Vec<u8> {}
unsafe impl IoBufMut for Box<[u8]> {}
