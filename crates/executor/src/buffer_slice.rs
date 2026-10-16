// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{IoBuf, IoBufMut};
use std::ops::{Bound, Range, RangeBounds};

/// An owned view into initialized storage. Dropping it drops its owner;
/// `into_inner` recovers the complete buffer after an operation returns it.
#[derive(Debug)]
pub struct BufferSlice<B> {
    buffer: B,
    range: Range<usize>,
}

impl<B: IoBuf> BufferSlice<B> {
    pub(crate) fn new(buffer: B, range: impl RangeBounds<usize>) -> Self {
        let start = match range.start_bound() {
            Bound::Included(&start) => start,
            Bound::Excluded(&start) => start.checked_add(1).expect("buffer range start overflow"),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&end) => end.checked_add(1).expect("buffer range end overflow"),
            Bound::Excluded(&end) => end,
            Bound::Unbounded => buffer.as_ref().len(),
        };
        assert!(
            start <= end && end <= buffer.as_ref().len(),
            "buffer range out of bounds"
        );
        Self {
            buffer,
            range: start..end,
        }
    }

    pub fn into_inner(self) -> B {
        self.buffer
    }
}

impl<B: IoBuf> AsRef<[u8]> for BufferSlice<B> {
    fn as_ref(&self) -> &[u8] {
        &self.buffer.as_ref()[self.range.clone()]
    }
}

impl<B: IoBufMut> AsMut<[u8]> for BufferSlice<B> {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.buffer.as_mut()[self.range.clone()]
    }
}

// SAFETY: The view retains its owner and always exposes the same validated
// subrange. Moving the view does not move or resize the backing allocation.
unsafe impl<B: IoBuf> IoBuf for BufferSlice<B> {}
unsafe impl<B: IoBufMut> IoBufMut for BufferSlice<B> {}
