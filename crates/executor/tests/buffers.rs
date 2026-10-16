// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use socketry_executor::{Cancelled, IoBuf, OperationError};
use std::{error::Error, io, ops::Bound};

#[test]
fn owned_ranges_preserve_storage_and_validate_boundaries() {
    let buffer = vec![1, 2, 3, 4];
    let allocation = buffer.as_ptr();
    let mut slice = buffer.slice(1..=2);
    assert_eq!(slice.as_ref(), &[2, 3]);
    slice.as_mut()[0] = 7;
    let slice = slice.slice(..1);
    assert_eq!(slice.as_ref(), &[7]);
    let buffer = slice.into_inner().into_inner();
    assert_eq!(buffer.as_ptr(), allocation);
    assert_eq!(buffer, vec![1, 7, 3, 4]);
    assert_eq!(
        buffer
            .slice((Bound::Excluded(0), Bound::Unbounded))
            .as_ref(),
        &[7, 3, 4]
    );
    assert!(Vec::new().slice(..).as_ref().is_empty());
    assert_eq!(
        vec![1].into_boxed_slice().slice(..).into_inner().as_ref(),
        &[1]
    );
    for range in [
        (Bound::Included(2), Bound::Excluded(1)),
        (Bound::Included(0), Bound::Included(4)),
        (Bound::Excluded(usize::MAX), Bound::Unbounded),
        (Bound::Unbounded, Bound::Included(usize::MAX)),
    ] {
        assert!(std::panic::catch_unwind(|| vec![0; 4].slice(range)).is_err());
    }
}

#[test]
fn operation_errors_keep_cancellation_and_os_errors_distinct() {
    let cancelled = OperationError::from(Cancelled);
    assert_eq!(cancelled.kind(), io::ErrorKind::Interrupted);
    assert_eq!(cancelled.raw_os_error(), None);
    assert!(cancelled.source().unwrap().is::<Cancelled>());
    assert_eq!(cancelled.to_string(), Cancelled.to_string());
    let io_cancelled: io::Error = cancelled.into();
    assert!(io_cancelled.get_ref().unwrap().is::<Cancelled>());
    let error = OperationError::from(io::Error::from_raw_os_error(2));
    assert_eq!(error.raw_os_error(), Some(2));
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    assert!(error.source().unwrap().is::<io::Error>());
    assert_eq!(
        error.to_string(),
        io::Error::from_raw_os_error(2).to_string()
    );
    let error: io::Error = error.into();
    assert_eq!(error.raw_os_error(), Some(2));
}
