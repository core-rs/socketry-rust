// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::OperationError;

#[test]
fn cancellation_before_execution_prevents_work_but_cannot_undo_progress() {
    let cancellation = Cancellation::new();
    cancellation.cancel();
    assert!(matches!(
        run::<()>(&cancellation, || panic!("cancelled work ran")),
        Err(OperationError::Cancelled(_))
    ));
    let cancellation = Cancellation::new();
    assert_eq!(
        run(&cancellation, || {
            cancellation.cancel();
            Ok(7)
        })
        .unwrap(),
        7
    );
    let error = run::<()>(&Cancellation::never(), || {
        Err(io::ErrorKind::PermissionDenied.into())
    })
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
}
