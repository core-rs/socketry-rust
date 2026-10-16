// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::OperationError;

#[test]
fn cancellation_before_poll_prevents_execution_and_ready_progress_wins_a_race() {
    futures_lite::future::block_on(async {
        let cancellation = Cancellation::new();
        cancellation.cancel();
        assert!(matches!(
            cancellable::<()>(&cancellation, async {
                panic!("pre-cancelled operation polled")
            })
            .await,
            Err(OperationError::Cancelled(_))
        ));
        let cancellation = Cancellation::new();
        let result = cancellable(
            &cancellation,
            poll_fn(|_| {
                cancellation.cancel();
                Poll::Ready(Ok(42))
            }),
        )
        .await;
        assert_eq!(result.unwrap(), 42);
        let cancellation = Cancellation::new();
        let result = cancellable::<()>(
            &cancellation,
            poll_fn(|_| {
                cancellation.cancel();
                Poll::Pending
            }),
        )
        .await;
        assert!(matches!(result, Err(OperationError::Cancelled(_))));
    });
}
