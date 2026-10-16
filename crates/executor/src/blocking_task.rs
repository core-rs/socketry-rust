// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::TaskError;
use futures_channel::oneshot;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

/// Owns a submitted blocking job. Await for completion; move to transfer ownership.
///
/// Dropping this handle prevents execution if observed before the worker's final cancellation check. Already-started work retains its inputs and capacity slot until it finishes. Unobserved results are discarded.
#[must_use = "await or retain the job; dropping its handle abandons its result"]
pub struct BlockingTask<Output> {
    pub(crate) result: oneshot::Receiver<Result<Output, TaskError>>,
    pub(crate) abandoned: Arc<AtomicBool>,
}

impl<Output> Future for BlockingTask<Output> {
    type Output = Result<Output, TaskError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.result)
            .poll(context)
            .map(|result| result.unwrap_or(Err(TaskError::Cancelled)))
    }
}

impl<Output> Drop for BlockingTask<Output> {
    fn drop(&mut self) {
        self.abandoned.store(true, Ordering::Release);
    }
}
