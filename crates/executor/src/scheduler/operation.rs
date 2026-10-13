// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Cancellation, OperationResult};
use std::{
    future::{Future, poll_fn},
    io,
    pin::pin,
    task::Poll,
};

/// Only for readiness operations: dropping a pending operation leaves no
/// kernel access to its borrowed inputs. Completion and blocking backends must
/// retain their operation and await its terminal result instead.
pub(crate) async fn cancellable<T>(
    cancellation: &Cancellation,
    operation: impl Future<Output = io::Result<T>>,
) -> OperationResult<T> {
    cancellation.check()?;
    let mut operation = pin!(operation);
    let mut cancelled = pin!(cancellation.cancelled());
    poll_fn(|context| {
        if let Poll::Ready(result) = operation.as_mut().poll(context) {
            return Poll::Ready(result.map_err(Into::into));
        }
        cancelled
            .as_mut()
            .poll(context)
            .map(|()| Err(crate::Cancelled.into()))
    })
    .await
}

#[cfg(test)]
mod tests;
