// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::Cancellation;
use std::future::{Future, poll_fn};
use std::pin::pin;

/// Notify protected work of cancellation and continue awaiting its completion.
///
/// When cancellation is observed, call `on_cancel` exactly once before
/// polling the protected future again. This includes an already-cancelled signal
/// on the first poll. If completion and cancellation are both ready in that poll,
/// the callback runs before returning the future's normal output. A callback
/// panic propagates to the caller.
///
/// The callback is synchronous: request graceful shutdown and wake the work;
/// keep asynchronous draining in the protected future. Its operations must use
/// cancellation inputs which allow draining. This wrapper does not mask signals,
/// prevent its own destruction, or intercept scheduler task cancellation.
///
/// ```
/// use socketry_executor::{Cancellation, Scheduler, defer_cancel, yield_now};
///
/// let scheduler = Scheduler::with_workers(1)?;
/// let shutdown = Cancellation::new();
/// let drain = Cancellation::new();
/// shutdown.cancel();
/// let output = scheduler.block_on(defer_cancel(&shutdown, async {
///     drain.cancelled().await;
///     yield_now().await;
///     42
/// }, || { drain.cancel(); }));
/// assert_eq!(output, 42);
/// # Ok::<(), std::io::Error>(())
/// ```
pub async fn defer_cancel<FutureType, OnCancel>(
    cancellation: &Cancellation,
    future: FutureType,
    on_cancel: OnCancel,
) -> FutureType::Output
where
    FutureType: Future,
    OnCancel: FnOnce(),
{
    let mut notification = pin!(cancellation.cancelled());
    let mut future = pin!(future);
    let mut on_cancel = Some(on_cancel);
    poll_fn(move |context| {
        if on_cancel.is_some() && notification.as_mut().poll(context).is_ready() {
            // Remove the callback before invoking user code, including on panic.
            on_cancel.take().expect("cancellation callback is present")();
        }
        future.as_mut().poll(context)
    })
    .await
}
