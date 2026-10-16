// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Cancellation, OperationResult};
use std::{
    future::Future,
    io,
    time::{Duration, Instant},
};

/// Monotonic time and cooperative waits in one scheduler's clock domain.
///
/// Obtain current time through this trait rather than `Instant::now()` or
/// `Instant::elapsed()` so simulated and paused clocks remain substitutable.
/// Deadlines from independently advancing clock domains are not interchangeable.
pub trait Clock: Send + Sync {
    /// Observe this scheduler's monotonic, nondecreasing clock.
    fn now(&self) -> Instant;

    /// Wait until an absolute deadline, or until cancellation is observed.
    ///
    /// Futures are lazy. Pre-cancelled waits return cancellation without
    /// registering a timer, including for elapsed deadlines. Otherwise an
    /// elapsed deadline can complete immediately; no scheduler yield is promised.
    /// Once waiting, expiry wins if expiry and cancellation are observed together.
    /// Dropping the future releases its timer registration.
    fn sleep_until(
        &self,
        deadline: Instant,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send;

    /// Wait for a duration measured from the first poll, not construction.
    /// An unrepresentable deadline returns `io::ErrorKind::InvalidInput`.
    fn sleep(
        &self,
        duration: Duration,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send {
        async move {
            cancellation.check()?;
            let deadline = self.now().checked_add(duration).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "sleep deadline is out of range",
                )
            })?;
            self.sleep_until(deadline, cancellation).await
        }
    }
}

#[cfg(feature = "native")]
pub(crate) async fn native_sleep_until(
    deadline: Instant,
    cancellation: &Cancellation,
) -> OperationResult<()> {
    super::operation::cancellable(cancellation, async {
        async_io::Timer::at(deadline).await;
        Ok(())
    })
    .await
}
