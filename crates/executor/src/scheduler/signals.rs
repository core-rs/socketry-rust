// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::Notify;
use std::io;

/// Explicit signal subscriptions provided by a scheduler.
///
/// This optional capability is separate from cancellation. A destination can
/// request cancellation, enqueue the signal, or implement application policy.
/// The `signals` feature supplies Unix implementations for Socketry and the
/// Tokio adapter. Unsupported platforms do not implement this capability.
pub trait Signals: Send + Sync {
    /// A backend's signal identifier; Unix implementations use `c_int`.
    type Signal: Copy + Send + 'static;

    /// Owns one subscription. Dropping it stops admission of notifications;
    /// dispatch already in progress may finish and must retain its destination.
    /// Drop does not wait for callbacks or asynchronous backend cleanup.
    type Registration: Send + 'static;

    /// Subscribe synchronously, returning a guard that must remain alive while
    /// notifications are wanted. Setup failures are returned before success.
    ///
    /// Callbacks run outside raw signal handlers and scheduler locks. Multiple
    /// subscriptions are independent. Signals may coalesce before delivery;
    /// this is not an exact counter or a globally ordered stream of OS events.
    fn signal_notify<Target>(
        &self,
        signals: &[Self::Signal],
        target: Target,
    ) -> io::Result<Self::Registration>
    where
        Target: Notify<Self::Signal> + 'static;
}
