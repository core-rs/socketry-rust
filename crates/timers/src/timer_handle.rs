// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::timers::{Key, Shared, lock};
use std::sync::Arc;
use std::time::Instant;

/// A clonable cancellation handle for one timer.
///
/// Handles share the queue's allocation and identify a reusable slot. Dropping
/// a handle leaves its timer scheduled. Call [`cancel`](Self::cancel) explicitly,
/// for example from a sleep future's destructor. Handles can be transferred
/// between threads when their value is `Send`.
///
/// A completed or cancelled timer's slot remains occupied until every handle
/// is dropped and its queue entry is reclaimed. This preserves cancellation
/// status for clones and prevents an old handle from affecting a reused slot.
pub struct TimerHandle<Value> {
    pub(crate) shared: Shared<Value>,
    pub(crate) key: Key,
    deadline: Instant,
}

impl<Value> Clone for TimerHandle<Value> {
    fn clone(&self) -> Self {
        lock(&self.shared).clone_handle(self.key);
        Self {
            shared: Arc::clone(&self.shared),
            key: self.key,
            deadline: self.deadline,
        }
    }
}

impl<Value> TimerHandle<Value> {
    pub(crate) fn new(shared: &Shared<Value>, key: Key, deadline: Instant) -> Self {
        Self {
            shared: Arc::clone(shared),
            key,
            deadline,
        }
    }

    /// The timer's monotonic deadline.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Cancel in constant time, releasing the value immediately.
    ///
    /// Returns `true` if cancellation claimed the value before expiry or queue
    /// destruction. Repeated cancellation returns `false`. If expiry already
    /// returned the value, its delivery belongs to the caller and cannot be
    /// stopped by this handle. The value's destructor runs outside the lock.
    pub fn cancel(&self) -> bool {
        let value = lock(&self.shared).cancel(self.key);
        let cancelled = value.is_some();
        drop(value);
        cancelled
    }

    /// Whether this timer was successfully cancelled, including after removal.
    pub fn is_cancelled(&self) -> bool {
        lock(&self.shared).is_cancelled(self.key)
    }
}

impl<Value> Drop for TimerHandle<Value> {
    fn drop(&mut self) {
        lock(&self.shared).drop_handle(self.key);
    }
}
