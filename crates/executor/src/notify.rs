// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::Cancellation;
use std::sync::{Arc, mpsc};

/// A synchronous destination for a notification value.
///
/// Called in ordinary thread context, never directly in a raw signal handler.
/// Implementations should return promptly: enqueue work or wake a future rather
/// than waiting for it to finish. Waiting remains specific to the destination.
///
/// Channel implementations enqueue without waiting for capacity. A closed
/// receiver discards the value. Bounded senders have no default implementation:
/// an application must choose its own overflow policy in a wrapper.
pub trait Notify<Value>: Send + Sync {
    fn notify(&self, value: Value);
}

impl<Value> Notify<Value> for Cancellation {
    fn notify(&self, _value: Value) {
        self.cancel();
    }
}

impl<Value, Target: Notify<Value> + ?Sized> Notify<Value> for Arc<Target> {
    fn notify(&self, value: Value) {
        (**self).notify(value);
    }
}

impl<Value: Send> Notify<Value> for mpsc::Sender<Value> {
    fn notify(&self, value: Value) {
        let _ = self.send(value);
    }
}

impl<Value: Send> Notify<Value> for futures_channel::mpsc::UnboundedSender<Value> {
    fn notify(&self, value: Value) {
        let _ = self.unbounded_send(value);
    }
}
