// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use socketry_executor::{Cancellation, Notify};
use std::sync::{Arc, mpsc};

#[test]
fn cancellation_ignores_values_and_propagates_to_children() {
    let cancellation = Cancellation::new();
    let child = cancellation.child();
    cancellation.notify("stop");
    cancellation.notify(42);
    assert!(cancellation.is_cancelled());
    assert!(child.is_cancelled());
    let never = Cancellation::never();
    never.notify(());
    assert!(!never.is_cancelled());
}

#[test]
fn shared_targets_and_standard_channels_retain_values_in_order() {
    let (sender, receiver) = mpsc::channel();
    let target: Arc<dyn Notify<usize>> = Arc::new(sender);
    target.notify(1);
    target.clone().notify(2);
    assert_eq!(receiver.recv().unwrap(), 1);
    assert_eq!(receiver.recv().unwrap(), 2);
    drop(receiver);
    target.notify(3);
}

#[test]
fn asynchronous_channels_retain_values_and_ignore_closed_receivers() {
    let (sender, mut receiver) = futures_channel::mpsc::unbounded();
    sender.notify(1);
    sender.notify(2);
    assert_eq!(receiver.try_recv().unwrap(), 1);
    assert_eq!(receiver.try_recv().unwrap(), 2);
    drop(receiver);
    sender.notify(3);
}
