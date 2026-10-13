// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Barrier, Mutex};

#[test]
fn deadlines_are_exact_and_expire_in_order() {
    let now = Instant::now();
    let mut timers = Timers::default();
    assert!(timers.is_empty());
    assert_eq!(timers.wait_interval(now), None);
    assert_eq!(timers.pop_expired(now), None);
    for offset in [8, 2, 9, 1, 7, 3, 6, 4, 5, 0] {
        let deadline = now + Duration::from_nanos(offset);
        let handle = timers.schedule(deadline, offset);
        assert_eq!(handle.deadline(), deadline);
    }
    assert_eq!(timers.len(), 10);
    assert_eq!(timers.wait_interval(now), Some(Duration::ZERO));
    assert_eq!(timers.pop_expired(now), Some(0));
    assert_eq!(timers.pop_expired(now), None);
    assert_eq!(timers.wait_interval(now), Some(Duration::from_nanos(1)));
    for offset in 1..10 {
        assert_eq!(
            timers.pop_expired(now + Duration::from_nanos(10)),
            Some(offset)
        );
    }
    assert!(timers.is_empty());
    assert_eq!(timers.next_deadline(), None);
    let handle = timers.schedule(now, 10);
    assert_eq!(
        timers.wait_interval(now + Duration::from_secs(1)),
        Some(Duration::ZERO)
    );
    assert_eq!(timers.pop_expired(now), Some(10));
    assert!(!handle.cancel());
    assert!(!handle.is_cancelled());
}

#[test]
fn cancellation_before_flush_releases_values_and_is_idempotent() {
    struct Released(Arc<AtomicUsize>);
    impl Drop for Released {
        fn drop(&mut self) {
            self.0.fetch_add(1, AtomicOrdering::Relaxed);
        }
    }
    let released = Arc::new(AtomicUsize::new(0));
    let mut timers = Timers::new();
    let handle = timers.schedule(Instant::now(), Released(Arc::clone(&released)));
    let clone = handle.clone();
    assert!(handle.cancel());
    assert!(clone.is_cancelled());
    assert!(!clone.cancel());
    assert_eq!(released.load(AtomicOrdering::Relaxed), 1);
    assert!(timers.is_empty());
    timers.flush();
    assert!(lock(&timers.shared).heap.is_empty());
    assert!(lock(&timers.shared).scheduled.is_empty());
    assert_eq!(timers.next_deadline(), None);
}

#[test]
fn cancelled_roots_are_discarded_before_waiting_or_firing() {
    let now = Instant::now();
    let mut timers = Timers::new();
    let first = timers.schedule(now, 0);
    let second = timers.schedule(now + Duration::from_secs(1), 1);
    timers.schedule(now + Duration::from_secs(2), 2);
    timers.flush();
    first.cancel();
    assert_eq!(timers.wait_interval(now), Some(Duration::from_secs(1)));
    second.cancel();
    assert_eq!(timers.pop_expired(now + Duration::from_secs(1)), None);
    assert_eq!(timers.len(), 1);
    assert_eq!(timers.pop_expired(now + Duration::from_secs(2)), Some(2));
    assert_eq!(lock(&timers.shared).cancelled, 0);
}

#[test]
fn small_fully_cancelled_heaps_are_cleared() {
    let mut timers = Timers::new();
    let handles: Vec<_> = (0..127)
        .map(|value| timers.schedule(Instant::now(), value))
        .collect();
    timers.flush();
    for handle in handles {
        handle.cancel();
    }
    timers.flush();
    assert!(lock(&timers.shared).heap.is_empty());
    assert_eq!(lock(&timers.shared).cancelled, 0);
}

#[test]
fn compaction_threshold_and_pending_batch_are_preserved() {
    let now = Instant::now();
    let mut timers = Timers::new();
    let handles: Vec<_> = (0..256)
        .map(|value| timers.schedule(now + Duration::from_secs(value), value))
        .collect();
    timers.flush();
    for handle in &handles[128..] {
        handle.cancel();
    }
    timers.flush();
    assert_eq!(lock(&timers.shared).heap.len(), 256); // Exactly half does not compact.
    assert_eq!(timers.len(), 128); // len counts live entries, unlike Ruby's size.
    handles[127].cancel();
    let cancelled = timers.schedule(now, 1000);
    cancelled.cancel();
    timers.schedule(now, 999);
    timers.flush();
    assert_eq!(lock(&timers.shared).heap.len(), 128);
    assert_eq!(lock(&timers.shared).cancelled, 0);
    let mut expired = Vec::new();
    while let Some(value) = timers.pop_expired(now + Duration::from_secs(256)) {
        expired.push(value);
    }
    assert_eq!(expired.len(), 128);
    assert!(expired.contains(&999));
    assert!(!expired.contains(&1000));
    for value in 0..127 {
        assert!(expired.contains(&value));
    }
}

#[test]
fn heapify_and_incremental_batches_preserve_order() {
    let now = Instant::now();
    let mut timers = Timers::new();
    for value in (10..20).rev() {
        timers.schedule(now + Duration::from_secs(value), value);
    }
    timers.flush();
    for value in (5..10).rev() {
        timers.schedule(now + Duration::from_secs(value), value);
    }
    timers.flush(); // Small batch uses push.
    for value in (20..100).rev().chain((0..5).rev()) {
        timers.schedule(now + Duration::from_secs(value), value);
    }
    timers.flush(); // Dominant batch uses heapify.
    for expected in 0..100 {
        assert_eq!(
            timers.pop_expired(now + Duration::from_secs(100)),
            Some(expected)
        );
    }
}

#[test]
fn handles_do_not_require_ordered_or_cloneable_values() {
    struct Value;
    let mut timers = Timers::new();
    let now = Instant::now();
    let handle = timers.schedule(now, Value);
    drop(handle.clone());
    drop(handle); // Dropping every handle does not cancel.
    let state = lock(&timers.shared);
    let first = &state.scheduled[0];
    assert!(first == first);
    assert_eq!(first.partial_cmp(first), Some(Ordering::Equal));
    drop(state);
    assert!(timers.pop_expired(now).is_some());
}

#[test]
fn clear_and_drop_release_values_with_outstanding_handles() {
    let mut timers = Timers::new();
    let first = timers.schedule(Instant::now(), Arc::new(()));
    timers.flush();
    let second = timers.schedule(Instant::now(), Arc::new(()));
    timers.clear();
    assert!(timers.is_empty());
    assert!(!first.cancel());
    assert!(!second.cancel());
    let value = Arc::new(());
    let pending = timers.schedule(Instant::now(), Arc::clone(&value));
    drop(timers);
    assert_eq!(Arc::strong_count(&value), 1);
    assert!(!pending.cancel());
}

#[cfg(feature = "driver")]
#[test]
fn clear_rejects_reentrant_registration_until_buffers_are_restored() {
    struct RegisterOnDrop(Option<Box<dyn FnOnce() + Send>>);
    impl Drop for RegisterOnDrop {
        fn drop(&mut self) {
            if let Some(register) = self.0.take() {
                register();
            }
        }
    }

    for flush in [false, true] {
        let now = Instant::now();
        let mut timers = Timers::with_capacity(4);
        let capacity = timers.capacity();
        let mut producer = timers.shared_producer();
        let attempted = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&attempted);
        let value = RegisterOnDrop(Some(Box::new(move || {
            assert!(producer.try_schedule(now, RegisterOnDrop(None)).is_err());
            observed.fetch_add(1, AtomicOrdering::Relaxed);
        })));
        let handle = timers.try_schedule(now, value).ok().unwrap();
        if flush {
            timers.flush();
        }
        timers.clear();
        assert_eq!(attempted.load(AtomicOrdering::Relaxed), 1);
        assert!(timers.is_empty());
        assert_eq!(timers.capacity(), capacity);
        assert!(!handle.cancel());
        drop(handle);
        drop(timers.try_schedule(now, RegisterOnDrop(None)).ok().unwrap());
        assert!(timers.pop_expired(now).is_some());
        assert!(timers.is_empty());
    }
}

#[test]
fn cancellation_and_expiry_claim_each_value_once_across_threads() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<TimerHandle<usize>>();
    assert_send_sync::<Timers<usize>>();
    let now = Instant::now();
    let mut timers = Timers::new();
    let handles: Vec<_> = (0..10_000)
        .map(|value| timers.schedule(now, value))
        .collect();
    timers.flush();
    let start = Arc::new(Barrier::new(2));
    let canceller_start = Arc::clone(&start);
    let canceller = std::thread::spawn(move || {
        canceller_start.wait();
        handles.iter().filter(|handle| handle.cancel()).count()
    });
    start.wait();
    let mut fired = 0;
    while timers.pop_expired(now).is_some() {
        fired += 1;
    }
    assert_eq!(fired + canceller.join().unwrap(), 10_000);
    timers.flush();
    assert!(timers.is_empty());
    assert!(lock(&timers.shared).heap.is_empty());
    assert_eq!(lock(&timers.shared).cancelled, 0);
}

#[test]
fn cancellation_can_race_with_pending_insertion_and_compaction() {
    let now = Instant::now();
    let mut timers = Timers::new();
    let existing: Vec<_> = (0..20_000)
        .map(|value| timers.schedule(now, value))
        .collect();
    timers.flush();
    for handle in &existing[..15_000] {
        handle.cancel();
    }
    let pending: Vec<_> = (20_000..40_000)
        .map(|value| timers.schedule(now, value))
        .collect();
    let start = Arc::new(Barrier::new(2));
    let canceller_start = Arc::clone(&start);
    let canceller = std::thread::spawn(move || {
        canceller_start.wait();
        existing
            .iter()
            .chain(&pending)
            .filter(|handle| handle.cancel())
            .count()
    });
    start.wait();
    timers.flush();
    let mut fired = 0;
    while timers.pop_expired(now).is_some() {
        fired += 1;
    }
    assert_eq!(15_000 + fired + canceller.join().unwrap(), 40_000);
    timers.flush();
    assert!(timers.is_empty());
    assert!(lock(&timers.shared).heap.is_empty());
    assert_eq!(lock(&timers.shared).cancelled, 0);
}

#[test]
fn value_destructors_can_reenter_cancellation() {
    type Callback = Box<dyn FnOnce() + Send>;
    struct OnDrop(Option<Callback>);
    impl Drop for OnDrop {
        fn drop(&mut self) {
            self.0.take().unwrap()();
        }
    }
    let slot = Arc::new(Mutex::new(None::<TimerHandle<OnDrop>>));
    let callback_slot = Arc::clone(&slot);
    let mut timers = Timers::new();
    let handle = timers.schedule(
        Instant::now(),
        OnDrop(Some(Box::new(move || {
            assert!(!callback_slot.lock().unwrap().as_ref().unwrap().cancel());
        }))),
    );
    *slot.lock().unwrap() = Some(handle.clone());
    assert!(handle.cancel());
    slot.lock().unwrap().take();
}

#[test]
fn slots_are_reused_only_after_the_entry_and_last_handle_are_gone() {
    let now = Instant::now();
    let mut timers = Timers::with_capacity(8);
    let capacity = timers.capacity();
    let mut handles: Vec<_> = (0..capacity)
        .map(|value| {
            timers
                .try_schedule(now + Duration::from_secs(value as u64), value)
                .unwrap()
        })
        .collect();
    let first = handles.remove(0);
    let old_key = first.key;
    let clone = first.clone();
    assert!(first.cancel());
    assert!(timers.try_schedule(now, 999).is_err());
    assert_eq!(timers.next_deadline(), Some(now + Duration::from_secs(1)));
    drop(first);
    assert!(clone.is_cancelled());
    assert!(!clone.cancel());
    assert!(timers.try_schedule(now, 999).is_err());
    drop(clone);
    let replacement = timers.try_schedule(now, 999).unwrap();
    assert_eq!(replacement.key.index, old_key.index);
    assert_ne!(replacement.key.generation, old_key.generation);
    assert!(!replacement.is_cancelled());
    assert_eq!(timers.pop_expired(now), Some(999));
    assert!(!replacement.cancel());
    drop(replacement);
    for handle in handles {
        handle.cancel();
    }
    timers.flush();
    assert!(timers.is_empty());
    assert_eq!(timers.capacity(), capacity);
    assert_eq!(lock(&timers.shared).free_count, capacity);
}

#[test]
fn expired_handles_keep_their_status_and_capacity_until_dropped() {
    let now = Instant::now();
    let mut timers = Timers::with_capacity(4);
    let capacity = timers.capacity();
    let handles: Vec<_> = (0..capacity)
        .map(|value| timers.try_schedule(now, value).unwrap())
        .collect();
    let clone = handles[0].clone();
    for _ in 0..capacity {
        assert!(timers.pop_expired(now).is_some());
    }
    assert!(timers.is_empty());
    assert!(timers.try_schedule(now, 999).is_err());
    drop(handles);
    for _ in 1..capacity {
        timers.try_schedule(now, 999).unwrap();
    }
    assert!(!clone.cancel());
    assert!(!clone.is_cancelled());
    assert!(timers.try_schedule(now, 999).is_err());
    drop(clone);
    timers.try_schedule(now, 1000).unwrap();
    assert_eq!(timers.len(), capacity);
    timers.clear();
    assert_eq!(lock(&timers.shared).free_count, capacity);
}

#[test]
fn reserve_adds_room_beyond_occupied_slots_and_full_returns_the_value() {
    let mut timers = Timers::new();
    assert_eq!(timers.capacity(), 0);
    assert!(
        matches!(timers.try_schedule(Instant::now(), String::from("full")), Err(value) if value == "full")
    );
    timers.reserve(10);
    let initial_capacity = timers.capacity();
    assert!(initial_capacity >= 10);
    for _ in 0..initial_capacity {
        timers.try_schedule(Instant::now(), String::new()).unwrap();
    }
    timers.reserve(3);
    assert!(timers.capacity() >= initial_capacity + 3);
    for _ in 0..3 {
        timers.try_schedule(Instant::now(), String::new()).unwrap();
    }
    let capacity = timers.capacity();
    timers.clear();
    assert_eq!(timers.capacity(), capacity);
    for _ in 0..capacity {
        timers.try_schedule(Instant::now(), String::new()).unwrap();
    }
}

#[test]
fn capacity_overflow_does_not_damage_the_queue_or_prevent_cleanup() {
    let now = Instant::now();
    let mut timers = Timers::new();
    let handle = timers.schedule(now, 42);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| timers.reserve(usize::MAX)))
            .is_err()
    );
    assert_eq!(timers.pop_expired(now), Some(42));
    assert!(!handle.cancel());
    timers.schedule(now, 43);
    assert_eq!(timers.pop_expired(now), Some(43));
}

#[test]
fn clear_finishes_cleanup_and_preserves_storage_if_a_value_destructor_panics() {
    struct Released {
        count: Arc<AtomicUsize>,
        panic: bool,
    }
    impl Drop for Released {
        fn drop(&mut self) {
            self.count.fetch_add(1, AtomicOrdering::Relaxed);
            assert!(!self.panic, "injected destructor panic");
        }
    }
    let count = Arc::new(AtomicUsize::new(0));
    let mut timers = Timers::with_capacity(10);
    let capacity = timers.capacity();
    let mut handles = Vec::new();
    for index in 0..10 {
        handles.push(timers.schedule(
            Instant::now(),
            Released {
                count: Arc::clone(&count),
                panic: index == 2,
            },
        ));
    }
    timers.flush();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| timers.clear())).is_err());
    assert_eq!(count.load(AtomicOrdering::Relaxed), 10);
    assert!(timers.is_empty());
    assert_eq!(timers.capacity(), capacity);
    for handle in handles {
        assert!(!handle.cancel());
    }
    timers.schedule(
        Instant::now(),
        Released {
            count: Arc::clone(&count),
            panic: false,
        },
    );
    timers.clear();
    assert_eq!(count.load(AtomicOrdering::Relaxed), 11);
}

#[test]
fn batch_expiry_respects_output_capacity_cancellation_and_future_deadlines() {
    let now = Instant::now();
    let mut timers = Timers::with_capacity(256);
    let handles: Vec<_> = (0..256)
        .rev()
        .map(|value| timers.schedule(now + Duration::from_nanos(value), value))
        .collect();
    timers.flush();
    for value in [80, 129, 255] {
        handles[(255 - value) as usize].cancel();
    }
    let mut output = Vec::with_capacity(32);
    output.push(999); // Existing caller values must remain untouched.
    assert_eq!(
        timers.pop_expired_batch(now + Duration::from_nanos(127), &mut output),
        31
    );
    assert_eq!(output[0], 999);
    assert_eq!(timers.len(), 222);
    assert_eq!(timers.pop_expired_batch(now, &mut output), 0);
    assert_eq!(timers.len(), 222); // A full vector leaves registrations alone.
    let mut delivered = output.split_off(1);
    output.clear();
    loop {
        let count = timers.pop_expired_batch(now + Duration::from_nanos(127), &mut output);
        if count == 0 {
            break;
        }
        delivered.append(&mut output);
    }
    delivered.sort_unstable();
    assert_eq!(
        delivered,
        (0..128).filter(|value| *value != 80).collect::<Vec<_>>()
    );
    assert_eq!(timers.len(), 126);
    assert_eq!(
        timers.next_deadline(),
        Some(now + Duration::from_nanos(128))
    );
    for value in delivered {
        assert!(!handles[(255 - value) as usize].cancel());
    }
    // Rebuilt survivors still support strictly ordered individual pops.
    for expected in (128..256).filter(|value| ![129, 255].contains(value)) {
        assert_eq!(
            timers.pop_expired(now + Duration::from_secs(1)),
            Some(expected)
        );
    }
    assert_eq!(timers.pop_expired_batch(now, &mut output), 0);
    drop(handles);
    for _ in 0..256 {
        timers.try_schedule(now, 42).unwrap();
    }
    assert_eq!(timers.capacity(), 256);
}

#[test]
fn sparse_batch_expiry_prunes_future_subtrees_and_flushes_new_timers() {
    let now = Instant::now();
    let mut timers = Timers::with_capacity(1024);
    let handles: Vec<_> = (0..1024)
        .map(|value| timers.schedule(now + Duration::from_nanos(value), value))
        .collect();
    let mut output = Vec::new();
    assert_eq!(timers.pop_expired_batch(now, &mut output), 0);
    assert_eq!(timers.len(), 1024);
    output.reserve(1024);
    assert_eq!(
        timers.pop_expired_batch(now - Duration::from_nanos(1), &mut output),
        0
    );
    handles[1].cancel();
    let mut single = Vec::with_capacity(1);
    assert_eq!(timers.pop_expired_batch(now, &mut single), 1);
    assert_eq!(single, [0]);
    assert_eq!(
        timers.pop_expired_batch(now + Duration::from_nanos(2), &mut output),
        1
    );
    assert_eq!(output, [2]);
    output.clear();
    timers.schedule(now, 2000);
    assert_eq!(timers.pop_expired_batch(now, &mut output), 1);
    assert_eq!(output, [2000]);
    assert_eq!(timers.next_deadline(), Some(now + Duration::from_nanos(3)));
}

#[test]
fn batch_expiry_claims_values_once_when_cancellation_races() {
    let now = Instant::now();
    let mut timers = Timers::with_capacity(10_000);
    let handles: Vec<_> = (0..10_000)
        .map(|value| timers.schedule(now, value))
        .collect();
    let start = Arc::new(Barrier::new(2));
    let canceller_start = Arc::clone(&start);
    let canceller = std::thread::spawn(move || {
        canceller_start.wait();
        handles.iter().filter(|handle| handle.cancel()).count()
    });
    let mut output = Vec::with_capacity(10_000);
    start.wait();
    let fired = timers.pop_expired_batch(now, &mut output);
    assert_eq!(fired + canceller.join().unwrap(), 10_000);
    output.sort_unstable();
    output.dedup();
    assert_eq!(output.len(), fired);
    assert!(timers.is_empty());
    assert_eq!(timers.pop_expired_batch(now, &mut output), 0);
}

#[test]
fn batch_values_are_processed_after_unlocking_and_can_reenter_cancellation() {
    struct Value(Option<TimerHandle<Value>>);
    impl Drop for Value {
        fn drop(&mut self) {
            if let Some(handle) = self.0.take() {
                assert!(!handle.cancel());
            }
        }
    }
    let now = Instant::now();
    let mut timers = Timers::new();
    let first = timers.schedule(now, Value(None));
    timers.schedule(now, Value(Some(first)));
    let mut output = Vec::with_capacity(2);
    assert_eq!(timers.pop_expired_batch(now, &mut output), 2);
    output.clear();
    assert!(timers.is_empty());
}
