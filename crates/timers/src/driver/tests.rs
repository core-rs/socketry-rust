use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Barrier, mpsc};
use std::time::Duration;

#[test]
fn pool_capacity_leases_and_cancellation() {
    let now = Instant::now();
    let mut driver = TimerDriver::default();
    let mut producer = driver.producer(2);
    let first = producer.try_schedule(now, 1).unwrap();
    let second = producer
        .try_schedule(now + Duration::from_secs(1), 2)
        .unwrap();
    assert_eq!(first.deadline(), now);
    assert_eq!(producer.try_schedule(now, 3).err(), Some(3));
    assert!(second.cancel());
    assert!(!second.cancel());
    assert!(second.is_cancelled());
    assert_eq!(driver.next_deadline(), Some(now));
    assert_eq!(producer.try_schedule(now, 3).err(), Some(3));
    drop(second);
    let third = producer.try_schedule(now, 3).unwrap();
    let mut output = Vec::with_capacity(2);
    assert_eq!(driver.poll(now, &mut output), 2);
    output.sort();
    assert_eq!(output, [1, 3]);
    assert!(!first.cancel());
    assert!(!first.is_cancelled());
    assert_eq!(producer.try_schedule(now, 4).err(), Some(4));
    drop(first);
    drop(third);
    let fourth = producer.try_schedule(now, 4).unwrap();
    drop(fourth); // Dropping a handle does not cancel.
    output.clear();
    assert_eq!(driver.poll(now, &mut output), 1);
    assert_eq!(output, [4]);
    assert_eq!(driver.next_deadline(), None);
}

#[test]
fn sparse_dense_and_bounded_output() {
    let now = Instant::now();
    let mut driver = TimerDriver::new();
    let mut left = driver.producer(128);
    let mut right = driver.producer(128);
    for index in 0..128 {
        left.try_schedule(now + Duration::from_nanos(index), index)
            .unwrap();
        right
            .try_schedule(
                now + Duration::from_secs(1) + Duration::from_nanos(index),
                index,
            )
            .unwrap();
    }
    assert_eq!(driver.next_deadline(), Some(now));
    let mut output = Vec::new();
    assert_eq!(driver.poll(now, &mut output), 0);
    output = Vec::with_capacity(1);
    assert_eq!(driver.poll(now - Duration::from_secs(1), &mut output), 0);
    assert_eq!(driver.poll(now, &mut output), 1);
    assert_eq!(output, [0]);
    assert_eq!(driver.poll(now + Duration::from_secs(2), &mut output), 0);
    output.clear();
    output.reserve(256);
    assert_eq!(driver.poll(now + Duration::from_secs(2), &mut output), 255);
    assert_eq!(driver.next_deadline(), None);
    assert_eq!(driver.poll(now, &mut output), 0);
    // Small incremental insertion into an existing heap, and compaction of a
    // non-root cancelled timer alongside new submissions.
    let a = left.try_schedule(now + Duration::from_secs(2), 1).unwrap();
    let b = left.try_schedule(now + Duration::from_secs(3), 2).unwrap();
    driver.next_deadline();
    right.try_schedule(now + Duration::from_secs(4), 3).unwrap();
    assert!(b.cancel());
    assert_eq!(driver.next_deadline(), Some(a.deadline()));
    right.try_schedule(now + Duration::from_secs(5), 4).unwrap();
    driver.next_deadline();
    output.clear();
    assert_eq!(driver.poll(now + Duration::from_secs(2), &mut output), 1);
    assert_eq!(output, [1]);
}

#[test]
fn cancelled_inbox_and_heap_reclaim_without_output_room() {
    let now = Instant::now();
    let mut driver = TimerDriver::new();
    let mut producer = driver.producer(1);
    let handle = producer.try_schedule(now, 7).unwrap();
    assert!(handle.cancel());
    drop(handle);
    assert_eq!(driver.poll(now, &mut Vec::new()), 0);
    let handle = producer.try_schedule(now, 8).unwrap();
    driver.next_deadline();
    assert!(handle.cancel());
    drop(handle);
    assert_eq!(driver.next_deadline(), None);
    producer.try_schedule(now, 9).unwrap();
}

#[test]
fn staged_cancellation_does_not_compact_sparse_heap_cancellations() {
    let now = Instant::now();
    let mut driver = TimerDriver::new();
    let mut producer = driver.producer(256);
    producer.try_schedule(now, 0).unwrap();
    let later = producer
        .try_schedule(now + Duration::from_secs(1), 1)
        .unwrap();
    driver.next_deadline();
    assert!(later.cancel());
    drop(later);
    for _ in 0..8 {
        let staged = producer
            .try_schedule(now + Duration::from_secs(2), 2)
            .unwrap();
        assert!(staged.cancel());
        drop(staged);
        assert_eq!(driver.next_deadline(), Some(now));
        // Sparse heap tombstones are retained; staged cancellations never enter
        // the heap or turn these small flushes into whole-heap compaction.
        assert_eq!(driver.heap.len(), 2);
        assert_eq!(producer.pool.cancelled_heap.load(Ordering::Relaxed), 1);
    }
    let mut output = Vec::with_capacity(2);
    assert_eq!(driver.poll(now, &mut output), 1);
    assert_eq!(output, [0]);
    assert_eq!(driver.next_deadline(), None);
    assert_eq!(producer.pool.cancelled_heap.load(Ordering::Relaxed), 0);
}

#[test]
fn heap_compaction_batches_cancellations_and_new_submissions() {
    let now = Instant::now();
    let mut driver = TimerDriver::new();
    let mut producer = driver.producer(1024);
    let handles: Vec<_> = (0..256)
        .map(|index| {
            producer
                .try_schedule(now + Duration::from_nanos(index), index)
                .unwrap()
        })
        .collect();
    driver.next_deadline();
    for handle in &handles[1..129] {
        assert!(handle.cancel());
    }
    assert_eq!(driver.next_deadline(), Some(now));
    assert_eq!(driver.heap.len(), 256); // Exactly half: defer the repair.
    assert!(handles[129].cancel());
    producer
        .try_schedule(now + Duration::from_secs(1), 256)
        .unwrap();
    assert_eq!(driver.next_deadline(), Some(now));
    assert_eq!(driver.heap.len(), 128);
    assert_eq!(producer.pool.cancelled_heap.load(Ordering::Relaxed), 0);
    let mut output = Vec::with_capacity(128);
    assert_eq!(driver.poll(now + Duration::from_secs(1), &mut output), 128);
    assert!(output.contains(&0));
    assert!(output.contains(&256));
}

#[test]
fn full_pool_reclaims_non_root_cancellation_below_batch_threshold() {
    let now = Instant::now();
    let mut driver = TimerDriver::new();
    let mut producer = driver.producer(2);
    producer.try_schedule(now, 0).unwrap();
    let later = producer
        .try_schedule(now + Duration::from_secs(1), 1)
        .unwrap();
    driver.next_deadline();
    assert!(later.cancel());
    drop(later);
    assert_eq!(producer.try_schedule(now, 2).err(), Some(2));
    assert_eq!(driver.next_deadline(), Some(now));
    producer.try_schedule(now, 2).unwrap();
    let mut output = Vec::with_capacity(2);
    assert_eq!(driver.poll(now, &mut output), 2);
    output.sort();
    assert_eq!(output, [0, 2]);
}

#[test]
fn small_fully_cancelled_heap_reclaims_without_capacity_pressure() {
    let now = Instant::now();
    let mut driver = TimerDriver::new();
    let mut producer = driver.producer(8);
    let handles: Vec<_> = (0..2)
        .map(|value| producer.try_schedule(now, value).unwrap())
        .collect();
    driver.next_deadline();
    for handle in handles {
        assert!(handle.cancel());
    }
    assert_eq!(driver.next_deadline(), None);
    assert_eq!(producer.pool.free.len(), 8);
    assert_eq!(producer.pool.cancelled_heap.load(Ordering::Relaxed), 0);
}

#[test]
fn remote_cancellation_during_drain_skips_heap_admission() {
    let now = Instant::now();
    let mut driver = TimerDriver::new();
    let mut producer = driver.producer(1);
    let handle = producer.try_schedule(now, 1).unwrap();
    driver.before_admit = Some(Box::new(move || {
        assert!(thread::spawn(move || handle.cancel()).join().unwrap());
    }));
    assert_eq!(driver.next_deadline(), None);
    assert_eq!(producer.pool.free.len(), 1);
    assert_eq!(producer.pool.cancelled_heap.load(Ordering::Relaxed), 0);
    producer.try_schedule(now, 2).unwrap();
}

#[test]
fn handles_migrate_and_cancel_immediately() {
    struct Value(Arc<AtomicUsize>);
    impl Drop for Value {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut driver = TimerDriver::new();
    let mut producer = driver.producer(1);
    let handle = producer
        .try_schedule(Instant::now(), Value(Arc::clone(&dropped)))
        .ok()
        .unwrap();
    thread::spawn(move || {
        assert!(handle.cancel());
    })
    .join()
    .unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(driver.next_deadline(), None);
}

#[test]
fn cancellation_expiry_race_delivers_or_drops_once() {
    struct Value(Arc<AtomicUsize>);
    impl Drop for Value {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    for _ in 0..64 {
        let dropped = Arc::new(AtomicUsize::new(0));
        let mut driver = TimerDriver::new();
        let mut producer = driver.producer(1);
        let now = Instant::now();
        let handle = producer
            .try_schedule(now, Value(Arc::clone(&dropped)))
            .ok()
            .unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let start = Arc::clone(&barrier);
        let cancelling = thread::spawn(move || {
            start.wait();
            handle.cancel()
        });
        barrier.wait();
        let mut output = Vec::with_capacity(1);
        let delivered = driver.poll(now, &mut output);
        let cancelled = cancelling.join().unwrap();
        assert_eq!(delivered, usize::from(!cancelled));
        drop(output);
        drop(driver);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(producer.pool.cancelled_heap.load(Ordering::Relaxed), 0);
    }
}

#[test]
fn shutdown_races_registration_and_preserves_handles() {
    for _ in 0..32 {
        let mut driver = TimerDriver::new();
        let mut producer = driver.producer(1);
        let barrier = Arc::new(Barrier::new(2));
        let start = Arc::clone(&barrier);
        let submitting = thread::spawn(move || {
            start.wait();
            let result = producer.try_schedule(Instant::now(), 11);
            (producer, result)
        });
        barrier.wait();
        drop(driver);
        let (mut producer, result) = submitting.join().unwrap();
        if let Ok(handle) = result {
            assert!(!handle.cancel());
            assert!(!handle.is_cancelled());
        }
        assert_eq!(producer.try_schedule(Instant::now(), 12).err(), Some(12));
    }
}

#[test]
fn parked_driver_wakes_for_earlier_timer_and_cancellation() {
    let mut driver = TimerDriver::new();
    let mut producer = driver.producer(2);
    let late = producer
        .try_schedule(Instant::now() + Duration::from_secs(30), 1)
        .unwrap();
    let (bound_tx, bound_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let late_deadline = late.deadline();
    let owner = thread::spawn(move || {
        assert_eq!(driver.next_deadline(), Some(late_deadline));
        bound_tx.send(()).unwrap();
        let mut output = Vec::with_capacity(2);
        loop {
            driver.park();
            if driver.poll(Instant::now(), &mut output) > 0 {
                break;
            }
        }
        assert_eq!(output, [2]);
        bound_tx.send(()).unwrap();
        // Cancellation must wake an empty/distant driver too.
        while driver.next_deadline().is_some() {
            driver.park();
        }
        result_tx.send(()).unwrap();
    });
    bound_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let early = producer.try_schedule(Instant::now(), 2).unwrap();
    bound_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    drop(early);
    assert!(late.cancel());
    result_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    owner.join().unwrap();
}

#[test]
fn empty_driver_and_overdue_park_return() {
    let mut driver = TimerDriver::<usize>::new();
    driver.park();
    let mut producer = driver.producer(1);
    producer.try_schedule(Instant::now(), 1).unwrap();
    driver.park();
}

#[test]
#[should_panic(expected = "producer capacity must be nonzero")]
fn zero_capacity_is_rejected() {
    TimerDriver::<usize>::new().producer(0);
}

#[test]
fn moving_bound_driver_is_rejected() {
    let mut driver = TimerDriver::<usize>::new();
    let _producer = driver.producer(1);
    driver.next_deadline();
    assert!(
        thread::spawn(move || driver.next_deadline())
            .join()
            .is_err()
    );
}

#[test]
fn shutdown_finishes_after_destructor_panic_and_allows_reentry() {
    struct Value {
        action: Option<Box<dyn FnOnce() + Send>>,
    }
    impl Drop for Value {
        fn drop(&mut self) {
            if let Some(action) = self.action.take() {
                action();
            }
        }
    }
    let mut driver = TimerDriver::new();
    let producer = Arc::new(Mutex::new(driver.producer(4)));
    let reentrant = Arc::clone(&producer);
    let a = lock(&producer)
        .try_schedule(
            Instant::now(),
            Value {
                action: Some(Box::new(move || {
                    assert!(
                        lock(&reentrant)
                            .try_schedule(Instant::now(), Value { action: None })
                            .is_err()
                    );
                    panic!("user destructor");
                })),
            },
        )
        .ok()
        .unwrap();
    driver.next_deadline();
    let dropped = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&dropped);
    let b = lock(&producer)
        .try_schedule(
            Instant::now(),
            Value {
                action: Some(Box::new(move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                })),
            },
        )
        .ok()
        .unwrap();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(driver))).is_err());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(!a.cancel());
    assert!(!b.cancel());
}

#[test]
fn remote_cancellation_after_flush_wins_both_expiry_strategies() {
    for count in [1, 16] {
        let mut driver = TimerDriver::new();
        let mut producer = driver.producer(count);
        let now = Instant::now();
        let handle = producer.try_schedule(now, 1).unwrap();
        for _ in 1..count {
            producer
                .try_schedule(now + Duration::from_secs(1), 2)
                .unwrap();
        }
        driver.before_claim = Some(Box::new(move || {
            assert!(thread::spawn(move || handle.cancel()).join().unwrap());
        }));
        let mut output = Vec::with_capacity(count);
        assert_eq!(driver.poll(now, &mut output), 0);
        assert_eq!(
            driver.poll(now + Duration::from_secs(1), &mut output),
            count - 1
        );
    }
}

#[test]
fn empty_parked_driver_wakes_on_first_submission() {
    let mut driver = TimerDriver::new();
    let mut producer = driver.producer(1);
    let (bound_tx, bound_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let owner = thread::spawn(move || {
        assert_eq!(driver.next_deadline(), None);
        bound_tx.send(()).unwrap();
        let mut output = Vec::with_capacity(1);
        loop {
            driver.park();
            if driver.poll(Instant::now(), &mut output) > 0 {
                break;
            }
        }
        result_tx.send(output).unwrap();
    });
    bound_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    producer.try_schedule(Instant::now(), 9).unwrap();
    assert_eq!(result_rx.recv_timeout(Duration::from_secs(2)).unwrap(), [9]);
    owner.join().unwrap();
}

// Model the notification protocol, not the heap or Crossbeam internals. Release
// publication stands in for the inbox's publication, and Loom's park token
// stands in for std::thread::unpark. Two submissions test wake coalescing.
#[test]
fn notification_protocol_has_no_lost_wake() {
    loom::model(|| {
        use loom::sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        };
        let pending = Arc::new(AtomicBool::new(false));
        let published = Arc::new(AtomicUsize::new(0));
        let receiver_pending = Arc::clone(&pending);
        let receiver_published = Arc::clone(&published);
        let receiver = loom::thread::spawn(move || {
            loop {
                receiver_pending.swap(false, Ordering::AcqRel);
                if receiver_published.load(Ordering::Acquire) == 2 {
                    break;
                }
                loom::thread::park();
            }
        });
        for index in 1..=2 {
            published.store(index, Ordering::Release);
            if !pending.swap(true, Ordering::AcqRel) {
                receiver.thread().unpark();
            }
            loom::thread::yield_now();
        }
        receiver.join().unwrap();
    });
}

#[test]
fn shared_registration_view_has_the_same_capacity_and_shutdown_contract() {
    let now = Instant::now();
    let mut timers = crate::Timers::with_capacity(1);
    let mut producer = timers.shared_producer();
    let handles: Vec<_> = (0..timers.capacity())
        .map(|_| producer.try_schedule(now, 5).unwrap())
        .collect();
    assert_eq!(producer.try_schedule(now, 6).err(), Some(6));
    for _ in &handles {
        assert_eq!(timers.pop_expired(now), Some(5));
    }
    drop(handles);
    producer.try_schedule(now, 7).unwrap();
    drop(timers);
    assert_eq!(producer.try_schedule(now, 8).err(), Some(8));
}

#[test]
fn cancellation_traffic_does_not_block_other_producers() {
    let mut driver = TimerDriver::new();
    let mut busy = driver.producer(1);
    let mut other = driver.producer(1);
    let now = Instant::now();
    let first = busy.try_schedule(now + Duration::from_secs(60), 0).unwrap();
    assert!(first.cancel());
    drop(first);
    other.try_schedule(now, 1).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let done = Arc::clone(&stop);
    let sender = thread::spawn(move || {
        while !done.load(Ordering::Acquire) {
            if let Ok(handle) = busy.try_schedule(now + Duration::from_secs(60), 0) {
                assert!(handle.cancel());
            } else {
                thread::yield_now();
            }
        }
    });
    let mut output = Vec::with_capacity(1);
    assert_eq!(driver.poll(now, &mut output), 1);
    assert_eq!(output, [1]);
    stop.store(true, Ordering::Release);
    sender.join().unwrap();
}
