use super::*;
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Barrier, mpsc};

#[test]
fn pending_deadlines_are_visible_and_assistance_is_capacity_limited() {
    let mut owner = WorkerTimers::with_capacity(8);
    let helper = owner.assistance();
    let now = Instant::now();
    assert_eq!(helper.next_deadline(), None);
    let mut output = Vec::with_capacity(1);
    assert_eq!(helper.try_poll(now, &mut output), Some(0));
    let later = now + Duration::from_secs(2);
    owner.try_schedule(later, 2).unwrap();
    assert_eq!(helper.next_deadline(), Some(later));
    assert_eq!(helper.try_poll(now, &mut output), Some(0));
    helper.try_schedule(now, 1).unwrap();
    assert_eq!(helper.next_deadline(), Some(now));
    assert_eq!(helper.try_poll(now, &mut Vec::new()), Some(0));
    assert_eq!(helper.try_poll(now, &mut output), Some(1));
    assert_eq!(output, [1]);
    assert_eq!(helper.try_poll(later, &mut output), Some(0));
    assert_eq!(owner.next_deadline(), Some(later));
    output.clear();
    assert_eq!(owner.poll(later, &mut output), 1);
    assert_eq!(output, [2]);
    assert_eq!(helper.next_deadline(), None);
}

#[test]
fn historical_deadlines_and_busy_queues_can_be_assisted() {
    let now = Instant::now();
    let mut owner = WorkerTimers::with_capacity(1);
    let helper = owner.assistance();
    owner.try_schedule(now, 3).unwrap();
    let guard = lock(&owner.timers.shared);
    let mut output = Vec::with_capacity(1);
    assert_eq!(helper.try_poll(now, &mut output), None);
    drop(guard);
    assert_eq!(helper.try_poll(now, &mut output), Some(1));
    assert_eq!(output, [3]);
}

#[test]
fn pressure_cleanup_reuses_slots_and_preserves_cloned_handles() {
    let mut owner = WorkerTimers::with_capacity(2);
    let helper = owner.assistance();
    let now = Instant::now();
    let handles: Vec<_> = (0..owner.capacity())
        .map(|index| {
            owner
                .try_schedule(now + Duration::from_secs(index as u64 + 60), index)
                .unwrap()
        })
        .collect();
    let clone = handles[1].clone();
    owner.next_deadline();
    assert!(handles[1].cancel());
    assert!(clone.is_cancelled());
    assert!(!clone.cancel());
    assert_eq!(helper.try_schedule(now, 99).err(), Some(99));
    let mut output = Vec::with_capacity(owner.capacity());
    assert_eq!(helper.try_poll(now, &mut output), Some(0));
    assert_eq!(helper.try_schedule(now, 99).err(), Some(99));
    drop(clone);
    drop(handles);
    owner.try_schedule(now, 99).unwrap();
    assert_eq!(helper.try_poll(now, &mut output), Some(1));
    assert_eq!(output, [99]);
}

#[test]
fn cancel_all_pending_requests_cleanup_before_their_deadline() {
    let mut owner = WorkerTimers::with_capacity(4);
    let helper = owner.assistance();
    let now = Instant::now();
    let handle = owner
        .try_schedule(now + Duration::from_secs(60), 1)
        .unwrap();
    assert!(handle.cancel());
    drop(handle);
    let mut output = Vec::with_capacity(1);
    assert_eq!(helper.try_poll(now, &mut output), Some(0));
    assert_eq!(helper.next_deadline(), None);
    for _ in 0..owner.capacity() {
        owner.try_schedule(now, 2).unwrap();
    }
    assert_eq!(owner.try_schedule(now, 3).err(), Some(3));
}

#[test]
fn stalled_owner_can_be_assisted_with_send_only_values() {
    let owner = WorkerTimers::<Cell<usize>>::with_capacity(4);
    let helper = owner.assistance();
    let (registered_tx, registered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let stalled = thread::spawn(move || {
        let mut owner = owner;
        let now = Instant::now();
        owner.try_schedule(now, Cell::new(7)).unwrap();
        registered_tx.send(now).unwrap();
        resume_rx.recv().unwrap(); // No owner flush or poll before assistance.
    });
    let now = registered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let mut output = Vec::with_capacity(1);
    assert_eq!(helper.try_poll(now, &mut output), Some(1));
    assert_eq!(output[0].get(), 7);
    output[0].set(8);
    resume_tx.send(()).unwrap();
    stalled.join().unwrap();
}

#[test]
fn assistance_and_remote_cancellation_claim_values_once() {
    struct Value(Arc<AtomicUsize>);
    impl Drop for Value {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    for _ in 0..32 {
        let mut owner = WorkerTimers::with_capacity(1);
        let helper = owner.assistance();
        let now = Instant::now();
        let dropped = Arc::new(AtomicUsize::new(0));
        let handle = owner
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
        loop {
            if helper.try_poll(now, &mut output).is_some() {
                break;
            }
            thread::yield_now();
        }
        let cancelled = cancelling.join().unwrap();
        assert_eq!(output.len(), usize::from(!cancelled));
        drop(output);
        drop(owner);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn poisoned_queue_and_watcher_locks_recover() {
    let mut owner = WorkerTimers::with_capacity(4);
    let helper = owner.assistance();
    let now = Instant::now();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = lock_watchers(&owner.publication.watchers);
            panic!("watcher poison");
        }))
        .is_err()
    );
    owner.try_schedule(now, 4).unwrap();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = lock(&owner.timers.shared);
            panic!("queue poison");
        }))
        .is_err()
    );
    let mut output = Vec::with_capacity(1);
    assert_eq!(helper.try_poll(now, &mut output), Some(1));
    assert_eq!(output, [4]);
}

#[test]
fn shutdown_rejects_external_registration_and_cached_assistance() {
    let mut owner = WorkerTimers::with_capacity(4);
    let helper = owner.assistance();
    let now = Instant::now();
    let handle = owner.try_schedule(now, 5).unwrap();
    assert!(helper.publication.due(now)); // A helper already observed this hint.
    drop(owner);
    assert_eq!(helper.try_schedule(now, 6).err(), Some(6));
    assert_eq!(helper.collect(now, &mut Vec::with_capacity(1)), Some(0));
    assert_eq!(helper.next_deadline(), None);
    assert!(!handle.cancel());
    assert!(!handle.is_cancelled());
}

#[test]
fn waiter_wakes_for_an_unflushed_earlier_deadline_and_shutdown() {
    let mut owner = WorkerTimers::with_capacity(4);
    owner
        .try_schedule(Instant::now() + Duration::from_secs(60), 1)
        .unwrap();
    let peer = owner.assistance();
    let (bound_tx, bound_rx) = mpsc::channel();
    let (delivered_tx, delivered_rx) = mpsc::channel();
    let waiting = thread::spawn(move || {
        let waiter = TimerWaiter::new(std::slice::from_ref(&peer));
        bound_tx.send(()).unwrap();
        let mut output = Vec::with_capacity(1);
        loop {
            waiter.park();
            if peer.try_poll(Instant::now(), &mut output) == Some(1) {
                break;
            }
        }
        delivered_tx.send(output[0]).unwrap();
        while !peer.publication.closed.load(Ordering::Acquire) {
            waiter.park();
        }
        waiter.park(); // All queues closed: returns instead of parking forever.
    });
    bound_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    owner.try_schedule(Instant::now(), 2).unwrap();
    assert_eq!(
        delivered_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        2
    );
    drop(owner);
    waiting.join().unwrap();
}

#[test]
fn empty_waiter_wakes_on_first_submission_and_unregisters() {
    let mut owner = WorkerTimers::with_capacity(4);
    let peer = owner.assistance();
    let observed = peer.clone();
    let (bound_tx, bound_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let waiting = thread::spawn(move || {
        let waiter = TimerWaiter::new(std::slice::from_ref(&peer));
        bound_tx.send(()).unwrap();
        let mut output = Vec::with_capacity(1);
        loop {
            waiter.park();
            if peer.try_poll(Instant::now(), &mut output) == Some(1) {
                break;
            }
        }
        done_tx.send(output[0]).unwrap();
    });
    bound_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    owner.try_schedule(Instant::now(), 9).unwrap();
    assert_eq!(done_rx.recv_timeout(Duration::from_secs(2)).unwrap(), 9);
    waiting.join().unwrap();
    assert!(lock_watchers(&observed.publication.watchers).is_empty());
    let empty = TimerWaiter::new(&[owner.assistance()]);
    thread::current().unpark(); // Spurious tokens are valid, including when empty.
    empty.park();
    TimerWaiter::new::<usize>(&[]).park();
}

#[test]
fn waiter_uses_the_earliest_of_multiple_queues_and_is_thread_bound() {
    let mut left = WorkerTimers::with_capacity(1);
    let mut right = WorkerTimers::with_capacity(1);
    let now = Instant::now();
    left.try_schedule(now + Duration::from_secs(60), 1).unwrap();
    right.try_schedule(now, 2).unwrap();
    let waiter = TimerWaiter::new(&[left.assistance(), right.assistance()]);
    waiter.park(); // Already due; no real deadline waiting.
    assert!(thread::spawn(move || waiter.park()).join().is_err());
}

#[test]
fn publication_before_park_preserves_a_wake_token() {
    loom::model(|| {
        use loom::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        let hint = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&hint);
        let waiter = loom::thread::spawn(move || {
            if observed.load(Ordering::Acquire) == 0 {
                loom::thread::park();
            }
            assert_eq!(observed.load(Ordering::Acquire), 1);
        });
        hint.store(1, Ordering::Release);
        waiter.thread().unpark();
        waiter.join().unwrap();
    });
}
