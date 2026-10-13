use super::*;
use crate::sharded::WorkerTimers;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::task::Wake;
use std::thread;
use std::time::Duration;

#[derive(Default)]
struct Counter(AtomicUsize);
impl Wake for Counter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn poll(sleep: &mut Sleep, waker: &Waker) -> Poll<Result<(), SleepError>> {
    Pin::new(sleep).poll(&mut Context::from_waker(waker))
}

#[test]
fn dormant_cancellation_never_locks_or_reserves_a_slot() {
    let owner = WorkerTimers::<SleepWake>::with_capacity(1);
    let clock = owner.assistance();
    let deadline = Instant::now() + Duration::from_secs(60);
    let guard = lock(&clock.shared);
    let sleep = clock.sleep_until(deadline);
    assert_eq!(sleep.deadline(), deadline);
    drop(sleep); // Must finish even with the queue lock held.
    drop(guard);
    assert_eq!(clock.next_deadline(), None);
}

#[test]
fn full_and_closed_queues_fail_without_waking_and_results_are_stable() {
    let owner = WorkerTimers::<SleepWake>::with_capacity(0);
    let clock = owner.assistance();
    let counter = Arc::new(Counter::default());
    let waker = Waker::from(Arc::clone(&counter));
    let mut full = clock.sleep_until(Instant::now());
    assert_eq!(poll(&mut full, &waker), Poll::Ready(Err(SleepError::Full)));
    assert_eq!(poll(&mut full, &waker), Poll::Ready(Err(SleepError::Full)));
    drop(owner);
    let mut closed = clock.sleep_until(Instant::now());
    assert_eq!(
        poll(&mut closed, &waker),
        Poll::Ready(Err(SleepError::Closed))
    );
    assert_eq!(counter.0.load(Ordering::SeqCst), 0);
    assert_eq!(SleepError::Full.to_string(), "sleep timer queue is full");
    assert_eq!(
        SleepError::Closed.to_string(),
        "sleep timer queue is closed"
    );
}

#[test]
fn pending_and_admitted_cancellation_disarm_wakeups_and_reuse_slots() {
    let mut owner = WorkerTimers::<SleepWake>::with_capacity(1);
    let clock = owner.assistance();
    let counter = Arc::new(Counter::default());
    let waker = Waker::from(Arc::clone(&counter));
    for admitted in [false, true] {
        let mut sleep = clock.sleep_until(Instant::now() + Duration::from_secs(60));
        assert!(poll(&mut sleep, &waker).is_pending());
        if admitted {
            owner.next_deadline();
        }
        drop(sleep);
        assert_eq!(owner.next_deadline(), None);
    }
    assert_eq!(counter.0.load(Ordering::SeqCst), 0);
}

#[test]
fn completion_releases_the_slot_and_queue_reference_before_future_destruction() {
    let mut owner = WorkerTimers::<SleepWake>::with_capacity(1);
    let clock = owner.assistance();
    let resident_deadline = Instant::now() + Duration::from_secs(60);
    let _residents: Vec<_> = (1..owner.capacity())
        .map(|_| {
            clock
                .try_schedule(resident_deadline, SleepWake(None))
                .ok()
                .unwrap()
        })
        .collect();
    let references = Arc::strong_count(&clock.shared);
    let now = Instant::now();
    let mut sleep = clock.sleep_until(now);
    assert_eq!(Arc::strong_count(&clock.shared), references + 1);
    assert!(poll(&mut sleep, Waker::noop()).is_pending());
    assert_eq!(Arc::strong_count(&clock.shared), references + 1);
    let mut ready = Vec::with_capacity(1);
    assert_eq!(owner.poll(now, &mut ready), 1);
    ready.clear();

    // An expired but unobserved registration still owns its slot.
    let mut full = clock.sleep_until(now);
    assert_eq!(
        poll(&mut full, Waker::noop()),
        Poll::Ready(Err(SleepError::Full))
    );
    assert_eq!(poll(&mut sleep, Waker::noop()), Poll::Ready(Ok(())));
    assert_eq!(Arc::strong_count(&clock.shared), references);

    // Reuse while the completed future remains alive, then drop that future
    // while holding the queue lock. It must retain neither a key nor an Arc.
    let mut replacement = clock.sleep_until(now);
    assert!(poll(&mut replacement, Waker::noop()).is_pending());
    let guard = lock(&clock.shared);
    assert_eq!(poll(&mut sleep, Waker::noop()), Poll::Ready(Ok(())));
    assert_eq!(
        poll(&mut full, Waker::noop()),
        Poll::Ready(Err(SleepError::Full))
    );
    drop(sleep);
    drop(full);
    drop(guard);
    assert_eq!(owner.poll(now, &mut ready), 1);
    ready.clear();
    assert_eq!(poll(&mut replacement, Waker::noop()), Poll::Ready(Ok(())));
}

#[test]
fn cancellation_releases_its_slot_before_running_waker_destructors() {
    struct ReenterDrop {
        clock: crate::sharded::TimerAssist<SleepWake>,
        dropped: Arc<AtomicUsize>,
    }
    impl Wake for ReenterDrop {
        fn wake(self: Arc<Self>) {
            panic!("cancellation must not wake the task");
        }
    }
    impl Drop for ReenterDrop {
        fn drop(&mut self) {
            let now = Instant::now();
            let mut ready = Vec::with_capacity(1);
            assert_eq!(self.clock.try_poll(now, &mut ready), Some(0));
            let mut replacement = self.clock.sleep_until(now);
            assert!(poll(&mut replacement, Waker::noop()).is_pending());
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
    let mut owner = WorkerTimers::<SleepWake>::with_capacity(1);
    let clock = owner.assistance();
    let resident_deadline = Instant::now() + Duration::from_secs(60);
    let _residents: Vec<_> = (1..owner.capacity())
        .map(|_| {
            clock
                .try_schedule(resident_deadline, SleepWake(None))
                .ok()
                .unwrap()
        })
        .collect();
    let dropped = Arc::new(AtomicUsize::new(0));
    for admitted in [false, true] {
        let waker = Waker::from(Arc::new(ReenterDrop {
            clock: clock.clone(),
            dropped: Arc::clone(&dropped),
        }));
        let mut sleep = clock.sleep_until(Instant::now() + Duration::from_secs(60));
        assert!(poll(&mut sleep, &waker).is_pending());
        drop(waker);
        if admitted {
            owner.next_deadline();
        }
        drop(sleep);
        assert_eq!(owner.next_deadline(), Some(resident_deadline));
    }
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
}

#[test]
fn replacement_and_delivery_run_callbacks_outside_the_queue_lock() {
    struct Reenter {
        clock: crate::sharded::TimerAssist<SleepWake>,
        dropped: Arc<AtomicUsize>,
    }
    impl Wake for Reenter {
        fn wake(self: Arc<Self>) {
            assert!(self.clock.shared.try_lock().is_ok());
        }
    }
    impl Drop for Reenter {
        fn drop(&mut self) {
            assert!(self.clock.shared.try_lock().is_ok());
            drop(self.clock.sleep_until(Instant::now()));
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
    let owner = WorkerTimers::<SleepWake>::with_capacity(1);
    let clock = owner.assistance();
    let now = Instant::now();
    let dropped = Arc::new(AtomicUsize::new(0));
    let original = Waker::from(Arc::new(Reenter {
        clock: clock.clone(),
        dropped: Arc::clone(&dropped),
    }));
    let counter = Arc::new(Counter::default());
    let replacement = Waker::from(Arc::clone(&counter));
    let mut sleep = clock.sleep_until(now);
    assert!(poll(&mut sleep, &original).is_pending());
    drop(original);
    assert!(poll(&mut sleep, &replacement).is_pending());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(poll(&mut sleep, &replacement).is_pending()); // Same waker.
    thread::scope(|scope| {
        let clock = &clock;
        scope
            .spawn(move || {
                let mut ready = Vec::with_capacity(1);
                assert_eq!(clock.try_poll(now, &mut ready), Some(1));
                ready.clear();
            })
            .join()
            .unwrap();
    });
    assert_eq!(counter.0.load(Ordering::SeqCst), 1);
    assert_eq!(poll(&mut sleep, &replacement), Poll::Ready(Ok(())));
    assert_eq!(poll(&mut sleep, &replacement), Poll::Ready(Ok(())));

    // The waker can also reenter during actual delivery.
    let reenter = Waker::from(Arc::new(Reenter {
        clock: clock.clone(),
        dropped: Arc::clone(&dropped),
    }));
    let mut sleep = clock.sleep_until(now);
    assert!(poll(&mut sleep, &reenter).is_pending());
    let mut ready = Vec::with_capacity(1);
    assert_eq!(clock.try_poll(now, &mut ready), Some(1));
    drop(sleep); // Already claimed: cancellation must not suppress delivery.
    ready.clear();
    drop(reenter);
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
}

#[test]
fn shutdown_wakes_registered_sleeps_and_can_race_a_poll() {
    struct BlockingWake(mpsc::Sender<()>, std::sync::Mutex<mpsc::Receiver<()>>);
    impl Wake for BlockingWake {
        fn wake(self: Arc<Self>) {
            self.0.send(()).unwrap();
            self.1.lock().unwrap().recv().unwrap();
        }
    }
    let owner = WorkerTimers::<SleepWake>::with_capacity(2);
    let clock = owner.assistance();
    let deadline = Instant::now() + Duration::from_secs(60);
    let counter = Arc::new(Counter::default());
    let waker = Waker::from(Arc::clone(&counter));
    let mut first = clock.sleep_until(deadline);
    assert!(poll(&mut first, &waker).is_pending());
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let blocker = Waker::from(Arc::new(BlockingWake(
        entered_tx,
        std::sync::Mutex::new(release_rx),
    )));
    let mut second = clock.sleep_until(deadline);
    assert!(poll(&mut second, &blocker).is_pending());
    let closing = thread::spawn(move || drop(owner));
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    // Clear drops the last pending value first. Its callback stalls shutdown
    // outside the queue lock, while the first sleep still retains its waker.
    assert_eq!(
        poll(&mut first, &waker),
        Poll::Ready(Err(SleepError::Closed))
    );
    assert_eq!(counter.0.load(Ordering::SeqCst), 0);
    release_tx.send(()).unwrap();
    closing.join().unwrap();
    assert_eq!(
        poll(&mut second, &blocker),
        Poll::Ready(Err(SleepError::Closed))
    );

    let owner = WorkerTimers::<SleepWake>::with_capacity(1);
    let mut sleep = owner.assistance().sleep_until(deadline);
    assert!(poll(&mut sleep, &waker).is_pending());
    drop(owner);
    assert_eq!(counter.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        poll(&mut sleep, &waker),
        Poll::Ready(Err(SleepError::Closed))
    );
}
