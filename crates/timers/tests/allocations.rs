// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use socketry_timers::Timers;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::task::Waker;
use std::time::{Duration, Instant};

thread_local! {
    static TRACKING: Cell<bool> = const { Cell::new(false) };
    static CALLS: Cell<usize> = const { Cell::new(0) };
}

struct CountingAllocator;

fn record() {
    let _ = TRACKING.try_with(|tracking| {
        if tracking.get() {
            CALLS.with(|calls| calls.set(calls.get() + 1));
        }
    });
}

// SAFETY: every operation forwards its unchanged pointer/layout to System.
// The thread-local counters have constant initialization and never allocate.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record();
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record();
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record();
        unsafe { System.realloc(pointer, layout, size) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct Tracking;
impl Drop for Tracking {
    fn drop(&mut self) {
        TRACKING.with(|tracking| tracking.set(false));
    }
}

fn allocations(operation: impl FnOnce()) -> usize {
    CALLS.with(|calls| calls.set(0));
    TRACKING.with(|tracking| tracking.set(true));
    let tracking = Tracking;
    operation();
    drop(tracking);
    CALLS.with(Cell::get)
}

#[test]
fn reserved_timer_lifecycles_and_reuse_allocate_nothing() {
    let now = Instant::now();
    let mut timers = Timers::with_capacity(512);
    let mut handles = Vec::with_capacity(512);
    let mut output = Vec::with_capacity(512);
    let capacity = timers.capacity();
    let calls = allocations(|| {
        for _ in 0..3 {
            // Pending cancellation, compaction with a simultaneous new batch,
            // expiry, and slot reuse all run inside the counting window.
            for index in 0..256 {
                handles.push(
                    timers
                        .try_schedule(now + Duration::from_nanos(index), Waker::noop().clone())
                        .unwrap(),
                );
            }
            drop(handles[0].clone());
            timers.flush();
            for handle in &handles[..129] {
                handle.cancel();
            }
            for _ in 0..200 {
                handles.push(timers.try_schedule(now, Waker::noop().clone()).unwrap());
            }
            timers.flush();
            while let Some(waker) = timers.pop_expired(now + Duration::from_secs(1)) {
                waker.wake();
            }
            handles.clear();
            for _ in 0..10 {
                handles.push(timers.schedule(now, Waker::noop().clone()));
            }
            for handle in &handles {
                handle.cancel();
            }
            handles.clear();
            timers.flush();
            for _ in 0..10 {
                handles.push(timers.schedule(now, Waker::noop().clone()));
            }
            timers.flush();
            for handle in &handles {
                handle.cancel();
            }
            handles.clear();
            timers.flush();
            for _ in 0..10 {
                handles.push(timers.schedule(now, Waker::noop().clone()));
            }
            timers.clear();
            handles.clear();
            timers.wait_interval(now);
        }
        for _ in 0..capacity {
            timers.try_schedule(now, Waker::noop().clone()).unwrap();
        }
        assert!(timers.try_schedule(now, Waker::noop().clone()).is_err());
        assert_eq!(timers.pop_expired_batch(now, &mut output), capacity);
        output.clear();
        for index in 0..capacity {
            timers
                .try_schedule(
                    now + Duration::from_nanos(index as u64),
                    Waker::noop().clone(),
                )
                .unwrap();
        }
        assert_eq!(timers.pop_expired_batch(now, &mut output), 1);
        output.clear();
        assert_eq!(
            timers.pop_expired_batch(now + Duration::from_secs(1), &mut output),
            capacity - 1
        );
        output.clear();
        timers.clear();
    });
    assert_eq!(calls, 0, "reserved queue operations must not allocate");
    assert_eq!(timers.capacity(), capacity);
    assert!(timers.is_empty());
}

#[cfg(feature = "driver")]
#[test]
fn bounded_driver_lifecycles_allocate_nothing() {
    use socketry_timers::driver::TimerDriver;
    let now = Instant::now();
    let mut driver = TimerDriver::new();
    let mut producer = driver.producer(512);
    let mut handles = Vec::with_capacity(512);
    let mut output = Vec::with_capacity(512);
    driver.next_deadline(); // Thread binding is setup, not a timer operation.
    let calls = allocations(|| {
        for _ in 0..3 {
            for index in 0..512 {
                handles.push(
                    producer
                        .try_schedule(now + Duration::from_nanos(index), Waker::noop().clone())
                        .unwrap(),
                );
            }
            assert!(producer.try_schedule(now, Waker::noop().clone()).is_err());
            driver.next_deadline();
            for handle in &handles[..256] {
                assert!(handle.cancel());
            }
            driver.next_deadline();
            assert_eq!(driver.poll(now + Duration::from_secs(1), &mut output), 256);
            output.clear();
            handles.clear();
            producer.try_schedule(now, Waker::noop().clone()).unwrap();
            assert_eq!(driver.poll(now, &mut output), 1);
            output.clear();
        }
        // Keep a resident heap while staged cancellations are filtered and
        // sparse heap cancellations accumulate into one batch repair.
        for index in 0..256 {
            handles.push(
                producer
                    .try_schedule(now + Duration::from_nanos(index), Waker::noop().clone())
                    .unwrap(),
            );
        }
        driver.next_deadline();
        for handle in &handles[1..129] {
            assert!(handle.cancel());
        }
        driver.next_deadline();
        for _ in 0..8 {
            let staged = producer.try_schedule(now, Waker::noop().clone()).unwrap();
            assert!(staged.cancel());
            drop(staged);
            driver.next_deadline();
        }
        assert!(handles[129].cancel());
        driver.next_deadline();
        assert_eq!(driver.poll(now + Duration::from_secs(1), &mut output), 127);
        output.clear();
        handles.clear();
        producer.try_schedule(now, Waker::noop().clone()).unwrap();
        drop(driver);
    });
    assert_eq!(calls, 0);
}

#[cfg(feature = "sharded")]
#[test]
fn sharded_registration_assistance_and_reuse_allocate_nothing() {
    use socketry_timers::sharded::{TimerWaiter, WorkerTimers};
    let mut owner = WorkerTimers::with_capacity(512);
    let peer = owner.assistance();
    let waiter = TimerWaiter::new(std::slice::from_ref(&peer));
    let now = Instant::now();
    let capacity = owner.capacity();
    let mut handles = Vec::with_capacity(capacity);
    let mut output = Vec::with_capacity(capacity);
    let calls = allocations(|| {
        for _ in 0..3 {
            for index in 0..capacity {
                handles.push(
                    owner
                        .try_schedule(
                            now + Duration::from_nanos(index as u64),
                            Waker::noop().clone(),
                        )
                        .unwrap(),
                );
            }
            assert!(owner.try_schedule(now, Waker::noop().clone()).is_err());
            owner.next_deadline();
            drop(handles[0].clone());
            for handle in &handles[1..129] {
                assert!(handle.cancel());
            }
            peer.try_poll(now, &mut output); // Pressure cleanup plus first expiry.
            output.clear();
            assert_eq!(
                peer.try_poll(now + Duration::from_secs(1), &mut output),
                Some(capacity - 129)
            );
            output.clear();
            handles.clear();
            for _ in 0..capacity {
                let handle = owner
                    .try_schedule(now + Duration::from_secs(60), Waker::noop().clone())
                    .unwrap();
                assert!(handle.cancel());
                drop(handle);
            }
            assert_eq!(peer.try_poll(now, &mut output), Some(0));
            owner.try_schedule(now, Waker::noop().clone()).unwrap();
            waiter.park(); // Past deadline: returns without real waiting.
            assert_eq!(owner.poll(now, &mut output), 1);
            output.clear();
        }
        peer.try_schedule(now, Waker::noop().clone()).unwrap();
        drop(owner);
    });
    assert_eq!(calls, 0);
}

#[cfg(feature = "sharded")]
#[test]
fn lazy_sleep_lifecycles_allocate_nothing() {
    use socketry_timers::sharded::{SleepWake, WorkerTimers};
    use std::future::Future;
    use std::pin::Pin;
    use std::task::Context;
    let mut owner = WorkerTimers::<SleepWake>::with_capacity(128);
    let clock = owner.assistance();
    let mut ready = Vec::with_capacity(128);
    let mut sleeps = Vec::with_capacity(128);
    let now = Instant::now();
    let mut context = Context::from_waker(Waker::noop());
    let calls = allocations(|| {
        for _ in 0..3 {
            for _ in 0..128 {
                drop(clock.sleep_until(now));
            }
            for _ in 0..128 {
                let mut sleep = clock.sleep_until(now);
                assert!(Pin::new(&mut sleep).poll(&mut context).is_pending());
                sleeps.push(sleep);
            }
            sleeps.clear();
            assert_eq!(owner.next_deadline(), None);
            for _ in 0..128 {
                let mut sleep = clock.sleep_until(now);
                assert!(Pin::new(&mut sleep).poll(&mut context).is_pending());
                assert!(Pin::new(&mut sleep).poll(&mut context).is_pending());
                sleeps.push(sleep);
            }
            assert_eq!(clock.try_poll(now, &mut ready), Some(128));
            ready.clear();
            for sleep in &mut sleeps {
                assert_eq!(
                    Pin::new(sleep).poll(&mut context),
                    std::task::Poll::Ready(Ok(()))
                );
            }
            sleeps.clear();
        }
        let mut sleep = clock.sleep_until(now);
        assert!(Pin::new(&mut sleep).poll(&mut context).is_pending());
        drop(owner);
        assert!(Pin::new(&mut sleep).poll(&mut context).is_ready());
    });
    assert_eq!(calls, 0);
}
