// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Allocation counts are measured separately, without instrumenting timings.

use socketry_timers::Timers;
use std::alloc::{GlobalAlloc, Layout, System};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Waker};
use std::time::{Duration, Instant};

static TRACKING: AtomicBool = AtomicBool::new(false);
static CALLS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);

struct CountingAllocator;

fn record(size: usize) {
    if TRACKING.load(Ordering::Relaxed) {
        CALLS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(size, Ordering::Relaxed);
    }
}

// SAFETY: every operation delegates to System with the original pointer and
// layout. Recording uses only atomics and cannot allocate or unwind.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record(size);
        unsafe { System.realloc(pointer, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn measure(label: &str, operation: impl FnOnce()) {
    CALLS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    TRACKING.store(true, Ordering::Relaxed);
    operation();
    TRACKING.store(false, Ordering::Relaxed);
    println!(
        "{label},{},{}",
        CALLS.load(Ordering::Relaxed),
        BYTES.load(Ordering::Relaxed)
    );
}

fn main() {
    let count = if cfg!(debug_assertions) { 40 } else { 10_000 };
    println!(
        "{count} timers; allocation/reallocation calls and cumulative requested bytes (not peak memory)"
    );
    println!("case,calls,bytes");
    measure("socketry register + cancel + reclaim", || {
        let now = Instant::now() + Duration::from_secs(60);
        let mut timers = Timers::new();
        let handles: Vec<_> = (0..count)
            .map(|index| timers.schedule(now + Duration::from_millis(index), Waker::noop().clone()))
            .collect();
        timers.flush();
        for handle in handles {
            assert!(handle.cancel());
        }
        timers.flush();
        assert!(timers.is_empty());
    });

    let mut reserved = None;
    measure("socketry reserved queue setup", || {
        reserved = Some(Timers::with_capacity(count as usize));
    });
    let mut reserved = reserved.unwrap();
    // Caller-owned handle storage is prepared outside the measured lifecycle.
    let mut handles = Vec::with_capacity(count as usize);
    measure("socketry reserved cancel + reclaim", || {
        let now = Instant::now() + Duration::from_secs(60);
        for index in 0..count {
            handles.push(
                reserved
                    .try_schedule(now + Duration::from_millis(index), Waker::noop().clone())
                    .unwrap(),
            );
        }
        reserved.flush();
        for handle in &handles {
            assert!(handle.cancel());
        }
        reserved.flush();
        handles.clear();
        assert!(reserved.is_empty());
    });
    assert_eq!(
        CALLS.load(Ordering::Relaxed),
        0,
        "reserved cancellation must not allocate"
    );
    measure("socketry reserved reuse + expire", || {
        let now = Instant::now();
        for index in 0..count {
            handles.push(
                reserved
                    .try_schedule(now + Duration::from_millis(index), Waker::noop().clone())
                    .unwrap(),
            );
        }
        let mut fired = 0;
        while let Some(waker) = reserved.pop_expired(now + Duration::from_millis(count)) {
            waker.wake();
            fired += 1;
        }
        handles.clear();
        assert_eq!(fired, count);
        reserved.clear();
    });
    assert_eq!(
        CALLS.load(Ordering::Relaxed),
        0,
        "reserved expiry must not allocate"
    );

    let mut output = Vec::with_capacity(count as usize);
    measure("socketry reserved bulk expiry", || {
        let now = Instant::now();
        for index in 0..count {
            handles.push(
                reserved
                    .try_schedule(now + Duration::from_millis(index), Waker::noop().clone())
                    .unwrap(),
            );
        }
        assert_eq!(
            reserved.pop_expired_batch(now + Duration::from_millis(count), &mut output),
            count as usize
        );
        for waker in output.drain(..) {
            waker.wake();
        }
        handles.clear();
    });
    assert_eq!(
        CALLS.load(Ordering::Relaxed),
        0,
        "reserved bulk expiry must not allocate"
    );

    #[cfg(feature = "driver")]
    {
        use socketry_timers::driver::TimerDriver;
        let mut bounded = None;
        measure("driver bounded pool setup", || {
            let mut driver = TimerDriver::new();
            let producer = driver.producer(count as usize);
            driver.next_deadline();
            bounded = Some((driver, producer));
        });
        let (mut driver, mut producer) = bounded.unwrap();
        let mut handles = Vec::with_capacity(count as usize);
        measure("driver reserved cancel + reclaim", || {
            let now = Instant::now();
            for _ in 0..count {
                handles.push(producer.try_schedule(now, Waker::noop().clone()).unwrap());
            }
            driver.next_deadline();
            for handle in &handles {
                assert!(handle.cancel());
            }
            driver.next_deadline();
            handles.clear();
        });
        assert_eq!(CALLS.load(Ordering::Relaxed), 0);
        measure("driver reserved reuse + bulk expire", || {
            let now = Instant::now();
            for _ in 0..count {
                handles.push(producer.try_schedule(now, Waker::noop().clone()).unwrap());
            }
            assert_eq!(driver.poll(now, &mut output), count as usize);
            for waker in output.drain(..) {
                waker.wake();
            }
            handles.clear();
        });
        assert_eq!(CALLS.load(Ordering::Relaxed), 0);
    }

    #[cfg(feature = "sharded")]
    {
        use socketry_timers::sharded::{SleepWake, TimerWaiter, WorkerTimers};
        let mut sharded = None;
        measure("sharded queue + waiter setup", || {
            let owner = WorkerTimers::with_capacity(count as usize);
            let peer = owner.assistance();
            let waiter = TimerWaiter::new(std::slice::from_ref(&peer));
            sharded = Some((owner, peer, waiter));
        });
        let (mut owner, peer, waiter) = sharded.unwrap();
        let mut handles = Vec::with_capacity(count as usize);
        measure("sharded reserved pending cancel + reclaim", || {
            let now = Instant::now();
            for _ in 0..count {
                handles.push(owner.try_schedule(now, Waker::noop().clone()).unwrap());
            }
            for handle in &handles {
                assert!(handle.cancel());
            }
            handles.clear();
            assert_eq!(peer.try_poll(now, &mut output), Some(0));
            waiter.park(); // Consume the preserved registration wake token.
        });
        assert_eq!(CALLS.load(Ordering::Relaxed), 0);
        measure("sharded reserved admitted cancel + reclaim", || {
            let now = Instant::now();
            for _ in 0..count {
                handles.push(owner.try_schedule(now, Waker::noop().clone()).unwrap());
            }
            owner.next_deadline();
            for handle in &handles {
                assert!(handle.cancel());
            }
            handles.clear();
            assert_eq!(peer.try_poll(now, &mut output), Some(0));
        });
        assert_eq!(CALLS.load(Ordering::Relaxed), 0);
        measure("sharded reserved reuse + assisted expiry", || {
            let now = Instant::now();
            for _ in 0..count {
                handles.push(owner.try_schedule(now, Waker::noop().clone()).unwrap());
            }
            assert_eq!(peer.try_poll(now, &mut output), Some(count as usize));
            for waker in output.drain(..) {
                waker.wake();
            }
            handles.clear();
        });
        assert_eq!(CALLS.load(Ordering::Relaxed), 0);
        let mut owner = WorkerTimers::<SleepWake>::with_capacity(count as usize);
        let clock = owner.assistance();
        let mut sleeps = Vec::with_capacity(count as usize);
        let mut ready = Vec::with_capacity(count as usize);
        let mut context = Context::from_waker(Waker::noop());
        measure("lazy sleep reserved lifecycles", || {
            let now = Instant::now();
            for _ in 0..count {
                drop(clock.sleep_until(now));
            }
            for _ in 0..count {
                let mut sleep = clock.sleep_until(now);
                assert!(Pin::new(&mut sleep).poll(&mut context).is_pending());
                sleeps.push(sleep);
            }
            sleeps.clear();
            owner.next_deadline();
            for _ in 0..count {
                let mut sleep = clock.sleep_until(now);
                assert!(Pin::new(&mut sleep).poll(&mut context).is_pending());
                sleeps.push(sleep);
            }
            assert_eq!(clock.try_poll(now, &mut ready), Some(count as usize));
            ready.clear();
            for sleep in &mut sleeps {
                assert_eq!(
                    Pin::new(sleep).poll(&mut context),
                    std::task::Poll::Ready(Ok(()))
                );
            }
            sleeps.clear();
        });
        assert_eq!(CALLS.load(Ordering::Relaxed), 0);
    }

    let mut runtime = None;
    measure("tokio runtime setup", || {
        runtime = Some(
            tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()
                .unwrap(),
        );
    });
    let runtime = runtime.unwrap();
    let _scope = runtime.enter();
    measure("tokio register + cancel + reclaim", || {
        let now = tokio::time::Instant::now() + Duration::from_secs(60);
        let sleeps: Box<[_]> = (0..count)
            .map(|index| tokio::time::sleep_until(now + Duration::from_millis(index)))
            .collect();
        let mut sleeps = Box::into_pin(sleeps);
        let mut context = Context::from_waker(Waker::noop());
        for index in 0..count as usize {
            // SAFETY: the fixed-length boxed slice remains pinned until drop;
            // no registered Sleep is moved out or replaced.
            let sleep: Pin<&mut tokio::time::Sleep> =
                unsafe { sleeps.as_mut().map_unchecked_mut(|slice| &mut slice[index]) };
            assert!(sleep.poll(&mut context).is_pending());
        }
        drop(sleeps); // Pinned destruction deregisters all entries.
    });
}
