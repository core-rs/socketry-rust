// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use socketry_executor::{Cancellation, Clock, OperationError, OperationResult};
use std::{
    io,
    sync::Mutex,
    time::{Duration, Instant},
};

static NEVER: Cancellation = Cancellation::never();

// This simulator advances directly to the requested deadline when polled.
// It demonstrates the Clock contract without choosing an I/O backend.
struct SimulatedClock {
    time: Mutex<Instant>,
    deadlines: Mutex<Vec<Instant>>,
}

impl SimulatedClock {
    fn new() -> Self {
        Self {
            time: Mutex::new(Instant::now()),
            deadlines: Mutex::new(Vec::new()),
        }
    }
}

impl Clock for SimulatedClock {
    fn now(&self) -> Instant {
        *self.time.lock().unwrap()
    }

    async fn sleep_until(
        &self,
        deadline: Instant,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        self.deadlines.lock().unwrap().push(deadline);
        let mut time = self.time.lock().unwrap();
        *time = (*time).max(deadline);
        Ok(())
    }
}

#[test]
fn relative_sleep_observes_the_scheduler_clock_on_first_poll() {
    let clock = SimulatedClock::new();
    let started = clock.now();
    let sleep = clock.sleep(Duration::from_secs(5), &NEVER);
    assert!(clock.deadlines.lock().unwrap().is_empty());
    *clock.time.lock().unwrap() += Duration::from_secs(20);
    futures_lite::future::block_on(sleep).unwrap();
    assert_eq!(clock.now(), started + Duration::from_secs(25));
    assert_eq!(*clock.deadlines.lock().unwrap(), vec![clock.now()]);
}

#[test]
fn pre_cancelled_and_overflowing_relative_sleeps_do_not_register() {
    let clock = SimulatedClock::new();
    let cancellation = Cancellation::new();
    cancellation.cancel();
    assert!(matches!(
        futures_lite::future::block_on(clock.sleep(Duration::MAX, &cancellation)),
        Err(OperationError::Cancelled(_))
    ));
    assert!(matches!(
        futures_lite::future::block_on(clock.sleep(Duration::MAX, &NEVER)),
        Err(OperationError::Io(error)) if error.kind() == io::ErrorKind::InvalidInput
    ));
    assert!(clock.deadlines.lock().unwrap().is_empty());
}

#[cfg(feature = "native")]
mod native {
    use super::*;
    use futures_lite::future::{block_on, poll_once};
    use socketry_executor::Scheduler;
    use std::{
        future::Future,
        pin::pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Wake, Waker},
    };

    fn immediate_waits(clock: &impl Clock) {
        let started = clock.now();
        block_on(clock.sleep_until(started, &NEVER)).unwrap();
        block_on(clock.sleep(Duration::ZERO, &NEVER)).unwrap();
        assert!(clock.now() >= started);
        let cancellation = Cancellation::new();
        cancellation.cancel();
        assert!(matches!(
            block_on(clock.sleep_until(started, &cancellation)),
            Err(OperationError::Cancelled(_))
        ));
    }

    #[test]
    fn scheduler_handle_and_selector_use_monotonic_deadlines() {
        let scheduler = Scheduler::with_workers(1).unwrap();
        immediate_waits(&scheduler);
        immediate_waits(&scheduler.handle());
        immediate_waits(
            &socketry_executor::scheduler::selector::readiness::Selector::new().unwrap(),
        );
        #[cfg(all(feature = "io-uring", target_os = "linux"))]
        immediate_waits(
            &socketry_executor::scheduler::selector::io_uring::Selector::new().unwrap(),
        );
        let deadline = scheduler.now() + Duration::from_millis(5);
        scheduler
            .block_on(scheduler.sleep_until(deadline, &NEVER))
            .unwrap();
        assert!(scheduler.now() >= deadline);
    }

    #[test]
    fn a_pending_wait_observes_cancellation() {
        let scheduler = Scheduler::with_workers(1).unwrap();
        let cancellation = Cancellation::new();
        let mut sleep = pin!(scheduler.sleep(Duration::from_secs(3600), &cancellation));
        assert!(block_on(poll_once(sleep.as_mut())).is_none());
        cancellation.cancel();
        assert!(matches!(block_on(sleep), Err(OperationError::Cancelled(_))));
    }

    struct WakeCount(AtomicUsize);
    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn dropping_a_wait_unregisters_its_cancellation_listener() {
        let scheduler = Scheduler::with_workers(1).unwrap();
        let cancellation = Cancellation::new();
        let count = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = Waker::from(count.clone());
        let mut context = Context::from_waker(&waker);
        let mut sleep = Box::pin(scheduler.sleep(Duration::from_secs(3600), &cancellation));
        assert!(sleep.as_mut().poll(&mut context).is_pending());
        drop(sleep);
        count.0.store(0, Ordering::SeqCst);
        cancellation.cancel();
        assert_eq!(count.0.load(Ordering::SeqCst), 0);
    }
}

#[cfg(feature = "tokio")]
mod tokio_clock {
    use super::*;
    use futures_lite::future::{block_on, poll_once};
    use socketry_executor::scheduler::tokio::Scheduler;
    use std::pin::pin;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap()
    }

    #[test]
    fn now_uses_the_selected_runtime_even_inside_another_runtime() {
        let selected = runtime();
        let other = runtime();
        let scheduler = Scheduler::new(selected.handle().clone());
        let started = scheduler.now();
        selected.block_on(tokio::time::advance(Duration::from_secs(10)));
        assert_eq!(scheduler.now(), started + Duration::from_secs(10));
        other.block_on(async {
            tokio::time::advance(Duration::from_secs(100)).await;
            assert_eq!(scheduler.now(), started + Duration::from_secs(10));
            assert_eq!(scheduler.handle().now(), scheduler.now());
        });
    }

    #[test]
    fn relative_and_absolute_waits_preserve_virtual_time_and_laziness() {
        let runtime = runtime();
        let scheduler = Scheduler::new(runtime.handle().clone());
        let started = scheduler.now();
        let relative = scheduler.sleep(Duration::from_secs(10), &NEVER);
        let absolute = scheduler.sleep_until(started + Duration::from_secs(5), &NEVER);
        // Creating a future outside its runtime must not register it.
        runtime.block_on(async {
            tokio::time::advance(Duration::from_secs(20)).await;
            absolute.await.unwrap();
            assert_eq!(scheduler.now(), started + Duration::from_secs(20));
            relative.await.unwrap();
            assert!(scheduler.now() >= started + Duration::from_secs(30));
            scheduler.sleep(Duration::ZERO, &NEVER).await.unwrap();
        });
    }

    #[test]
    fn cancellation_before_and_during_a_wait_does_not_advance_time() {
        let runtime = runtime();
        let scheduler = Scheduler::new(runtime.handle().clone());
        let started = scheduler.now();
        let cancellation = Cancellation::new();
        cancellation.cancel();
        assert!(matches!(
            block_on(scheduler.sleep_until(started, &cancellation)),
            Err(OperationError::Cancelled(_))
        ));
        let cancellation = Cancellation::new();
        let mut sleep = pin!(scheduler.sleep(Duration::from_secs(3600), &cancellation));
        // Poll through an unrelated executor; the adapter selects its runtime.
        assert!(block_on(poll_once(sleep.as_mut())).is_none());
        cancellation.cancel();
        assert!(matches!(block_on(sleep), Err(OperationError::Cancelled(_))));
        assert_eq!(scheduler.now(), started);
    }

    #[test]
    fn elapsed_wait_wins_a_cancellation_race_after_registration() {
        let runtime = runtime();
        let scheduler = Scheduler::new(runtime.handle().clone());
        let cancellation = Cancellation::new();
        runtime.block_on(async {
            let mut sleep = pin!(scheduler.sleep(Duration::from_secs(1), &cancellation));
            assert!(poll_once(sleep.as_mut()).await.is_none());
            tokio::time::advance(Duration::from_secs(2)).await;
            cancellation.cancel();
            sleep.await.unwrap();
        });
    }
}
