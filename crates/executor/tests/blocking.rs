// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

mod support;

use futures_lite::future::{block_on, poll_once};
use socketry_executor::{
    Blocking, BlockingError, BlockingOptions, Cancellation, Scheduler, TaskError,
};
use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Wake, Waker};
use support::{CountDrop, receive};

static NEVER: Cancellation = Cancellation::never();

fn options(capacity: usize) -> BlockingOptions {
    BlockingOptions {
        threads: 1,
        capacity,
    }
}

// Holding this guard keeps one job running. Dropping it always releases the worker, including when a test assertion panics.
struct Gate(Option<mpsc::Sender<()>>);
impl Drop for Gate {
    fn drop(&mut self) {
        let _ = self.0.take().unwrap().send(());
    }
}

fn running<S: Blocking>(scheduler: &S, cancellation: &Cancellation) -> (S::Handle<usize>, Gate) {
    let (started, start) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let job = block_on(scheduler.spawn_blocking(
        move || {
            started.send(()).unwrap();
            receive(&released);
            42
        },
        cancellation,
    ))
    .unwrap();
    let gate = Gate(Some(release));
    receive(&start);
    (job, gate)
}

fn lazy_and_owned(scheduler: &impl Blocking) {
    let drops = Arc::new(AtomicUsize::new(0));
    let captured = CountDrop(drops.clone());
    let admission = scheduler.spawn_blocking(
        move || {
            drop(captured);
            panic!("an unpolled admission must not execute");
        },
        &NEVER,
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(admission);
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    let input = Box::new(String::from("owned input"));
    let job =
        block_on(scheduler.spawn_blocking(move || Ok::<_, &'static str>(input), &NEVER)).unwrap();
    assert_eq!(block_on(job).unwrap().unwrap().as_str(), "owned input");

    let cancelled = Cancellation::new();
    cancelled.cancel();
    assert!(matches!(
        block_on(scheduler.spawn_blocking(|| panic!("cancelled"), &cancelled)),
        Err(BlockingError::Cancelled)
    ));
}

#[derive(Default)]
struct WakeCount(AtomicUsize);
impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn bounded_admission(scheduler: &impl Blocking) {
    let (first, gate) = running(scheduler, &NEVER);
    let cancellation = Cancellation::new();
    let count = Arc::new(WakeCount::default());
    let waker = Waker::from(count.clone());
    let mut context = Context::from_waker(&waker);
    let mut second = Box::pin(scheduler.spawn_blocking(|| 2, &cancellation));
    assert!(second.as_mut().poll(&mut context).is_pending());
    cancellation.cancel();
    assert!(count.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(block_on(second), Err(BlockingError::Cancelled)));

    // Dropping a waiter returns its captures and does not reserve capacity.
    let drops = Arc::new(AtomicUsize::new(0));
    let captured = CountDrop(drops.clone());
    let mut abandoned = Box::pin(scheduler.spawn_blocking(move || drop(captured), &NEVER));
    assert!(block_on(poll_once(abandoned.as_mut())).is_none());
    drop(abandoned);
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    // Dropping the running job's handle must not release its capacity early.
    drop(first);
    let mut third = pin!(scheduler.spawn_blocking(|| 3, &NEVER));
    assert!(third.as_mut().poll(&mut context).is_pending());
    drop(gate);
    let job = block_on(third).unwrap();
    assert_eq!(block_on(job).unwrap(), 3);
}

fn queued_cancellation(scheduler: &impl Blocking) {
    let (first, gate) = running(scheduler, &NEVER);
    let cancellation = Cancellation::new();
    let skipped =
        block_on(scheduler.spawn_blocking(|| panic!("cancelled queued job ran"), &cancellation))
            .unwrap();
    cancellation.cancel();
    let calls = Arc::new(AtomicUsize::new(0));
    let executed = calls.clone();
    let abandoned =
        block_on(scheduler.spawn_blocking(move || executed.fetch_add(1, Ordering::SeqCst), &NEVER))
            .unwrap();
    drop(abandoned);
    let mut full = pin!(scheduler.spawn_blocking(|| 9, &NEVER));
    assert!(block_on(poll_once(full.as_mut())).is_none());
    drop(gate);
    assert_eq!(block_on(first).unwrap(), 42);
    assert!(matches!(block_on(skipped), Err(TaskError::Cancelled)));
    assert_eq!(block_on(block_on(full).unwrap()).unwrap(), 9);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

fn completion_and_panic(scheduler: &impl Blocking) {
    let cancellation = Cancellation::new();
    let (job, gate) = running(scheduler, &cancellation);
    cancellation.cancel();
    drop(gate);
    assert_eq!(block_on(job).unwrap(), 42);

    let job = block_on(scheduler.spawn_blocking(|| panic!("job panic"), &NEVER)).unwrap();
    assert!(
        matches!(block_on(job), Err(TaskError::Panicked(payload)) if payload.downcast_ref::<&str>() == Some(&"job panic"))
    );
    let job = block_on(scheduler.spawn_blocking(|| 7, &NEVER)).unwrap();
    assert_eq!(block_on(job).unwrap(), 7);
}

fn parallel_workers(scheduler: &impl Blocking) {
    let (first, first_gate) = running(scheduler, &NEVER);
    let (second, second_gate) = running(scheduler, &NEVER);
    let (started, start) = mpsc::channel();
    let third =
        block_on(scheduler.spawn_blocking(move || started.send(()).unwrap(), &NEVER)).unwrap();
    assert!(matches!(start.try_recv(), Err(mpsc::TryRecvError::Empty)));
    drop(first_gate);
    receive(&start);
    block_on(first).unwrap();
    block_on(third).unwrap();
    drop(second_gate);
    block_on(second).unwrap();
}

fn shutdown<S: Blocking>(scheduler: S, close: impl FnOnce()) {
    let (first, gate) = running(&scheduler, &NEVER);
    let queued =
        block_on(scheduler.spawn_blocking(|| panic!("shutdown queued job ran"), &NEVER)).unwrap();
    let mut admission = pin!(scheduler.spawn_blocking(|| panic!("shutdown admitted work"), &NEVER));
    let count = Arc::new(WakeCount::default());
    let waker = Waker::from(count.clone());
    let mut context = Context::from_waker(&waker);
    assert!(admission.as_mut().poll(&mut context).is_pending());
    close();
    assert!(count.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        block_on(admission),
        Err(BlockingError::SchedulerClosed)
    ));
    assert!(matches!(block_on(queued), Err(TaskError::Cancelled)));
    assert!(matches!(
        block_on(scheduler.spawn_blocking(|| (), &NEVER)),
        Err(BlockingError::SchedulerClosed)
    ));
    drop(gate);
    assert_eq!(block_on(first).unwrap(), 42);
}

#[test]
fn native_contract() {
    let scheduler = Scheduler::with_workers_and_blocking(1, options(1)).unwrap();
    lazy_and_owned(&scheduler);
    bounded_admission(&scheduler.handle());
    completion_and_panic(&scheduler);
    let scheduler = Scheduler::with_workers_and_blocking(1, options(3)).unwrap();
    queued_cancellation(&scheduler);
    let scheduler = Scheduler::with_workers_and_blocking(1, options(2)).unwrap();
    shutdown(scheduler.handle(), || scheduler.shutdown());
    let scheduler = Scheduler::with_workers_and_blocking(
        1,
        BlockingOptions {
            threads: 2,
            capacity: 3,
        },
    )
    .unwrap();
    parallel_workers(&scheduler);
}

#[test]
fn native_inherent_methods_and_future_workers_remain_responsive() {
    let scheduler = Scheduler::with_workers_and_blocking(1, options(1)).unwrap();
    let handle = scheduler.handle();
    scheduler.block_on(async {
        assert_eq!(
            scheduler
                .spawn_blocking(|| 1, &NEVER)
                .await
                .unwrap()
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            handle
                .spawn_blocking(|| 2, &NEVER)
                .await
                .unwrap()
                .await
                .unwrap(),
            2
        );
    });
    let (job, gate) = running(&scheduler, &NEVER);
    let (waiting, waited) = mpsc::channel();
    let task = scheduler
        .spawn(async move {
            let mut admission = pin!(handle.spawn_blocking(|| 5, &NEVER));
            assert!(poll_once(admission.as_mut()).await.is_none());
            waiting.send(()).unwrap();
            admission.await.unwrap().await.unwrap()
        })
        .unwrap();
    receive(&waited);
    assert_eq!(
        scheduler
            .block_on(scheduler.spawn(async { 6 }).unwrap())
            .unwrap(),
        6
    );
    drop(gate);
    assert_eq!(scheduler.block_on(job).unwrap(), 42);
    assert_eq!(scheduler.block_on(task).unwrap(), 5);
}

#[test]
fn options_reject_zero_limits() {
    for options in [
        BlockingOptions {
            threads: 0,
            capacity: 1,
        },
        options(0),
    ] {
        assert!(
            matches!(Scheduler::with_workers_and_blocking(1, options), Err(error) if error.kind() == std::io::ErrorKind::InvalidInput)
        );
    }
}

#[test]
fn admission_errors_expose_their_cause() {
    use std::error::Error;
    for (error, text) in [
        (BlockingError::Cancelled, "blocking admission cancelled"),
        (BlockingError::SchedulerClosed, "the scheduler is closed"),
    ] {
        assert_eq!(error.to_string(), text);
        assert!(error.source().is_none());
        assert!(!format!("{error:?}").is_empty());
    }
    let error = BlockingError::Io(std::io::Error::other("thread startup failed"));
    assert_eq!(error.to_string(), "thread startup failed");
    assert_eq!(error.source().unwrap().to_string(), "thread startup failed");
    assert!(!format!("{:?}", BlockingOptions::default()).is_empty());
}

#[cfg(feature = "tokio")]
mod tokio {
    use super::*;
    use socketry_executor::scheduler::tokio::Scheduler;

    #[test]
    fn adapter_contract() {
        let runtime = ::tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let scheduler = Scheduler::with_blocking(runtime.handle().clone(), options(1)).unwrap();
        lazy_and_owned(&scheduler);
        bounded_admission(&scheduler.handle());
        completion_and_panic(&scheduler);
        let scheduler = Scheduler::with_blocking(runtime.handle().clone(), options(3)).unwrap();
        queued_cancellation(&scheduler);
        let scheduler = Scheduler::with_blocking(runtime.handle().clone(), options(2)).unwrap();
        shutdown(scheduler.handle(), || {
            runtime.block_on(scheduler.shutdown())
        });
        let scheduler = Scheduler::with_blocking(
            runtime.handle().clone(),
            BlockingOptions {
                threads: 2,
                capacity: 3,
            },
        )
        .unwrap();
        parallel_workers(&scheduler);
    }

    #[test]
    fn adapter_inherent_methods_and_pool_are_independent_of_runtime() {
        let runtime = ::tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let scheduler = Scheduler::new(runtime.handle().clone());
        let handle = scheduler.handle();
        // The dedicated pool can finish synchronous work even when the external runtime stops.
        drop(runtime);
        block_on(async {
            assert_eq!(
                scheduler
                    .spawn_blocking(|| 1, &NEVER)
                    .await
                    .unwrap()
                    .await
                    .unwrap(),
                1
            );
            assert_eq!(
                handle
                    .spawn_blocking(|| 2, &NEVER)
                    .await
                    .unwrap()
                    .await
                    .unwrap(),
                2
            );
        });
    }

    #[test]
    fn adapter_rejects_zero_limits() {
        let runtime = ::tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        for options in [
            BlockingOptions {
                threads: 0,
                capacity: 1,
            },
            options(0),
        ] {
            assert!(
                matches!(Scheduler::with_blocking(runtime.handle().clone(), options), Err(error) if error.kind() == std::io::ErrorKind::InvalidInput)
            );
        }
    }
}
