// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

mod support;

use socketry_executor::{Scheduler, SchedulerHandle, Task, TaskError, TaskHandle, yield_now};
use std::future::{pending, poll_fn};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::task::Poll;
use std::thread;
use support::{CountDrop, receive};

#[test]
fn rejects_zero_workers() {
    assert!(Scheduler::with_workers(0).is_err());
}

#[test]
fn public_handles_are_thread_safe() {
    fn assert_send_sync<Output: Send + Sync>() {}
    assert_send_sync::<Scheduler>();
    assert_send_sync::<SchedulerHandle>();
    assert_send_sync::<Task>();
    assert_send_sync::<TaskHandle<usize>>();
}

#[test]
fn joins_output_and_tracks_current_context() {
    let scheduler = Scheduler::with_workers(2).unwrap();
    assert!(Scheduler::current().is_none());
    assert!(Task::current().is_none());
    let handle = scheduler
        .spawn(async {
            let task = Task::current().unwrap();
            assert_eq!(Scheduler::current().unwrap().worker_count(), 2);
            assert_eq!(task.scheduler().unwrap().worker_count(), 2);
            for _ in 0..100 {
                yield_now().await;
                assert_eq!(Task::current().unwrap().id(), task.id());
            }
            (task.id(), 42)
        })
        .unwrap();
    let identifier = handle.id();
    assert_eq!(scheduler.block_on(handle).unwrap(), (identifier, 42));
    assert!(Scheduler::current().is_none());
    assert!(Task::current().is_none());
}

#[test]
fn root_can_borrow_non_send_data() {
    let scheduler = Scheduler::with_workers(1).unwrap();
    let value = Rc::new(42);
    assert_eq!(
        scheduler.block_on(async {
            assert!(Task::current().is_none());
            assert!(Scheduler::current().is_some());
            yield_now().await;
            *value
        }),
        42
    );
    assert_eq!(*value, 42);
}

#[test]
fn remote_wake_and_late_wake_are_safe() {
    let scheduler = Scheduler::with_workers(2).unwrap();
    let ready = Arc::new(AtomicBool::new(false));
    let task_ready = Arc::clone(&ready);
    let (sender, receiver) = mpsc::channel();
    let handle = scheduler
        .spawn(poll_fn(move |context| {
            if task_ready.load(Ordering::Acquire) {
                Poll::Ready(42)
            } else {
                sender.send(context.waker().clone()).unwrap();
                Poll::Pending
            }
        }))
        .unwrap();
    let waker = receive(&receiver);
    ready.store(true, Ordering::Release);
    waker.wake_by_ref();
    assert_eq!(scheduler.block_on(handle).unwrap(), 42);
    scheduler.shutdown();
    for _ in 0..100 {
        waker.wake_by_ref();
    }
}

#[test]
fn wake_during_poll_is_retained_without_concurrent_polling() {
    let scheduler = Scheduler::with_workers(4).unwrap();
    let (wake_sender, wake_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let polling = Arc::new(AtomicBool::new(false));
    let active_poll = Arc::clone(&polling);
    let mut first = true;
    let handle = scheduler
        .spawn(poll_fn(move |context| {
            assert!(!active_poll.swap(true, Ordering::SeqCst));
            let result = if first {
                first = false;
                wake_sender.send(context.waker().clone()).unwrap();
                receive(&release_receiver);
                Poll::Pending
            } else {
                Poll::Ready(42)
            };
            active_poll.store(false, Ordering::SeqCst);
            result
        }))
        .unwrap();
    let waker = receive(&wake_receiver);
    thread::scope(|scope| {
        for _ in 0..8 {
            let waker = waker.clone();
            scope.spawn(move || {
                for _ in 0..1000 {
                    waker.wake_by_ref();
                }
            });
        }
    });
    assert!(polling.load(Ordering::SeqCst));
    release_sender.send(()).unwrap();
    assert_eq!(scheduler.block_on(handle).unwrap(), 42);
}

#[test]
fn dropping_an_unfinished_handle_releases_its_captures() {
    let scheduler = Scheduler::with_workers(1).unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let guard = CountDrop(dropped.clone());
    let task = scheduler
        .spawn(async move {
            let _guard = guard;
            pending::<()>().await;
        })
        .unwrap();
    drop(task);
    scheduler.run();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn panic_is_reported_and_worker_continues() {
    let scheduler = Scheduler::with_workers(1).unwrap();
    let failed = scheduler.spawn(async { panic!("task failure") }).unwrap();
    let error = scheduler.block_on(failed).unwrap_err();
    let TaskError::Panicked(payload) = error else {
        panic!("expected panic")
    };
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"task failure"));
    let healthy = scheduler.spawn(async { 42 }).unwrap();
    assert_eq!(scheduler.block_on(healthy).unwrap(), 42);
}

#[test]
fn shutdown_drops_pending_futures_and_closes_surviving_handles() {
    let scheduler = Scheduler::with_workers(2).unwrap();
    let handle = scheduler.handle();
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for _ in 0..100 {
        let guard = CountDrop(Arc::clone(&dropped));
        let retained_handle = handle.clone();
        tasks.push(
            scheduler
                .spawn(async move {
                    let _guard = guard;
                    let _retained_handle = retained_handle;
                    pending::<()>().await;
                })
                .unwrap(),
        );
    }
    scheduler.shutdown();
    assert_eq!(dropped.load(Ordering::SeqCst), 100);
    assert!(handle.spawn(async {}).is_err());
}

#[test]
fn scheduler_can_be_dropped_by_its_own_task() {
    let scheduler = Scheduler::with_workers(2).unwrap();
    let handle = scheduler.handle();
    let (sender, receiver) = mpsc::channel();
    let _task = handle
        .spawn(async move {
            drop(scheduler);
            sender.send(()).unwrap();
        })
        .unwrap();
    receive(&receiver);
    assert!(handle.spawn(async {}).is_err());
}

#[test]
fn repeated_external_submission_wakes_parked_workers() {
    let scheduler = Scheduler::with_workers(4).unwrap();
    for value in 0..500 {
        let (sender, receiver) = mpsc::channel();
        let _task = scheduler
            .spawn(async move {
                sender.send(value).unwrap();
            })
            .unwrap();
        assert_eq!(receive(&receiver), value);
    }
    scheduler.run();
}

#[test]
fn blocking_entry_points_reject_worker_calls() {
    let scheduler = Arc::new(Scheduler::with_workers(1).unwrap());
    let task_scheduler = Arc::clone(&scheduler);
    let handle = scheduler
        .spawn(async move {
            task_scheduler.block_on(async {});
        })
        .unwrap();
    assert!(matches!(
        scheduler.block_on(handle),
        Err(TaskError::Panicked(_))
    ));
}

#[test]
fn current_scheduler_is_restored_after_root_panic() {
    let scheduler = Scheduler::with_workers(1).unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        scheduler.block_on(async { panic!("root failure") });
    }));
    assert!(result.is_err());
    assert!(Scheduler::current().is_none());
}
