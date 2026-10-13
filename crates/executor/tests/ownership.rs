// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

mod support;

use socketry_executor::{Scheduler, Spawn, TaskError, yield_now};
use std::future::{Future, pending, poll_fn};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::task::Poll;
use support::{CountDrop, receive};

#[test]
fn schedulers_and_handles_share_the_spawn_contract() {
    fn submit(scheduler: &impl Spawn) -> impl Future<Output = Result<usize, TaskError>> + Send {
        scheduler
            .spawn(async {
                yield_now().await;
                42
            })
            .unwrap()
    }
    let scheduler = Scheduler::with_workers(2).unwrap();
    assert_eq!(scheduler.block_on(submit(&scheduler)).unwrap(), 42);
    assert_eq!(scheduler.block_on(submit(&scheduler.handle())).unwrap(), 42);
}

#[test]
fn scheduler_can_observe_completion_while_handles_remain_owned() {
    let scheduler = Scheduler::with_workers(4).unwrap();
    let completed = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for _ in 0..500 {
        let completed = Arc::clone(&completed);
        tasks.push(
            scheduler
                .spawn(async move {
                    yield_now().await;
                    completed.fetch_add(1, Ordering::SeqCst);
                })
                .unwrap(),
        );
    }
    scheduler.run();
    assert_eq!(completed.load(Ordering::SeqCst), 500);
}

#[test]
fn cancellation_before_first_poll_drops_captured_values() {
    let scheduler = Scheduler::with_workers(1).unwrap();
    let (started_sender, started_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let blocker = scheduler
        .spawn(async move {
            started_sender.send(()).unwrap();
            receive(&release_receiver);
        })
        .unwrap();
    receive(&started_receiver);
    let polled = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicUsize::new(0));
    let task_polled = Arc::clone(&polled);
    let guard = CountDrop(Arc::clone(&dropped));
    let handle = scheduler
        .spawn(async move {
            let _guard = guard;
            task_polled.store(true, Ordering::SeqCst);
        })
        .unwrap();
    assert!(handle.task().cancel());
    release_sender.send(()).unwrap();
    assert!(matches!(
        scheduler.block_on(handle),
        Err(TaskError::Cancelled)
    ));
    assert!(!polled.load(Ordering::SeqCst));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    scheduler.block_on(blocker).unwrap();
}

#[test]
fn cancellation_does_not_destroy_an_in_progress_poll() {
    let scheduler = Scheduler::with_workers(2).unwrap();
    let (started_sender, started_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let dropped = Arc::new(AtomicUsize::new(0));
    let guard = CountDrop(Arc::clone(&dropped));
    let handle = scheduler
        .spawn(poll_fn(move |_| {
            let _ = &guard;
            started_sender.send(()).unwrap();
            receive(&release_receiver);
            Poll::<()>::Pending
        }))
        .unwrap();
    receive(&started_receiver);
    assert!(handle.task().cancel());
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    release_sender.send(()).unwrap();
    assert!(matches!(
        scheduler.block_on(handle),
        Err(TaskError::Cancelled)
    ));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn dropping_a_handle_waits_for_an_active_poll_before_destroying_the_future() {
    let scheduler = Scheduler::with_workers(2).unwrap();
    let (started_sender, started_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let dropped = Arc::new(AtomicUsize::new(0));
    let guard = CountDrop(Arc::clone(&dropped));
    let handle = scheduler
        .spawn(poll_fn(move |_| {
            let _ = &guard;
            started_sender.send(()).unwrap();
            receive(&release_receiver);
            Poll::<()>::Pending
        }))
        .unwrap();
    receive(&started_receiver);
    let task = handle.task();
    drop(handle);
    assert!(!task.is_finished());
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    release_sender.send(()).unwrap();
    scheduler.run();
    assert!(task.is_finished());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn cancellation_method_awaits_cleanup() {
    let scheduler = Scheduler::with_workers(1).unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let guard = CountDrop(Arc::clone(&dropped));
    let handle = scheduler
        .spawn(async move {
            let _guard = guard;
            pending::<()>().await;
        })
        .unwrap();
    assert!(matches!(
        scheduler.block_on(handle.cancel()),
        Err(TaskError::Cancelled)
    ));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn cancelled_tasks_report_finished_state_and_reject_late_cancellation() {
    let scheduler = Scheduler::with_workers(1).unwrap();
    let (started, waiting) = std::sync::mpsc::channel();
    let handle = scheduler
        .spawn(async move {
            started.send(()).unwrap();
            pending::<()>().await;
        })
        .unwrap();
    let task = handle.task();

    waiting.recv().unwrap();
    assert!(!handle.is_finished());
    assert!(task.cancel());
    assert!(matches!(
        scheduler.block_on(handle),
        Err(TaskError::Cancelled)
    ));
    assert!(task.is_finished());
    assert!(!task.cancel());
}

#[test]
fn future_destructor_can_submit_work_without_holding_the_registry_lock() {
    struct SubmitOnDrop(mpsc::Sender<u64>);
    impl Drop for SubmitOnDrop {
        fn drop(&mut self) {
            let task = socketry_executor::Task::current().unwrap();
            drop(Scheduler::current().unwrap().spawn(async {}).unwrap());
            self.0.send(task.id()).unwrap();
        }
    }

    let scheduler = Scheduler::with_workers(1).unwrap();
    let (sender, receiver) = mpsc::channel();
    let guard = SubmitOnDrop(sender);
    let handle = scheduler
        .spawn(async move {
            let _guard = guard;
            pending::<()>().await;
        })
        .unwrap();
    let identifier = handle.id();
    assert!(handle.task().cancel());
    assert_eq!(receive(&receiver), identifier);
    assert!(matches!(
        scheduler.block_on(handle),
        Err(TaskError::Cancelled)
    ));
    scheduler.run();
}

#[test]
fn cancelling_a_parent_does_not_cancel_independently_spawned_work() {
    let scheduler = Scheduler::with_workers(2).unwrap();
    let (spawned, receiver) = mpsc::channel();
    let parent = scheduler
        .spawn(async move {
            let child = Scheduler::current()
                .unwrap()
                .spawn(pending::<()>())
                .unwrap();
            spawned.send(child).unwrap();
            pending::<()>().await;
        })
        .unwrap();
    let child = receive(&receiver);
    assert!(matches!(
        scheduler.block_on(parent.cancel()),
        Err(TaskError::Cancelled)
    ));
    assert!(!child.task().is_finished());
    assert!(matches!(
        scheduler.block_on(child.cancel()),
        Err(TaskError::Cancelled)
    ));
}
