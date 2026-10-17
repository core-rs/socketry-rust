// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::Scheduler;
use crate::{Spawn, SpawnError, Task, TaskError};
use std::sync::Arc;

type ReadyTask = std::future::Ready<usize>;

fn ready_task(value: usize) -> ReadyTask {
    std::future::ready(value)
}

#[cfg(feature = "native")]
#[test]
fn shutdown_drains_accepted_closes_after_their_waiters_are_dropped() {
    use crate::{BlockingOptions, File};
    use futures_lite::future::{block_on, poll_once};
    use std::{sync::mpsc, time::Duration};
    let timeout = Duration::from_secs(10);
    let scheduler = Scheduler::with_workers_and_blocking(
        1,
        BlockingOptions {
            threads: 1,
            capacity: 3,
        },
    )
    .unwrap();
    let handle = scheduler.handle();
    let (started, start) = mpsc::channel();
    let (release, released) = mpsc::channel();
    drop(
        block_on(handle.shared.cleanup.submit(move || {
            started.send(()).unwrap();
            released.recv_timeout(timeout).unwrap();
        }))
        .unwrap(),
    );
    start.recv_timeout(timeout).unwrap();

    let mut closing = Box::pin(handle.file_close(tempfile::tempfile().unwrap().into()));
    assert!(block_on(poll_once(&mut closing)).is_none());
    drop(closing);
    block_on(handle.file_close_submit(tempfile::tempfile().unwrap().into())).unwrap();
    let (finished, finish) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        scheduler.shutdown();
        finished.send(()).unwrap();
    });
    let deadline = std::time::Instant::now() + timeout;
    while handle.shared.cleanup.shared_listener_count() == 0 {
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert!(finish.try_recv().is_err());
    release.send(()).unwrap();
    finish.recv_timeout(timeout).unwrap();
    thread.join().unwrap();
}

#[test]
fn default_constructor_and_worker_count_report_available_workers() {
    let scheduler = Scheduler::new().unwrap();
    assert!(scheduler.worker_count() > 0);
    assert_eq!(scheduler.worker_count(), scheduler.handle().worker_count());
}

#[test]
fn closing_shared_state_is_idempotent() {
    let scheduler = Scheduler::with_workers(1).unwrap();
    let shared = Arc::clone(&scheduler.handle.shared);

    shared.close();
    shared.close();

    assert!(shared.finished());
}

#[test]
fn closing_scheduler_waits_for_cancelled_futures_to_be_destroyed() {
    let scheduler = Scheduler::with_workers(1).unwrap();
    let shared = Arc::clone(&scheduler.handle.shared);
    let (started_sender, started_receiver) = std::sync::mpsc::channel();
    let (continue_sender, continue_receiver) = std::sync::mpsc::channel();
    let task = scheduler
        .spawn(async move {
            started_sender.send(()).unwrap();
            continue_receiver.recv().unwrap();
            std::future::pending::<()>().await;
        })
        .unwrap();

    // Hold the future inside its first poll while the scheduler closes it.
    started_receiver.recv().unwrap();

    shared.close();

    assert!(!shared.finished());
    continue_sender.send(()).unwrap();
    assert!(matches!(
        scheduler.block_on(task),
        Err(TaskError::Cancelled)
    ));
    assert!(shared.finished());
}

#[test]
fn scheduler_remains_usable_after_its_registry_mutex_is_poisoned() {
    let scheduler = Scheduler::with_workers(1).unwrap();
    let handle = scheduler.handle();
    let shared = Arc::clone(&handle.shared);
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _registry = shared.registry.lock().unwrap();
        panic!("poison the registry mutex");
    }));
    assert!(poisoned.is_err());

    let completed = scheduler.spawn(ready_task(42)).unwrap();
    assert_eq!(scheduler.block_on(completed).unwrap(), 42);

    let pending = scheduler.spawn(std::future::pending::<()>()).unwrap();
    assert!(pending.task().cancel());
    assert!(matches!(
        scheduler.block_on(pending),
        Err(TaskError::Cancelled)
    ));

    drop(scheduler);
    assert!(shared.closed.load(std::sync::atomic::Ordering::Acquire));
}

#[test]
fn task_identifier_exhaustion_is_reported() {
    let scheduler = Scheduler::with_workers(1).unwrap();
    let shared = Arc::clone(&scheduler.handle.shared);
    shared.registry.lock().unwrap().next_identifier = u64::MAX;

    assert!(matches!(
        scheduler.spawn(ready_task(42)),
        Err(SpawnError::IdentifiersExhausted)
    ));
}

#[test]
fn scheduler_lifecycle_and_current_context_are_available_to_owned_tasks() {
    let scheduler = Scheduler::new().unwrap();
    assert!(Scheduler::current().is_none());
    assert!(Task::current().is_none());
    assert!(scheduler.worker_count() > 0);

    scheduler.block_on(async {
        assert!(Scheduler::current().is_some());
        assert!(Task::current().is_none());
    });
    scheduler.block_on(crate::yield_now());

    let (started, waiting) = std::sync::mpsc::channel();
    let handle = <Scheduler as Spawn>::spawn(&scheduler, async move {
        let current = Task::current().map(|task| {
            (
                task.id(),
                task.scheduler().is_some(),
                Scheduler::current().is_some(),
            )
        });
        started.send(current).unwrap();
        std::future::pending::<()>().await;
    })
    .unwrap();
    let task = handle.task();
    assert_eq!(handle.id(), task.id());
    let (identifier, task_scheduler_is_available, scheduler_is_current) = waiting
        .recv()
        .unwrap()
        .expect("owned task has a current context");
    assert_eq!(identifier, task.id());
    assert!(task_scheduler_is_available);
    assert!(scheduler_is_current);
    assert!(!handle.is_finished());

    assert!(task.cancel());
    scheduler.run();

    assert!(task.is_finished());
    assert!(matches!(
        scheduler.block_on(handle),
        Err(TaskError::Cancelled)
    ));
    scheduler.handle.shared.wake(u64::MAX);
    scheduler.shutdown();
}

#[test]
fn worker_thread_creation_errors_are_returned() {
    let result = Scheduler::with_worker_spawner(1, crate::BlockingOptions::default(), |_, _, _| {
        Err(std::io::Error::other("injected thread creation failure"))
    });

    assert!(matches!(result, Err(error) if error.kind() == std::io::ErrorKind::Other));
}
