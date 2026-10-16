// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::{
    IdleSearch, SearchResult, WorkerState, classify_search, finish_idle_search, search_result,
    take_idle_runnable, take_stolen,
};
use crate::task::{Runnable, TaskState, UNASSIGNED_WORKER};
use crossbeam_deque::{Steal, Worker};
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

fn worker_state() -> WorkerState {
    let queue = Worker::<Runnable>::new_fifo();
    WorkerState::new(queue.stealer())
}

fn runnable() -> Runnable {
    let state = Arc::new(TaskState {
        identifier: 0,
        scheduler: Weak::new(),
        cancelled: AtomicBool::new(false),
        finished: AtomicBool::new(false),
        worker: AtomicUsize::new(UNASSIGNED_WORKER),
    });
    let (runnable, _task) = async_task::Builder::new()
        .metadata(state)
        .spawn(|_| std::future::pending::<()>(), |_| {});
    runnable
}

#[test]
fn records_retry_separately_from_empty_and_success() {
    let mut retry = false;
    assert_eq!(take_stolen(Steal::<()>::Empty, &mut retry), None);
    assert!(!retry);

    assert_eq!(take_stolen(Steal::<()>::Retry, &mut retry), None);
    assert!(retry);

    assert_eq!(take_stolen(Steal::Success(()), &mut retry), Some(()));
    assert!(retry);
}

#[test]
fn search_reports_retry_only_when_a_queue_raced() {
    assert!(matches!(search_result(true), Steal::Retry));
    assert!(matches!(search_result(false), Steal::Empty));
}

#[test]
fn worker_yields_after_a_concurrent_steal_retry() {
    assert!(matches!(
        classify_search(Steal::<crate::task::Runnable>::Retry),
        SearchResult::Retry
    ));
}

#[test]
fn idle_retry_restores_worker_sleep_state() {
    let worker = worker_state();
    let idle_workers = AtomicUsize::new(1);
    worker.sleeping.store(true, Ordering::SeqCst);

    assert!(matches!(
        finish_idle_search(
            &worker,
            &idle_workers,
            Steal::Retry,
            &|| panic!("retry results do not check whether the worker should park"),
            &|| panic!("retry results do not park"),
        ),
        IdleSearch::Retry
    ));
    assert!(!worker.sleeping.load(Ordering::SeqCst));
    assert_eq!(idle_workers.load(Ordering::SeqCst), 0);
}

#[test]
fn idle_success_restores_worker_state_and_returns_the_runnable() {
    let worker = worker_state();
    let idle_workers = AtomicUsize::new(1);
    worker.sleeping.store(true, Ordering::SeqCst);

    let runnable = match finish_idle_search(
        &worker,
        &idle_workers,
        Steal::Success(runnable()),
        &|| panic!("successful results do not check whether the worker should park"),
        &|| panic!("successful results do not park"),
    ) {
        IdleSearch::Runnable(runnable) => runnable,
        IdleSearch::Retry | IdleSearch::Empty => panic!("successful steal was discarded"),
    };
    assert!(!worker.sleeping.load(Ordering::SeqCst));
    assert_eq!(idle_workers.load(Ordering::SeqCst), 0);
    assert!(take_idle_runnable(IdleSearch::Runnable(runnable)).is_some());
}

#[test]
fn idle_empty_search_skips_park_after_shutdown() {
    let worker = worker_state();
    let idle_workers = AtomicUsize::new(1);
    worker.sleeping.store(true, Ordering::SeqCst);

    let result = finish_idle_search(&worker, &idle_workers, Steal::Empty, &|| false, &|| {
        panic!("a shut down scheduler must not park its worker")
    });

    assert!(matches!(result, IdleSearch::Empty));
    assert!(!worker.sleeping.load(Ordering::SeqCst));
    assert_eq!(idle_workers.load(Ordering::SeqCst), 0);
}

#[test]
fn idle_empty_search_parks_while_scheduler_is_running() {
    let worker = worker_state();
    let idle_workers = AtomicUsize::new(1);
    let parked = AtomicBool::new(false);
    worker.sleeping.store(true, Ordering::SeqCst);

    let result = finish_idle_search(&worker, &idle_workers, Steal::Empty, &|| true, &|| {
        parked.store(true, Ordering::SeqCst)
    });

    assert!(matches!(result, IdleSearch::Empty));
    assert!(parked.load(Ordering::SeqCst));
    assert!(!worker.sleeping.load(Ordering::SeqCst));
    assert_eq!(idle_workers.load(Ordering::SeqCst), 0);
}
