// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use futures_lite::future::{block_on, poll_once};
use std::sync::mpsc;
use std::time::Duration;

static NEVER: Cancellation = Cancellation::never();
const TIMEOUT: Duration = Duration::from_secs(10);

struct DropGate {
    started: mpsc::Sender<()>,
    released: mpsc::Receiver<()>,
}

impl Drop for DropGate {
    fn drop(&mut self) {
        self.started.send(()).unwrap();
        self.released.recv_timeout(TIMEOUT).unwrap();
    }
}

#[test]
fn skipped_jobs_release_captures_before_reporting_completion() {
    for shutdown in [false, true] {
        let pool = BlockingPool::new(BlockingOptions {
            threads: 1,
            capacity: 2,
        });
        let (release_worker, worker_released) = mpsc::channel();
        let (started, start) = mpsc::channel();
        let running = block_on(pool.spawn(
            move || {
                started.send(()).unwrap();
                worker_released.recv_timeout(TIMEOUT).unwrap();
            },
            &NEVER,
        ))
        .unwrap();
        start.recv_timeout(TIMEOUT).unwrap();

        let (started, start) = mpsc::channel();
        let (release_drop, released) = mpsc::channel();
        let captured = DropGate { started, released };
        let cancellation = Cancellation::new();
        let mut queued =
            Box::pin(block_on(pool.spawn(move || drop(captured), &cancellation)).unwrap());
        thread::scope(|scope| {
            let closing = if shutdown {
                Some(scope.spawn(|| pool.close()))
            } else {
                cancellation.cancel();
                release_worker.send(()).unwrap();
                None
            };
            start.recv_timeout(TIMEOUT).unwrap();
            assert!(block_on(poll_once(queued.as_mut())).is_none());
            release_drop.send(()).unwrap();
            assert!(matches!(block_on(queued), Err(TaskError::Cancelled)));
            if let Some(closing) = closing {
                closing.join().unwrap();
                release_worker.send(()).unwrap();
            }
        });
        block_on(running).unwrap();
    }
}

#[test]
fn admission_reports_worker_startup_failure() {
    let mut pool = BlockingPool::new(BlockingOptions {
        threads: 2,
        capacity: 2,
    });
    pool.fail_start_at = Some(1);
    assert!(
        matches!(block_on(pool.spawn(|| (), &NEVER)), Err(BlockingError::Io(error)) if error.to_string() == "injected thread startup failure")
    );
    assert!(matches!(
        block_on(pool.spawn(|| (), &NEVER)),
        Err(BlockingError::SchedulerClosed)
    ));
}

#[test]
fn partial_startup_failure_closes_admission_and_releases_started_workers() {
    let pool = BlockingPool::new(BlockingOptions {
        threads: 2,
        capacity: 2,
    });
    let (exited, exit) = mpsc::channel();
    let result = pool.start_with(|shared, identifier| {
        if identifier == 1 {
            return Err(io::Error::other("injected thread startup failure"));
        }
        let exited = exited.clone();
        thread::spawn(move || {
            work(shared);
            exited.send(()).unwrap();
        });
        Ok(())
    });
    assert!(
        matches!(result, Err(BlockingError::Io(error)) if error.to_string() == "injected thread startup failure")
    );
    exit.recv_timeout(TIMEOUT).unwrap();
    assert!(matches!(
        block_on(pool.spawn(|| (), &NEVER)),
        Err(BlockingError::SchedulerClosed)
    ));
    assert!(matches!(
        pool.start_with(|_, _| panic!("startup must not retry")),
        Err(BlockingError::Io(_))
    ));
}

#[test]
fn shutdown_during_startup_cannot_admit_work() {
    let pool = BlockingPool::new(BlockingOptions {
        threads: 1,
        capacity: 1,
    });
    pool.start_with(|shared, _| {
        pool.close();
        thread::spawn(move || work(shared));
        Ok(())
    })
    .unwrap();
    assert!(matches!(
        block_on(pool.spawn(|| (), &NEVER)),
        Err(BlockingError::SchedulerClosed)
    ));
}

struct Destructor {
    shared: Arc<Shared>,
    dropped: mpsc::Sender<()>,
}

impl Drop for Destructor {
    fn drop(&mut self) {
        // Taking the lock proves user destructors run outside it.
        drop(self.shared.state.lock().unwrap());
        self.dropped.send(()).unwrap();
        panic!("user destructor panic");
    }
}

#[test]
fn abandoned_result_panics_release_capacity_and_preserve_the_worker() {
    let pool = BlockingPool::new(BlockingOptions {
        threads: 1,
        capacity: 1,
    });
    let (release, released) = mpsc::channel();
    let (started, start) = mpsc::channel();
    let (dropped, drops) = mpsc::channel();
    let output = Destructor {
        shared: pool.shared.clone(),
        dropped,
    };
    let job = block_on(pool.spawn(
        move || {
            started.send(()).unwrap();
            released.recv_timeout(TIMEOUT).unwrap();
            output
        },
        &NEVER,
    ))
    .unwrap();
    start.recv_timeout(TIMEOUT).unwrap();
    drop(job);
    release.send(()).unwrap();
    drops.recv_timeout(TIMEOUT).unwrap();
    assert_eq!(
        block_on(block_on(pool.spawn(|| 7, &NEVER)).unwrap()).unwrap(),
        7
    );
}

#[test]
fn shutdown_catches_capture_destructor_panics_and_cancels_other_queued_jobs() {
    let pool = BlockingPool::new(BlockingOptions {
        threads: 1,
        capacity: 3,
    });
    let (release, released) = mpsc::channel();
    let (started, start) = mpsc::channel();
    let first = block_on(pool.spawn(
        move || {
            started.send(()).unwrap();
            released.recv_timeout(TIMEOUT).unwrap();
        },
        &NEVER,
    ))
    .unwrap();
    start.recv_timeout(TIMEOUT).unwrap();
    let (dropped, drops) = mpsc::channel();
    let captured = Destructor {
        shared: pool.shared.clone(),
        dropped,
    };
    let second = block_on(pool.spawn(move || drop(captured), &NEVER)).unwrap();
    let third = block_on(pool.spawn(|| panic!("queued job ran after shutdown"), &NEVER)).unwrap();
    let mut waiting = pin!(pool.spawn(|| (), &NEVER));
    assert!(block_on(poll_once(waiting.as_mut())).is_none());
    pool.close();
    drops.recv_timeout(TIMEOUT).unwrap();
    assert!(matches!(block_on(second), Err(TaskError::Cancelled)));
    assert!(matches!(block_on(third), Err(TaskError::Cancelled)));
    assert!(matches!(
        block_on(waiting),
        Err(BlockingError::SchedulerClosed)
    ));
    release.send(()).unwrap();
    block_on(first).unwrap();
}

#[test]
fn concurrent_first_admissions_share_one_startup() {
    let pool = BlockingPool::new(BlockingOptions {
        threads: 2,
        capacity: 2,
    });
    thread::scope(|scope| {
        for value in 0..8 {
            let pool = &pool;
            scope.spawn(move || {
                assert_eq!(
                    block_on(block_on(pool.spawn(move || value, &NEVER)).unwrap()).unwrap(),
                    value
                )
            });
        }
    });
}
