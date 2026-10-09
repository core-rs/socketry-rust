// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::{Completion, Task, TaskError, TaskState, UNASSIGNED_WORKER};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll, Waker};

fn task_state(cancelled: bool) -> Arc<TaskState> {
    Arc::new(TaskState {
        identifier: 1,
        scheduler: Weak::new(),
        cancelled: AtomicBool::new(cancelled),
        finished: AtomicBool::new(false),
        worker: AtomicUsize::new(UNASSIGNED_WORKER),
    })
}

#[test]
fn task_errors_have_descriptive_messages() {
    assert_eq!(TaskError::Cancelled.to_string(), "task cancelled");
    assert_eq!(
        TaskError::Panicked(Box::new("failure".to_owned())).to_string(),
        "task panicked"
    );
    assert!(std::error::Error::source(&TaskError::Cancelled).is_none());
}

#[test]
fn task_without_a_live_scheduler_can_be_cancelled() {
    let state = task_state(false);
    let task = Task {
        state: Arc::clone(&state),
    };

    assert!(task.cancel());
    assert!(!task.cancel());
}

#[test]
fn finished_tasks_cannot_be_cancelled() {
    let state = task_state(false);
    state.finished.store(true, Ordering::Release);
    let task = Task {
        state: Arc::clone(&state),
    };

    assert!(!task.cancel());
    assert!(!state.cancelled.load(Ordering::Acquire));
}

#[test]
fn completion_marks_finished_after_its_scheduler_is_gone() {
    let state = task_state(false);

    drop(Completion(Arc::clone(&state)));

    assert!(state.finished.load(Ordering::Acquire));
}

#[test]
fn dropping_a_task_handle_detaches_without_cancelling_the_task() {
    let scheduler = crate::scheduler::Scheduler::with_workers(1).unwrap();
    let handle = scheduler.spawn(std::future::pending::<()>()).unwrap();
    let task = handle.task();

    drop(handle);
    assert!(task.cancel());

    scheduler.run();

    assert!(task.is_finished());
}

#[test]
fn task_future_reports_completion_cancellation_and_panics() {
    #[derive(Clone, Copy)]
    enum Behavior {
        PendingThenReady,
        Ready,
        Panic,
    }

    struct ControlledFuture {
        behavior: Behavior,
        polled: bool,
    }

    impl Future for ControlledFuture {
        type Output = usize;

        fn poll(self: std::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            match this.behavior {
                Behavior::PendingThenReady if !this.polled => {
                    this.polled = true;
                    Poll::Pending
                }
                Behavior::Panic => panic!("injected task panic"),
                Behavior::PendingThenReady | Behavior::Ready => Poll::Ready(42),
            }
        }
    }

    let state = task_state(false);
    let mut future = Box::pin(super::TaskFuture {
        future: ControlledFuture {
            behavior: Behavior::PendingThenReady,
            polled: false,
        },
        completion: Completion(Arc::clone(&state)),
    });
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Ok(42))
    ));
    drop(future);
    assert!(state.finished.load(Ordering::Acquire));

    let state = task_state(true);
    let mut future = Box::pin(super::TaskFuture {
        future: ControlledFuture {
            behavior: Behavior::Ready,
            polled: false,
        },
        completion: Completion(state),
    });
    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Err(TaskError::Cancelled))
    ));

    let mut future = Box::pin(super::TaskFuture {
        future: ControlledFuture {
            behavior: Behavior::Panic,
            polled: false,
        },
        completion: Completion(task_state(false)),
    });
    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Err(TaskError::Panicked(_)))
    ));
}
