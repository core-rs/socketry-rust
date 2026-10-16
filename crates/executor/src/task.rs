// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::scheduler::{SchedulerHandle, Shared};
use pin_project_lite::pin_project;
use std::any::Any;
use std::cell::RefCell;
use std::fmt;
use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

pub(crate) const UNASSIGNED_WORKER: usize = usize::MAX;
pub(crate) type Runnable = async_task::Runnable<Arc<TaskState>>;

pub(crate) struct TaskState {
    pub(crate) identifier: u64,
    pub(crate) scheduler: Weak<Shared>,
    pub(crate) cancelled: AtomicBool,
    pub(crate) finished: AtomicBool,
    pub(crate) worker: AtomicUsize,
}

thread_local! {
    static CURRENT_TASK: RefCell<Option<Task>> = const { RefCell::new(None) };
}

/// A thread-safe task reference. Holding it does not prevent task cancellation.
#[derive(Clone)]
pub struct Task {
    pub(crate) state: Arc<TaskState>,
}

impl Task {
    /// Return the task currently being polled or destroyed by this worker.
    /// Returns `None` outside a scheduler task, including a `block_on` root.
    pub fn current() -> Option<Self> {
        CURRENT_TASK.with(|current| current.borrow().clone())
    }

    /// An identifier unique within this task's scheduler.
    pub fn id(&self) -> u64 {
        self.state.identifier
    }

    /// Return this task's scheduler if it is still alive.
    pub fn scheduler(&self) -> Option<SchedulerHandle> {
        self.state
            .scheduler
            .upgrade()
            .map(SchedulerHandle::from_shared)
    }

    /// Request cancellation and wake the task. An in-progress poll may finish.
    /// Returns false if cancellation was already requested or cleanup finished.
    pub fn cancel(&self) -> bool {
        if self.is_finished() || self.state.cancelled.swap(true, Ordering::AcqRel) {
            return false;
        }
        if let Some(shared) = self.state.scheduler.upgrade() {
            shared.wake(self.id());
        }
        true
    }

    /// Whether the task's future has been destroyed.
    pub fn is_finished(&self) -> bool {
        self.state.finished.load(Ordering::Acquire)
    }

    pub(crate) fn enter(self) -> TaskScope {
        TaskScope(CURRENT_TASK.with(|current| current.replace(Some(self))))
    }
}

pub(crate) struct TaskScope(Option<Task>);

impl Drop for TaskScope {
    fn drop(&mut self) {
        CURRENT_TASK.with(|current| current.replace(self.0.take()));
    }
}

/// A task ended without producing its normal output.
#[derive(Debug)]
pub enum TaskError {
    /// The future was destroyed or the blocking closure was skipped before execution.
    Cancelled,
    /// Polling a future or executing a blocking closure panicked. The original payload is retained for the caller.
    Panicked(Box<dyn Any + Send + 'static>),
}

impl fmt::Display for TaskError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => formatter.write_str("task cancelled"),
            Self::Panicked(_) => formatter.write_str("task panicked"),
        }
    }
}

impl std::error::Error for TaskError {}

/// An awaitable result of an owned task.
///
/// The handle owns the task lifetime. Moving it transfers that ownership.
/// Dropping it requests destruction once any active poll returns; it does not
/// wait or run asynchronous cleanup. Await the handle to observe completion.
#[must_use = "await or retain the task; dropping its handle abandons execution"]
pub struct TaskHandle<Output> {
    pub(crate) inner: Option<async_task::FallibleTask<Result<Output, TaskError>, Arc<TaskState>>>,
    pub(crate) task: Task,
}

impl<Output> TaskHandle<Output> {
    /// Return a reference usable for cancellation and task identity.
    pub fn task(&self) -> Task {
        self.task.clone()
    }

    /// An identifier unique within the scheduler.
    pub fn id(&self) -> u64 {
        self.task.id()
    }

    /// Whether the task's future has been destroyed.
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    /// Request cancellation and await destruction of the future.
    /// A result that won the race with cancellation is returned normally.
    /// Cancellation runs synchronous destructors, not asynchronous cleanup.
    pub async fn cancel(self) -> Result<Output, TaskError> {
        self.task.cancel();
        self.await
    }
}

impl<Output> Future for TaskHandle<Output> {
    type Output = Result<Output, TaskError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self
            .inner
            .as_mut()
            .expect("task handle polled after completion");
        match Pin::new(inner).poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.inner.take();
                Poll::Ready(result.unwrap_or(Err(TaskError::Cancelled)))
            }
        }
    }
}

impl<Output> Drop for TaskHandle<Output> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            self.task.cancel();
            // The registry retains execution until cancellation is observed.
            inner.detach();
        }
    }
}

pub(crate) struct Completion(pub(crate) Arc<TaskState>);

impl Drop for Completion {
    fn drop(&mut self) {
        self.0.finished.store(true, Ordering::Release);
        if let Some(shared) = self.0.scheduler.upgrade() {
            shared.finish(&self.0);
        }
    }
}

pin_project! {
    pub(crate) struct TaskFuture<FutureType> {
        // Field order ensures the future is destroyed before completion is reported.
        #[pin]
        pub(crate) future: FutureType,
        pub(crate) completion: Completion,
    }
}

impl<FutureType: Future> Future for TaskFuture<FutureType> {
    type Output = Result<FutureType::Output, TaskError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if this.completion.0.cancelled.load(Ordering::Acquire) {
            return Poll::Ready(Err(TaskError::Cancelled));
        }
        match catch_unwind(AssertUnwindSafe(|| this.future.poll(context))) {
            Ok(result) => result.map(Ok),
            Err(payload) => Poll::Ready(Err(TaskError::Panicked(payload))),
        }
    }
}

#[cfg(test)]
mod tests;

/// Cooperatively reschedule the current future once.
/// Works with any executor implementing the standard future/waker contract.
pub async fn yield_now() {
    let mut yielded = false;
    poll_fn(|context| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            context.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await
}
