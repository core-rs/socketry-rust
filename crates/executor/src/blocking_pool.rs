// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{BlockingError, BlockingOptions, BlockingTask, Cancellation, TaskError};
use event_listener::Event;
use futures_channel::oneshot;
use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::task::Poll;
use std::thread;

type Job = Box<dyn FnOnce() + Send + 'static>;

// Field order also releases captures before notifying completion when shutdown discards a queued job.
struct Operation<F, Output> {
    operation: F,
    completion: oneshot::Sender<Result<Output, TaskError>>,
}

impl<F: FnOnce() -> Output, Output> Operation<F, Output> {
    fn run(self, cancelled: bool) {
        let result = if cancelled {
            drop(self.operation);
            Err(TaskError::Cancelled)
        } else {
            catch_unwind(AssertUnwindSafe(self.operation)).map_err(TaskError::Panicked)
        };
        let _ = self.completion.send(result);
    }
}

struct State {
    closed: bool,
    outstanding: usize,
    queue: VecDeque<Job>,
}

struct Shared {
    state: Mutex<State>,
    work: Condvar,
    capacity: Event,
}

/// A scheduler-owned service, independent of its future workers and I/O selector.
pub(crate) struct BlockingPool {
    shared: Arc<Shared>,
    options: BlockingOptions,
    started: OnceLock<io::Result<()>>,
    #[cfg(test)]
    fail_start_at: Option<usize>,
}

impl BlockingPool {
    pub(crate) fn new(options: BlockingOptions) -> Self {
        Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    closed: false,
                    outstanding: 0,
                    queue: VecDeque::new(),
                }),
                work: Condvar::new(),
                capacity: Event::new(),
            }),
            options,
            started: OnceLock::new(),
            #[cfg(test)]
            fail_start_at: None,
        }
    }

    fn start(&self) -> Result<(), BlockingError> {
        self.start_with(|shared, identifier| {
            #[cfg(test)]
            if self.fail_start_at == Some(identifier) {
                return Err(io::Error::other("injected thread startup failure"));
            }
            thread::Builder::new()
                .name(format!("socketry-blocking-{identifier}"))
                .spawn(move || work(shared))
                .map(drop)
        })
    }

    fn start_with(
        &self,
        mut spawn: impl FnMut(Arc<Shared>, usize) -> io::Result<()>,
    ) -> Result<(), BlockingError> {
        self.started
            .get_or_init(|| {
                for identifier in 0..self.options.threads {
                    if let Err(error) = spawn(Arc::clone(&self.shared), identifier) {
                        // A partial startup must release any threads already waiting for work.
                        self.close();
                        return Err(error);
                    }
                }
                Ok(())
            })
            .as_ref()
            .copied()
            .map_err(|error| BlockingError::Io(io::Error::new(error.kind(), error.to_string())))
    }

    pub(crate) async fn spawn<F, Output>(
        &self,
        operation: F,
        cancellation: &Cancellation,
    ) -> Result<BlockingTask<Output>, BlockingError>
    where
        F: FnOnce() -> Output + Send + 'static,
        Output: Send + 'static,
    {
        let mut cancelled = pin!(cancellation.cancelled());
        loop {
            let listener = self.shared.capacity.listen();
            if cancellation.is_cancelled() {
                return Err(BlockingError::Cancelled);
            }
            {
                let mut state = self.shared.state.lock().unwrap();
                if state.closed {
                    return Err(BlockingError::SchedulerClosed);
                }
                if state.outstanding < self.options.capacity {
                    // Initialize without the queue lock: startup failure closes the pool.
                    if self.started.get().is_none() {
                        drop(state);
                        self.start()?;
                        continue;
                    }
                    let (sender, result) = oneshot::channel();
                    let abandoned = Arc::new(AtomicBool::new(false));
                    let owner = Arc::clone(&abandoned);
                    let cancellation = cancellation.clone();
                    let operation = Operation {
                        operation,
                        completion: sender,
                    };
                    state.outstanding += 1;
                    state.queue.push_back(Box::new(move || {
                        operation.run(owner.load(Ordering::Acquire) || cancellation.is_cancelled());
                    }));
                    drop(state);
                    self.shared.work.notify_one();
                    return Ok(BlockingTask { result, abandoned });
                }
            }
            let mut listener = pin!(listener);
            poll_fn(|context| {
                if cancelled.as_mut().poll(context).is_ready() {
                    return Poll::Ready(());
                }
                listener.as_mut().poll(context)
            })
            .await;
        }
    }

    pub(crate) fn close(&self) {
        let queue = {
            let mut state = self.shared.state.lock().unwrap();
            state.closed = true;
            let queue = std::mem::take(&mut state.queue);
            state.outstanding -= queue.len();
            queue
        };
        self.shared.capacity.notify(usize::MAX);
        self.shared.work.notify_all();
        // Destroy closures and wake their result waiters outside the queue lock. Destructors may reenter the scheduler or panic.
        for job in queue {
            let _ = catch_unwind(AssertUnwindSafe(|| drop(job)));
        }
    }
}

impl Drop for BlockingPool {
    fn drop(&mut self) {
        self.close();
    }
}

fn work(shared: Arc<Shared>) {
    loop {
        let job = {
            let mut state = shared.state.lock().unwrap();
            loop {
                if let Some(job) = state.queue.pop_front() {
                    break job;
                }
                if state.closed {
                    return;
                }
                state = shared.work.wait(state).unwrap();
            }
        };
        // Catch abandoned-result and capture destructor panics as well as user execution panics, so capacity is always returned and the worker survives.
        let _ = catch_unwind(AssertUnwindSafe(job));
        shared.state.lock().unwrap().outstanding -= 1;
        shared.capacity.notify(usize::MAX);
    }
}

#[cfg(test)]
mod tests;
