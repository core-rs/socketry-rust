// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::timers::{Key, Shared, lock};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Instant;

/// A wakeup collected from a sleep queue. Drop it outside timer operations to
/// wake its task. Queue shutdown also drops wakeups outside the queue lock.
pub struct SleepWake(Option<Waker>);

impl Drop for SleepWake {
    fn drop(&mut self) {
        if let Some(waker) = self.0.take() {
            waker.wake();
        }
    }
}

/// A bounded sleep queue could not accept or retain a registration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SleepError {
    Full,
    Closed,
}

impl fmt::Display for SleepError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Full => "sleep timer queue is full",
            Self::Closed => "sleep timer queue is closed",
        })
    }
}

impl std::error::Error for SleepError {}

enum Registration {
    Dormant,
    Registered(Key),
    Complete(Result<(), SleepError>),
}

/// A lazy, allocation-free sleep created by a worker queue's assistance view.
///
/// Construction retains the queue and deadline without locking or reserving a
/// slot. The first poll registers its waker; later polls replace that waker.
/// Dropping the future cancels an existing registration without waking it.
/// The caller must drive the queue and drop collected SleepWake values outside
/// timer operations, including for deadlines already in the past.
///
/// A full queue fails the first poll with Full. Shutdown wakes registered sleeps
/// and they resolve with Closed. Dormant sleeps observe closure on first poll.
/// Shutdown takes precedence until a sleep is polled to completion.
/// User-provided waker clone/drop/wake implementations may allocate separately.
pub struct Sleep {
    // Retain one queue reference from construction through completion. Unlike
    // a cloneable TimerHandle, this future uniquely owns its slot reference.
    shared: Option<Shared<SleepWake>>,
    registration: Registration,
    deadline: Instant,
}

impl Sleep {
    pub(super) fn new(shared: Shared<SleepWake>, deadline: Instant) -> Self {
        Self {
            shared: Some(shared),
            registration: Registration::Dormant,
            deadline,
        }
    }

    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    fn complete(&mut self, result: Result<(), SleepError>) -> Poll<Result<(), SleepError>> {
        self.registration = Registration::Complete(result);
        drop(self.shared.take());
        Poll::Ready(result)
    }
}

impl Future for Sleep {
    type Output = Result<(), SleepError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match self.registration {
            Registration::Dormant => {
                // Waker callbacks, including cloning and dropping, run outside
                // the queue lock. Failure disarms the unused wakeup.
                let wake = SleepWake(Some(context.waker().clone()));
                let admission = {
                    let mut state = lock(self.shared.as_ref().unwrap());
                    state.try_insert(self.deadline, wake).map_err(|wake| {
                        let error = if state.is_closed() {
                            SleepError::Closed
                        } else {
                            SleepError::Full
                        };
                        (error, wake)
                    })
                };
                match admission {
                    Ok(key) => {
                        self.registration = Registration::Registered(key);
                        Poll::Pending
                    }
                    Err((error, mut wake)) => {
                        let result = self.complete(Err(error));
                        drop(wake.0.take());
                        result
                    }
                }
            }
            Registration::Registered(key) => {
                let waker = context.waker().clone();
                let (result, retired) = {
                    let mut state = lock(self.shared.as_ref().unwrap());
                    if state.is_closed() {
                        let retired = state.cancel(key).and_then(|mut wake| wake.0.take());
                        state.drop_handle(key);
                        (Some(Err(SleepError::Closed)), retired)
                    } else if let Some(wake) = state.value_mut(key) {
                        let retired = if wake.0.as_ref().unwrap().will_wake(&waker) {
                            None
                        } else {
                            wake.0.replace(waker)
                        };
                        (None, retired)
                    } else {
                        state.drop_handle(key);
                        (Some(Ok(())), None)
                    }
                };
                let result = match result {
                    Some(result) => self.complete(result),
                    None => Poll::Pending,
                };
                // Mark completion before callbacks can panic: the slot
                // reference has already been released under the queue lock.
                drop(retired);
                result
            }
            Registration::Complete(result) => Poll::Ready(result),
        }
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let Registration::Registered(key) = self.registration {
            let wake = {
                let mut state = lock(self.shared.as_ref().unwrap());
                let wake = state.cancel(key);
                state.drop_handle(key);
                wake
            };
            if let Some(mut wake) = wake {
                drop(wake.0.take());
            }
        }
    }
}

#[cfg(test)]
mod tests;
