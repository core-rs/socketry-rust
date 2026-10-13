// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Worker-owned timer queues with opportunistic assistance and deadline wakes.
//!
//! Each queue has one mutex, reusable slots and the usual pending-buffer/heap
//! algorithm. Helpers can collect values while the owner is stalled outside
//! queue operations. Timer identity stays with its original queue. This module
//! creates no threads and does not integrate with an executor.

use crate::timers::{Shared, lock, try_schedule_shared};
use crate::{TimerHandle, Timers};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, TryLockError};
use std::thread::{self, Thread};
use std::time::{Duration, Instant};

mod sleep;
pub use sleep::{Sleep, SleepError, SleepWake};

const EMPTY: u64 = u64::MAX;

fn lock_watchers<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

struct Wake(Thread);

// Deadline writes must not invalidate another worker's publication cache line.
#[repr(align(128))]
pub(crate) struct Publication {
    pub(crate) epoch: Instant,
    next: AtomicU64,
    closed: AtomicBool,
    watchers: Mutex<Vec<Arc<Wake>>>,
}

impl Publication {
    fn new() -> Self {
        let publication = Self {
            epoch: Instant::now(),
            next: AtomicU64::new(EMPTY),
            closed: AtomicBool::new(false),
            watchers: Mutex::new(Vec::new()),
        };
        // Include macOS's lazy native mutex initialization in setup.
        drop(lock_watchers(&publication.watchers));
        publication
    }

    fn tick(&self, deadline: Instant) -> u64 {
        deadline
            .saturating_duration_since(self.epoch)
            .as_nanos()
            .min(u128::from(EMPTY - 1)) as u64
    }

    fn deadline(&self) -> Option<Instant> {
        let tick = self.next.load(Ordering::Acquire);
        (tick != EMPTY).then(|| self.epoch + Duration::from_nanos(tick))
    }

    fn due(&self, now: Instant) -> bool {
        let tick = self.next.load(Ordering::Acquire);
        // Zero includes deadlines before the epoch and cleanup requests. Treat
        // it as immediately due even when the caller supplies an older snapshot.
        tick != EMPTY && (tick == 0 || now >= self.epoch + Duration::from_nanos(tick))
    }

    // All writers hold their queue's mutex. A store suffices; there is no
    // global atomic read-modify-write on registration or cancellation.
    pub(crate) fn lower(&self, deadline: Instant) {
        let tick = self.tick(deadline);
        if tick < self.next.load(Ordering::Relaxed) {
            self.next.store(tick, Ordering::Release);
            self.notify();
        }
    }

    pub(crate) fn refresh(&self, deadline: Option<Instant>) {
        let next = if self.closed.load(Ordering::Relaxed) {
            EMPTY
        } else {
            deadline.map_or(EMPTY, |deadline| self.tick(deadline))
        };
        self.next.store(next, Ordering::Release);
    }

    fn notify(&self) {
        for watcher in lock_watchers(&self.watchers).iter() {
            watcher.0.unpark();
        }
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.next.store(EMPTY, Ordering::Release);
        self.notify();
    }
}

/// A queue normally driven by one worker, independently of other workers.
///
/// Capacity is reserved during construction. Scheduling publishes a deadline
/// before returning, even while registrations are still in the pending buffer.
/// Handles support remote cancellation and cloning; values need only be Send
/// for cross-thread use. No per-timer mutex, inbox or notification allocation.
pub struct WorkerTimers<Value> {
    timers: Timers<Value>,
    publication: Arc<Publication>,
}

impl<Value> WorkerTimers<Value> {
    /// Reserve at least this many slots and both heap buffers.
    pub fn with_capacity(capacity: usize) -> Self {
        let timers = Timers::with_capacity(capacity);
        let publication = Arc::new(Publication::new());
        lock(&timers.shared).publication = Some(Arc::clone(&publication));
        Self {
            timers,
            publication,
        }
    }

    /// Actual reserved capacity, including completed handles and tombstones.
    pub fn capacity(&self) -> usize {
        self.timers.capacity()
    }

    /// Schedule without allocating, returning the original value when full.
    pub fn try_schedule(
        &mut self,
        deadline: Instant,
        value: Value,
    ) -> Result<TimerHandle<Value>, Value> {
        self.timers.try_schedule(deadline, value)
    }

    /// Merge submissions, reclaim cancellations and return a live deadline.
    pub fn next_deadline(&mut self) -> Option<Instant> {
        self.timers.next_deadline()
    }

    /// Collect due values within the output's spare capacity, in any order.
    ///
    /// A full output leaves the queue unchanged. Process values after returning.
    pub fn poll(&mut self, now: Instant, output: &mut Vec<Value>) -> usize {
        self.timers.pop_expired_batch(now, output)
    }

    /// A clonable view allowing another worker to assist this queue.
    pub fn assistance(&self) -> TimerAssist<Value> {
        TimerAssist {
            shared: Arc::clone(&self.timers.shared),
            publication: Arc::clone(&self.publication),
        }
    }
}

/// Deadline observation and nonblocking access to a worker's existing queue.
///
/// Dropping the owner shuts down the queue even if assistance views survive.
pub struct TimerAssist<Value> {
    shared: Shared<Value>,
    publication: Arc<Publication>,
}

impl<Value> Clone for TimerAssist<Value> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            publication: Arc::clone(&self.publication),
        }
    }
}

impl<Value> TimerAssist<Value> {
    /// Register from an external caller through this queue's existing mutex.
    ///
    /// Returns the original value when the queue is full or its owner shut down.
    pub fn try_schedule(
        &self,
        deadline: Instant,
        value: Value,
    ) -> Result<TimerHandle<Value>, Value> {
        try_schedule_shared(&self.shared, deadline, value)
    }

    /// A conservative published wake time, including unflushed submissions.
    ///
    /// It may be earlier than the next live timer after cancellation. Deadlines
    /// before queue construction are represented by the construction instant;
    /// timers beyond the nanosecond hint's range are represented earlier.
    pub fn next_deadline(&self) -> Option<Instant> {
        self.publication.deadline()
    }

    /// Collect due values or reclaim capacity without waiting for a queue lock.
    ///
    /// None means the queue is busy. Some(0) means no work was collected. Helpers
    /// should visit other queues after a busy result. Values are owned by the
    /// caller on return; route wakeups to an available worker outside this call.
    pub fn try_poll(&self, now: Instant, output: &mut Vec<Value>) -> Option<usize> {
        if output.len() == output.capacity() || !self.publication.due(now) {
            return Some(0);
        }
        self.collect(now, output)
    }

    fn collect(&self, now: Instant, output: &mut Vec<Value>) -> Option<usize> {
        let mut state = match self.shared.try_lock() {
            Ok(state) => state,
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
            Err(TryLockError::WouldBlock) => return None,
        };
        if state.is_closed() {
            return Some(0);
        }
        Some(state.pop_expired_batch(now, output))
    }
}

impl TimerAssist<SleepWake> {
    /// Create a lazy sleep. Only its first poll registers a timer.
    pub fn sleep_until(&self, deadline: Instant) -> Sleep {
        Sleep::new(Arc::clone(&self.shared), deadline)
    }
}

/// A thread-bound waiter watching the deadline hints of several queues.
///
/// Construct on the waiting thread during setup. Construction reserves watcher
/// lists; repeated parking allocates nothing. Earlier deadlines and shutdown
/// unpark it. Publication happens before unpark, and park tokens are never reset
/// after inspection, so a concurrent insertion cannot be lost. Spurious wakes
/// are permitted. Visit queues after waking; parking does not collect values.
pub struct TimerWaiter {
    publications: Vec<Arc<Publication>>,
    wake: Arc<Wake>,
}

impl TimerWaiter {
    /// Watch these queues from the current thread.
    pub fn new<Value>(queues: &[TimerAssist<Value>]) -> Self {
        let wake = Arc::new(Wake(thread::current()));
        let publications = queues
            .iter()
            .map(|queue| {
                lock_watchers(&queue.publication.watchers).push(Arc::clone(&wake));
                Arc::clone(&queue.publication)
            })
            .collect();
        Self { publications, wake }
    }

    /// Wait until a watched hint is due, a notification arrives, or a spurious wake.
    ///
    /// Returns immediately if all watched queues are closed (or the set is empty).
    pub fn park(&self) {
        assert_eq!(
            self.wake.0.id(),
            thread::current().id(),
            "timer waiter moved after binding"
        );
        if self
            .publications
            .iter()
            .all(|queue| queue.closed.load(Ordering::Acquire))
        {
            return;
        }
        match self
            .publications
            .iter()
            .filter_map(|queue| queue.deadline())
            .min()
        {
            Some(deadline) => {
                thread::park_timeout(deadline.saturating_duration_since(Instant::now()))
            }
            None => thread::park(),
        }
    }
}

impl Drop for TimerWaiter {
    fn drop(&mut self) {
        for publication in &self.publications {
            lock_watchers(&publication.watchers)
                .retain(|watcher| !Arc::ptr_eq(watcher, &self.wake));
        }
    }
}

#[cfg(test)]
mod tests;
