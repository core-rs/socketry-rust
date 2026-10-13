// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Experimental bounded producer inboxes feeding a single heap owner.
//!
//! Producers and handles can move between threads. After its first poll or
//! deadline query, the driver must remain on that thread. Handles are deliberately
//! not clonable. Dropping a handle leaves its timer scheduled; cancellation is
//! explicit. There is no global heap lock, but each fixed slot has a small mutex
//! and each producer has an admission mutex that synchronizes with shutdown.

use crossbeam_queue::ArrayQueue;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::thread::{self, Thread};
use std::time::Instant;

const COMPACT_MINIMUM_COUNT: usize = 128;
const HEAPIFY_INSERT_RATIO: usize = 2;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Default)]
struct Notification {
    thread: OnceLock<Thread>,
    pending: AtomicBool,
}

impl Notification {
    fn send(&self) {
        if !self.pending.swap(true, AtomicOrdering::AcqRel)
            && let Some(thread) = self.thread.get()
        {
            thread.unpark();
        }
    }

    fn begin_poll(&self) {
        let owner = self.thread.get_or_init(thread::current);
        assert_eq!(
            owner.id(),
            thread::current().id(),
            "timer driver moved after binding"
        );
        // Reset BEFORE draining. Publications before this acquire are visible;
        // publications afterwards leave a token for the next park. Never clear
        // the notification between inspecting inboxes and parking.
        self.pending.swap(false, AtomicOrdering::AcqRel);
    }
}

struct Slot<Value> {
    value: Option<Value>,
    handle: bool,
    queued: bool,
    in_heap: bool,
    cancelled: bool,
}

struct Pool<Value> {
    slots: Box<[Mutex<Slot<Value>>]>,
    free: ArrayQueue<usize>,
    inbox: ArrayQueue<(Instant, usize)>,
    closed: Mutex<bool>,
    cancelled_heap: AtomicUsize,
    notification: Notification,
}

impl<Value> Pool<Value> {
    fn release(&self, index: usize, slot: &Slot<Value>) {
        // A slot cannot be reused until both its command/heap entry and handle
        // are gone. No generations or per-entry Arc allocations are necessary.
        if !slot.handle && !slot.queued {
            self.free.push(index).unwrap();
        }
    }

    fn retire(&self, index: usize) -> Option<Value> {
        let mut slot = lock(&self.slots[index]);
        if slot.in_heap && slot.cancelled {
            self.cancelled_heap.fetch_sub(1, AtomicOrdering::Relaxed);
        }
        slot.in_heap = false;
        slot.queued = false;
        let value = slot.value.take();
        self.release(index, &slot);
        value
    }

    fn admit(&self, index: usize) -> bool {
        let mut slot = lock(&self.slots[index]);
        if slot.cancelled {
            slot.queued = false;
            self.release(index, &slot);
            false
        } else {
            // Cancellation and admission share this lock, so only admitted
            // entries contribute to the compaction count. This includes the
            // brief interval between filtering and actual heap insertion.
            slot.in_heap = true;
            true
        }
    }

    fn is_cancelled(&self, index: usize) -> bool {
        lock(&self.slots[index]).cancelled
    }
}

/// A fixed-capacity registration path, intended to be owned by one worker.
///
/// Use a dedicated producer for external submissions. Moving a task does not
/// move its timer: its cancellation handle retains the original pool.
pub struct TimerProducer<Value> {
    pool: Arc<Pool<Value>>,
}

impl<Value> TimerProducer<Value> {
    /// Register without allocating, returning the value if full or closed.
    ///
    /// Capacity includes cancelled entries not yet drained and completed timers
    /// whose handles still exist. Shutdown serializes with this producer only.
    pub fn try_schedule(
        &mut self,
        deadline: Instant,
        value: Value,
    ) -> Result<DriverHandle<Value>, Value> {
        let closed = lock(&self.pool.closed);
        if *closed {
            return Err(value);
        }
        let Some(index) = self.pool.free.pop() else {
            return Err(value);
        };
        *lock(&self.pool.slots[index]) = Slot {
            value: Some(value),
            handle: true,
            queued: true,
            in_heap: false,
            cancelled: false,
        };
        // Each inbox command retains a distinct slot, so acquiring a free slot
        // guarantees inbox space, even when consumer and producer overlap.
        self.pool.inbox.push((deadline, index)).unwrap();
        drop(closed);
        self.pool.notification.send();
        Ok(DriverHandle {
            pool: Arc::clone(&self.pool),
            index,
            deadline,
        })
    }
}

/// A movable cancellation handle. Dropping it does not cancel the timer.
pub struct DriverHandle<Value> {
    pool: Arc<Pool<Value>>,
    index: usize,
    deadline: Instant,
}

impl<Value> DriverHandle<Value> {
    /// The timer's monotonic deadline.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Claim and release the value immediately, outside synchronization.
    ///
    /// Returns false when cancellation, expiry or shutdown already claimed it.
    pub fn cancel(&self) -> bool {
        let value = {
            let mut slot = lock(&self.pool.slots[self.index]);
            let value = slot.value.take();
            if value.is_some() {
                slot.cancelled = true;
                if slot.in_heap {
                    self.pool
                        .cancelled_heap
                        .fetch_add(1, AtomicOrdering::Relaxed);
                }
            }
            value
        };
        let cancelled = value.is_some();
        if cancelled {
            self.pool.notification.send();
        }
        drop(value);
        cancelled
    }

    /// Whether this handle successfully cancelled its timer.
    pub fn is_cancelled(&self) -> bool {
        self.pool.is_cancelled(self.index)
    }
}

impl<Value> Drop for DriverHandle<Value> {
    fn drop(&mut self) {
        let mut slot = lock(&self.pool.slots[self.index]);
        slot.handle = false;
        self.pool.release(self.index, &slot);
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct Entry {
    deadline: Instant,
    pool: usize,
    slot: usize,
}
impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .deadline
            .cmp(&self.deadline)
            .then_with(|| self.pool.cmp(&other.pool))
            .then_with(|| self.slot.cmp(&other.slot))
    }
}
impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn count_due(entries: &[Entry], index: usize, now: Instant, limit: usize) -> usize {
    if limit == 0 {
        return 0;
    }
    let Some(entry) = entries.get(index) else {
        return 0;
    };
    if entry.deadline > now {
        return 0;
    }
    let left = count_due(entries, index * 2 + 1, now, limit - 1);
    1 + left + count_due(entries, index * 2 + 2, now, limit - 1 - left)
}

/// One heap owner, with bounded inboxes and reusable per-producer timer slots.
///
/// Add producers before starting workers. Setup allocates pools, inboxes and
/// heap buffers; subsequent registration, cancellation and delivery do not
/// allocate. Caller output buffers must also be reserved. Poll regularly or
/// use [`park`](Self::park) for a dedicated driver. Callbacks run after polling.
///
/// Cancellation destroys values immediately and wakes the owner. Cancelled
/// submissions skip heap insertion; heap cancellations are reclaimed in batches
/// or at the root. A full producer pool forces compaction at the next poll.
/// Dropping the driver rejects new registrations and releases pending values;
/// outstanding handles remain valid. This is an experiment, not a scheduler.
pub struct TimerDriver<Value> {
    pools: Vec<Arc<Pool<Value>>>,
    heap: BinaryHeap<Entry>,
    pending: Vec<Entry>,
    capacity: usize,
    #[cfg(test)]
    before_admit: Option<Box<dyn FnOnce() + Send>>,
    #[cfg(test)]
    before_claim: Option<Box<dyn FnOnce() + Send>>,
}

impl<Value> Default for TimerDriver<Value> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Value> TimerDriver<Value> {
    /// Construct an empty driver. It may move until the first poll/query.
    pub fn new() -> Self {
        Self {
            pools: Vec::new(),
            heap: BinaryHeap::new(),
            pending: Vec::new(),
            capacity: 0,
            #[cfg(test)]
            before_admit: None,
            #[cfg(test)]
            before_claim: None,
        }
    }

    /// Add a fixed pool and inbox. Panics if capacity is zero.
    ///
    /// Each producer has a separate notification flag, avoiding a global atomic
    /// read-modify-write on every registration. Its flag coalesces driver wakes.
    pub fn producer(&mut self, capacity: usize) -> TimerProducer<Value> {
        assert!(capacity > 0, "producer capacity must be nonzero");
        self.capacity = self
            .capacity
            .checked_add(capacity)
            .expect("timer capacity overflow");
        self.heap.reserve(self.capacity - self.heap.len());
        self.pending.reserve(self.capacity - self.pending.len());
        let pool = Arc::new(Pool {
            slots: (0..capacity)
                .map(|_| {
                    Mutex::new(Slot {
                        value: None,
                        handle: false,
                        queued: false,
                        in_heap: false,
                        cancelled: false,
                    })
                })
                .collect(),
            free: ArrayQueue::new(capacity),
            inbox: ArrayQueue::new(capacity),
            closed: Mutex::new(false),
            cancelled_heap: AtomicUsize::new(0),
            notification: Notification::default(),
        });
        // macOS's standard mutex allocates native storage on first use. Warm
        // each lock during pool setup so registration itself never allocates.
        drop(lock(&pool.closed));
        for slot in &pool.slots {
            drop(lock(slot));
        }
        for index in 0..capacity {
            pool.free.push(index).unwrap();
        }
        self.pools.push(Arc::clone(&pool));
        TimerProducer { pool }
    }

    fn flush(&mut self) {
        for (index, pool) in self.pools.iter().enumerate() {
            pool.notification.begin_poll();
            // Bound this pass so submissions cannot starve other producers or
            // expiry. New publications retain a notification/park token.
            for _ in 0..pool.inbox.capacity() {
                let Some((deadline, slot)) = pool.inbox.pop() else {
                    break;
                };
                self.pending.push(Entry {
                    deadline,
                    pool: index,
                    slot,
                });
            }
        }
        #[cfg(test)]
        if let Some(before_admit) = self.before_admit.take() {
            before_admit();
        }
        // Filter only after draining all inboxes: cancellations that arrive
        // during that drain can still avoid heap insertion and heap repair.
        self.pending
            .retain(|entry| self.pools[entry.pool].admit(entry.slot));

        let mut cancelled = 0;
        let mut pressure = false;
        for pool in &self.pools {
            let count = pool.cancelled_heap.load(AtomicOrdering::Relaxed);
            cancelled += count;
            pressure |= count > 0 && pool.free.is_empty();
        }
        let retained = self.heap.len() + self.pending.len();
        // These snapshots are policy hints, not payload ownership. Cancellations
        // racing the snapshot notify us for another poll. Avoid O(n) scans for
        // sparse cancellations, but do not strand a bounded pool's capacity.
        let compact = pressure
            || (cancelled >= COMPACT_MINIMUM_COUNT && cancelled > retained / 2)
            || (cancelled > 0 && cancelled == retained);
        if compact || self.pending.len() > self.heap.len().saturating_mul(HEAPIFY_INSERT_RATIO) {
            let mut entries = std::mem::take(&mut self.heap).into_vec();
            if compact {
                entries.retain(|entry| {
                    let pool = &self.pools[entry.pool];
                    if pool.is_cancelled(entry.slot) {
                        pool.retire(entry.slot);
                        false
                    } else {
                        true
                    }
                });
            }
            entries.append(&mut self.pending);
            self.heap = BinaryHeap::from(entries);
        } else {
            for entry in self.pending.drain(..) {
                self.heap.push(entry);
            }
        }
        while self
            .heap
            .peek()
            .is_some_and(|entry| self.pools[entry.pool].is_cancelled(entry.slot))
        {
            let entry = self.heap.pop().unwrap();
            self.pools[entry.pool].retire(entry.slot);
        }
    }

    /// Drain submissions and return the earliest live deadline.
    pub fn next_deadline(&mut self) -> Option<Instant> {
        self.flush();
        self.heap.peek().map(|entry| entry.deadline)
    }

    /// Append expired values using only the output's spare capacity.
    ///
    /// Order is unspecified. Small batches pop the heap; dense batches partition
    /// entries and repair the heap once. Even a full output drains submissions
    /// and reclaims cancellations. Cancellation can race delivery; one wins.
    pub fn poll(&mut self, now: Instant, output: &mut Vec<Value>) -> usize {
        self.flush();
        // Deterministic tests pause at the snapshot/claim boundary to exercise
        // remote cancellation after submissions have been incorporated.
        #[cfg(test)]
        if let Some(before_claim) = self.before_claim.take() {
            before_claim();
        }
        let start = output.len();
        let room = output.capacity() - start;
        if room == 0 || self.heap.peek().is_none_or(|entry| entry.deadline > now) {
            return 0;
        }
        let threshold = (self.heap.len() / (self.heap.len().ilog2() as usize + 1)).max(1);
        if count_due(self.heap.as_slice(), 0, now, room.min(threshold)) >= threshold {
            let mut entries = std::mem::take(&mut self.heap).into_vec();
            entries.retain(|entry| {
                if entry.deadline <= now && output.len() < output.capacity() {
                    if let Some(value) = self.pools[entry.pool].retire(entry.slot) {
                        output.push(value);
                    }
                    false
                } else {
                    true
                }
            });
            self.heap = BinaryHeap::from(entries);
        } else {
            while output.len() < output.capacity()
                && self.heap.peek().is_some_and(|entry| entry.deadline <= now)
            {
                let entry = self.heap.pop().unwrap();
                if let Some(value) = self.pools[entry.pool].retire(entry.slot) {
                    output.push(value);
                }
            }
        }
        output.len() - start
    }

    /// Wait for a notification or the earliest deadline, allowing spurious wakes.
    ///
    /// Publish-before-notify plus reset-before-drain preserve an unpark token
    /// when a producer races the transition to sleep. Call poll after waking.
    /// A driver without producers returns immediately.
    pub fn park(&mut self) {
        match self.next_deadline() {
            Some(deadline) => {
                thread::park_timeout(deadline.saturating_duration_since(Instant::now()))
            }
            None if !self.pools.is_empty() => thread::park(),
            None => (),
        }
    }
}

// Pop each entry before invoking user destructors. If one panics, the guard
// finishes all remaining cleanup during unwinding (a second panic still aborts).
struct Closing<'a, Value>(&'a mut TimerDriver<Value>);
impl<Value> Closing<'_, Value> {
    fn finish(&mut self) {
        while let Some(entry) = self.0.heap.pop().or_else(|| self.0.pending.pop()) {
            drop(self.0.pools[entry.pool].retire(entry.slot));
        }
        for pool in &self.0.pools {
            while let Some((_, slot)) = pool.inbox.pop() {
                drop(pool.retire(slot));
            }
        }
    }
}
impl<Value> Drop for Closing<'_, Value> {
    fn drop(&mut self) {
        self.finish();
    }
}
impl<Value> Drop for TimerDriver<Value> {
    fn drop(&mut self) {
        // Taking each admission lock waits for in-flight publication. Producers
        // never acquire another pool's admission lock, so this cannot deadlock.
        for pool in &self.pools {
            *lock(&pool.closed) = true;
        }
        let mut closing = Closing(self);
        closing.finish();
    }
}

#[cfg(test)]
mod tests;
