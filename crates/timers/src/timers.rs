// Released under the MIT License.
// Copyright, 2021, by Wander Hillen.
// Copyright, 2021-2026, by Samuel Williams.
// Based on IO::Event::Timers by Samuel Williams and IO::Event::PriorityHeap
// by Wander Hillen and Samuel Williams (MIT).

use crate::TimerHandle;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

const COMPACT_MINIMUM_COUNT: usize = 128;
const HEAPIFY_INSERT_RATIO: usize = 2;
const NO_SLOT: usize = usize::MAX;

// Heap order lets us prune an entire subtree when its root is not expired.
// Stop counting once there are enough entries to justify rebuilding the heap.
// Recursion uses O(log n) stack space, without allocating a traversal buffer.
fn count_expired(entries: &[Entry], index: usize, now: Instant, limit: usize) -> usize {
    if limit == 0 {
        return 0;
    }
    let Some(entry) = entries.get(index) else {
        return 0;
    };
    if entry.deadline > now {
        return 0;
    }
    let left = count_expired(entries, index * 2 + 1, now, limit - 1);
    1 + left + count_expired(entries, index * 2 + 2, now, limit - 1 - left)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Key {
    index: usize,
    generation: u64,
}

#[derive(Clone, Copy)]
struct Entry {
    deadline: Instant,
    key: Key,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline
    }
}
impl Eq for Entry {}
impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        other.deadline.cmp(&self.deadline)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Position {
    Pending,
    Heap,
    Removed,
}

struct Slot<Value> {
    value: Option<Value>,
    generation: u64,
    handles: usize,
    position: Position,
    cancelled: bool,
    next_free: usize,
}

pub(crate) type Shared<Value> = Arc<Mutex<State<Value>>>;

pub(crate) fn lock<Value>(shared: &Shared<Value>) -> MutexGuard<'_, State<Value>> {
    // A capacity-overflow panic can poison the queue without damaging its data.
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}

// Separate worker queues' Arc counters, locks and state on platforms with
// 128-byte cache lines. This reserves padding once per queue, not per timer.
#[cfg_attr(feature = "sharded", repr(align(128)))]
pub(crate) struct State<Value> {
    #[cfg(any(feature = "driver", feature = "sharded"))]
    closed: bool,
    #[cfg(feature = "sharded")]
    pub(crate) publication: Option<Arc<crate::sharded::Publication>>,
    slots: Vec<Slot<Value>>,
    heap: BinaryHeap<Entry>,
    scheduled: Vec<Entry>,
    free: usize,
    free_count: usize,
    live: usize,
    cancelled: usize,
}

impl<Value> State<Value> {
    fn capacity(&self) -> usize {
        self.slots
            .capacity()
            .min(self.heap.capacity())
            .min(self.scheduled.capacity())
    }

    fn occupied(&self) -> usize {
        self.slots.len() - self.free_count
    }

    fn reserve(&mut self, additional: usize) {
        let required = self
            .occupied()
            .checked_add(additional)
            .expect("timer capacity overflow");
        if required > self.capacity() {
            self.slots
                .reserve(required.saturating_sub(self.slots.len()));
            let capacity = self.slots.capacity();
            self.heap.reserve(capacity.saturating_sub(self.heap.len()));
            self.scheduled
                .reserve(capacity.saturating_sub(self.scheduled.len()));
        }
    }

    fn insert(&mut self, deadline: Instant, value: Value) -> Key {
        let index = if self.free == NO_SLOT {
            let index = self.slots.len();
            self.slots.push(Slot {
                value: None,
                generation: 0,
                handles: 0,
                position: Position::Removed,
                cancelled: false,
                next_free: NO_SLOT,
            });
            index
        } else {
            let index = self.free;
            self.free = self.slots[index].next_free;
            self.free_count -= 1;
            index
        };
        let slot = &mut self.slots[index];
        // A slot is reused only after all handles AND queue entries are gone.
        // Consequently wrapping cannot collide with an outstanding key.
        slot.generation = slot.generation.wrapping_add(1);
        slot.value = Some(value);
        slot.handles = 1;
        slot.position = Position::Pending;
        slot.cancelled = false;
        let key = Key {
            index,
            generation: slot.generation,
        };
        self.scheduled.push(Entry { deadline, key });
        self.live += 1;
        #[cfg(feature = "sharded")]
        if let Some(publication) = &self.publication {
            publication.lower(deadline);
        }
        key
    }

    pub(crate) fn try_insert(&mut self, deadline: Instant, value: Value) -> Result<Key, Value> {
        #[cfg(any(feature = "driver", feature = "sharded"))]
        if self.closed {
            return Err(value);
        }
        if self.occupied() == self.capacity() {
            return Err(value);
        }
        Ok(self.insert(deadline, value))
    }

    fn slot(&self, key: Key) -> &Slot<Value> {
        let slot = &self.slots[key.index];
        assert_eq!(
            slot.generation, key.generation,
            "timer slot generation mismatch"
        );
        slot
    }

    pub(crate) fn clone_handle(&mut self, key: Key) {
        let handles = self
            .slot(key)
            .handles
            .checked_add(1)
            .expect("timer handle count overflow");
        self.slots[key.index].handles = handles;
    }

    pub(crate) fn drop_handle(&mut self, key: Key) {
        self.slot(key);
        self.slots[key.index].handles -= 1;
        self.release(key);
    }

    fn release(&mut self, key: Key) {
        let slot = &mut self.slots[key.index];
        if slot.handles == 0 && slot.position == Position::Removed {
            slot.next_free = self.free;
            self.free = key.index;
            self.free_count += 1;
        }
    }

    pub(crate) fn is_cancelled(&self, key: Key) -> bool {
        self.slot(key).cancelled
    }

    pub(crate) fn cancel(&mut self, key: Key) -> Option<Value> {
        self.slot(key);
        let slot = &mut self.slots[key.index];
        let value = slot.value.take();
        if value.is_some() {
            slot.cancelled = true;
            self.live -= 1;
            if slot.position == Position::Heap {
                self.cancelled += 1;
            }
        }
        #[cfg(feature = "sharded")]
        if value.is_some()
            && let Some(publication) = &self.publication
            && (self.live == 0 || self.occupied() == self.capacity())
        {
            // Request assistance for empty queues or capacity pressure even
            // when the cancelled timer's deadline is still in the future.
            publication.lower(publication.epoch);
        }
        value
    }

    fn remove(&mut self, key: Key) -> Option<Value> {
        let slot = &mut self.slots[key.index];
        if slot.position == Position::Heap && slot.cancelled {
            self.cancelled -= 1;
        }
        slot.position = Position::Removed;
        let value = slot.value.take();
        if value.is_some() {
            self.live -= 1;
        }
        self.release(key);
        value
    }

    fn flush(&mut self) {
        #[cfg(feature = "sharded")]
        let capacity = self.capacity();
        // Temporarily split field borrows to filter without allocating or
        // taking a lock for each registration. No user data is dropped here.
        let mut scheduled = std::mem::take(&mut self.scheduled);
        scheduled.retain(|entry| {
            if self.is_cancelled(entry.key) {
                self.remove(entry.key);
                false
            } else {
                self.slots[entry.key.index].position = Position::Heap;
                true
            }
        });

        let compact =
            self.cancelled >= COMPACT_MINIMUM_COUNT && self.cancelled > self.heap.len() / 2;
        #[cfg(feature = "sharded")]
        let compact = compact
            || (self.publication.is_some() && self.cancelled > 0 && self.occupied() == capacity);
        if compact {
            let mut contents = std::mem::take(&mut self.heap).into_vec();
            contents.retain(|entry| {
                if self.is_cancelled(entry.key) {
                    self.remove(entry.key);
                    false
                } else {
                    true
                }
            });
            contents.append(&mut scheduled);
            self.heap = BinaryHeap::from(contents);
        } else if self.live == 0 {
            let mut contents = std::mem::take(&mut self.heap).into_vec();
            for entry in contents.drain(..) {
                self.remove(entry.key);
            }
            self.heap = BinaryHeap::from(contents);
        } else if !scheduled.is_empty() {
            if self.heap.is_empty()
                || scheduled.len() > self.heap.len().saturating_mul(HEAPIFY_INSERT_RATIO)
            {
                let mut contents = std::mem::take(&mut self.heap).into_vec();
                contents.append(&mut scheduled);
                self.heap = BinaryHeap::from(contents);
            } else {
                for entry in scheduled.drain(..) {
                    self.heap.push(entry);
                }
            }
        }
        self.scheduled = scheduled;
        #[cfg(feature = "sharded")]
        self.refresh_publication();
    }

    fn discard_cancelled(&mut self) {
        while self
            .heap
            .peek()
            .is_some_and(|entry| self.is_cancelled(entry.key))
        {
            let entry = self.heap.pop().unwrap();
            self.remove(entry.key);
        }
        #[cfg(feature = "sharded")]
        self.refresh_publication();
    }

    pub(crate) fn pop_expired_batch(&mut self, now: Instant, output: &mut Vec<Value>) -> usize {
        let start = output.len();
        let room = output.capacity() - start;
        if room == 0 {
            return 0;
        }
        self.flush();
        self.discard_cancelled();
        if self.heap.peek().is_none_or(|entry| entry.deadline > now) {
            return 0;
        }

        // Compare roughly k * log2(n) individual heap repairs with an O(n)
        // partition and heapify. Count entries, including cancellations, only
        // up to the crossover or the caller's available output capacity.
        let threshold = (self.heap.len() / (self.heap.len().ilog2() as usize + 1)).max(1);
        if count_expired(self.heap.as_slice(), 0, now, room.min(threshold)) >= threshold {
            let mut entries = std::mem::take(&mut self.heap).into_vec();
            entries.retain(|entry| {
                if self.is_cancelled(entry.key) {
                    self.remove(entry.key);
                    false
                } else if entry.deadline <= now && output.len() < output.capacity() {
                    output.push(self.remove(entry.key).unwrap());
                    false
                } else {
                    true
                }
            });
            self.heap = BinaryHeap::from(entries);
        } else {
            while output.len() < output.capacity() {
                self.discard_cancelled();
                if self.heap.peek().is_none_or(|entry| entry.deadline > now) {
                    break;
                }
                let entry = self.heap.pop().unwrap();
                output.push(self.remove(entry.key).unwrap());
            }
        }
        #[cfg(feature = "sharded")]
        self.refresh_publication();
        output.len() - start
    }

    #[cfg(feature = "sharded")]
    pub(crate) fn refresh_publication(&self) {
        if let Some(publication) = &self.publication {
            publication.refresh(self.heap.peek().map(|entry| entry.deadline));
        }
    }

    #[cfg(feature = "sharded")]
    pub(crate) fn is_closed(&self) -> bool {
        self.closed
    }

    #[cfg(feature = "sharded")]
    pub(crate) fn value_mut(&mut self, key: Key) -> Option<&mut Value> {
        self.slot(key);
        self.slots[key.index].value.as_mut()
    }
}

// Restores buffers and finishes releasing values even if one user destructor
// panics during clear. Values are always destroyed outside the queue lock.
struct Clearing<'a, Value> {
    shared: &'a Shared<Value>,
    contents: Vec<Entry>,
    scheduled: Vec<Entry>,
    restored: bool,
}

impl<Value> Clearing<'_, Value> {
    fn finish(&mut self) {
        if self.restored {
            return;
        }
        while let Some(entry) = self.contents.pop().or_else(|| self.scheduled.pop()) {
            let value = lock(self.shared).remove(entry.key);
            drop(value);
        }
        let mut state = lock(self.shared);
        state.heap = BinaryHeap::from(std::mem::take(&mut self.contents));
        state.scheduled = std::mem::take(&mut self.scheduled);
        #[cfg(feature = "sharded")]
        state.refresh_publication();
        self.restored = true;
    }
}

impl<Value> Drop for Clearing<'_, Value> {
    fn drop(&mut self) {
        self.finish();
    }
}

/// A batched queue of monotonic deadlines and arbitrary values.
///
/// Registrations live in reusable slots. Handles share one queue allocation;
/// there is no allocation or mutex per timer. Scheduling appends in amortized
/// O(1); cancellation is O(1), excluding the value's destructor. Queue operations
/// and cross-thread cancellation synchronize through one shared mutex.
///
/// [`with_capacity`](Self::with_capacity) and [`reserve`](Self::reserve) reserve
/// slots and both queue buffers. Within that capacity, scheduling, flushing,
/// expiry and cancellation allocate no storage. User values and their
/// destructors may allocate independently. [`try_schedule`](Self::try_schedule)
/// never grows storage and returns the value when capacity is exhausted.
///
/// Capacity includes pending/heap entries and completed registrations with
/// outstanding handles. A slot is reused once its queue entry and all handles
/// are gone. Dropping a handle leaves its timer scheduled. Deadlines preserve
/// `Instant` precision; equal deadlines have unspecified order.
pub struct Timers<Value> {
    pub(crate) shared: Shared<Value>,
}

/// A registration-only view of the shared queue, for multi-worker comparisons.
///
/// Dropping this view leaves the queue intact. The owning [`Timers`] still
/// controls queue destruction. Available with the experimental `driver` feature.
#[cfg(feature = "driver")]
pub struct SharedTimerProducer<Value> {
    shared: Shared<Value>,
}

#[cfg(feature = "driver")]
impl<Value> SharedTimerProducer<Value> {
    /// Register without growing the shared queue, returning the value if full.
    pub fn try_schedule(
        &mut self,
        deadline: Instant,
        value: Value,
    ) -> Result<TimerHandle<Value>, Value> {
        try_schedule_shared(&self.shared, deadline, value)
    }
}

pub(crate) fn try_schedule_shared<Value>(
    shared: &Shared<Value>,
    deadline: Instant,
    value: Value,
) -> Result<TimerHandle<Value>, Value> {
    let key = lock(shared).try_insert(deadline, value)?;
    Ok(TimerHandle::new(shared, key, deadline))
}

impl<Value> Default for Timers<Value> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Value> Timers<Value> {
    /// Obtain a registration view sharing this queue's existing mutex.
    #[cfg(feature = "driver")]
    pub fn shared_producer(&self) -> SharedTimerProducer<Value> {
        SharedTimerProducer {
            shared: Arc::clone(&self.shared),
        }
    }
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Reserve space for at least `capacity` registrations.
    pub fn with_capacity(capacity: usize) -> Self {
        let timers = Self {
            shared: Arc::new(Mutex::new(State {
                #[cfg(any(feature = "driver", feature = "sharded"))]
                closed: false,
                #[cfg(feature = "sharded")]
                publication: None,
                slots: Vec::new(),
                heap: BinaryHeap::new(),
                scheduled: Vec::new(),
                free: NO_SLOT,
                free_count: 0,
                live: 0,
                cancelled: 0,
            })),
        };
        // Acquire the mutex now, including native lock initialization on macOS.
        lock(&timers.shared).reserve(capacity);
        timers
    }

    /// Total registration capacity, including occupied and reusable slots.
    pub fn capacity(&self) -> usize {
        lock(&self.shared).capacity()
    }

    /// Reserve room for `additional` registrations beyond currently occupied slots.
    pub fn reserve(&mut self, additional: usize) {
        lock(&self.shared).reserve(additional);
    }

    /// Number of live timers, excluding cancellations even before a flush.
    pub fn len(&self) -> usize {
        lock(&self.shared).live
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Schedule a value, growing queue storage if necessary.
    pub fn schedule(&mut self, deadline: Instant, value: Value) -> TimerHandle<Value> {
        let key = {
            let mut state = lock(&self.shared);
            state.reserve(1);
            state.insert(deadline, value)
        };
        TimerHandle::new(&self.shared, key, deadline)
    }

    /// Schedule without allocating queue storage, returning the value when full.
    ///
    /// Cancelled queue entries occupy slots until reclaimed by a flush or
    /// deadline query. Outstanding handles also retain their slots after expiry.
    pub fn try_schedule(
        &mut self,
        deadline: Instant,
        value: Value,
    ) -> Result<TimerHandle<Value>, Value> {
        try_schedule_shared(&self.shared, deadline, value)
    }

    /// Merge pending timers and reclaim sufficiently numerous cancellations.
    ///
    /// Small batches use O(m log(n + m)) incremental insertion; large batches
    /// and compaction use O(n + m) heapify. At least 128 entries and more than
    /// half the heap must be cancelled to trigger compaction.
    pub fn flush(&mut self) {
        lock(&self.shared).flush();
    }

    /// Earliest live deadline after flushing pending timers.
    ///
    /// Cross-thread cancellation can invalidate the returned deadline. Drivers
    /// should recheck the queue after waking.
    pub fn next_deadline(&mut self) -> Option<Instant> {
        let mut state = lock(&self.shared);
        state.flush();
        state.discard_cancelled();
        state.heap.peek().map(|entry| entry.deadline)
    }

    /// Wait until the earliest timer, clamping elapsed deadlines to zero.
    pub fn wait_interval(&mut self, now: Instant) -> Option<Duration> {
        self.next_deadline()
            .map(|deadline| deadline.saturating_duration_since(now))
    }

    /// Return the earliest expired value, or `None` if none is ready.
    ///
    /// Removes cancelled entries even before their deadlines. The returned
    /// value belongs to the caller, which can invoke callbacks or wake tasks
    /// after releasing any outer driver lock.
    pub fn pop_expired(&mut self, now: Instant) -> Option<Value> {
        let mut state = lock(&self.shared);
        state.flush();
        state.discard_cancelled();
        if state.heap.peek()?.deadline > now {
            return None;
        }
        let entry = state.heap.pop().unwrap();
        let value = state.remove(entry.key);
        #[cfg(feature = "sharded")]
        state.refresh_publication();
        value
    }

    /// Append expired values using only the output vector's spare capacity.
    ///
    /// Returns the number appended. Neither the output nor queue storage grows.
    /// A full output vector leaves the queue unchanged. Reserve output capacity
    /// before calling, then process values after this method releases the lock.
    ///
    /// Values in a batch have unspecified order, and a capacity-limited batch
    /// may leave earlier expired timers queued. Use [`pop_expired`](Self::pop_expired)
    /// when strict deadline order is required. All returned values are claimed
    /// under one lock; subsequent cancellation cannot stop their delivery.
    ///
    /// A pruned heap traversal estimates the expired count. Small batches use
    /// individual heap pops; large batches filter entries and heapify survivors
    /// once. The latter takes O(n) time, including cancellation reclamation.
    pub fn pop_expired_batch(&mut self, now: Instant, output: &mut Vec<Value>) -> usize {
        lock(&self.shared).pop_expired_batch(now, output)
    }

    /// Remove all timers and release values outside the shared queue lock.
    ///
    /// Storage capacity is retained; outstanding handles remain queryable.
    pub fn clear(&mut self) {
        let (contents, scheduled) = {
            let mut state = lock(&self.shared);
            (
                std::mem::take(&mut state.heap).into_vec(),
                std::mem::take(&mut state.scheduled),
            )
        };
        let mut clearing = Clearing {
            shared: &self.shared,
            contents,
            scheduled,
            restored: false,
        };
        clearing.finish();
    }
}

impl<Value> Drop for Timers<Value> {
    fn drop(&mut self) {
        #[cfg(any(feature = "driver", feature = "sharded"))]
        {
            let mut state = lock(&self.shared);
            state.closed = true;
            #[cfg(feature = "sharded")]
            if let Some(publication) = &state.publication {
                publication.close();
            }
        }
        self.clear();
    }
}

#[cfg(test)]
mod tests;
