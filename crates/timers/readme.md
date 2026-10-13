# `socketry-timers`

A runtime-independent monotonic timer queue, ported from `IO::Event::Timers`.

Scheduling appends to a pending batch. Registrations use reusable queue slots and share one queue lock; there is no allocation or mutex per timer. Cancellation takes constant time and releases the stored value immediately. Flushing filters pending cancellations, chooses incremental heap insertion for small batches or bottom-up heapify for large batches, and compacts cancelled entries when they are both numerous and more than half the heap. The default queue uses only the Rust standard library.

## Usage

```rust
use socketry_timers::Timers;
use std::time::{Duration, Instant};

let now = Instant::now();
let mut timers = Timers::new();
let cancelled = timers.schedule(now + Duration::from_secs(1), "cancelled");
timers.schedule(now + Duration::from_secs(2), "ready");
cancelled.cancel();

assert_eq!(timers.len(), 1);
assert_eq!(timers.wait_interval(now), Some(Duration::from_secs(2)));
assert_eq!(timers.pop_expired(now + Duration::from_secs(2)), Some("ready"));
assert!(timers.is_empty());
```

The caller drives expiry; the queue creates no thread and selects no executor. Store callbacks, wakers, or application data. Invoke callbacks and wakeups after releasing any outer queue lock. Dropping a handle leaves its timer scheduled; cancellation is explicit. Dropping or clearing the queue releases values even when handles remain alive.

`Instant` preserves deadline precision without floating-point validation or millisecond rounding. Equal deadlines have unspecified order. `len()` counts live timers, including pending ones and excluding cancellations; unlike the Ruby implementation's `size`, it does not flush or count cancelled heap entries. Elapsed wait intervals clamp to zero.

## Allocation-free operation

Reserve capacity before the event loop starts:

```rust
use socketry_timers::Timers;
use std::time::Instant;

let mut timers = Timers::with_capacity(1024);
let handle = timers.try_schedule(Instant::now(), 42).unwrap();
assert_eq!(timers.pop_expired(Instant::now()), Some(42));
drop(handle); // The completed slot can now be reused.
```

`with_capacity` and `reserve(additional)` reserve the slot arena, heap and pending buffer. Scheduling, flushing, cancellation, expiry, clearing and slot reuse allocate no queue storage within that capacity. User values and their destructors can allocate independently. `schedule` grows storage when needed; `try_schedule` never grows it and returns `Err(value)` when full. Both APIs return handles without allocating a separate registration.

Capacity counts retained registrations, including cancelled queue entries awaiting reclamation and completed timers with outstanding handles. Drop completed handles and their clones promptly: slots are reused only when their queue entry and last handle are gone. This preserves status queries and prevents stale cancellation after reuse. Handles keep the shared queue storage alive even after the queue is dropped; dropping the queue still releases all values. `clear()` retains buffers for reuse. The allocation-counting regression test verifies zero calls across repeated reserved timer lifecycles, compaction and reuse.

## Batch expiry

Reserve an output vector once, then reuse it to collect due timers:

```rust
use socketry_timers::Timers;
use std::time::Instant;

let now = Instant::now();
let mut timers = Timers::with_capacity(1024);
let mut ready = Vec::with_capacity(1024);
timers.try_schedule(now, 42).unwrap();
assert_eq!(timers.pop_expired_batch(now, &mut ready), 1);
assert_eq!(ready, [42]);
ready.clear(); // Retain the output capacity for the next event loop iteration.
```

`pop_expired_batch` appends up to the output vector's spare capacity and returns the number appended. It never grows either the vector or queue storage. Process or wake the returned values after the method releases the shared lock; all returned values have already been claimed against cancellation.

The queue counts due heap entries by traversing the heap and pruning future subtrees. It stops counting at the crossover between individual heap repairs and rebuilding. Small batches use ordinary pops under one lock; large batches filter the heap and heapify survivors once in O(n) time. This avoids repeated heap repair when many timers expire together and retains the heap's storage capacity.

Batch values have unspecified order. With limited output capacity, some earlier due timers may remain queued while later due timers are returned. Use `pop_expired` when strict deadline order is needed. An output vector with no spare capacity leaves the queue unchanged. Clearing or draining the vector lets the driver collect another batch without allocating.

## Scheduler clocks

This crate has no dependency on `socketry-executor`, its `Clock` trait, Tokio, or async-io. A scheduler can put `Timers<Arc<SleepState>>` behind its own lock. Its sleep future updates the state's waker when polled and cancels its handle when dropped. The event loop waits for `wait_interval`, pops expired states, marks them ready, and wakes them outside the queue lock. Registration of an earlier deadline must interrupt an already waiting event loop. Polling and firing must synchronize the ready flag and waker to preserve racing wakeups.

The driver must define shutdown behavior for outstanding sleeps; removing a registration alone does not wake a future. The existing Socketry clock continues to use async-io. This package supplies its independent timer queue, without changing the scheduler's lifecycle or event loop.

## Experimental timer driver

Enable the `driver` feature to explore one heap owner with bounded per-worker submission queues:

```rust
# #[cfg(feature = "driver")]
# {
use socketry_timers::driver::TimerDriver;
use std::time::Instant;

let mut driver = TimerDriver::new();
let mut producer = driver.producer(1024);
let mut ready = Vec::with_capacity(1024);
let handle = producer.try_schedule(Instant::now(), 42).unwrap();
assert_eq!(driver.poll(Instant::now(), &mut ready), 1);
assert_eq!(ready, [42]);
assert!(!handle.cancel()); // Delivery already claimed the value.
# }
```

The producer and cancellation handle can move between threads; the driver remains on its bound thread after its first poll or deadline query. An event loop polls, processes ready values, then calls `driver.park()` to wait for submissions or deadlines. Registration and delivery allocate no storage after pool setup and thread binding. The prototype has no global heap mutex, but uses a mutex per reserved slot and an admission lock per producer. On macOS those native slot locks allocate during setup. Handles are not clonable. Cancellation releases values immediately and defers slot reclamation to the driver; full/closed producers return the original value. Cancelled submissions skip heap insertion. Heap cancellations accumulate for batch compaction or root removal, with earlier compaction when a producer pool has no free slots.

This feature adds the optional Crossbeam Queue dependency and remains independent of scheduler APIs. The experiment also exposes `Timers::shared_producer()` for a direct comparison with the existing global mutex. See [design, concurrent benchmarks, latency and limitations](context/driver.md). The current measurements show strong throughput gains under contention, an overhead at low contention, and no consistent deadline-latency improvement.

## Experimental worker queues

Enable `sharded` to keep one timer queue per worker while allowing available workers to collect due timers from a stalled owner:

```rust
# #[cfg(feature = "sharded")]
# {
use socketry_timers::sharded::WorkerTimers;
use std::time::Instant;

let mut worker = WorkerTimers::with_capacity(1024);
let peer = worker.assistance(); // Move or clone this view for another worker.
let mut ready = Vec::with_capacity(1024);
let now = Instant::now();
let handle = worker.try_schedule(now, 42).unwrap();
assert_eq!(peer.try_poll(now, &mut ready), Some(1));
assert_eq!(ready, [42]);
assert!(!handle.cancel());
# }
```

Each worker retains its reusable arena, pending buffer and binary heap, protected by one queue mutex. Scheduling publishes an atomic deadline hint immediately, so assistance can see unflushed timers. Helpers inspect hints without locking and use `try_poll` to claim due values without waiting on a busy queue. Remote cancellation and cloned handles continue to refer to the originating queue. Values need only be `Send` for cross-thread use.

Construct `TimerWaiter` on an idle worker during setup to watch several assistance views. Earlier deadlines and shutdown unpark it; `park()` waits for the earliest published deadline. After waking, visit queues and route collected wakeups into the scheduler outside timer operations. Reserved scheduling, cancellation, assistance and repeated parking allocate nothing. Waiter setup allocates its watcher storage; notifications briefly lock a watcher list.

Assistance requires an available thread and cannot make progress if the owner stalls while holding its queue mutex. This prototype creates no thread and has no scheduler integration. Dropping the owner closes its queue and releases stored values; surviving assistance views reject registration. See [design, performance and limitations](context/sharded.md).

For actual sleep futures, use a `WorkerTimers<SleepWake>` queue. Construction and cancellation before the first poll require no queue operations:

```rust
# #[cfg(feature = "sharded")]
# {
use socketry_timers::sharded::{SleepWake, WorkerTimers};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Instant;

let mut worker = WorkerTimers::<SleepWake>::with_capacity(1024);
let clock = worker.assistance();
let now = Instant::now();
drop(clock.sleep_until(now)); // No slot reservation, lock, or deadline publication.
let mut sleep = clock.sleep_until(now);
let mut context = Context::from_waker(Waker::noop());
assert!(Pin::new(&mut sleep).poll(&mut context).is_pending());
let mut ready = Vec::with_capacity(1024);
worker.poll(now, &mut ready);
ready.clear(); // Dropping SleepWake values wakes tasks outside the queue lock.
assert_eq!(Pin::new(&mut sleep).poll(&mut context), Poll::Ready(Ok(())));
# }
```

First polling registers the current waker and publishes the deadline immediately. Later polls update the waker. Dropping a registered sleep cancels it without waking the task. A full queue returns `SleepError::Full`; shutdown wakes registered sleeps and they return `SleepError::Closed`. Shutdown takes precedence until a sleep is polled to completion. The driver must service past deadlines as well as future deadlines. Future construction retains an Arc to the original queue, so it has a small reference-count cost even when never polled. Waker callbacks run outside queue locks and can allocate independently.

Each sleep owns its slot key directly and keeps one queue reference through registration. Cancellation and slot-reference release share one lock acquisition; completion checking and release do likewise. A completed future releases its queue reference immediately, even if the future itself remains alive.

## Benchmarks

```sh
cargo bench -p socketry-timers --bench timers
cargo bench -p socketry-timers --bench allocations
cargo bench -p socketry-timers --bench expiry
cargo bench -p socketry-timers --all-features --bench cancellation -- --no-cancel
cargo bench -p socketry-timers --all-features --bench cancellation -- --cancellation-sweep
```

The lifecycle benchmark compares growing and reserved queue variants with registered Tokio `Sleep` values using a current-thread runtime and paused time. Each Tokio sleep is polled to register it, and the complete slice is pinned in one allocation, avoiding a box per sleep. All variants store the same no-op waker. Every timed batch includes queue construction, reservation when applicable, registration, cancellation or expiry when applicable, and destruction; offsets and the Tokio runtime are prepared outside timing. Samples rotate implementation order. Expiry includes driving Tokio's timer service and polling completed sleeps; Socketry's caller explicitly supplies elapsed time and wakes returned values. The bulk expiry case includes allocating its output vector inside timing. The separate `expiry` benchmark excludes setup and destruction to compare sparse and dense expiry batches with individual pops. The allocation benchmark checks that reserved lifecycles, including bulk expiry, make zero allocation calls.

These measurements describe single-thread timer bookkeeping. They do not measure real-time wakeup latency, scheduler throughput, contention, or an implemented Socketry sleep future. See [benchmark results and methodology](context/benchmarks.md).

The `cancellation --no-cancel` fixture compares the experimental worker sleep future with Tokio when every timer fires, including registration, bulk expiry, no-op wakeups, completion polling and future destruction. Queue/runtime setup and fixed storage reservation are excluded. See [worker sleep lifecycle results](context/sharded.md#no-cancellation-lifecycle).

Use `--cancellation-sweep` for the same complete future lifecycle with 0%, 10%, 50%, 90% and 100% cancellation after registration. Every row uses the same timing window and has no resident timers. See [cancellation sweep results](context/sharded.md#cancellation-sweep).

## Provenance

Ported from io-event revision `66caf64fbe27d752e4c64a60a7d0fba8bceaddbc`, specifically [`lib/io/event/timers.rb`](https://github.com/socketry/io-event/blob/66caf64fbe27d752e4c64a60a7d0fba8bceaddbc/lib/io/event/timers.rb) and [`lib/io/event/priority_heap.rb`](https://github.com/socketry/io-event/blob/66caf64fbe27d752e4c64a60a7d0fba8bceaddbc/lib/io/event/priority_heap.rb). The timer implementation is by Samuel Williams; the original priority heap is by Wander Hillen and Samuel Williams. Both are MIT licensed. Rust uses `BinaryHeap` for heap repair, an arena for registrations, and a shared queue lock for thread-safe cancellation. See [license.md](license.md).
