# socketry-executor

Owned future tasks and a work-stealing scheduler for Socketry's Rust packages. Use `socketry` as the common entry point, or depend on this package directly.

The [executor design](design.md) records the agreed direction for cooperative cancellation, scheduler capabilities, portable I/O, and owned buffers, distinguishing implemented APIs from proposals.

## Execution

- `Scheduler::new()` starts workers using available hardware parallelism.
- `Scheduler::with_workers(count)` selects a nonzero worker count explicitly.
- `scheduler.spawn(future)` registers an owned task and schedules it immediately.
- `handle.await` returns `Result<Output, TaskError>`.
- `scheduler.block_on(future)` polls a root future on the calling thread. This root can borrow local data and need not be Send. It is not a spawned task.
- `scheduler.run()` waits until all owned tasks finish; it leaves admission open.
- `scheduler.shutdown()` closes admission, cancels tasks and joins workers.

```rust
use socketry_executor::{Scheduler, yield_now};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scheduler = Scheduler::with_workers(4)?;
    let children = scheduler.barrier();
    let task = children.spawn(async {
        yield_now().await;
        42
    })?;

    children.close();
    scheduler.block_on(async {
        assert_eq!(task.await?, 42);
        children.wait().await;
        Ok::<(), socketry_executor::TaskError>(())
    })?;
    Ok(())
}
```

Workers poll pinned `Send + 'static` futures on ordinary thread stacks. A task may migrate between polls; its pinned future stays at the same address. Only one worker polls a given task at a time. Futures returning Pending must arrange wakeups according to the standard Future contract.

`Scheduler::current()` returns a scheduler handle on workers and within a `block_on` root. `Task::current()` returns the task being polled or destroyed; it returns None at the root. `Task` references are thread-safe and support identity and cancellation. They do not keep the task executing.

Blocking scheduler entry points panic on worker threads. Ordinary blocking system calls still block a worker, and CPU-intensive code must yield explicitly.

## Queues and affinity

Each worker owns a FIFO `crossbeam-deque::Worker` and has a concurrent incoming queue for remote wakeups. New tasks submitted outside a worker enter the global injector. New tasks spawned by a worker enter its local queue.

Once a task has run, wakeups target its last worker. That worker uses its local queue when waking itself; other threads use its incoming queue. A worker checks external work periodically even while its local queue stays busy. A worker without work steals batches from other workers' local queues or incoming queues. This preserves affinity until another worker needs work; it does not pin tasks to threads.

Workers publish their sleeping state and recheck work before parking. Enqueuers wake a sleeping worker after publishing work. Crossbeam's Retry result causes another search rather than parking. These transitions preserve racing wakeups. Paired sequentially consistent fences order publication against idle registration. A Loom model covers that handshake; integration tests exercise the actual queues and task implementation.

## Ownership and cancellation

Schedulers own top-level tasks. A `Barrier` explicitly owns its direct children while the same scheduler executes them. Both implement `Spawn`; its associated handle type leaves room for adapters using different runtimes.

- Dropping a join handle abandons its result; it does not cancel the owned task.
- `task.task().cancel()` requests cancellation without waiting.
- `task.cancel().await` requests cancellation and awaits future destruction.
- `barrier.close()` prevents new children while existing children continue.
- `barrier.wait().await` waits until there are no direct children. Close first when the set of children must remain closed.
- `barrier.stop().await` closes, cancels and waits.
- Dropping a barrier closes it and requests cancellation without waiting.
- A scheduler's surviving handles reject submissions after shutdown begins.

Cancellation is observed before the next poll. A running poll may return a successful result before cancellation is observed. Destruction waits until that poll returns. Dropping the future runs ordinary destructors, not the remainder of its async body. A task that never returns from poll prevents joined shutdown.

Future panics become `TaskError::Panicked` containing the original payload. Await join handles to observe errors. Barrier waits and scheduler.run do not aggregate task results or propagate unobserved failures.

Parent tasks must explicitly await their barriers for joined child cleanup. Dropping a parent can drop its barriers and request cancellation, but parent completion does not automatically wait for descendants. Owned tasks must be `'static`; ownership does not permit borrowing a parent's local variables.

Dropping a scheduler outside a worker cancels tasks and joins workers. Dropping one on a Socketry worker requests cancellation and lets workers finish without joining synchronously, avoiding a worker waiting for itself.

## Cooperative cancellation

`Cancellation` is a runtime-independent shutdown signal. It does not own tasks or destroy futures. `cancel()` requests cancellation and returns true for the first request on that signal; subsequent requests return false and never escalate. `is_cancelled()` observes the persistent state, `check()` returns `Err(Cancelled)` when cancelled, and `cancelled().await` waits using ordinary polling and wakeups.

Clones share state. `child()` creates an independent cancellation boundary that also receives ancestor cancellation. Cancelling a child leaves its parent and siblings running. Descendants retain this relationship even after intermediate handles are dropped. Dropping signals never requests cancellation. A child created after its parent is cancelled starts cancelled. `Cancellation::never()` requires no allocation and remains uncancelled; its children are independent cancellable signals.

`defer_cancel(&signal, future, on_cancel)` invokes a synchronous `FnOnce()` callback when cancellation is observed, then continues polling the protected future to completion. Keep asynchronous cleanup in that future; the callback should request shutdown or wake it. The callback runs before the next protected poll, including the first poll when the signal is already cancelled. If both cancellation and completion are observed in the same poll, the callback runs and the wrapper returns the future's normal output. Cancellation arriving during the protected poll can lose that race to completion.

```rust
use socketry_executor::{Cancellation, Scheduler, defer_cancel, yield_now};

let scheduler = Scheduler::with_workers(1)?;
let shutdown = Cancellation::new();
let drain = Cancellation::new();
shutdown.cancel();
let output = scheduler.block_on(defer_cancel(&shutdown, async {
    drain.cancelled().await;
    yield_now().await; // Asynchronous cleanup remains executable.
    42
}, || { drain.cancel(); }));
assert_eq!(output, 42);
# Ok::<(), std::io::Error>(())
```

Use a root signal for process shutdown and child signals for individual services. Keep task owners and the runtime alive until task handles or barriers confirm draining is complete. The protected work must use cancellation inputs that allow cleanup to continue; the wrapper does not mask signals passed into its operations. Callback panics propagate, and dropping the wrapper still drops its work.

These primitives do not change `Task::cancel`, `Barrier::stop`, or scheduler shutdown, which still destroy futures. Signal handlers and cancellation-aware I/O signatures remain separate integration work. Cooperative requests have no forced abort or deadline escalation; a process supervisor can enforce SIGTERM followed by SIGKILL.

## Implementation costs

`async-task` supplies pinned task storage, wakers, runnable state and join handles. Socketry adds a separately allocated, reference-counted task record for identity, cancellation, ownership and affinity. Each barrier also has a shared owner record. The task registry retains a waker while the task is alive.

Spawning and completion take the ownership registry mutex. Cancellation and owner closure also use it. Ordinary polling, wakeups and ready-queue operations do not take that mutex. Polling establishes task context with an Arc clone; explicit current-task/current-scheduler lookups clone shared references.

Each cancellable signal allocates a reference-counted node. A signal family shares a mutex for child registration and cancellation propagation; parent nodes hold weak child registrations, and children retain ancestors. Cancellation walks descendants iteratively and releases locks before waking listeners. Waiting uses the existing event-listener dependency. Dropped children remove their registrations, and deep ancestor chains are released iteratively. Ordinary signal checks use an atomic flag; `never()` needs no node or listener.

Queues allocate backing storage as needed. Rescheduling reuses the existing task; it does not allocate another future or a coroutine stack. Idle worker selection can scan worker flags, with a count allowing the scan to be skipped when all workers are busy. No throughput or allocation benchmark is claimed yet.

## I/O and selectors

The public `scheduler` module contains portable `Socket`, `File`, and `Clock` traits, the Socketry implementation in `socketry.rs`, the optional Tokio adapter in `tokio.rs`, and native implementations under `selector/`.

- `native` (default) supplies TCP connect/accept/read/write/readiness through async-io's process-wide reactor: epoll on Linux, kqueue on Apple/BSD, and IOCP/AFD socket readiness on Windows. The three platform modules expose the shared implementation; they do not duplicate its registration machinery.
- `io-uring` selects a dedicated Linux completion selector for socket and file reads/writes. Connection setup, accept, readiness and timers use async-io. On other supported platforms this feature leaves the platform default intact.
- `tokio` adds an adapter using a supplied `tokio::runtime::Handle`. It does not construct or drive a runtime. Enable that runtime's I/O and time facilities.
- With default features disabled, the executor and public contracts still build. Enabling only `tokio` avoids the native I/O dependencies.

`Scheduler` and `SchedulerHandle` implement the traits. Register an owned TCP socket/listener once, or use `connect`/`accept`, and retain the resulting resource across operations. Returned operation futures are Send. Registrations remain with their original reactor as tasks migrate; they are not re-created per poll. Tokio resources passed to an adapter for another runtime return InvalidInput.

Read/write operations take ownership of a `Vec<u8>` and return the buffer with the result, including ordinary errors. Read buffers must have an initialized length (`vec![0; capacity]`); capacity alone supplies no writable bytes. Buffer length is unchanged; only the first returned byte count contains new data. Reads and writes can be partial. Each read/write call is one operation, not a read-exact/write-all convenience method.

`File::file_read_at` and `file_write_at` accept an `Arc<std::fs::File>` and an explicit offset. The readiness and Tokio implementations use blocking pools. Use ordinary files opened without append mode, and offsets fitting i64. Unix positioned operations leave the shared cursor unchanged; the Windows blocking fallback updates it according to std's seek\_read/seek\_write semantics.

`Clock::sleep` uses async-io timers or Tokio timers. The io-event timer algorithm has not yet been ported. There is no general public blocking-task API yet.

### Cancellation and shutdown

Dropping a read/write future abandons its result. A submitted operation may still consume or transmit bytes. Ownership of the buffer and file/socket continues until kernel access ends. Await a result when the byte count matters.

The io\_uring selector probes required opcodes and completion-overflow support. It returns initialization errors rather than silently switching backend. Cancellation requests and original completions have distinct identifiers; only an original terminal completion releases the operation's resources. Socketry shutdown outside a worker joins tasks and drains its ring. Dropping the scheduler on a worker requests cancellation without blocking that worker.

Readiness resources use a process-wide reactor, which is not shut down with an individual Socketry scheduler. An already-started blocking file operation can continue after its waiting task is cancelled. The Tokio adapter's shutdown joins its owned tasks, not the external runtime or its blocking-operation pool.

An unexpected io\_uring selector failure after submission cannot return memory whose kernel lifetime is unknown. That exceptional path retains the affected resources and panics the waiting operation; it does not free in-flight buffers.

### Costs and remaining work

Readiness registrations and buffers are reused. io\_uring currently uses a dedicated selector thread, a command channel and one completion channel per operation; it does not pool operation records or register buffers. The Tokio file fallback retains an Arc/Mutex buffer owner to return the buffer even when a queued blocking job is cancelled during runtime shutdown. Neither fallback copies the buffer bytes just to transfer ownership.

Native overlapped Windows file operations, arbitrary descriptor/UDP APIs, operation and buffer pools, and the io-event timer port remain future work. There is no thread-local non-Send task facility. No I/O throughput claim has been established by benchmarks.

The portable example uses the same generic TCP exchange with either runtime:

```sh
cargo run -p socketry-executor --example portable_io
cargo run -p socketry-executor --example portable_io --no-default-features --features tokio
cargo run -p socketry-executor --example portable_io --features io-uring # Linux
```

## Preserved prototype

The native coroutine prototype, verbatim CRuby vendor sources and its tests are preserved in commit `b520f3d` on branch `coroutine`.

Run the example and tests with:

```sh
cargo run --package socketry-executor --example work_stealing
cargo test --workspace --all-targets --locked
cargo test --workspace --doc --locked
```
