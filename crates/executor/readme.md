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
    let task = scheduler.spawn(async {
        yield_now().await;
        42
    })?;

    scheduler.block_on(async {
        assert_eq!(task.await?, 42);
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

Task handles own the execution lifetime of tasks dispatched through a scheduler. The scheduler retains their storage until completion or destruction. Both implement the primitive `Spawn` contract; its associated handle type leaves room for adapters using different runtimes. Dispatch takes a future and returns a task handle, without requiring a group or owner argument.

- Moving a task handle transfers ownership. Dropping it abandons execution after any active poll returns; it does not wait.
- Await a handle explicitly for completion. Cooperative shutdown uses `cancellation.cancel()` followed by awaiting that handle.
- `task.task().cancel()` requests cancellation without waiting.
- `task.cancel().await` requests cancellation and awaits future destruction.
- A scheduler's surviving handles reject submissions after shutdown begins.

Cancellation is observed before the next poll. A running poll may return a successful result before cancellation is observed. Destruction waits until that poll returns. Dropping the future runs ordinary destructors, not the remainder of its async body. A task that never returns from poll prevents joined shutdown.

Future panics become `TaskError::Panicked` containing the original payload. Await join handles to observe errors. Scheduler::run do not aggregate task results or propagate unobserved failures.

There is no implicit task tree. Destroying a future drops task handles it still owns, which abandons those tasks. Handles moved to another task or container remain owned there. Await their handles when their completion matters. Higher-level task groups can be built above dispatch and are outside the executor for now. Owned tasks must be `'static`; ownership does not permit borrowing a parent's local variables.

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

Use a root signal for process shutdown and child signals for individual services. Keep the scheduler and runtime alive until task handles confirm draining is complete. The protected work must use cancellation inputs that allow cleanup to continue; the wrapper does not mask signals passed into its operations. Callback panics propagate, and dropping the wrapper still drops its work.

These primitives do not change `Task::cancel` or scheduler shutdown, which still destroy futures. File and socket operations accept explicit cancellation inputs and return `OperationError::Cancelled` separately from OS errors. Clock cancellation remains proposed. Cooperative requests have no forced abort or deadline escalation; a process supervisor can enforce SIGTERM followed by SIGKILL.

## Signal notifications

Enable the optional `signals` feature on Unix together with `native` (the default) or `tokio` to use `Signals::signal_notify` on Socketry's scheduler or handle, or the Tokio adapter. Registration is explicit, synchronous, and fallible. Scheduler construction installs no signal handlers. The returned `#[must_use]` guard owns one subscription; keep it alive during graceful draining and drop it to unsubscribe.

```rust,no_run
# #[cfg(all(unix, feature = "signals", feature = "native"))]
# fn main() -> std::io::Result<()> {
use socketry_executor::{Cancellation, Scheduler, Signals, signal::{SIGINT, SIGTERM, SIGHUP}};

let scheduler = Scheduler::with_workers(1)?;
let shutdown = Cancellation::new();
let shutdown_registration = scheduler.signal_notify(&[SIGINT, SIGTERM], shutdown.clone())?;

let (reload, mut reloads) = futures_channel::mpsc::unbounded();
let reload_registration = scheduler.signal_notify(&[SIGHUP], reload)?;
// Receive reloads using StreamExt::next(); shutdown is independent of reloads.
scheduler.block_on(shutdown.cancelled());
// Drain application work while the scheduler and registrations remain alive.
drop(reload_registration);
drop(shutdown_registration);
# Ok(())
# }
# #[cfg(not(all(unix, feature = "signals", feature = "native")))]
# fn main() {}
```

`Notify<Value>::notify(&self, value)` is a synchronous target interface called in ordinary thread context, outside raw signal handlers and scheduler locks. `Cancellation` ignores the value and requests cancellation. Standard unbounded `std::sync::mpsc::Sender` and `futures_channel::mpsc::UnboundedSender` enqueue it; a closed receiver discards it. `Arc<T>` forwards to its target. Bounded channels need an application wrapper with an explicit overflow policy. A target should enqueue or wake work and return promptly. Waiting remains target-specific: `cancelled()` or the channel's receive operation.

Each registration spawns one scheduler task that waits asynchronously on signal-hook's self-pipe using the chosen backend: async-io for native, or the supplied Tokio runtime for the adapter. It creates no dedicated thread. Dropping the guard stops new notification admission and abandons its owned task; after any active poll returns, task destruction releases the subscription. An already-admitted callback may finish, retaining its target until completion. Other subscriptions remain active. A callback panic ends its own task through normal scheduler panic handling. Scheduler/runtime shutdown also ends delivery, so keep those services alive during draining. No repeated-signal escalation is installed.

OS signals and the backend can coalesce repeated occurrences before delivery; the channel retains delivered values but cannot count every occurrence or promise ordering across different signals. Unsubscribing releases the registration asynchronously and does not restore original process signal dispositions, including after partial setup failure. Coordinate process-wide signal ownership with other libraries and existing handlers. Linux signalfd and kqueue-specific delivery remain possible future backends.

## Implementation costs

`async-task` supplies pinned task storage, wakers, runnable state and join handles. Socketry adds a separately allocated, reference-counted task record for identity, cancellation, ownership and affinity. The task registry retains a waker while the task is alive.

Spawning and completion take the ownership registry mutex. Cancellation and scheduler closure also use it. Ordinary polling, wakeups and ready-queue operations do not take that mutex. Polling establishes task context with an Arc clone; explicit current-task/current-scheduler lookups clone shared references.

Each cancellable signal allocates a reference-counted node. A signal family shares a mutex for child registration and cancellation propagation; parent nodes hold weak child registrations, and children retain ancestors. Cancellation walks descendants iteratively and releases locks before waking listeners. Waiting uses the existing event-listener dependency. Dropped children remove their registrations, and deep ancestor chains are released iteratively. Ordinary signal checks use an atomic flag; `never()` needs no node or listener.

Queues allocate backing storage as needed. Rescheduling reuses the existing task; it does not allocate another future or a coroutine stack. Idle worker selection can scan worker flags, with a count allowing the scan to be skipped when all workers are busy. No throughput or allocation benchmark is claimed yet.

## I/O and selectors

The public `scheduler` module contains portable `Socket`, `File`, and `Clock` traits, the Socketry implementation in `socketry.rs`, the optional Tokio adapter in `tokio.rs`, and native implementations under `selector/`.

- `native` (default) supplies TCP connect/accept/read/write/readiness through async-io's process-wide reactor: epoll on Linux, kqueue on Apple/BSD, and IOCP/AFD socket readiness on Windows. The three platform modules expose the shared implementation; they do not duplicate its registration machinery.
- `io-uring` selects a dedicated Linux completion selector for socket and file reads/writes. Connection setup, accept, readiness and timers use async-io. On other supported platforms this feature leaves the platform default intact.
- `tokio` adds an adapter using a supplied `tokio::runtime::Handle`. It does not construct or drive a runtime. Enable that runtime's I/O and time facilities.
- With default features disabled, the executor and public contracts still build. Enabling only `tokio` avoids the native I/O dependencies.

`Scheduler` and `SchedulerHandle` implement the traits. `file_open` returns `File::File`; `socket_listen` creates, binds, and listens on a TCP socket, and `listener_addr` reports its bound address. `connect` and `accept` return associated socket resources, with `socket_local_addr` and `socket_peer_addr` for inspection. Importing existing standard-library TCP resources uses the separate optional `SocketRegistration` trait. Returned operation futures are Send. Registrations remain with their original reactor as tasks migrate; they are not re-created per poll. Tokio resources passed to an adapter for another runtime return InvalidInput.

Read/write operations take an owned `IoBuf` (`IoBufMut` for reads) and return `BufferResult<B> = (OperationResult<usize>, B)`, including errors and cancellation. `OperationError` distinguishes `Cancelled` and `Io`. `Vec<u8>` and `Box<[u8]>` are supported. `buffer.slice(range)` creates an owned `BufferSlice` without copying; `into_inner()` recovers its original owner for reuse. Read buffers must have an initialized length (`vec![0; capacity]`); capacity alone supplies no writable bytes. Buffer length is unchanged; only the first returned byte count contains new data. Reads and writes can be partial. Each read/write call is one operation, not a read-exact/write-all convenience method.

`File::file_read_at` and `file_write_at` borrow `&Self::File` and take an explicit offset. `file_open(path, FileOptions, cancellation)` creates the associated resource. Current native/Tokio backends internally retain an `Arc<std::fs::File>`; generic callers do not require that representation. The readiness and Tokio implementations use blocking pools. Use ordinary files opened without append mode, and offsets fitting i64. Unix positioned operations leave the shared cursor unchanged; the Windows blocking fallback updates it according to std's seek\_read/seek\_write semantics.

`Clock::sleep` uses async-io timers or Tokio timers. The independent `socketry-timers` crate implements the io-event timer algorithm; integration with the scheduler clock remains pending. There is no general public blocking-task API yet.

### Cancellation and shutdown

Pre-cancelled file/socket operations do not start. Readiness waits stop on cancellation. io\_uring requests cancellation but waits for the original completion before returning the buffer; successful progress wins a racing cancellation. Blocking jobs check cancellation again on their pool worker, before execution; a running syscall returns its actual result.

Dropping a read/write future abandons its result. A submitted operation may still consume or transmit bytes. Ownership of the buffer and file/socket continues until kernel access ends. Await a result when the byte count matters.

The io\_uring selector probes required opcodes and completion-overflow support. It returns initialization errors rather than silently switching backend. Cancellation requests and original completions have distinct identifiers; only an original terminal completion releases the operation's resources. Socketry shutdown outside a worker joins tasks and drains its ring. Dropping the scheduler on a worker requests cancellation without blocking that worker.

Readiness resources use a process-wide reactor, which is not shut down with an individual Socketry scheduler. An already-started blocking file operation can continue after its waiting task is cancelled. The Tokio adapter's shutdown joins its owned tasks, not the external runtime or its blocking-operation pool.

An unexpected io\_uring selector failure after submission cannot return memory whose kernel lifetime is unknown. That exceptional path retains the affected resources and panics the waiting operation; it does not free in-flight buffers.

### Costs and remaining work

Readiness registrations and buffers are reused. io\_uring currently uses a dedicated selector thread, a command channel, one boxed request for its concrete buffer type, and one completion channel per operation; it does not pool operation records or register buffers. The Tokio file fallback retains an Arc/Mutex buffer owner to return the buffer even when a queued blocking job is cancelled during runtime shutdown. Neither fallback copies the buffer bytes just to transfer ownership.

Native overlapped Windows file operations, arbitrary descriptor/UDP APIs, operation and buffer pools, and scheduler integration with `socketry-timers` remain future work. There is no thread-local non-Send task facility. No I/O throughput claim has been established by benchmarks.

The portable example uses the same generic TCP exchange with either runtime:

```sh
cargo run -p socketry-executor --example portable_io
cargo run -p socketry-executor --example portable_io --no-default-features --features tokio
cargo run -p socketry-executor --example portable_io --features io-uring # Linux
```

The [interface experiment](examples/interface_probe.rs) runs generic file-copy and echo-server functions against native, Tokio, and in-memory backends:

```sh
cargo run -p socketry-executor --example interface_probe --features tokio
cargo test -p socketry-executor --example interface_probe --features tokio
cargo run -p socketry-executor --example interface_probe --no-default-features
```

It uses the exported `File` and `Socket` traits, associated resources, cancellation, and owned buffer views. Its in-memory implementation supplies the same capabilities without importing OS descriptors. See the [design findings](design.md#executable-interface-experiment) for results and remaining contracts.

## Preserved prototype

The native coroutine prototype, verbatim CRuby vendor sources and its tests are preserved in commit `b520f3d` on branch `coroutine`.

Run the example and tests with:

```sh
cargo run --package socketry-executor --example work_stealing
cargo test --workspace --all-targets --locked
cargo test --workspace --doc --locked
```
