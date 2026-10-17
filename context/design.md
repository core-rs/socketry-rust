# Future foundation

This document records the direction for Socketry's Rust foundation. The executor now polls futures directly using async-task and Crossbeam queues, with worker threads, work stealing and transferable task ownership. The coroutine prototype is preserved in commit `b520f3d` on branch `coroutine`. Portable TCP, file and clock capabilities, native selectors and a Tokio adapter are now implemented. The independent io-event timer queue is implemented in `socketry-timers`; scheduler integration and further optimizations remain planned.

## Goals

- Port the useful designs and modular organization of Socketry's Ruby libraries into Rust.
- Use futures for task execution, with no private coroutine stack per task.
- Let applications choose a Socketry runtime, Tokio, or another implementation of the capabilities a library needs.
- Support Linux io\_uring without compromising buffer lifetime or cancellation safety, with an explicit strategy for other platforms.
- Port io-event's timer algorithm and preserve its batching and cancellation behavior.
- Establish conventions and agent context that other Socketry crates can use.

## Shared interfaces and concrete implementations

Rust already standardizes the interface between a future and an executor: `Future::poll`, `Context`, and `Waker`. Awaiting a future does not inherently select a particular scheduler. Spawning tasks, creating timers, and opening sockets require additional services, which are not all standardized by `std::future`.

Make those services explicit. A library should receive the capabilities it uses: task dispatch, a clock, a connector, or an existing stream. Avoid requiring a complete runtime just to parse a protocol or operate on an existing stream.

Zig's `std.Io` is a useful reference for passing an implementation into code. The Rust translation uses future-returning operations and ordinary `.await`. It does not reproduce Zig's synchronous-looking suspension mechanism.

Prefer generic traits and concrete future types initially. Runtime selection can happen when constructing the application. Supporting different runtimes does not require a trait object or heap allocation for every operation. Add dynamic dispatch where a real caller needs runtime selection at that boundary.

### Candidate package boundaries

The workspace currently contains `socketry`, `socketry-executor` and `socketry-timers`. The facade re-exports the component packages as `socketry::executor` and `socketry::timers`. The other names below describe possible extractions from the executor, not packages to publish before they have code. Split modules into packages when consumers or optional dependencies justify it.

All published workspace packages inherit one version from `[workspace.package]` and are released together. Keep internal dependency versions aligned with that workspace version. Component crates provide dependency and implementation isolation within this coordinated release; the private `bake` task package is not published.

| Package | Responsibility |
| --- | --- |
| `socketry` | Public facade, common exports, conventions, and agent context. |
| `socketry-executor` | Future execution, workers, task dispatch, ownership, cancellation, and integration with I/O selectors and timers. |
| `socketry-io` | Portable I/O contracts, resource and buffer ownership, and stream interfaces. |
| `socketry-timers` | Timer ordering, cancellation, batching, lazy sleeps and worker queue assistance, independent of an executor or OS selector. |
| `socketry-tokio` | Tokio implementations of the portable contracts and focused compatibility adapters. |

Keep protocol parsing and serialization independent of the runtime wherever possible. Higher-level protocol packages need not live in this repository.

## Execution and ownership

The executor polls pinned future state on worker threads' ordinary stacks. Use Send futures for tasks eligible to run on different workers. A local task facility can accept non-Send futures with explicit thread restrictions. Pinned future storage stays at a stable address while the worker allowed to poll it may change. Only one worker may poll a task at a time.

Returned task handles own execution lifetimes; the scheduler retains task storage until completion or destruction. Schedulers and their handles expose the primitive `Spawn` contract: dispatch accepts a future and returns a task handle, without a group or owner argument. Independently spawned tasks have no implicit parent-child lifecycle.

- Register the task before making it runnable.
- Await its handle to observe completion and failures.
- Moving the handle transfers ownership. Dropping it requests future destruction after any active poll, without waiting.
- Destroying a task drops handles it still owns. Handles moved elsewhere remain owned there, independently of their spawning task.
- Closing the scheduler prevents new submissions; shutdown joins task destruction.

Higher-level groups can be built above dispatch. The executor does not provide a Barrier or require group membership for spawning.

Use owned, `'static` futures for independently spawned tasks initially. A parent-child relationship alone does not make borrowing a parent's local variables safe. Scoped borrowing requires a separate lifetime design that remains sound when futures or handles are dropped or forgotten.

### Blocking execution

`Blocking` is an optional scheduler capability for synchronous closures, delegating queue and worker management to an internal dedicated pool. Generic libraries still receive a scheduler. `spawn_blocking(operation, cancellation).await` waits for bounded admission and returns an owning handle; awaiting that handle observes completion. Both closure and result are `Send + 'static`.

The pool bounds queued plus running jobs. Cancellation or dropping the handle can skip queued work, but started closures retain their inputs and capacity until they return. Scheduler shutdown closes admission and discards queued jobs without joining running closures. Graceful callers request cooperation and explicitly await jobs before dropping services. There is no forced interruption. Native and Tokio implementations share the pool implementation, with a separately configured pool per scheduler; file-I/O fallbacks stay separate initially.

### Cleanup in a future executor

Ordinary Drop cannot await asynchronous cleanup. Task handles provide explicit completion tracking while the scheduler remains alive and continues polling. Any higher-level group must define how its caller confirms draining; dropping a group alone cannot promise completion on other workers.

Distinguish cooperative cancellation, dropping a future, and waiting for termination. Dropping a future runs synchronous destructors but does not run the remainder of its async body. Graceful asynchronous shutdown requires a live future that the runtime continues to poll. Runtime shutdown must define how cancellation and pending I/O completions are drained.

Contextual APIs such as `Scheduler::current()` can remain conveniences. Set and restore their context around every poll, including when polling panics; do not assume a future always runs on the thread where it was created.

### Cooperative cancellation

The shutdown direction is cooperative: request graceful termination and let the application continue polling until its work completes. Cooperative requests have no automatic abort, second-request escalation, or grace-period deadline. Explicitly dropping an owning task handle abandons its future and bypasses asynchronous cleanup. If a process refuses to stop, its supervisor can follow SIGTERM with SIGKILL.

`Cancellation` is implemented in `socketry-executor` and re-exported by `socketry`. It works independently of the executor. Clones share a persistent signal; child signals receive ancestor cancellation without propagating it to parents or siblings. This explicit cancellation tree expresses service boundaries without imposing an implicit task hierarchy. Dropping a signal does not cancel it. Rust destructors release retained resources, but asynchronous draining still requires live futures and explicit completion tracking through task handles.

`cancel()` signals intent; `check()` returns `Cancelled` at cooperative cancellation points; `cancelled().await` suspends using wakers. `never()` represents work without cooperative cancellation. File and socket operations now accept explicit signals and return cancellation separately from OS errors. `Clock` now supplies monotonic `now`, absolute `sleep_until`, and lazy relative `sleep`, with explicit cancellation and `OperationResult<()>`.

`defer_cancel(&signal, future, on_cancel)` observes the signal, invokes a synchronous shutdown callback once, and keeps awaiting the future's normal output. The callback tells the protected work to stop accepting new work or begin draining. Async cleanup belongs in that future, with cancellation inputs that allow it to finish. The wrapper uses standard future polling, handles pinned futures, and neither busy-waits nor prevents its own destruction.

The intended application sequence is to request application or service cancellation, await task completion while runtime services remain available, then release the scheduler. Applications explicitly await the tasks they want to finish; there is no implicit wait for every child. Backends still retain resources for outstanding operations and must honour accepted submissions. Existing Task cancellation and scheduler shutdown still destroy futures and bypass deferred async cleanup; they are distinct from cooperative cancellation.

`Signals::signal_notify` explicitly connects signals to an ordinary `Notify<Value>` target. Cancellation requests shutdown; an unbounded channel queues other events such as reload requests. Waiting remains specific to the destination. An associated, must-use registration guard owns the subscription; the scheduler/runtime must remain alive and driven for delivery. Dropping it stops admission without awaiting already-dispatching callbacks. The optional Unix `signals` backend spawns one task per subscription, asynchronously reading signal-hook's self-pipe through the chosen runtime's I/O services. The guard retains the task handle; removal drops it after stopping dispatch admission, and scheduler shutdown also ends delivery. It creates no dedicated signal thread. It never calls user notification code in a raw signal handler. OS/backend coalescing and persistent process-wide signal dispositions remain part of the contract; no forced termination or escalation policy is installed.

### Current executor

The implementation currently lives in socketry-executor. async-task owns pinned future storage and its runnable/waker state. Socketry supplies thread management, ownership, cancellation flags, task identity and queue selection. Each worker has a FIFO Crossbeam queue plus an incoming injector for remote wakeups. External submissions enter a global injector; worker submissions go to that worker's queue. Wakeups target the last worker. Only workers without their own work steal from other local queues or incoming queues.

Periodic external checks prevent a busy local queue from starving submissions. Idle flags and a post-registration queue search coordinate parking and wakeups. Task registration and completion use an ownership mutex; ordinary polling and wakeups use the task state and queues without that mutex.

Scheduler and SchedulerHandle implement Spawn. Its associated handle type permits runtime-specific join implementations. Public TaskHandle drop abandons execution; moving the handle transfers its lifetime to the recipient. Awaiting it reports output, cancellation or a panic payload. There is no implicit task hierarchy; group coordination belongs above dispatch.

Native I/O and sleep capabilities now accompany the executor. Regular-file fallbacks use blocking pools; there is no public blocking-task API or local non-Send task executor. Coroutine sources and historical tests are preserved on their branch and are no longer built by this workspace.

## Compatibility with Tokio and other runtimes

Separate three levels of compatibility:

1. A future that needs only standard polling and wakeups can run on a suitable executor, subject to its Send and lifetime requirements.
2. A library written against portable spawn, clock, and I/O contracts can use different implementations of those contracts.
3. A library that directly calls Tokio APIs needs the relevant Tokio runtime services. Implementing an I/O trait does not replace those services.

Tokio and futures-io expose different AsyncRead and AsyncWrite traits. `tokio-util::compat` already adapts these traits in both directions. Reuse those adapters where they fit. An adapted Tokio socket still belongs to its Tokio I/O driver, which must remain alive and be driven.

Entering a Tokio runtime context provides access to its services; entering alone does not drive the runtime. Some mixed execution is possible when the required services are running, but must be established for the concrete APIs being used. Do not advertise universal Tokio compatibility from a Waker or stream adapter alone.

The optional `scheduler::tokio` adapter implements Socket, File, Clock and Spawn against an existing runtime. It preserves scheduler task ownership and joins owned task destruction on asynchronous shutdown. The same generic TCP program runs on Socketry and Tokio. The adapter scopes runtime context to individual polls when registering resources; its futures can also be polled by Socketry workers while Tokio drives the underlying services. Socketry contextual lookups still identify Socketry execution; portable code passes handles explicitly.

For existing libraries tied to Tokio, either keep their work on Tokio and bridge owned messages/results, or provide the particular trait adapter they consume. Avoid a broad imitation of Tokio's API.

## I/O and io\_uring

Account for completion-based I/O before fixing the portable API. An operation submitted to io\_uring may still access its buffer after the Rust future waiting for it is dropped. A borrowed byte slice and a synchronous destructor are not sufficient to establish that the kernel has stopped using memory.

Use an operation model that transfers ownership of a stable buffer to the selector and returns it with the result on completion. The selector retains the buffer and required descriptor ownership until it knows all kernel accesses are finished, even when the caller abandons its future. Requesting cancellation does not by itself prove that the original operation has finished.

Plan for:

- Reusable buffers, resource registrations, and operation records.
- Batched submission and completion processing.
- Backpressure when submission capacity or buffer pools are exhausted.
- Distinguishing cancellation requests from original operation completions.
- Preventing late completions from referencing a recycled operation record.
- Runtime capability probing and explicit fallback or unsupported errors.
- Non-Linux implementations using suitable readiness or OS completion APIs.

A borrowed-buffer AsyncRead/AsyncWrite facade can sit above an owned-buffer selector using internal buffers when necessary. Document the copying and buffering costs of adapters. Do not require the lowest-level io\_uring API to pretend every operation has borrowed-buffer semantics.

Keep ring ownership explicit. The initial implementation uses a dedicated selector thread. Work stealing routes operations to that selector without moving their registrations. A pinned address does not establish Send.

Tokio-uring is useful reference code for owned-buffer operations. It has its own driver/runtime requirements; using it does not make ordinary Tokio I/O and io\_uring interchangeable.

### Current selectors

The implementations live under `scheduler/selector/`, selected with Cargo features and target configuration. epoll, kqueue and iocp expose shared async-io readiness operations; the process-wide reactor owns persistent registrations. Windows uses IOCP/AFD socket readiness. Regular-file fallback operations use a blocking pool, because readiness does not make ordinary file I/O nonblocking.

The Linux `io-uring` feature selects a dedicated ring for socket/file reads and writes and `file_sync`. `file_truncate` uses the ring when its opcode is available, with a blocking-file fallback on older kernels. Optional `FileAllocate` adds Linux range reservation with `AllocationMode::Extend` or `KeepSize`, using a probed `Fallocate` request or the blocking syscall fallback. Other platforms currently omit that capability. Synchronization, allocation, and native truncation retain the file without a payload buffer and use the same original-completion retirement contract as transfers. Connect, accept, explicit readiness waits and sleep still use async-io. Initialization probes required opcodes and reliable completion overflow; it returns an error if unavailable rather than silently changing implementation. Shutdown cancels and drains original completions. Operation identifiers are never reused, and cancellation completions cannot release original buffers. Unexpected selector failure retains resources whose kernel lifetime cannot be established and panics their waiting operations. Normal cancellation does not retain completed resources.

The owned-buffer API supports `IoBuf`, `IoBufMut`, and `BufferSlice`, returning `(OperationResult<usize>, B)` with ownership on every normal result. Read buffers must already have an initialized length. Operations may be partial; dropping a waiting future can abandon I/O that consumes or transmits bytes. Socket and listener types are associated with the concrete implementation. Tokio registrations reject use through another runtime's adapter.

This is an initial implementation: each ring operation uses a boxed typed request, a command, and a completion channel, and operation records and buffers are not pooled. General descriptors, UDP and native overlapped Windows files remain future work. Blocking file operations can outlive task cancellation; neither Socketry nor the Tokio adapter shuts down the process-wide or external blocking pool.

## Timer port

The source reviewed is socketry/io-event commit `66caf64fbe27d752e4c64a60a7d0fba8bceaddbc`:

- `lib/io/event/timers.rb`
- `lib/io/event/priority_heap.rb`
- `test/io/event/timers.rb`
- `test/io/event/priority_heap.rb`

This is a binary min-heap with deferred insertion and lazy cancellation. Preserve these choices in the Rust port:

- Scheduling appends to a pending batch; heap insertion is deferred.
- Cancellation clears the payload and updates bookkeeping in constant time.
- Cancelled pending timers are filtered before insertion.
- Cancelled timers at the heap root are removed when querying or firing.
- Compact the heap when at least 128 retained entries are cancelled and cancelled entries are more than half the heap.
- Clear an entirely cancelled heap even below that threshold when there is no live pending batch.
- Combine compaction and pending insertion into one heap rebuild.
- Build from empty or rebuild when the incoming batch exceeds twice the existing heap size; otherwise insert incrementally.

Keep the timer queue independent of a scheduler, OS sleep, or Tokio. Let the selector provide monotonic time and consume expired entries. An executor-facing sleep future registers a waker with the selector, which uses the queue to choose its next wake deadline. Wake callbacks outside queue locks.

Keep the reusable queue, lazy `Sleep` future and worker assistance primitives together in `socketry-timers`. The default queue has no runtime dependencies. Its optional `sharded` feature provides independent worker queues, published deadlines and nonblocking assistance using only the standard library. The optional `driver` feature retains the experimental producer-inbox alternative and adds Crossbeam Queue. Tokio and Loom are development dependencies for comparisons and verification.

The executor owns the `Clock` adapter, worker-to-queue assignment, assistance policy, idle-worker selection and selector wake integration. The timer crate accepts deadlines, wakers and explicit expiry snapshots; it does not choose workers, spawn a thread or depend on `socketry-executor`. A migrated task retains its original timer queue identity. This dependency boundary allows the queue and future to be tested and benchmarked independently.

Use Instant/Duration or an explicit monotonic tick type rather than floating point deadlines. Accept the current time explicitly in queue operations so behavior can be checked without wall-clock sleeps. Record where Rust behavior differs: expired deadlines can use a zero wait duration, and public live counts should be distinguished from retained cancelled entries.

Reusable registration handles must not allow an old cancellation to affect a new timer occupying the same storage. Document reset behavior and whether it reuses an allocation. Preserve MIT attribution for Samuel Williams and Wander Hillen, the source paths, and the revision used for the translated code.

The algorithm's existing Ruby performance motivates the port. Rust performance claims require measurements of scheduling, cancellation, expiration, batching, and retained memory under representative workloads.

## Implementation sequence

1. Record boundaries and reusable Rust conventions (implemented).
2. Replace stackful execution with async-task and Crossbeam worker queues (implemented). Keep the coroutine prototype in its saved branch.
3. Implement primitive task dispatch, scheduler ownership, cancellation and shutdown (implemented). Runtime-independent cooperative signals and deferred cancellation are implemented; cancellation-aware file/socket operations and transferable task ownership are implemented. Clock cancellation is implemented; submission-only cleanup remains future work. Higher-level task groups are outside the executor for now.
4. Establish minimal clock and I/O contracts with concrete consumers (implemented with Socket, File, Clock and a portable TCP example).
5. Implement native readiness and the Tokio adapter, running the same consumers with both (implemented). Native sleep uses async-io until scheduler integration with `socketry-timers`.
6. Implement io\_uring's owned-buffer lifecycle, socket/file operations, cancellation and runtime probing (implemented). Improve operation reuse, buffer registration and submission backpressure in subsequent work.
7. Port the timer queue with upstream attribution and deterministic verification (implemented in `socketry-timers`). Integrate a driven sleep future with the scheduler in subsequent work; see the [timer package](../crates/timers/readme.md) and [benchmarks](../crates/timers/context/benchmarks.md).
8. Measure the executor and complete system. Multiple workers and work stealing are implemented, but performance claims require benchmarks.

When implementation verification is requested, cover timer threshold boundaries, duplicate and late wakeups, cancellation races, parent cleanup, I/O dropped in flight, and backend compatibility. Linux execution is required to establish io\_uring behavior; a build on another platform does not do so.

## References

- [async-task](https://docs.rs/async-task/latest/async_task/)
- [Crossbeam deque](https://docs.rs/crossbeam-deque/latest/crossbeam_deque/)
- [Rust Future contract](https://doc.rust-lang.org/std/future/trait.Future.html)
- [Rust Drop](https://doc.rust-lang.org/std/ops/trait.Drop.html)
- [Zig 0.16 I/O interface](https://ziglang.org/download/0.16.0/release-notes.html#I-O-as-an-Interface)
- [Tokio runtime handle](https://docs.rs/tokio/latest/tokio/runtime/struct.Handle.html)
- [Tokio I/O compatibility adapters](https://docs.rs/tokio-util/latest/tokio_util/compat/index.html)
- [Tokio-uring ownership model](https://docs.rs/tokio-uring/latest/tokio_uring/)
- [io-event timers at the reviewed revision](https://github.com/socketry/io-event/blob/66caf64fbe27d752e4c64a60a7d0fba8bceaddbc/lib/io/event/timers.rb)
- [io-event priority heap at the reviewed revision](https://github.com/socketry/io-event/blob/66caf64fbe27d752e4c64a60a7d0fba8bceaddbc/lib/io/event/priority_heap.rb)
