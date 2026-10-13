# Implementation Context

This guide describes the current implementation and its boundaries. Read [the design guide](design.md) before changing public APIs, runtime boundaries, package names or workspace layout. Shared Rust development guidance is provided by the `bake-agent-context` dependency.

## Implemented foundation

- `socketry` is the facade for `socketry-executor`. Shared Rust development guidance is provided by the `bake-agent-context` crate.
- `socketry-executor` executes futures using async-task and Crossbeam queues.
- Scheduler construction starts worker threads. Tasks require Send + 'static; the pinned future remains stationary while workers may change between polls.
- Workers own FIFO ready queues and concurrent incoming queues. Wakeups target the last worker; idle workers steal from ready queues and incoming queues.
- A shared injector accepts submissions originating outside a worker.
- Worker parking publishes an idle flag/count before rechecking all work. Preserve this protocol and treat Steal::Retry differently from empty.
- Queue operations use Crossbeam synchronization. Task admission, completion, cancellation and closure use a separate ownership registry mutex.
- Never run user code or invoke wakers while holding the registry mutex.
- Task context is installed around execution and destruction and restored on unwinding. Scheduler context is installed on workers and block\_on roots.
- No coroutine stacks, nested synchronous wait or explicit transfer remain in the current executor. The full prototype is on branch coroutine at b520f3d.

## Ownership

- Scheduler and SchedulerHandle implement Spawn, with a runtime-specific associated handle type. Adapters need to preserve its ownership and result contract.
- Register ownership before publishing a runnable task.
- Dropping TaskHandle abandons the result without cancelling the scheduler-owned task.
- Task cancellation sets a flag and wakes it. The future is destroyed after an in-progress poll returns. TaskHandle::cancel awaits that destruction.
- Awaited task results report cancellation or the original panic payload. Scheduler::run does not aggregate task errors.
- Dispatch takes a future and returns a handle without a group or owner argument. Independently spawned work has no implicit parent-child lifecycle. Higher-level groups belong above Spawn and are outside the executor for now.
- Scheduler Drop cancels all tasks, joining threads outside a worker. On a worker it requests shutdown without joining. Surviving handles reject spawn.

## Cooperative cancellation

- `cancellation.rs`, `cancelled.rs`, and `defer_cancel.rs` implement runtime-independent primitives, exported by both `socketry-executor` and the `socketry` facade.
- Cancellation clones share state. Children receive ancestor cancellation, retain ancestors after intermediate handles are dropped, and never cancel parents or siblings. Dropping signals does not cancel work. `Cancellation::never()` allocates nothing; its children are independent cancellable signals.
- Each family serializes child registration and cancellation with one mutex. Atomic flags support cheap checks. Nodes hold weak child registrations; child destruction unregisters them. Cancellation and exclusively owned ancestor destruction are iterative, avoiding stack exhaustion on deep chains.
- Register event listeners before checking cancellation to avoid lost notifications. Dropping a wait unregisters its listener. Release all signal locks before invoking wakers so wakeups can reenter cancellation and child registration.
- defer\_cancel pins the work and its signal wait. Observed cancellation consumes the synchronous callback once, before polling the work again, and the wrapper preserves the work's normal output. Callback panics propagate; dropping the wrapper destroys its future. It does not mask operation signals or intercept task destruction.
- For graceful shutdown, request cancellation and await task handles before releasing the scheduler or shutting down runtime services. Signals express intent rather than completion. There is no forced abort or escalation in this cooperative contract; a supervisor can enforce SIGTERM followed by SIGKILL.
- Existing Task cancellation and scheduler shutdown retain their destruction semantics. Cooperative runtime shutdown and cancellation inputs on I/O methods are not yet integrated.

## Signal notifications

- `Notify<Value>` is a synchronous destination, independent of waiting. Cancellation ignores the value; standard unbounded mpsc and futures-channel senders queue it and discard values after receiver closure. Arc forwards to a destination. Bounded channels require an explicit application overflow policy.
- `Signals` is a separate scheduler capability with associated signal and registration types. The optional `signals` feature implements it on Unix for native and Tokio schedulers and their handles, using their respective I/O services. Other platforms lack the implementation at compile time.
- Signal registration is explicit and fallible. Check the complete signal set for forbidden and out-of-range values before installing handlers; report further platform-specific registration errors from the backend. No signal handlers are installed merely by creating a scheduler. Closed scheduler handles reject new registrations.
- `SignalRegistration` spawns a task through the chosen scheduler. It uses signal-hook's self-pipe registered with async-io or the supplied Tokio runtime, without a dedicated thread or hidden cross-runtime reactor. Call Notify outside raw signal handlers and all scheduler locks. The task owns its target; callback panics follow normal scheduler handling. Targets should return promptly.
- Dropping the non-clonable, must-use guard requests cancellation and wakes the task's asynchronous wait without joining it. A notification admitted before removal can finish. Other subscriptions remain active. Scheduler/runtime shutdown also ends delivery and releases the subscription. Keep services alive through graceful draining; no implicit shutdown or escalation is installed.
- The task checks cancellation before dispatch and yields after each batch to avoid monopolizing a worker under continuous signal traffic. The reader must clear backend readiness through actual asynchronous reads; observing readiness alone can spin. EOF or a terminal read error ends the subscription task.
- Signals may coalesce in the OS and backend before being queued. Cleanup does not restore original process dispositions, even after a partially successful setup. Coordinate with existing process-wide handlers. signalfd/kqueue optimizations remain future work.
- Actual signal delivery tests run in a deadline-bounded subprocess to avoid changing the parent test process's signal dispositions.

## I/O and runtime boundaries

- `scheduler.rs` re-exports Socket, File, Interest and Clock from `scheduler/socket.rs`, `file.rs`, `interest.rs` and `clock.rs`. Operations return concrete Send futures; portable consumers receive the required capabilities.
- `File` names the scheduler capability, while `std::fs::File` remains the resource type. Use the `StdFile` alias where both names are imported. Blocking positioned file helpers live in `scheduler/positioned_file.rs`.
- `scheduler/socketry.rs` owns the executor; `socketry/operations.rs` forwards capabilities to its lazily initialized, compile-time selected selector.
- `scheduler/selector/` contains readiness, epoll, kqueue, iocp and io\_uring. Platform readiness modules share async-io's persistent registrations and process-wide reactor. Registered sockets remain usable as tasks migrate.
- Default feature `native` provides TCP, positioned files and sleep. Feature `io-uring` selects Linux completion reads/writes; other supported platforms retain readiness. Feature `tokio` enables the separate runtime adapter. No default features builds the executor and contracts without native I/O.
- Read/write buffers are owned Vec values, returned with ordinary errors as well as success. Reads use the initialized length and leave it unchanged. Operations can be partial; cancellation can consume or transmit bytes.
- io\_uring's dedicated selector thread owns buffers and descriptors until the original terminal completion, never merely a cancellation completion. Numeric identifiers are not reused. Pending, unsubmitted requests can return buffers immediately. Unexpected failure retains kernel-accessible resources and panics waiting operations; do not free buffers with unknown completion.
- Ring shutdown closes admission, cancels and drains completions. Scheduler Drop outside a worker waits for this drain; Drop on a worker only requests it. Shutdown racing lazy initialization must still close the new selector.
- Regular files use blocking pools except with io\_uring. An already-started blocking operation can outlive its waiting task. Use non-append regular files and explicit offsets; the Windows fallback also changes the shared cursor.
- `scheduler/tokio.rs` adapts an existing runtime with I/O and time enabled. It preserves scheduler task ownership through Spawn, but does not own or drive that runtime. Its shutdown joins owned task destruction, not the runtime's blocking pool.
- Register Tokio task ownership before spawn, but release the registry mutex before calling into Tokio: a stopped runtime may synchronously drop a future. Future destruction precedes ownership completion. Never hold an EnterGuard across await; enter per poll for operations that register resources.
- Tokio resources carry runtime identity. Explicit handles select that runtime; Socketry's Task::current and Scheduler::current remain Socketry-specific.

## Next boundaries

- `socketry-timers`, also available through `socketry::timers`, implements the independent io-event timer queue with batched insertion, constant-time cancellation and lazy compaction. Its optional `sharded` feature adds worker queues, cross-worker assistance and lazy sleep futures; the experimental inbox driver remains optional. The crate has no executor dependency. Scheduler integration, operation pools, registered buffers, general descriptor/UDP interfaces and overlapped Windows file operations remain planned. Native sleep currently uses async-io; the adapter uses Tokio timers.
- Keep runtime requirements explicit; ordinary Future support does not provide another runtime's I/O or timers.
- There is no non-Send task executor, public blocking-task API or scoped borrowing.
- Do not introduce unsafe Send, stack migration or nested blocking waits.
- Native coroutine sources, sanitizer hooks and historical tests belong to the preserved coroutine branch, not the future executor's build.

## Verification

Public behavior is covered in crates/executor/tests. Deterministic channels force stealing, migration, concurrent wakeups and cancellation during polling. The parking test uses Loom to model the queue-publication/idle-registration handshake. Keep its atomics and fence order aligned with the implementation; this models the handshake, not Crossbeam or async-task internals. When verification is requested, run workspace tests and doctests with `tokio` enabled; on Linux also run all features to exercise io\_uring. Check executor-only and Tokio-only feature combinations. The same TCP/file consumers exercise both implementations; Linux tests cover cancellation batches and shutdown races. CI covers Linux, macOS, Windows and FreeBSD, with separate Linux io\_uring and sanitizer jobs; distinguish configured CI from executed results.

ThreadSanitizer loads `.github/tsan-suppressions.txt` to suppress Crossbeam's internal queue `Buffer::read` and `Buffer::write` race reports. Crossbeam reads slots speculatively and discards values when atomic validation fails. Its non-atomic volatile accesses remain a known Rust memory-model limitation; the suppression accepts that limitation rather than fixing it. See the [upstream discussion](https://github.com/crossbeam-rs/crossbeam/issues/589#issuecomment-720972996). Keep suppression patterns scoped to those buffer accesses and reassess them when updating Crossbeam. Other race reports continue to fail the sanitizer job.

Coverage measures source regions for native and Tokio schedulers on each supported OS and architecture, plus Linux io\_uring and FreeBSD. Executor-only and Tokio-only feature selections receive separate compilation checks.
