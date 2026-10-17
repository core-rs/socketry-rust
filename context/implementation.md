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
- TaskHandle owns execution lifetime. Moving it transfers ownership; Drop requests destruction after any active poll, without waiting or running asynchronous cleanup.
- Task cancellation sets a flag and wakes it. The future is destroyed after an in-progress poll returns. TaskHandle::cancel awaits that destruction.
- Awaited task results report cancellation or the original panic payload. Scheduler::run does not aggregate task errors.
- Dispatch takes a future and returns a handle without a group or owner argument. There is no implicit task tree: destruction drops handles still owned by a future; transferred handles remain owned elsewhere. Higher-level groups belong above Spawn and are outside the executor for now.
- Scheduler Drop cancels all tasks, joining threads outside future and blocking-pool workers. On either kind of worker it requests shutdown without joining or draining; explicit synchronous shutdown panics in those contexts. Surviving handles reject spawn.

## Blocking work

- `Blocking::spawn_blocking` returns a lazy, cancellable admission future and then an owning `BlockingTask`. Generic consumers require the capability; native and Tokio schedulers and handles implement it.
- `BlockingOptions` configures a dedicated internal pool per scheduler: four threads and 64 admitted jobs by default. Count queued and running jobs, retaining capacity until running closures finish even if their handles are dropped. Threads start on first admission.
- The pool uses a mutex-protected queue, a condition variable for workers, and event listeners for asynchronous admission. Register listeners before inspecting capacity; never invoke wakers or destroy user captures/results under the queue lock.
- Check cancellation before admission and again with abandonment before calling the closure. Queued cancellation need not complete until dequeued. Once started, preserve the real output; closures can cooperate explicitly using a captured signal. Catch job panics and release capacity even when abandoned result destructors panic.
- Close admission and discard queued jobs at scheduler shutdown; started jobs retain their data and finish independently. Do not join pool threads or implicitly drain jobs in `Scheduler::run`. Applications await them before shutting down services. Partial thread startup failure closes the pool and releases already-started workers.
- Existing file fallbacks stay on their backend pools. File close uses a separate cleanup pool built from the same internal implementation: accepted jobs survive abandonment/closure and graceful shutdown drains them. Each pool uses the configured `BlockingOptions` limits. Public pool sharing and idle thread retirement remain separate work.

## Cooperative cancellation

- `cancellation.rs`, `cancelled.rs`, and `defer_cancel.rs` implement runtime-independent primitives, exported by both `socketry-executor` and the `socketry` facade.
- Cancellation clones share state. Children receive ancestor cancellation, retain ancestors after intermediate handles are dropped, and never cancel parents or siblings. Dropping signals does not cancel work. `Cancellation::never()` allocates nothing; its children are independent cancellable signals.
- Each family serializes child registration and cancellation with one mutex. Atomic flags support cheap checks. Nodes hold weak child registrations; child destruction unregisters them. Cancellation and exclusively owned ancestor destruction are iterative, avoiding stack exhaustion on deep chains.
- Register event listeners before checking cancellation to avoid lost notifications. Dropping a wait unregisters its listener. Release all signal locks before invoking wakers so wakeups can reenter cancellation and child registration.
- defer\_cancel pins the work and its signal wait. Observed cancellation consumes the synchronous callback once, before polling the work again, and the wrapper preserves the work's normal output. Callback panics propagate; dropping the wrapper destroys its future. It does not mask operation signals or intercept task destruction.
- For graceful shutdown, request cancellation and await task handles before releasing the scheduler or shutting down runtime services. Signals express intent rather than completion. There is no forced abort or escalation in this cooperative contract; a supervisor can enforce SIGTERM followed by SIGKILL.
- Existing Task cancellation and scheduler shutdown retain their destruction semantics. File/socket methods accept cancellation. Applications explicitly await completion before destroying services; a general implicit child drain is not required. Accepted backend work retains its resources independently of abandoned callers.

## Signal notifications

- `Notify<Value>` is a synchronous destination, independent of waiting. Cancellation ignores the value; standard unbounded mpsc and futures-channel senders queue it and discard values after receiver closure. Arc forwards to a destination. Bounded channels require an explicit application overflow policy.
- `Signals` is a separate scheduler capability with associated signal and registration types. The optional `signals` feature implements it on Unix for native and Tokio schedulers and their handles, using their respective I/O services. Other platforms lack the implementation at compile time.
- Signal registration is explicit and fallible. Check the complete signal set for forbidden and out-of-range values before installing handlers; report further platform-specific registration errors from the backend. No signal handlers are installed merely by creating a scheduler. Closed scheduler handles reject new registrations.
- `SignalRegistration` retains an owning task handle from the chosen scheduler. It uses signal-hook's self-pipe registered with async-io or the supplied Tokio runtime, without a dedicated thread or hidden cross-runtime reactor. Call Notify outside raw signal handlers and all scheduler locks. The task owns its target; callback panics follow normal scheduler handling. Targets should return promptly.
- Dropping the non-clonable, must-use guard stops dispatch admission and abandons the owned task without joining it. A notification admitted before removal can finish. Other subscriptions remain active. Scheduler/runtime shutdown also ends delivery and releases the subscription. Keep services alive through graceful draining; no implicit shutdown or escalation is installed.
- The task checks cancellation before dispatch and yields after each batch to avoid monopolizing a worker under continuous signal traffic. The reader must clear backend readiness through actual asynchronous reads; observing readiness alone can spin. EOF or a terminal read error ends the subscription task.
- Signals may coalesce in the OS and backend before being queued. Cleanup does not restore original process dispositions, even after a partially successful setup. Coordinate with existing process-wide handlers. signalfd/kqueue optimizations remain future work.
- Actual signal delivery tests run in a deadline-bounded subprocess to avoid changing the parent test process's signal dispositions.

## I/O and runtime boundaries

- `scheduler.rs` re-exports Socket, File, Interest and Clock from `scheduler/socket.rs`, `file.rs`, `interest.rs` and `clock.rs`. Operations return concrete Send futures; portable consumers receive the required capabilities.
- `File` declares an associated resource and opens it with `FileOptions`; native/Tokio choose `Arc<std::fs::File>` internally. `Socket` creates TCP listeners and exposes address inspection; standard-library resource import belongs to `SocketRegistration`. Blocking truncation, synchronization, shared-cursor, and positioned file helpers live in `scheduler/file_io.rs`. `file_seek` accepts `std::io::SeekFrom` and returns the resulting offset through a cancellable blocking fallback on all current native backends and the Tokio runtime pool; await it and other cursor operations in order. `file_read`/`file_write` advance the OS cursor with no extra per-file state or locking; io\_uring represents current-position requests with `None` internally and encodes them as offset `-1`. Await cursor operations sequentially when ordering matters, and retain buffers through terminal completion as for positioned I/O.
- `FileAllocate: File` optionally reserves space with `AllocationMode::Extend`/`KeepSize`. Native readiness and Tokio implement it on Linux using `fallocate64` on their file pools; io\_uring probes optional `Fallocate` and falls back when absent. Other targets currently omit the implementation. Positive length and a range end within `i64` are required; data and cursor are preserved, and filesystem support errors remain runtime results. No resize/zero-write emulation, unsharing, or durability promise. The simulator reserves vector capacity within its 1 MiB bound.
- `File::file_truncate` changes the logical length with zero-filled extension, requiring write access and preserving the cursor. Lengths must fit in `i64`. Readiness and Tokio use `set_len` on their file pools; io\_uring probes optional `Ftruncate` support and falls back to the blocking implementation when absent. Buffer-free ring requests share resource retention and cancellation with sync. Await related operations sequentially; truncation provides neither space reservation nor durability. The simulator uses its 1 MiB bound.
- `File::file_sync` takes `SyncMode::All` or `SyncMode::Data` and waits for OS completion. io\_uring submits buffer-free `Fsync` requests through the same cancellation/completion machinery as transfers and requires opcode support at initialization. Readiness and Tokio use their existing file pools with `sync_all`/`sync_data`; the simulator completes immediately without modeling durability. Await preceding writes first; sync leaves the cursor unchanged and does not flush application buffers or synchronize directory entries. File ownership is retained through terminal completion even after waiter abandonment.
- `scheduler/socketry.rs` owns the executor; `socketry/operations.rs` forwards capabilities to its lazily initialized, compile-time selected selector.
- `scheduler/selector/` contains readiness, epoll, kqueue, iocp and io\_uring. Platform readiness modules share async-io's persistent registrations and process-wide reactor. Registered sockets remain usable as tasks migrate.
- Default feature `native` provides TCP, stateful and positioned files, and sleep. Feature `io-uring` selects Linux completion reads/writes; other supported platforms retain readiness. Feature `tokio` enables the separate runtime adapter. No default features builds the executor and contracts without native I/O.
- Read/write buffers implement unsafe `IoBuf`/`IoBufMut` contracts guaranteeing stable initialized storage. Vec, boxed slices, and owned `BufferSlice` ranges are supported and returned with errors or cancellation. Reads use the initialized length and leave it unchanged. Operations can be partial; cancellation can consume or transmit bytes.
- io\_uring's dedicated selector thread owns buffers and descriptors until the original terminal completion, never merely a cancellation completion. Numeric identifiers are not reused. Pending, unsubmitted requests can return buffers immediately. Unexpected failure retains kernel-accessible resources and panics waiting operations; do not free buffers with unknown completion.
- Ring shutdown closes admission, cancels and drains completions. Scheduler Drop outside a worker waits for this drain; Drop on a worker only requests it. Shutdown racing lazy initialization must still close the new selector.
- Regular files use blocking pools except with io\_uring. An already-started blocking operation can outlive its waiting task. Use non-append regular files and explicit offsets; the Windows fallback also changes the shared cursor.
- `scheduler/tokio.rs` adapts an existing runtime with I/O and time enabled. Its Spawn handles own execution lifetimes; the adapter does not own or drive that runtime. Its shutdown joins owned task destruction, not the runtime's blocking pool.
- Register Tokio task ownership before spawn, but release the registry mutex before calling into Tokio: a stopped runtime may synchronously drop a future. Future destruction precedes ownership completion. Never hold an EnterGuard across await; enter per poll for operations that register resources.
- Tokio resources carry runtime identity. Explicit handles select that runtime; Socketry's Task::current and Scheduler::current remain Socketry-specific.

## Next boundaries

- `socketry-timers`, also available through `socketry::timers`, implements the independent io-event timer queue with batched insertion, constant-time cancellation and lazy compaction. Its optional `sharded` feature adds worker queues, cross-worker assistance and lazy sleep futures; the experimental inbox driver remains optional. The crate has no executor dependency. Scheduler integration, operation pools, registered buffers, general descriptor/UDP interfaces and overlapped Windows file operations remain planned. `Clock::now` and cancellable absolute/relative waits use async-io on native and the selected runtime on Tokio. Relative sleeps start on first poll, and overflow maps to `InvalidInput`. Paused Tokio clocks are preserved; native timer-engine replacement remains separate work.
- Keep runtime requirements explicit; ordinary Future support does not provide another runtime's I/O or timers.
- There is no non-Send task executor or scoped borrowing.
- Do not introduce unsafe Send, stack migration or nested blocking waits.
- Native coroutine sources, sanitizer hooks and historical tests belong to the preserved coroutine branch, not the future executor's build.

## Interface experiment

`crates/executor/examples/interface_probe.rs` runs generic file-copy and echo-server functions against native, Tokio, and an in-memory implementation. It uses the exported `File` and `Socket` traits for associated resources, creation, cancellation and address inspection. It uses scheduler implementations directly and retries partial writes through owned buffer views. The simulator exercises short transfers and ordinary resource destruction without native I/O dependencies. It supports cancellation but does not model timers, kernel completion tracking, or a simulated task scheduler; findings and next changes are recorded in `crates/executor/design.md`.

## Verification

Public behavior is covered in crates/executor/tests. Deterministic channels force stealing, migration, concurrent wakeups and cancellation during polling. The parking test uses Loom to model the queue-publication/idle-registration handshake. Keep its atomics and fence order aligned with the implementation; this models the handshake, not Crossbeam or async-task internals. When verification is requested, run workspace tests and doctests with `tokio` enabled; on Linux also run all features to exercise io\_uring. Check executor-only and Tokio-only feature combinations. The same TCP/file consumers exercise both implementations; Linux tests cover cancellation batches and shutdown races. CI covers Linux, macOS, Windows and FreeBSD, with separate Linux io\_uring and sanitizer jobs; distinguish configured CI from executed results.

ThreadSanitizer loads `.github/tsan-suppressions.txt` to suppress Crossbeam's internal queue `Buffer::read` and `Buffer::write` race reports. Crossbeam reads slots speculatively and discards values when atomic validation fails. Its non-atomic volatile accesses remain a known Rust memory-model limitation; the suppression accepts that limitation rather than fixing it. See the [upstream discussion](https://github.com/crossbeam-rs/crossbeam/issues/589#issuecomment-720972996). Keep suppression patterns scoped to those buffer accesses and reassess them when updating Crossbeam. Other race reports continue to fail the sanitizer job.

Coverage measures source regions for native and Tokio schedulers on each supported OS and architecture, plus Linux io\_uring and FreeBSD. Executor-only and Tokio-only feature selections receive separate compilation checks.

## Explicit file closure

- Native and Tokio `File::File` is an owned, non-`Clone` `FileHandle`. Backend retention uses a private `Arc<std::fs::File>`; consuming it for close uses `Arc::try_unwrap` and panics on outstanding use. Do not cancel or wait for other operations during close.
- `file_close` awaits bounded admission and execution; `file_close_submit` awaits only admission. Neither accepts cancellation. Accepted close jobs survive waiter destruction and run on a dedicated cleanup pool independent of user blocking closures. Native shutdown outside future and blocking-pool workers and awaited Tokio shutdown drain them. Completion and capacity wakeups run in blocking-pool worker context, so reentrant scheduler destruction requests shutdown without waiting on its own cleanup queue.
- All operation result notifications must follow release of backend file references, including cancellation and requests discarded before submission. io_uring still retains published requests until original terminal CQEs; cancellation CQEs cannot release them.
- `examples/file_close.rs` explains cancellation clones, keeping the operation future alive through cancellation, completion races, and explicit close before propagating an I/O result. Dropping an operation future abandons completion and may leave backend work running; io_uring requests cancellation, but immediate close can still panic until backend access ends.
- Close consumes the OS handle once and reports syscall errors without retrying. `_submit` execution errors are currently discarded. Ordinary resource destruction remains best effort; native close SQEs/CQE suppression and richer debug tracking are deferred.
