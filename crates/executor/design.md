# Executor Design

This document records the agreed direction for `socketry-executor`: an explicit scheduler and I/O interface inspired by [Zig 0.16's I/O model](https://ziglang.org/download/0.16.0/release-notes.html#I-O-as-an-Interface), expressed through ordinary Rust futures, capability traits, and cooperative cancellation. It includes proposed interfaces; the implementation status below distinguishes them from APIs that exist today.

## Primary goal

Expose an abstract scheduler and I/O interface with one or more concrete implementations. Generic concurrency primitives and libraries receive an explicit scheduler implementation and build on its primitive operations. Their code should work with native, runtime-adapter, or simulated implementations without depending on concrete runtime types or operating-system resource creation.

Capability traits describe parts of this common interface on the same scheduler type. They let a consumer state the operations it needs while retaining compile-time checking and static dispatch. They must share compatible resource, cancellation, completion, and ownership contracts so higher-level code can compose them through one scheduler dependency.

Task groups, barriers, channels, and protocol helpers belong above the primitive boundary. A group can use dispatch, cooperative cancellation, and task handles; a channel can suspend and wake futures through Rust's standard future/waker contract. Primitives that need deadlines or I/O additionally require the corresponding scheduler capabilities. Higher-level code should not select a runtime or create OS resources directly.

## Execution model

Use Rust's existing `Future`, `Context`, and `Waker` contracts. A separate computation trait is unnecessary for the current design. Operations construct futures; awaiting them allows the executor to poll them and suspend until a wakeup permits progress. Polling does not mean busy waiting. Selected request operations distinguish waiting for admission from waiting for execution to complete, as described below; constructing their futures does not submit work.

The scheduler executes pinned futures on ordinary worker stacks. Independently spawned tasks are `Send + 'static` and can migrate between workers, while their pinned storage remains stationary. Only one worker polls a task at a time. Blocking system calls must run outside the workers that poll futures.

Pass the implementation into libraries explicitly. A library should require the capabilities it actually uses rather than implicitly selecting a global runtime. Task dispatch takes a future and returns a task handle; it does not require a group or owner argument. Existing resources can be passed directly to code that does not need to create them.

## Capability traits and static dispatch

`File`, `Socket`, and `Clock` name scheduler capabilities. `Spawn` provides primitive task dispatch; the scheduler retains spawned tasks independently of their result handles. `File` is the capability trait; `std::fs::File` is currently the resource type, conventionally imported as `StdFile` when both names are needed. `Socket` replaces the earlier capability name `Network`.

Prefer generic trait bounds, concrete resource types, and concrete future types. These allow compile-time capability checking, monomorphization, and inlining without requiring a boxed future for each operation. The compiler decides which calls to inline; the interface should make that optimization possible.

Keep core traits small and dependable. Implementing a core trait requires working implementations of its operations for the documented resource types and inputs. Avoid default methods that simply return `Unsupported`.

Additional capabilities belong in extension traits where their semantics or platform availability differ. A consumer requiring an extension declares that requirement through its trait bounds. An incompatible scheduler then fails at compile time. For example, `S: File + FileAllocate` illustrates how a consumer might require allocation; `FileAllocate` is a proposed name, not an existing trait or a settled trait boundary.

Do not create one trait per syscall automatically. Group operations around capabilities that real consumers need, and keep inherently platform-specific types and flags in platform extensions.

### Common scheduler dependency and resource creation

Generic code takes a scheduler such as `&S`, with bounds describing the portion of the interface it uses. A task group may require `S: Spawn`; a timed primitive may additionally require `Clock`; an HTTP server may require `Spawn + Socket`. These bounds describe the same chosen scheduler implementation. A named aggregate trait can make a standard bundle convenient once its required capabilities are settled; it should not force a task-only primitive to require file or socket support. The current public `Scheduler` is the concrete native implementation, not an aggregate scheduler trait.

Resource creation belongs inside the corresponding capability. `File` declares an associated file type, and file creation returns that type. `Socket` declares associated socket and listener types, with scheduler-level creation, binding, and listening hooks. Generic consumers use those associated types rather than creating a `std::fs::File` or `std::net::TcpListener` themselves. Exact socket-creation signatures remain to be specified.

The interface does not require `Arc` around every resource. Concrete implementations choose their resource representation and how to retain it during in-flight work. They may use OS handles, registered-resource tables, shared ownership, or simulated resource identifiers. Taking a resource by reference does not remove the backend's obligation to retain everything still accessible by a submitted operation after its waiting future is dropped.

These are agreed interface requirements, not implemented changes. The current `File` capability still accepts `Arc<std::fs::File>`, and socket registration still accepts `std::net` resources. Resource creation and representation must enter the abstraction before generic consumers can run entirely against a simulated scheduler.

## Platform support and fallbacks

Backend selection belongs inside scheduler construction or resource initialization, rather than in application error handling around every operation.

| Situation | Intended contract |
| --- | --- |
| Native asynchronous operation exists | Use the native implementation. |
| Equivalent blocking operation exists | Run it on a blocking pool and preserve the same future and result semantics. |
| Entire capability has no valid implementation | Do not implement the corresponding trait for that scheduler/platform. |
| Capability depends on the running kernel or a registration facility | Probe when initializing the backend or creating the capability/resource. |
| A particular filesystem, resource, or flag combination rejects an operation | Return an ordinary operation error, preserving the OS error where available. |
| Interface is inherently platform-specific | Expose it through an extension trait or platform module. |

A trait bound guarantees an implementation exists; it cannot guarantee that every filesystem supports every allocation mode or that every operation succeeds. Runtime failures remain part of the result contract. `io::ErrorKind::Unsupported` can represent a genuine resource or feature limitation, but should not be the routine result of calling a core operation on a supported platform.

Use a blocking pool when native asynchronous support is unavailable. This is acceptable on Windows and other platforms. A fallback may change performance, but must preserve the documented semantics. Allocation cannot silently become truncation, message I/O cannot discard ancillary data, and a positioned operation cannot alter the shared cursor if its contract promises to preserve it.

Bound blocking concurrency and provide asynchronous admission/backpressure. For ordinary cancellation-aware operations, cancellation can prevent queued work from starting. The non-cancellable submission/completion pairs below retain responsibility once admitted. Once a blocking syscall has started, the operation may have to finish; retain its resources and buffer throughout. A cancellation result must not imply that completed side effects were undone.

An application explicitly selecting a specialized backend may receive an initialization failure instead of automatic fallback. The policy must be clear at that boundary. The current Linux io\_uring backend follows this approach: it probes its requirements and fails initialization when they are unavailable. Automatic backend fallback remains a design option, not implemented behaviour.

## Cancellation and task ownership

Cancellation signals intent. Task handles establish completion. Resource ownership determines what remains alive. Keep these responsibilities distinct.

Rust's destruction semantics help release owned resources and make an implicit cancellation hierarchy unnecessary. They do not replace task ownership or asynchronous cleanup: ordinary `Drop` cannot await, and dropping a future does not execute the rest of its async body. The scheduler retains independently spawned tasks, and callers explicitly track completion through their handles.

Use explicit cancellation scopes to describe shutdown relationships. A process can have a root signal and each service can have a child signal. This cancellation tree need not mirror the task ownership graph. A task tree is not imposed on every future.

### Cancellation interface

`Cancellation` lives in `socketry-executor` and is re-exported by the `socketry` facade. It works independently of a scheduler.

| Operation | Behaviour |
| --- | --- |
| `Cancellation::new()` / `Default` | Create an independent, uncancelled signal. |
| `clone()` | Share the same signal. |
| `child()` | Create a distinct signal that also receives ancestor cancellation. |
| `cancel() -> bool` | Request cancellation; return true only for the first request on that signal. |
| `is_cancelled()` | Observe the persistent state. |
| `check() -> Result<(), Cancelled>` | Check at an explicit cooperative cancellation point. |
| `cancelled().await` | Wait using normal future polling and wakeups. |
| `Cancellation::never()` | Supply an allocation-free signal that cannot be cancelled. |

Cancelling a child leaves its parent and siblings running. A child created after its parent is cancelled starts cancelled. Dropping a signal does not cancel it. A child of `never()` is an independent cancellable signal. Repeated cancellation requests are idempotent and never escalate.

### System and service shutdown

The agreed shutdown contract is cooperative. Process signal integration should translate SIGINT/SIGTERM into a request on the application's root signal. Cancelling a service's child signal requests shutdown of that service without shutting down unrelated work.

Graceful shutdown proceeds as follows:

1. Request cancellation for the application or service scope.
2. Stop accepting new work into that scope.
3. Keep polling existing work so it can drain and perform asynchronous cleanup.
4. Await task handles to confirm completion.
5. Release resources and shut down runtime services after draining finishes.

Workers, selectors, timers, blocking-operation services, and the scheduler must remain available throughout draining. A process-level cancellation request must not immediately disable the services that cleanup needs.

There is no default forced abort, grace-period deadline, or escalation on a second request. Work may choose to ignore cancellation. A process supervisor can enforce termination by following SIGTERM with SIGKILL. An application can also abandon futures and destroy its runtime, giving up asynchronous cleanup; this does not remove the runtime's obligation to retain memory still accessible by the kernel.

### Signal notifications and registration lifetime

Signals are inputs to application policy, not inherently cancellation requests. The implemented `Notify<Value>` trait has one synchronous method, `notify(&self, value)`. Cancellation ignores the value and requests shutdown; unbounded `std::sync::mpsc::Sender` and `futures_channel::mpsc::UnboundedSender` queue it. A closed receiver discards values. Bounded senders require an application wrapper that defines overflow behavior. `Arc<T>` forwards to its target. Waiting stays target-specific: `Cancellation::cancelled()` or a channel receive; there is no shared waiting trait.

The implemented scheduler capability is:

```rust,ignore
pub trait Signals: Send + Sync {
    type Signal: Copy + Send + 'static;
    type Registration: Send + 'static;

    fn signal_notify<Target>(
        &self,
        signals: &[Self::Signal],
        target: Target,
    ) -> io::Result<Self::Registration>
    where
        Target: Notify<Self::Signal> + 'static;
}
```

Registration is explicit, synchronous, and fallible. The optional `signals` feature supplies Unix implementations for native and Tokio schedulers and their handles, with `c_int` signal identifiers and a `#[must_use] SignalRegistration`. The trait remains available for other implementations; unsupported platforms have no built-in implementation. Scheduler construction installs no handlers. Closed scheduler handles reject new registrations.

```rust,ignore
use socketry_executor::{Cancellation, Signals, signal::{SIGINT, SIGTERM, SIGHUP}};

let shutdown = Cancellation::new();
let shutdown_registration = scheduler.signal_notify(&[SIGINT, SIGTERM], shutdown.clone())?;
let (reload, reloads) = futures_channel::mpsc::unbounded();
let reload_registration = scheduler.signal_notify(&[SIGHUP], reload)?;
// Consume reloads through the channel; request shutdown through Cancellation.
// Keep both registrations alive while the application drains.
drop(reload_registration);
drop(shutdown_registration);
```

Dropping the guard stops new dispatch admission for that subscription and requests backend cleanup. Already-admitted dispatch may finish and must retain its destination until completion. Drop does not join callbacks or await cleanup. Other subscriptions remain active. Each registration spawns a task owned by its scheduler. The application must keep both the guard and scheduler/runtime alive and driven throughout graceful draining. Scheduler shutdown also ends delivery and releases the subscription, even if a guard survives. Repeated shutdown signals have no automatic escalation behavior.

`Notify` runs in ordinary thread context, outside raw signal handlers and scheduler locks. It may enqueue or wake work without being async-signal-safe, and should return promptly. The Unix backend uses signal-hook's self-pipe with an asynchronous reader in a spawned scheduler task: async-io for native, or the supplied Tokio runtime for the adapter. There is no dedicated thread per registration and no hidden cross-runtime reactor. Guard removal cancels the task's wait so it exits and releases the subscription. The task yields after each batch for fairness. Callback panics follow the scheduler's normal task panic handling; EOF or a terminal read error also ends delivery. Linux signalfd or kqueue can be future implementations with the same public contract, subject to their process/thread setup requirements.

The OS and backend can coalesce repeated signals before dispatch and need not preserve ordering between different signals. A queue retains delivered values while its consumer is busy; it cannot reconstruct every original occurrence. Removing a registration does not restore original process-wide signal dispositions, even after partial setup failure. Applications must coordinate signal ownership with other libraries and existing handlers.

### Deferred cancellation

The implemented wrapper is:

```text
defer_cancel(&cancellation, future, on_cancel)
```

It observes cancellation while polling the protected future. On the first observation, it invokes the synchronous `FnOnce()` callback and continues polling the future to its normal output. The callback can stop an accept loop, request draining, or wake the protected work. Asynchronous cleanup stays inside the future.

The callback runs before the next protected poll, including the first poll if the signal is already cancelled. If cancellation and completion are observed in the same wrapper poll, the callback runs and the normal output is returned. Callback panics propagate.

The wrapper does not mask cancellation inputs passed to operations inside the future. Cleanup must use inputs that permit it to complete, such as a separate scope or `Cancellation::never()`. Dropping the wrapper still drops its future; it does not intercept task destruction.

### Transitional runtime behaviour

The cooperative primitives are implemented, but existing `Task::cancel`, scheduler shutdown, and scheduler destruction still request destruction of task futures. They can bypass deferred asynchronous cleanup. Callers must currently request cooperative cancellation and await draining before invoking those lifecycle operations.

The executor has no implicit task hierarchy or task-group primitive. Completing or cancelling a task does not cancel independently spawned work. Await individual task handles when their completion matters. Higher-level groups can be implemented above `Spawn`; they are outside the executor for now. Cooperative scheduler shutdown remains future work.

## Scheduler operation interface

The intended low-level shape for ordinary I/O is a method on the scheduler capability that returns a future and accepts an explicit cancellation input. Cleanup and selected request operations use the submission/completion contract below. Keep operations recognizable from their system interfaces, for example:

```text
scheduler.file_open(path, options, cancellation)
scheduler.file_read(file, buffer, cancellation)
scheduler.file_read_at(file, buffer, offset, cancellation)
scheduler.socket_accept(listener, cancellation)
scheduler.socket_sendmsg(socket, message, flags, cancellation)
scheduler.address_resolve(name, options, cancellation)
scheduler.now()
scheduler.sleep_until(deadline, cancellation)
scheduler.sleep(duration, cancellation)
```

These are illustrative proposed signatures, not current APIs. Exact resource, options, cancellation borrowing, and error types still need to be chosen. Operations that return owned buffers must preserve that ownership contract for errors and cancellation as well as success.

An optional context pairing a scheduler reference with a cancellation signal could reduce repetitive arguments. Such a context should preserve explicit dependency and cancellation boundaries; it is a convenience, not a new computation abstraction.

`Clock` supplies synchronous monotonic time access and cancellable timer futures, as specified below. Current `Clock::sleep` only accepts a duration and returns `()`; `now`, `sleep_until`, and cancellation-aware waits remain proposed.

Blocking and unblocking a suspended computation should use registration and wakeups with protection against lost notifications. This synchronization interface is separate from running a blocking syscall on a thread pool; its precise API remains open.

## Clock and timer composition

`Clock::now()` returns monotonic, nondecreasing time in the scheduler's clock domain. Use it for durations and deadlines. Wall-clock timestamps would belong to a separate capability; changing calendar time must not change these deadlines.

Use `std::time::Instant` for instants and `std::time::Duration` for intervals initially. Generic code obtains every current-time observation through `scheduler.now()`, including elapsed-time measurements:

```rust
let started = scheduler.now();
// Perform work using this scheduler.
let elapsed = scheduler.now().duration_since(started);
```

Calling `Instant::now()` or an instant's `elapsed()` directly bypasses the scheduler's clock. The standard type permits those calls, so this is an interface convention rather than a type-enforced restriction. Deadline arithmetic should use `checked_add` and handle an unrepresentable deadline rather than relying on overflowing addition. The public error mapping for that case remains to be specified. Do not promise uniform behaviour across system suspend, which Rust leaves platform-dependent. See the [standard Instant contract](https://doc.rust-lang.org/std/time/struct.Instant.html).

A simulated scheduler can choose an initial instant and advance an offset under test control. The Tokio adapter can obtain time from its selected runtime's clock and convert Tokio instants through `into_std` and `from_std`, preserving virtual-time behaviour when that runtime pauses or advances time. These conversions are supported by [Tokio's Instant](https://docs.rs/tokio/latest/tokio/time/struct.Instant.html). Deadlines and observations must belong to the same clock domain; sharing the standard value type does not make independently advancing simulated clocks interchangeable.

`now` and `sleep_until` are the fundamental timer operations. `sleep(duration, cancellation)` is a convenience that can compute a deadline and delegate to `sleep_until`. The proposed timer contract is:

- Timer futures are lazy. Relative sleep measures its duration from the first poll; an absolute deadline stays fixed regardless of when its future is first polled.
- A successful wait completes at or after its deadline. Scheduling can delay resumption, and the interface does not promise precise execution at the deadline.
- An already-cancelled signal returns `Cancelled` without registering a timer. Otherwise, a deadline already reached can complete immediately. Zero-duration sleep does not promise a scheduler yield.
- Observed cancellation ends the timer wait with `OperationError::Cancelled`; it does not cancel the surrounding task.
- Dropping a timer future abandons the wait and safely releases its registration. Backend reclamation may be deferred, but racing wakeups must not access destroyed future state or a reused registration.
- Completion returns `OperationResult<()>`; callers measure elapsed time separately through the scheduler.

### Composing a deadline with I/O

A cooperative deadline wrapper can poll an operation and a timer within the same task. Each future registers wakeups for the enclosing task; no monitoring task or busy waiting is required. For a file read, give the operation a child cancellation signal and follow this sequence:

1. Poll the read and `sleep_until(deadline, cancellation)` together.
2. If the read completes first, return its result and buffer, dropping the timer.
3. If the timer expires first, request cancellation on the read's child signal and continue polling the read until it completes.

The deadline determines when cancellation is requested, not when the wrapper must return. Submitted I/O may still access its buffer or finish successfully after that request; preserve the operation's eventual result and returned buffer. A successful read observed alongside timer expiry remains useful progress. Work that ignores cancellation may continue beyond the deadline. Cancelling the parent signal also reaches the operation through its child signal. Abandoning the wrapper drops its futures under the ordinary operation-lifetime contract and gives up cooperative draining.

Timeouts, intervals, retry policies, and callback scheduling are higher-level operations built on this interface. Periodic work can advance an absolute deadline to avoid drift from repeated relative sleeps and choose its own missed-tick policy. Timer heaps, batching, registration storage, and wakeup mechanisms remain backend details. A reusable timer/reset interface can be considered later if measurements justify it.

## High-level interface sketch

The following is a proposed API for discussion, not a description of implemented interfaces or a compilable example. It specifies caller-visible behaviour without choosing backend machinery. Resource, options, buffer, and error type names are provisional. `IoBuf` and `IoBufMut` stand for the owned-buffer contracts described below; their definitions remain open.

### Results and ownership

Use a common operation error that distinguishes cooperative cancellation from an OS error, while preserving the latter for inspection:

```rust
pub enum OperationError {
    Cancelled(Cancelled),
    Io(std::io::Error),
}

pub type OperationResult<T> = Result<T, OperationError>;
pub type BufferResult<B> = (OperationResult<usize>, B);
```

Ordinary cancellation-aware operations borrowing a resource return `OperationResult<T>`. Operations taking a caller-owned buffer return `BufferResult<B>`, with the buffer outside the `Result` so it can be recovered on failure. Reads and writes return partial transfer counts. A read's count identifies the valid received prefix; a write sends only initialized bytes. Read-exact, write-all, and send-all loops belong above this interface. The non-cancellable submission/completion pairs below return `std::io::Result<()>` instead.

An already-cancelled input prevents new ordinary work from starting. For work already in flight, cancellation requests a cooperative stop; awaiting the operation still establishes its completion and resource ownership. Report a successful transfer that races cancellation as progress rather than replacing its count with `Cancelled`. Cancellation never promises to undo side effects.

The sketches borrow resource handles and cancellation signals. In-flight operations must retain the underlying resources for as long as necessary, even if the waiting future is abandoned. Resource handles belong to their creating scheduler/backend; cross-instance compatibility is checked at the appropriate boundary.

### Core capability shapes

Keep the receiver, resource, operation arguments, and cancellation input explicit:

```rust
pub trait File: Send + Sync {
    type File: Send + Sync;

    fn file_open(
        &self,
        path: &Path,
        options: FileOpenOptions,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<Self::File>> + Send;

    fn file_read<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> impl Future<Output = BufferResult<B>> + Send;

    fn file_write<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> impl Future<Output = BufferResult<B>> + Send;

    fn file_read_at<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> impl Future<Output = BufferResult<B>> + Send;

    fn file_write_at<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> impl Future<Output = BufferResult<B>> + Send;
}

pub trait Socket: Send + Sync {
    type Socket: Send + Sync;
    type Listener: Send + Sync;

    fn socket_accept(
        &self,
        listener: &Self::Listener,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<(Self::Socket, SocketAddr)>> + Send;

    fn socket_connect(
        &self,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<Self::Socket>> + Send;

    fn socket_recv<B: IoBufMut>(
        &self,
        socket: &Self::Socket,
        buffer: B,
        flags: RecvFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = BufferResult<B>> + Send;

    fn socket_send<B: IoBuf>(
        &self,
        socket: &Self::Socket,
        buffer: B,
        flags: SendFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = BufferResult<B>> + Send;
}

pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;

    fn sleep_until(
        &self,
        deadline: Instant,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send;

    fn sleep(
        &self,
        duration: Duration,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send;
}
```

The proposed associated file resource allows a backend-owned handle rather than fixing future implementations to `std::fs::File`. This is a change from the current `File` trait. The connect convenience above creates and connects a socket; a lower-level variant taking an existing socket may be needed for callers that configure it before connecting. Socket creation, binding, listening, registration, message ownership, and portable flags still need their own signatures. Explicit close consumes its handle and follows the shared admission contract below.

Additional capability shapes follow the same convention:

| Capability | Proposed operation shape | Output |
| --- | --- | --- |
| File allocation | `file_allocate(file, offset, length, mode, cancellation)` | `OperationResult<()>` |
| File synchronization | `file_sync(file, mode, cancellation)` | `OperationResult<()>` |
| Message send | `socket_sendmsg(socket, owned_message, flags, cancellation)` | Transfer result together with the owned message |
| Message receive | `socket_recvmsg(socket, owned_message, flags, cancellation)` | Receive result including address/control/status, together with the owned message |
| Address resolution | `address_resolve(name, options, cancellation)` | `OperationResult<ResolvedAddresses>` |

These rows identify extension capabilities; they do not require every scheduler to implement every operation. Synchronization wait/wake remains a separate interface to specify, including lost-wakeup protection. `Spawn::spawn(future)` is primitive dispatch with no group or owner argument. It returns an awaitable task handle; dispatch failures are reported before execution. Grouping and group lifecycle belong above this interface.

### Completion and explicit submission

Cleanup and selected request operations have two proposed forms:

| Await admission and completion | Await admission only | Resource input |
| --- | --- | --- |
| `file_close(file).await` | `file_close_submit(file).await` | Consume the file handle. |
| `socket_close(socket).await` | `socket_close_submit(socket).await` | Consume the socket handle. |
| `socket_shutdown(socket, how).await` | `socket_shutdown_submit(socket, how).await` | Borrow the socket; retain its backend resource while pending. |
| `file_advise(file, offset, length, advice).await` | `file_advise_submit(file, offset, length, advice).await` | Borrow the file; retain its backend resource while pending. |

Both forms return lazy futures yielding `std::io::Result<()>`. Polling first waits for admission capacity and transfers responsibility for the job to the scheduler. If the bounded submission queue is full, the future suspends through wakeups until capacity becomes available; it does not block an executor worker. Constructing or discarding an unpolled future does not enqueue the job.

The ordinary, unsuffixed form waits for admission and OS completion, reporting admission or execution errors. This keeps `.await` consistent with ordinary reads and writes: successful return establishes that execution has completed. The exceptional `_submit` form returns `Ok(())` once the scheduler accepts responsibility. This confirms admission, not that the syscall has started or finished; errors from this form concern admission. Both forms use the same admission path.

For example, these are alternative ways to close an owned handle:

```rust
scheduler.file_close(file).await?; // Completed; execution errors reported.
// Or, when only admission needs to be observed:
scheduler.file_close_submit(file).await?; // Accepted for execution.
```

All four operation pairs use the same admission queue and job ownership mechanism. Close consumes its handle, while shutdown and advice retain the borrowed resource's underlying backend ownership for the job. This contract does not require a separate per-fd retirement mechanism. Queue capacity and fairness remain implementation choices; bounding a scheduler queue does not itself bound the number of caller-owned futures waiting for admission.

These methods omit cancellation inputs. Before acceptance, the admission future owns its inputs: dropping it, or rejecting admission, releases those inputs according to their ordinary destruction rules. For close, this means dropping the consumed handle; no asynchronous close job has been accepted. The resource's ordinary destruction policy remains to be specified and is not replaced by constructing a close future.

Once the scheduler accepts the job, it must carry out the request even if the caller drops either form's future or its cancellation scope is cancelled. Acceptance is the ownership boundary even if the caller abandons the future before observing its result. Dropping a waiter after acceptance does not return a consumed handle or undo submission.

Submitting work does not establish completion order. Preserve the required dependencies on earlier I/O through operation tracking or explicit chains, especially for close and write-side shutdown. Retain underlying resources until dependent backend access ends, and do not reuse a registered descriptor slot merely because close was queued. Graceful runtime draining must include outstanding submitted jobs even when they have no application waiter.

Shutdown completion confirms the local shutdown operation, not remote receipt or processing. Advice completion confirms that the hint was processed, not that all requested data was prefetched; `WILLNEED` initiates nonblocking read-ahead. See [shutdown](https://man7.org/linux/man-pages/man2/shutdown.2.html) and [file advice](https://man7.org/linux/man-pages/man2/posix_fadvise.2.html).

The contract is portable. A blocking-pool backend admits a job that owns or retains the required resources into its bounded queue. The `_submit` form returns after admission without waiting for execution. The ordinary future additionally receives that job's result through a completion notification; it does not join the worker thread. Workers remain reusable. A readiness backend can retain admitted requests in its state, while an io\_uring backend can queue them for SQE submission. All backends must preserve admission backpressure, ownership, ordering, bounded execution concurrency, and draining semantics.

Not waiting at the application level does not automatically allow the backend to skip completion tracking. For eligible io\_uring requests, `IOSQE_CQE_SKIP_SUCCESS` can suppress successful CQEs; failures still produce CQEs that the selector must consume. Only use it when resource retirement and dependencies remain safe without the successful CQE. It is incompatible with `IOSQE_IO_DRAIN` in the same ring. The current selector expects a terminal CQE per request and needs changes before using this optimization. See [CQE suppression](https://man7.org/linux/man-pages/man2/io_uring_enter.2.html).

Close errors can still be useful diagnostics, including delayed write failures. The ordinary form exposes them; the `_submit` form leaves execution error handling to the backend's diagnostics policy. Do not blindly retry close after an error: Linux normally releases the descriptor before reporting such failures. See [close errors](https://man7.org/linux/man-pages/man2/close.2.html).

Keep `_submit` exceptional rather than adding a submission-only variant to every operation. Reads, opens, accepts, and address resolution produce outputs that callers need. Writes and sends expose partial counts and require buffer lifetime tracking. Allocation and file synchronization have meaningful completion/error guarantees, so retain their ordinary future/result contracts. Cancellation already separates the request (`cancel()`) from task completion through handles.

### Pool receive capability

A pool receive returns a selected filled lease, rather than accepting an already-selected buffer:

```rust
pub trait PoolReceive: Socket {
    type Pool: Send + Sync;
    type Lease: IoBufMut;

    fn buffer_pool(
        &self,
        options: BufferPoolOptions,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<Self::Pool>> + Send;

    fn socket_recv_with_pool(
        &self,
        socket: &Self::Socket,
        pool: &Self::Pool,
        flags: RecvFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<Self::Lease>> + Send;
}
```

The returned lease exposes only the initialized received payload for sending, and can therefore be passed directly to `socket_send`. Its readable length is the receive count, not the capacity of its backing allocation. For stream sockets, an empty successful lease represents EOF. The pool owns storage that does not escape as a lease: on error or cancellation, it is safely reclaimed only after backend access ends. This differs from an operation that must return a buffer supplied by the caller. If a receive has produced usable data, report that lease as progress rather than discarding it solely because cancellation raced completion.

`BufferPoolOptions` would describe a bounded number of buffers and their capacities. Native selection or fixed-registration requirements belong in separate extensions, rather than changing the meaning of this portable capability. Pool message receives need a corresponding result carrying the filled lease and message metadata.

### Example: positioned file read

```rust
async fn read_header<S: File>(
    scheduler: &S,
    path: &Path,
    cancellation: &Cancellation,
) -> OperationResult<Vec<u8>> {
    let file = scheduler
        .file_open(path, FileOpenOptions::read_only(), cancellation)
        .await?;

    let (result, mut buffer) = scheduler
        .file_read_at(&file, vec![0; 4096], 0, cancellation)
        .await;

    let count = result?;
    buffer.truncate(count);
    Ok(buffer)
}
```

This deliberately performs one read and permits a short result. A caller needing a complete header would use a read-exact helper. Keeping `result` and `buffer` separate also lets a caller reuse the allocation after an error instead of propagating it immediately.

### Example: forwarding pooled socket data

```rust
async fn forward<S: Socket + PoolReceive>(
    scheduler: &S,
    source: &S::Socket,
    destination: &S::Socket,
    pool: &S::Pool,
    cancellation: &Cancellation,
) -> OperationResult<()> {
    loop {
        let lease = scheduler
            .socket_recv_with_pool(source, pool, RecvFlags::empty(), cancellation)
            .await?;

        if lease.is_empty() {
            return Ok(());
        }

        let (result, lease) = send_all(
            scheduler, destination, lease, SendFlags::empty(), cancellation,
        ).await;

        // Sending is complete; the lease can return to its pool.
        drop(lease);
        result?;
    }
}
```

`send_all` here is a proposed higher-level helper, not a scheduler hook. It repeatedly calls `socket_send` with owned views of the unsent range, handles zero progress, and returns the lease on success or failure after all backend access ends. The view interface also remains to be specified. This example uses explicit sequential awaits and makes no assumption about `with_previous_buffer`.

Create the pool once and share it across connections, for example:

```rust
let pool = scheduler.buffer_pool(
    BufferPoolOptions { buffers: 128, buffer_capacity: 16 * 1024 },
    &cancellation,
).await?;

forward(&scheduler, &source, &destination, &pool, &cancellation).await?;
```

### Example: prepared accept followed by receive

```rust
let accept = scheduler.prepare_socket_accept(&listener);

let read = scheduler.prepare_socket_recv(
    accept.socket(),
    vec![0; 4096],
    RecvFlags::empty(),
);

let outcomes = scheduler
    .submit(accept.link(read), &cancellation)
    .await;
```

`accept.socket()` is proposed syntax for an owned, typed reference to the socket that accept will produce. It is not an already-accepted socket or a borrow that would prevent moving the prepared accept into the chain. The receive starts only after accept succeeds; a failed accept skips it. Outcomes must preserve the accepted socket even if the receive fails or is skipped, and return the receive buffer with its partial byte count or error.

Unlike forwarding a selected receive buffer, this dependency has a native io\_uring implementation target: direct accept into a reserved, known descriptor slot, followed by a linked receive using that slot. The Rust output-reference API and its type/lifetime rules are still proposed and unimplemented. They must prevent a consumer from running before its producing accept, or without that dependency. See the direct-descriptor linking section below.

### Example: prepared write followed by synchronization

```rust
let write = scheduler.prepare_file_write_at(&file, buffer, offset);
let sync = scheduler.prepare_file_sync(&file, SyncMode::Data);

let outcomes = scheduler.submit(write.link(sync), &cancellation).await;
```

This requires the proposed file-synchronization and `Linked` extensions. Both operations have known inputs. The intended example policy runs synchronization only after the complete requested write; errors or short writes skip it. `outcomes` must retain the write buffer and distinguish a completed successor from a skipped one. Its exact type and policy-selection syntax remain open, as detailed in the linked-operation section. This does not make a partial write transactional.

### Example: cooperative service shutdown

```rust
let system = Cancellation::new();
let service = system.child();
let drain = Cancellation::never();

defer_cancel(
    &service,
    server.run(&scheduler, &drain),
    || server.request_shutdown(),
).await?;
```

`server` is an application-defined service. `request_shutdown` synchronously signals and wakes its accept loop; `run` stops accepting and awaits its owned connection tasks before returning. Process signal integration requests `system.cancel()`, while a local service stop requests `service.cancel()`. The service remains polled during draining, and operations needed to finish accepted work use the non-cancelled drain input. Cancellation-aware accept uses the service's stopping signal or equivalent notification. This example adds no forced abort or shutdown deadline.

## Linked operations

Linking is a portable dependency capability. A proposed `Linked` extension trait would express ordered execution of prepared I/O operations, allowing consumers to require it through a bound such as `S: Linked`. A scheduler can implement this capability through kernel linking, a blocking pool, or a readiness state machine. Implement the trait when the backend can preserve the dependency contract; an unavailable capability is a compile-time failure.

### Preparation and submission

Prepare the whole chain before submitting it. Preparation retains arguments, buffers, and resources without starting I/O. Submission returns an ordinary future. A chain can use concrete generic types to preserve each operation's result type and allow static dispatch and compiler specialization.

An illustrative proposed API is:

```text
let write = scheduler.prepare_file_write_at(&file, buffer, offset);
let sync = scheduler.prepare_file_sync(&file, SyncMode::Data);

let results = scheduler.submit(write.link(sync), &cancellation).await;
```

The names and exact type signatures remain open. Existing single-operation methods can remain convenient wrappers around preparation and submission. This introduces a representation for scheduler I/O that can be composed before execution, while retaining `Future` as the execution interface.

Ordinary sequential `.await` calls do not expose the whole dependency chain to the backend in advance. An executor cannot inspect an arbitrary future to recover that sequence and turn it into a kernel chain. Explicit prepared operations give the backend the information it needs.

### Dependency contract

A chain must define when each successor may start, which preceding outcomes stop execution, and how errors, partial transfers, skipped operations, and cancellation are reported. Return individual outcomes and owned buffers, including those belonging to operations that never started. The exact result representation and continuation policies remain to be chosen.

Linking supplies ordering without transactional rollback. Kernel links do not automatically substitute an earlier operation's result into a later operation's arguments. A write followed by synchronization can be prepared in advance; a write whose length depends on the actual byte count of a preceding read generally needs a userspace continuation.

io\_uring's `IOSQE_IO_LINK` starts a successor after its predecessor completes, and breaks the chain on errors or unexpected results, including short reads. Unstarted successors then complete with `-ECANCELED`. `IOSQE_IO_HARDLINK` permits continuation despite completion errors, although submission failures can still break the chain. These dependency policies are distinct from task shutdown. See [kernel link semantics](https://man7.org/linux/man-pages/man2/io_uring_enter.2.html).

Use native linking only when its behaviour matches the defined chain contract. A fallback must apply the same outcome and continuation rules; submitting independent, unlinked operations concurrently would lose the ordering guarantee.

### Direct-descriptor linking for accept and receive

io\_uring supports a native accept-to-receive chain when the accepted socket is installed into a known slot in the ring's registered file table. This uses [io\_uring\_prep\_accept\_direct](https://man7.org/linux/man-pages/man3/io_uring_prep_accept_direct.3.html), rather than expecting an ordinary accept's returned OS descriptor to be substituted into the next SQE.

The backend can reserve an unused slot before submission, prepare accept with that explicit `file_index` and `IOSQE_IO_LINK`, then prepare receive with its `fd` set to the same slot and `IOSQE_FIXED_FILE` set. Both requests are submitted as one chain. The receive resolves its socket after accept succeeds, allowing execution without userspace processing the accept completion first.

This requires `IORING_FEAT_LINKED_FILE`, available since Linux 5.17, which defers descriptor lookup for dependent requests until they execute. The backend must also establish direct-accept support and initialize a suitable registered file table. See [dependent descriptor lookup](https://man7.org/linux/man-pages/man2/io_uring_setup.2.html).

Use an explicitly reserved slot for this native mapping. `IORING_FILE_INDEX_ALLOC` chooses an index dynamically and reports it in the accept completion, so it does not provide the known index needed by a prebuilt successor. An explicit slot must be empty and exclusively reserved: direct accept can replace an existing entry. Direct descriptors are private to their ring, not ordinary OS descriptor numbers.

The prepared chain must retain the slot through all dependent access and safely release it when the chain is abandoned. If accept succeeds but receive fails or is cancelled, ownership of the accepted socket must still be resolved; skipping the receive does not undo accept. A portable fallback can keep the accepted resource in chain state and pass it to the receive after completion.

This is a specific resource dependency with a known native target, not general substitution of arbitrary completion results into later SQEs. It does not implement the hypothetical `with_previous_buffer` handoff of a selected buffer and actual received length.

### Backend implementations

| Backend | Linking strategy |
| --- | --- |
| io_uring | Submit the prepared operations together as linked SQEs. |
| Blocking pool | Submit the whole chain as one worker job that executes its operations sequentially. |
| Readiness | Advance the chain through successive asynchronous operations as their dependencies complete. |

A blocking worker can retain the whole chain and return its results after execution, avoiding executor wakeups and job submission between operations. It can check cooperative cancellation between operations and before starting queued work. An already-running blocking syscall may need to finish. Keep resources alive through completion and allow cleanup chains to use cancellation inputs that permit draining.

For io\_uring, send the chain to the selector as one command, reserve sufficient SQ capacity, and publish its entries contiguously in the same submission. Links cannot cross submission boundaries. Track every member's completion and retain shared resources until all relevant kernel access ends. The current per-operation command model must be extended to support this. See [linked request submission](https://man7.org/linux/man-pages/man7/io_uring_linked_requests.7.html).

The portable `Linked` capability guarantees dependency behaviour. If a caller specifically requires kernel-linked submission, a separate native extension can express that requirement at compile time. SQE LINK is an implementation strategy for portable linking, rather than a restriction of linking to Linux.

## Candidate I/O hooks

Use the [liburing preparation helpers](https://github.com/axboe/liburing/blob/master/src/include/liburing.h) as an inventory of operations to consider. This is a candidate surface, not a requirement that every opcode become part of every scheduler's core traits.

| Capability area | Candidate operations |
| --- | --- |
| File lifecycle | `file_open`, `file_open_at`, `file_close`/`file_close_submit` |
| File transfer | Stateful `file_read`/`file_write`, positioned `file_read_at`/`file_write_at`, vectored equivalents |
| File management | `file_seek`, `file_allocate`, `file_truncate`, file/data synchronization, range synchronization, `file_advise`/`file_advise_submit` |
| Metadata and paths | Stat, extended attributes, mkdir, unlink, rename, hard links, symbolic links |
| Socket lifecycle | Create, bind, listen, accept, connect, `socket_shutdown`/`socket_shutdown_submit`, `socket_close`/`socket_close_submit` |
| Socket transfer | Send/receive, sendto/recvfrom, `socket_sendmsg`/`socket_recvmsg` |
| Socket control | Options, local/peer addresses, readiness |
| Address resolution | `address_resolve` through an appropriate resolver implementation |
| Time and synchronization | Monotonic clock access, sleep, deadlines, wait/wake operations |
| Additional extensions | Process waits, pipes, splice/tee and other descriptor operations |

The canonical message syscall names are `sendmsg` and `recvmsg`. `writemsg` is not the intended name.

Not every useful scheduler operation has an io\_uring opcode; seek and address resolution can use other implementation strategies. Multishot operations, fixed descriptors, ring messaging, device commands, and specialized zero-copy operations should remain optional extensions with their own lifecycle contracts.

### File semantics

Stateful reads and writes use and advance the file's shared cursor. Positioned operations correspond to `pread`/`pwrite` and should leave that cursor unchanged. Expose both explicitly rather than encoding stateful access through a magic public offset. io\_uring internally supports current-position reads using offset `-1`; its documentation warns that asynchronous shared-cursor access requires serialization for predictable behaviour. See [io\_uring\_prep\_read](https://man7.org/linux/man-pages/man3/io_uring_prep_read.3.html).

Allocation is distinct from truncation. An allocation interface needs to account for an offset, length, and supported modes. Platform-specific modes may require an extension rather than a universal flags type. See [io\_uring\_prep\_fallocate](https://man7.org/linux/man-pages/man3/io_uring_prep_fallocate.3.html).

Current `File` only provides positioned reads and writes of already-open, non-append regular files, using offsets that fit in `i64`. Its Windows fallback uses `seek_read`/`seek_write`, which update the shared cursor according to [Windows FileExt](https://doc.rust-lang.org/std/os/windows/fs/trait.FileExt.html). Resolving this difference is required before promising uniform cursor-preserving semantics.

### Syscall proximity and messages

Preserve partial transfer counts, relevant flags, addresses, ancillary data, receive status, and OS errors. Keep read-exact/write-all loops and protocol buffering as higher-level conveniences.

A safe owned message representation should retain its payload buffers and all address, iovec, control, and header storage required by the backend for the duration of kernel access. It can construct native metadata without inherently copying the payload. Specific raw layouts and ancillary features may be platform extensions. See [io\_uring\_prep\_sendmsg](https://man7.org/linux/man-pages/man3/io_uring_prep_sendmsg.3.html).

Cancellation is a request, not a transactional rollback: a cancelled read may consume bytes and a cancelled write may transmit bytes. Define how callers observe completion and recover buffer ownership before committing to the cancellation-aware result types.

## Buffer ownership

Completion-based I/O requires a buffer whose address remains valid while the kernel uses it. The operation must retain buffers and descriptors even if its waiting future is dropped. A cancellation completion alone does not establish that the original operation has finished accessing memory.

The current API transfers an owned `Vec<u8>` and returns `(io::Result<usize>, Vec<u8>)`. Reads operate on initialized length rather than spare capacity, leave that length unchanged, and report the number of valid bytes separately. Capacity alone is not writable input under this contract.

There is no standard owned completion-buffer trait to adopt directly. `Vec<u8>` and `Box<[u8]>` provide useful owned storage. Standard [`IoSlice`](https://doc.rust-lang.org/std/io/struct.IoSlice.html) and `IoSliceMut` provide borrowed vectored views rather than owning the underlying memory. [`bytes::Bytes` and `BytesMut`](https://docs.rs/bytes/latest/bytes/) provide useful sharing and slicing; `Buf`/`BufMut` alone do not establish completion-I/O lifetime safety.

Consider small owned buffer traits modeled on the contracts described by Tokio-uring's [`IoBuf`](https://docs.rs/tokio-uring/latest/tokio_uring/buf/trait.IoBuf.html) and [`IoBufMut`](https://docs.rs/tokio-uring/latest/tokio_uring/buf/trait.IoBufMut.html). They would express:

- A stable backing address while the owned buffer value moves.
- Initialized length distinct from writable capacity.
- Exclusive writable access and a sound way to mark newly initialized bytes.
- Ownership sufficient to keep storage alive through completion.
- `Send` where operations cross Socketry worker or selector threads.

Support ordinary owned buffers first, with optional implementations for bytes types and later registered-buffer leases. Any unsafe buffer contract must be explicit and enforceable. Supporting uninitialized capacity could avoid initialization work, but is a deliberate change from the current read contract, not an automatic property of passing a `Vec`.

## Buffer pools, registered buffers, and specialized I/O

Buffer pools should be optional capabilities that compose with owned-buffer operations. Keep a portable pool contract separate from requirements for native registration or kernel buffer selection. A software pool is an acceptable fallback where it preserves the contract; a consumer specifically requiring a native facility should declare that through an extension trait. Probe kernel-dependent facilities when constructing the pool or registration.

### Fixed registered buffers

Fixed registered buffers identify pre-registered memory by index and can reduce repeated memory-mapping work for I/O. A pool can own registrations and issue leases retaining both the memory and its registration lifetime. Validate compatible ring identity before submission and prevent reuse while an operation still accesses the lease. See [buffer registration](https://man7.org/linux/man-pages/man3/io_uring_register_buffers.3.html).

An operation using a fixed buffer normally receives a lease selected before submission. Registration optimizes access to that memory; it does not defer the assignment of a buffer to a waiting operation.

### Provided-buffer pools

Provided-buffer rings solve a different problem: many pending receives can share fewer payload buffers. The kernel selects an available buffer when a receive can make progress, rather than requiring one reserved buffer per idle socket. Registering a provided-buffer ring is distinct from registering fixed payload buffers. See [provided-buffer rings](https://man7.org/linux/man-pages/man3/io_uring_setup_buf_ring.3.html).

The proposed portable receive interface accepts the pool itself and returns a filled buffer lease, including the valid received length. Acquiring a lease before waiting for data would lose the memory advantage. A lease must distinguish readable initialized bytes from writable capacity and retain its pool and storage until all consumers finish.

For ordinary single-buffer selection, the lifecycle is:

1. The pool offers available buffers to the backend.
2. A receive selects a buffer when data is available.
3. Completion identifies the selected buffer and the received byte count.
4. The caller owns a filled lease and may process it or transfer it into a send/write operation.
5. Once no consumer or kernel operation can access the buffer, it can be returned to the pool and offered again.

Do not offer a leased buffer for another receive while it is still being read or sent. Dropping a waiting future or requesting cancellation must not recycle storage still accessible by the kernel. Pool shutdown must retain outstanding leases, registrations, and offered storage until their respective backend lifetimes have ended.

Pool exhaustion requires an explicit backpressure and replenishment policy. io\_uring can return `-ENOBUFS` rather than waiting for a buffer; exhaustion can also terminate a multishot receive. The backend must replenish and rearm as appropriate, or expose the error according to the operation contract. See [provided-buffer ownership and exhaustion](https://man7.org/linux/man-pages/man7/io_uring_provided_buffers.7.html).

A readiness fallback can wait for readability, acquire a lease, and attempt a nonblocking receive. If the receive returns `WouldBlock`, return the lease before waiting again. Waiting for pool availability must also suspend through wakeups. A blocking fallback may retain a lease throughout a running syscall; its bounded concurrency limits how many such leases are occupied.

Multishot receives could expose a stream of filled leases as a separate extension. The implementation must distinguish individual buffer lifetimes from the lifetime of the receive request, and detect when it needs rearming. See [multishot receive completions](https://man7.org/linux/man-pages/man3/io_uring_prep_recv_multishot.3.html). Streams, bundles, and incremental buffer consumption need their own contracts before implementation.

### Operation names and preparation

Use `with_pool` for operations that receive a pool argument. The proposed naming pattern is:

| Operation | Direct future | Prepared operation |
| --- | --- | --- |
| Receive into an owned buffer | `socket_recv` | `prepare_socket_recv` |
| Receive using a pool | `socket_recv_with_pool` | `prepare_socket_recv_with_pool` |
| Receive a message into owned storage | `socket_recvmsg` | `prepare_socket_recvmsg` |
| Receive a message using a pool | `socket_recvmsg_with_pool` | `prepare_socket_recvmsg_with_pool` |

These names are proposed, not implemented APIs. Exact arguments, lease types, message metadata, and error/cancellation results remain to be chosen. Direct operations accept explicit cancellation; prepared operations retain the pool and resource inputs, with cancellation supplied at submission as described under linked operations. Preparation must not reserve a payload buffer for each waiting receive.

`with_pool` describes how an operation obtains storage. The [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/naming.html) use `from_*` for conversions; they do not prescribe `from_pool` for this operation.

Pool receives can participate in prepared chains whose successors have known inputs and preserve the agreed dependency rules. A successor that needs the selected buffer and actual received length has an additional data dependency that ordinary SQE linking does not resolve.

### Hypothetical forwarding from a preceding operation

`with_previous_buffer` is only a hypothetical interface. There is no selected implementation target, agreed signature, or commitment to add it to the proposed `Linked` capability. It could express that a successor consumes the compatible filled lease produced by its predecessor, using the initialized length rather than the original capacity. Any eventual design would need typed compatibility, error and cancellation behaviour, partial-transfer results, and ownership for skipped successors.

Native SQE links provide ordering, not automatic forwarding of a receive's selected buffer and byte count. Send-side provided-buffer selection is supported on newer kernels, but it selects from an offered group rather than inheriting a preceding receive's result. See [send buffer selection](https://man7.org/linux/man-pages/man3/io_uring_prep_send.3.html).

Relevant liburing discussions include [using a preceding receive's result as the send length (#58)](https://github.com/axboe/liburing/issues/58) and [moving selected receive buffers into a send ring (#1126)](https://github.com/axboe/liburing/issues/1126). Userspace can reuse the received storage by preparing a send after completion, or offering the filled range to a send buffer ring. This avoids an additional userspace payload copy but still requires userspace to arrange the handoff.

A selector-managed continuation or a whole-chain blocking job might eventually implement forwarding without waking the application task between operations. These are possible strategies to investigate, not an implementation plan for `with_previous_buffer` or a promise of one kernel-linked submission.

### Zero-copy lifetime

Registration does not itself guarantee zero-copy transport. Zero-copy sends can report a transfer result before a subsequent notification allows buffer reuse, so one result is not necessarily the end of kernel access. See [io\_uring\_prep\_send\_zc](https://man7.org/linux/man-pages/man3/io_uring_prep_send_zc.3.html). The current selector's single-terminal-completion request model must be extended before exposing those operations safely.

## Current implementation and remaining work

| Area | Current status |
| --- | --- |
| Executor | Future workers, work stealing, scheduler-owned tasks and primitive `Spawn` dispatch implemented. Higher-level task groups are deferred. |
| Cancellation | `Cancellation`, `Cancelled`, and `defer_cancel` implemented and re-exported. |
| Signal notifications | `Notify<Value>`, `Signals`, and optional Unix signal registration guards implemented for native and Tokio schedulers; cancellation and unbounded queue targets supported. |
| Graceful runtime shutdown | Cooperative scheduler draining and cancellation-aware I/O remain pending; existing shutdown destroys task futures. |
| Capabilities | `File`, `Socket`, `Clock`, and `Spawn` implemented; a common scheduler contract, associated file resources, scheduler-level resource creation, broader hooks and optional extension boundaries remain design work. |
| Linked operations | Prepared chains and a portable `Linked` capability remain proposed; native SQE linking, whole-chain blocking jobs, and readiness sequencing are not implemented. |
| File operations | Owned-buffer positioned read/write implemented; stateful I/O, lifecycle, allocation and metadata hooks pending. |
| Submission and completion | Close, socket shutdown, and file advice pairs remain proposed; shared asynchronous admission, exceptional `_submit` futures, and safe CQE suppression are not implemented. |
| Socket operations | TCP registration, connect, accept, read/write and readiness implemented; general sockets and message I/O pending. |
| Backends | Shared native readiness, blocking file fallbacks, a Linux io_uring selector, and an adapter to an existing Tokio runtime implemented. |
| Buffers | Owned `Vec<u8>` implemented; generic owned-buffer traits, fixed registered leases, and provided/software pools remain proposed. |
| Buffer forwarding | `with_previous_buffer` is hypothetical, with no selected implementation target. |
| Optimization | Dedicated io_uring selector uses an unbounded command channel and a completion channel per operation; pooling and submission backpressure pending. |
| Timers | The independent `socketry-timers` crate implements the io-event timer algorithm; scheduler integration remains pending. Existing `Clock::sleep(duration)` returns `()` without cancellation. Monotonic `now`, cancellable `sleep_until` and relative sleep, and generic deadline composition remain proposed. |

Next design work is to choose the dependable core and extension boundaries, cancellation-aware error and buffer-return contracts, linked-chain continuation rules, resource lifecycle semantics, and owned-buffer trait safety requirements. Then integrate cooperative cancellation into operations and runtime shutdown before expanding optional optimizations.

The workspace's [architecture guide](https://github.com/socketry/socketry-rust/blob/main/context/design.md) and [implementation guide](https://github.com/socketry/socketry-rust/blob/main/context/implementation.md) provide further details. In the source checkout, `context/gaps.md` records the four architectural gaps. The executor [README](readme.md) documents existing APIs.
