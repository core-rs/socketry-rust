# Executor Design

This document records the agreed direction for `socketry-executor`: an explicit scheduler and I/O interface inspired by [Zig 0.16's I/O model](https://ziglang.org/download/0.16.0/release-notes.html#I-O-as-an-Interface), expressed through ordinary Rust futures, capability traits, and cooperative cancellation. It includes proposed interfaces; the implementation status below distinguishes them from APIs that exist today.

## Execution model

Use Rust's existing `Future`, `Context`, and `Waker` contracts. A separate computation trait is unnecessary for the current design. Calling an operation constructs a future; awaiting it allows the executor to poll it and suspend until a wakeup permits progress. Polling does not mean busy waiting.

The scheduler executes pinned futures on ordinary worker stacks. Independently spawned tasks are `Send + 'static` and can migrate between workers, while their pinned storage remains stationary. Only one worker polls a task at a time. Blocking system calls must run outside the workers that poll futures.

Pass the implementation into libraries explicitly. A library should require the capabilities it actually uses rather than implicitly selecting a global runtime. Spawning additionally requires an explicit task owner. Existing resources can be passed directly to code that does not need to create them.

## Capability traits and static dispatch

`File`, `Socket`, and `Clock` name scheduler capabilities. `Spawn` expresses task submission and ownership separately. `File` is the capability trait; `std::fs::File` is currently the resource type, conventionally imported as `StdFile` when both names are needed. `Socket` replaces the earlier capability name `Network`.

Prefer generic trait bounds, concrete resource types, and concrete future types. These allow compile-time capability checking, monomorphization, and inlining without requiring a boxed future for each operation. The compiler decides which calls to inline; the interface should make that optimization possible.

Keep core traits small and dependable. Implementing a core trait requires working implementations of its operations for the documented resource types and inputs. Avoid default methods that simply return `Unsupported`.

Additional capabilities belong in extension traits where their semantics or platform availability differ. A consumer requiring an extension declares that requirement through its trait bounds. An incompatible scheduler then fails at compile time. For example, `S: File + FileAllocate` illustrates how a consumer might require allocation; `FileAllocate` is a proposed name, not an existing trait or a settled trait boundary.

Do not create one trait per syscall automatically. Group operations around capabilities that real consumers need, and keep inherently platform-specific types and flags in platform extensions.

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

Bound blocking concurrency and provide asynchronous admission/backpressure. Cancellation can prevent queued work from starting. Once a blocking syscall has started, the operation may have to finish; retain its resources and buffer throughout. A cancellation result must not imply that completed side effects were undone.

An application explicitly selecting a specialized backend may receive an initialization failure instead of automatic fallback. The policy must be clear at that boundary. The current Linux io\_uring backend follows this approach: it probes its requirements and fails initialization when they are unavailable. Automatic backend fallback remains a design option, not implemented behaviour.

## Cancellation and task ownership

Cancellation signals intent. Task handles and barriers establish completion. Resource ownership determines what remains alive. Keep these responsibilities distinct.

Rust's destruction semantics help release owned resources and make an implicit cancellation hierarchy unnecessary. They do not replace task ownership or asynchronous cleanup: ordinary `Drop` cannot await, and dropping a future does not execute the rest of its async body. Independently spawned tasks still need owners and explicit completion tracking.

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
4. Await task handles or barriers to confirm completion.
5. Release resources and shut down runtime services after draining finishes.

Workers, selectors, timers, blocking-operation services, and task owners must remain available throughout draining. A process-level cancellation request must not immediately disable the services that cleanup needs.

There is no default forced abort, grace-period deadline, or escalation on a second request. Work may choose to ignore cancellation. A process supervisor can enforce termination by following SIGTERM with SIGKILL. An application can also abandon futures and destroy its runtime, giving up asynchronous cleanup; this does not remove the runtime's obligation to retain memory still accessible by the kernel.

### Deferred cancellation

The implemented wrapper is:

```text
defer_cancel(&cancellation, future, on_cancel)
```

It observes cancellation while polling the protected future. On the first observation, it invokes the synchronous `FnOnce()` callback and continues polling the future to its normal output. The callback can stop an accept loop, request draining, or wake the protected work. Asynchronous cleanup stays inside the future.

The callback runs before the next protected poll, including the first poll if the signal is already cancelled. If cancellation and completion are observed in the same wrapper poll, the callback runs and the normal output is returned. Callback panics propagate.

The wrapper does not mask cancellation inputs passed to operations inside the future. Cleanup must use inputs that permit it to complete, such as a separate scope or `Cancellation::never()`. Dropping the wrapper still drops its future; it does not intercept task destruction.

### Transitional runtime behaviour

The cooperative primitives are implemented, but existing `Task::cancel`, `Barrier::stop`, scheduler shutdown, and scheduler destruction still request destruction of task futures. They can bypass deferred asynchronous cleanup. Callers must currently request cooperative cancellation and await draining before invoking those lifecycle operations.

Barriers own and join their direct children. Parent completion does not automatically await all descendants; owners must explicitly await the groups whose completion matters. Signal integration, cooperative scheduler shutdown, and a portable group lifecycle remain future work.

## Scheduler operation interface

The intended low-level shape is a method on the scheduler capability that returns a future and accepts an explicit cancellation input. Keep operations recognizable from their system interfaces, for example:

```text
scheduler.file_open(path, options, cancellation)
scheduler.file_read(file, buffer, cancellation)
scheduler.file_read_at(file, buffer, offset, cancellation)
scheduler.socket_accept(listener, cancellation)
scheduler.socket_sendmsg(socket, message, flags, cancellation)
scheduler.address_resolve(name, options, cancellation)
scheduler.sleep(duration, cancellation)
```

These are illustrative proposed signatures, not current APIs. Exact resource, options, cancellation borrowing, and error types still need to be chosen. Operations that return owned buffers must preserve that ownership contract for errors and cancellation as well as success.

An optional context pairing a scheduler reference with a cancellation signal could reduce repetitive arguments. Such a context should preserve explicit dependency and cancellation boundaries; it is a convenience, not a new computation abstraction.

Sleep uses monotonic time and completes when the requested interval has elapsed. It need not return the elapsed time; callers can measure that separately. Cancellation-aware sleep is expected to return completion or `Cancelled`. Current `Clock::sleep` returns `()` and has no cancellation argument.

Blocking and unblocking a suspended computation should use registration and wakeups with protection against lost notifications. This synchronization interface is separate from running a blocking syscall on a thread pool; its precise API remains open.

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

Operations borrowing a resource return `OperationResult<T>`. Operations taking a caller-owned buffer return `BufferResult<B>`, with the buffer outside the `Result` so it can be recovered on failure. Reads and writes return partial transfer counts. A read's count identifies the valid received prefix; a write sends only initialized bytes. Read-exact, write-all, and send-all loops belong above this interface.

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
    fn sleep(
        &self,
        duration: Duration,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<()>> + Send;
}
```

The proposed associated file resource allows a backend-owned handle rather than fixing future implementations to `std::fs::File`. This is a change from the current `File` trait. The connect convenience above creates and connects a socket; a lower-level variant taking an existing socket may be needed for callers that configure it before connecting. Socket creation, binding, listening, registration, explicit close, message ownership, and portable flags still need their own signatures. Consuming resource operations such as explicit close must define what ownership is returned if cancellation prevents them from starting.

Additional capability shapes follow the same convention:

| Capability | Proposed operation shape | Output |
| --- | --- | --- |
| File allocation | `file_allocate(file, offset, length, mode, cancellation)` | `OperationResult<()>` |
| File synchronization | `file_sync(file, mode, cancellation)` | `OperationResult<()>` |
| Message send | `socket_sendmsg(socket, owned_message, flags, cancellation)` | Transfer result together with the owned message |
| Message receive | `socket_recvmsg(socket, owned_message, flags, cancellation)` | Receive result including address/control/status, together with the owned message |
| Address resolution | `address_resolve(name, options, cancellation)` | `OperationResult<ResolvedAddresses>` |

These rows identify extension capabilities; they do not require every scheduler to implement every operation. Synchronization wait/wake remains a separate interface to specify, including lost-wakeup protection. `Spawn` continues to require an explicit task owner.

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
| File lifecycle | `file_open`, `file_open_at`, `file_close` |
| File transfer | Stateful `file_read`/`file_write`, positioned `file_read_at`/`file_write_at`, vectored equivalents |
| File management | `file_seek`, `file_allocate`, `file_truncate`, file/data synchronization, range synchronization, access advice |
| Metadata and paths | Stat, extended attributes, mkdir, unlink, rename, hard links, symbolic links |
| Socket lifecycle | Create, bind, listen, accept, connect, shutdown, close |
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
| Executor | Future workers, work stealing, explicit owners, task handles and direct-child barriers implemented. |
| Cancellation | `Cancellation`, `Cancelled`, and `defer_cancel` implemented and re-exported. |
| Graceful runtime shutdown | Signal installation, cooperative scheduler draining, and cancellation-aware I/O remain pending; existing shutdown destroys task futures. |
| Capabilities | `File`, `Socket`, `Clock`, and `Spawn` implemented; broader hooks and optional extension boundaries remain proposed. |
| Linked operations | Prepared chains and a portable `Linked` capability remain proposed; native SQE linking, whole-chain blocking jobs, and readiness sequencing are not implemented. |
| File operations | Owned-buffer positioned read/write implemented; stateful I/O, lifecycle, allocation and metadata hooks pending. |
| Socket operations | TCP registration, connect, accept, read/write and readiness implemented; general sockets and message I/O pending. |
| Backends | Shared native readiness, blocking file fallbacks, a Linux io_uring selector, and an adapter to an existing Tokio runtime implemented. |
| Buffers | Owned `Vec<u8>` implemented; generic owned-buffer traits, fixed registered leases, and provided/software pools remain proposed. |
| Buffer forwarding | `with_previous_buffer` is hypothetical, with no selected implementation target. |
| Optimization | Dedicated io_uring selector uses an unbounded command channel and a completion channel per operation; pooling and submission backpressure pending. |
| Timers | Existing backend timers provide sleep; the io-event timer algorithm port remains planned. |

Next design work is to choose the dependable core and extension boundaries, cancellation-aware error and buffer-return contracts, linked-chain continuation rules, resource lifecycle semantics, and owned-buffer trait safety requirements. Then integrate cooperative cancellation into operations and runtime shutdown before expanding optional optimizations.

The workspace's [architecture guide](https://github.com/socketry/socketry-rust/blob/main/context/design.md) and [implementation guide](https://github.com/socketry/socketry-rust/blob/main/context/implementation.md) provide further details. In the source checkout, `context/gaps.md` records the four architectural gaps. The executor [README](readme.md) documents existing APIs.
