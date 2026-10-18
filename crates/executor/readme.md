# socketry-executor

Owned future tasks and a work-stealing scheduler for Socketry's Rust packages. Use `socketry` as the common entry point, or depend on this package directly.

The [executor design](design.md) records the agreed direction for cooperative cancellation, scheduler capabilities, portable I/O, and owned buffers, distinguishing implemented APIs from proposals.

## Execution

- `Scheduler::new()` starts workers using available hardware parallelism.
- `Scheduler::with_workers(count)` selects a nonzero worker count explicitly.
- `scheduler.spawn(future)` registers an owned task and schedules it immediately.
- `handle.await` returns `Result<Output, TaskError>`.
- `scheduler.block_on(future)` polls a root future on the calling thread. This root can borrow local data and need not be Send. It is not a spawned task.
- `scheduler.run()` waits until all owned future tasks finish; it leaves admission open. Await blocking jobs explicitly.
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

## Address resolution

Use the optional `AddressResolution` capability to resolve a hostname before creating or connecting a socket:

```rust
use socketry_executor::{AddressHints, AddressInfo, AddressResolution, AddressService, Cancellation, OperationResult, SocketType};

async fn addresses<S: AddressResolution>(scheduler: &S, cancellation: &Cancellation) -> OperationResult<Vec<AddressInfo>> {
    scheduler.address_resolve(
        Some("localhost"),
        AddressService::Port(9000),
        AddressHints { socket_type: Some(SocketType::Stream), ..AddressHints::default() },
        cancellation,
    ).await
}
```

Native and Tokio schedulers and their handles call the system resolver through their existing bounded blocking pools, sharing capacity with `spawn_blocking`. Hints select IP family, socket type, protocol, passive binding, numeric-only hosts, and configured families during lookup. Services can be ports or names. An absent hostname selects loopback addresses, or wildcard addresses with `passive`. Each result carries its address, socket type, and protocol for `socket_open`; unspecified types can include raw sockets and platform-specific native values. Results preserve resolver order and duplicates. Applications choose connection candidates and retry policy explicitly.

Resolver failures retain typed `AddressResolveError` details inside `OperationError::Io`, including a portable category and native resolver code. Validation and pool errors remain ordinary I/O errors. An unpolled future does no work. Cancellation can prevent admission or queued execution; once a system lookup starts, its actual result is returned. Dropping a future abandons its result while running work retains its owned inputs and capacity until completion. Await lookup before shutting down the scheduler for graceful completion. The simulator supplies deterministic name and service fixtures. Canonical names, reverse lookup, IDNA normalization guarantees, caching, and connection racing remain separate concerns.

## Blocking work

Use `Blocking::spawn_blocking` for an owned synchronous closure that should run on a dedicated thread. Both schedulers and their handles implement this optional capability; generic libraries can require `S: Blocking`.

```rust
use socketry_executor::{BlockingOptions, Cancellation, Scheduler};

let scheduler = Scheduler::with_workers_and_blocking(
    2,
    BlockingOptions { threads: 4, capacity: 64 },
)?;
let shutdown = Cancellation::new();
let input = String::from("owned by the blocking job");
scheduler.block_on(async {
    // This await suspends until there is room for the job.
    let job = scheduler.spawn_blocking(move || input.len(), &shutdown).await?;
    // This await receives the closure's result.
    let length = job.await?;
    assert!(length > 0);
    Ok::<(), Box<dyn std::error::Error>>(())
})?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Admission is lazy and cancellable. The default pool has four threads and capacity for 64 queued plus running jobs; both configurable limits must be nonzero. Threads start on first admission. Waiters use wakeups, and future workers remain available while synchronous jobs execute. The Tokio adapter configures its dedicated pool through `Scheduler::with_blocking(runtime_handle, options)`.

Moving a job handle transfers ownership. Dropping it requests skipping if still queued; cancellation is checked again immediately before execution. Queued cancellation can wait for a worker to reach the job. Once running, a closure retains its inputs and capacity until it finishes and returns its actual result. For cooperation during execution, capture a clone of `shutdown` and check it inside the closure. Panics become `TaskError::Panicked`.

Scheduler shutdown rejects new admission and discards queued jobs. Started closures finish independently, without a thread join or forced interruption. For graceful shutdown, request cancellation and await job handles before dropping the scheduler or services they use. `Scheduler::run()` does not wait for independent blocking jobs. Existing file-I/O fallbacks retain their backend pools.

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

These primitives do not change `Task::cancel` or scheduler shutdown, which still destroy futures. File and socket operations accept explicit cancellation inputs and return `OperationError::Cancelled` separately from OS errors. Clock waits also accept explicit cancellation and return `OperationResult<()>`. Cooperative requests have no forced abort or deadline escalation; a process supervisor can enforce SIGTERM followed by SIGKILL.

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

- `native` (default) supplies nonblocking IPv4/IPv6 stream and datagram sockets through async-io's process-wide reactor: epoll on Linux, kqueue on Apple/BSD, and IOCP/AFD socket readiness on Windows. The three platform modules expose the shared implementation; they do not duplicate its registration machinery.
- `io-uring` selects a dedicated Linux completion selector for socket and file reads/writes. Connection setup, accept, readiness and timers use async-io. On other supported platforms this feature leaves the platform default intact.
- `tokio` adds an adapter using a supplied `tokio::runtime::Handle`. It uses Tokio `AsyncFd` on Unix. Windows uses async-io's process-wide reactor for generic socket readiness because Tokio has no matching generic socket wrapper; this fallback is not driven by Tokio's I/O driver. The adapter does not construct or drive a runtime. Enable that runtime's I/O and time facilities for the operations that use it.
- With default features disabled, the executor and public contracts still build. Enabling only `tokio` omits the native blocking and io\_uring backends; async-io remains a dependency for Tokio's generic Windows socket-readiness fallback.

`Scheduler` and `SchedulerHandle` implement the traits. `file_open` returns `File::File`; `socket_open` returns the backend's single associated socket resource, `Socket::Socket`. Call `socket_bind` and then `socket_listen(socket, backlog, cancellation)` to listen, or `socket_connect(socket, address, cancellation)` to connect an opened socket. `socket_accept` returns another resource of the same type. Native and Tokio use non-`Clone` `SocketDescriptor` values. Importing existing standard-library TCP resources uses the separate optional `SocketRegistration` trait. Returned operation futures are Send. Registrations remain with their original reactor as tasks migrate; they are not re-created per poll. Tokio resources passed to an adapter for another runtime return InvalidInput.

The initial API selects IPv4 or IPv6, stream or datagram, and an optional TCP or UDP protocol. Connected datagram reads and writes preserve message boundaries. The higher-level `Socket` trait deliberately does not provide protocol buffering: line, frame, and message semantics belong in protocol adapters built above it. Generic static dispatch is the default, with dynamic dispatch available to consumers that need runtime selection or type erasure.

Read/write operations take an owned `IoBuf` (`IoBufMut` for reads) and return `BufferResult<B> = (OperationResult<usize>, B)`, including errors and cancellation. `OperationError` distinguishes `Cancelled` and `Io`. `Vec<u8>` and `Box<[u8]>` are supported. `buffer.slice(range)` creates an owned `BufferSlice` without copying; `into_inner()` recovers its original owner for reuse. Read buffers must have an initialized length (`vec![0; capacity]`); capacity alone supplies no writable bytes. Buffer length is unchanged; only the first returned byte count contains new data. Reads and writes can be partial. Each read/write call is one operation, not a read-exact/write-all convenience method.

`File::file_read` and `file_write` borrow `&Self::File` and advance its shared cursor by the transferred byte count. `file_read_at` and `file_write_at` take an explicit offset instead. Await cursor operations sequentially when ordering matters; concurrent calls have no portable ordering or non-overlap guarantee. Dropping an admitted operation can still advance the cursor. Each call can transfer fewer bytes than requested, and a read returns zero at EOF. `file_open(path, FileOptions, cancellation)` creates the associated resource. Current native/Tokio backends internally retain an `Arc<std::fs::File>`; generic callers do not require that representation. The readiness and Tokio implementations use blocking pools; io\_uring uses current-position SQEs for stateful access. There is no scheduler-side cursor or extra per-file locking. Use ordinary files opened without append mode, and offsets fitting i64. Unix positioned operations leave the shared cursor unchanged; the Windows blocking fallback updates it according to std's seek\_read/seek\_write semantics.

`file_stat(file, cancellation)` queries an open file without changing its cursor. It returns owned `File::Metadata`; import `FileMetadata` to use its portable accessors. Fields may be unavailable, and timestamps may be unrepresentable, so accessors return `Option`. Results can reflect concurrent modifications and are not atomic snapshots. Await preceding operations when ordering matters. Querying requires no write permission, and the result remains usable after the file is closed.

```rust
use socketry_executor::{Cancellation, File, FileMetadata, OperationResult};

async fn size<S: File>(scheduler: &S, file: &S::File, cancellation: &Cancellation) -> OperationResult<Option<u64>> {
    let metadata = scheduler.file_stat(file, cancellation).await?;
    Ok(metadata.size())
}
```

Readiness and Tokio retain `std::fs::Metadata` in `SystemFileMetadata`; `as_std()` exposes native extensions. Linux io_uring returns `StatxMetadata`, exposing `as_statx()` and preserving the native validity mask. Its output lives inside the existing request allocation and is returned by value after completion, with no separate metadata allocation or eager conversion. Rust may eliminate intermediate copies but this is not guaranteed. The io_uring backend requires `Statx` support at initialization. See the [commented metadata example](examples/file_stat.rs). Path lookup and symlink-following policies remain separate from this descriptor-based operation.

`file_seek(file, std::io::SeekFrom, cancellation)` repositions the shared cursor and returns its resulting `u64` offset. Use `Start(offset)` for an absolute position, `Current(delta)` for a relative move, or `End(delta)` for a position relative to EOF. `Current(0)` queries the position. Seeking past EOF does not extend the file; absolute offsets must fit in `i64`, and invalid positions or filesystem limits return errors. Seeks use blocking file pools on every current native backend, including io\_uring, and Tokio's blocking pool in the adapter. Cancellation before execution prevents seeking; a started seek preserves its actual result. Await seeks and other cursor operations sequentially when their ordering matters. A seek/read pair is not atomic with respect to other users of the resource.

`file_truncate(file, length, cancellation)` sets the logical file length. It discards bytes when shrinking and zero-fills an extension, leaving the shared cursor unchanged even if it is now past EOF. The file must be open for writing; lengths must fit in `i64`, and filesystem limits and OS errors are reported. Await related operations in order. Truncation does not guarantee disk-space reservation or durability; await `file_sync` afterward when needed.

Truncation uses native io\_uring requests when supported by the running kernel, with the blocking file fallback otherwise. Readiness and Tokio use their existing file-operation pools. Cancellation is checked before dispatch and again by blocking workers before execution; started blocking calls preserve their result. Ring cancellation is best effort and waits for the original completion. Dropping a waiter does not undo a resize or release a file still in use by the backend.

```rust,no_run
use socketry_executor::{Cancellation, File, OperationResult};

async fn resize<S: File>(
    scheduler: &S,
    file: &S::File,
    length: u64,
    cancellation: &Cancellation,
) -> OperationResult<()> {
    scheduler.file_truncate(file, length, cancellation).await
}
```

`FileAllocate` is an optional capability for reserving backing space. Native and Tokio schedulers implement it on Linux; other targets currently omit the implementation at compile time. `AllocationMode::Extend` reserves a range and extends the logical length if necessary. `KeepSize` reserves it without changing the length, including beyond EOF. Both modes preserve existing contents and the shared cursor. Length must be positive and `offset + length` must fit in `i64`.

The Linux implementation uses `fallocate` on the existing file pools, or a native io\_uring request when its opcode is available. Filesystem errors, including unsupported allocation, are preserved. There is no zero-writing or resize-only emulation. Cancellation and file retention follow the same completion contract as synchronization. Allocation does not unshare copy-on-write extents or guarantee durability; await related operations in order. Platform-specific operations can also be called explicitly through `spawn_blocking`.

```rust,no_run
use socketry_executor::{AllocationMode, Cancellation, FileAllocate, OperationResult};

async fn reserve<S: FileAllocate>(
    scheduler: &S,
    file: &S::File,
    cancellation: &Cancellation,
) -> OperationResult<()> {
    scheduler.file_allocate(file, 0, 1024 * 1024, AllocationMode::KeepSize, cancellation).await
}
```

`file_sync(file, SyncMode, cancellation)` waits for OS synchronization and reports its result. `SyncMode::All` synchronizes contents and metadata; `SyncMode::Data` can omit metadata unrelated to retrieving file contents. Backends may provide full synchronization for either mode. Await preceding writes first: sync does not order concurrent writes, flush application buffers, or synchronize the parent directory entry. It leaves the shared cursor unchanged. Durability follows the OS, filesystem, and device guarantees.

io\_uring uses native `Fsync` requests; readiness and Tokio use their existing blocking file pools. Cancellation before dispatch prevents submission, and blocking workers check again before starting. Once submitted, cancellation is best effort; a successful completion wins concurrent cancellation. Dropping a waiter does not undo the operation or release a file still in use by the backend.

```rust,no_run
use socketry_executor::{Cancellation, File, OperationResult, SyncMode};

async fn synchronize<S: File>(
    scheduler: &S,
    file: &S::File,
    cancellation: &Cancellation,
) -> OperationResult<()> {
    scheduler.file_sync(file, SyncMode::All, cancellation).await
}
```

Repeated calls to `file_read` continue from the position reached by the previous call. For example, this helper returns one chunk, which can be shorter than 4096 bytes:

```rust
use socketry_executor::{Cancellation, File, OperationResult};

async fn read_chunk<S: File>(
    scheduler: &S,
    file: &S::File,
    cancellation: &Cancellation,
) -> OperationResult<Vec<u8>> {
    let (result, mut buffer) = scheduler.file_read(file, vec![0; 4096], cancellation).await;
    buffer.truncate(result?);
    Ok(buffer)
}
```

To rewind an existing file and read from its beginning:

```rust
use socketry_executor::{Cancellation, File, OperationResult};
use std::io::SeekFrom;

async fn read_header<S: File>(
    scheduler: &S,
    file: &S::File,
    cancellation: &Cancellation,
) -> OperationResult<Vec<u8>> {
    scheduler.file_seek(file, SeekFrom::Start(0), cancellation).await?;
    let (result, mut buffer) = scheduler.file_read(file, vec![0; 16], cancellation).await;
    buffer.truncate(result?);
    Ok(buffer)
}
```

`Clock::now()` returns a monotonic `std::time::Instant` in the selected scheduler's clock domain. `sleep_until(deadline, cancellation)` and `sleep(duration, cancellation)` return `OperationResult<()>`. Relative waits start on first poll; pre-cancelled waits do not register, even for elapsed deadlines. Overflow returns `InvalidInput`. After registration, expiry wins a same-poll cancellation race. Obtain time through the scheduler to preserve simulation and Tokio paused-time behaviour. Native waits use async-io timers; the adapter uses its selected Tokio runtime. The independent `socketry-timers` crate implements the io-event timer algorithm; integration with the scheduler clock remains pending. The independent `Blocking` capability supplies general synchronous jobs as described above.

### Explicit file close

`File::file_close(file).await` consumes the handle, waits for bounded admission and OS completion, and reports close errors. `file_close_submit(file).await` waits only for admission; accepted work finishes even if its waiter is dropped, and graceful scheduler shutdown drains it. Neither form takes a cancellation token. Close does not synchronize data and does not retry an OS close error. `_submit` execution errors are discarded in the current backends.

Native and Tokio resources are owned, non-`Clone` `FileDescriptor` values with private backend retention. Existing concrete callers using `Arc<std::fs::File>` must instead move an OS file into `FileDescriptor::from(file)` or use `file_open`. `AsRef<std::fs::File>` permits synchronous platform access; generic consumers continue to use `S::File`. External `File` implementations must provide both close methods.

Finish outstanding operations before explicit close. Request cancellation, keep awaiting the operation, then close; dropping its future alone does not establish that the kernel or blocking worker has finished. Closing a handle still retained by an abandoned operation panics. Ordinary `Drop` is a fallback that releases ownership and discards close errors without joining outstanding operations.

The [commented example](examples/file_close.rs) explains shared cancellation clones, completion racing with cancellation, buffer ownership, and closing before propagating the operation result:

```sh
cargo run -p socketry-executor --example file_close --features tokio
```

Both close forms initially use a dedicated bounded cleanup pool, separate from arbitrary `spawn_blocking` closures. Each pool uses the configured `BlockingOptions` limits and starts workers lazily. Native io_uring close SQEs are a later optimization.

### Socket shutdown and close

`socket_shutdown(&socket, std::net::Shutdown::Write).await` finishes the local sending direction while keeping the socket available to receive a response. Use `Read` or `Both` for the other shutdown modes. Await preceding writes first. Shutdown borrows the descriptor; `socket_close(socket).await` consumes it and reports OS completion. Neither operation accepts cancellation. Repeated shutdown and later I/O follow platform-specific behavior, and local completion does not confirm remote application receipt.

Both have exceptional `_submit` forms which await only bounded admission. Accepted work survives abandonment and shares the dedicated cleanup pool and graceful shutdown draining used for file close. Execution errors are discarded by `_submit`. Await ordinary shutdown before explicitly closing the same socket: submission alone does not establish completion, and retained backend use causes close to panic. No per-descriptor operation registry or implicit cancellation/join is added.

Socket descriptors remain non-`Clone`; private references retain the registration for submitted shutdown and io_uring work. Deregistration and close execute on the cleanup worker. On Windows, the exclusively owned socket is made blocking there before close to accommodate positive linger. Ordinary `Drop` remains best-effort cleanup without joining backend work.

The [commented half-close example](examples/socket_shutdown.rs) demonstrates a request followed by write shutdown, a response in the opposite direction, and explicit close after completion:

```sh
cargo run -p socketry-executor --example socket_shutdown --features tokio
```

External `Socket` implementations must provide `socket_shutdown`, `socket_shutdown_submit`, `socket_close`, and `socket_close_submit`.

### Typed socket options

Require `SocketOptions<O>` for each option your generic code needs. `SocketReuseAddress`, `SocketKeepalive`, and `TcpNoDelay` are typed identifiers with boolean values, implemented by native and Tokio schedulers/handles and the native selectors. This is an extension of `Socket`; existing implementations need not support every option.

```rust,no_run
use socketry_executor::{Cancellation, OperationResult, SocketOptions, TcpNoDelay};

async fn configure<S: SocketOptions<TcpNoDelay>>(
    scheduler: &S,
    socket: &S::Socket,
    cancellation: &Cancellation,
) -> OperationResult<()> {
    scheduler.socket_set_option(socket, TcpNoDelay, true, cancellation).await?;
    assert!(scheduler.socket_get_option(socket, TcpNoDelay, cancellation).await?);
    Ok(())
}
```

Both operations are lazy and cancellable before execution. Await setters before dependent operations such as bind/connect. OS errors and option semantics remain visible; keepalive uses system timing defaults, and address reuse and accepted-socket inheritance vary by platform. The capability does not add socket locking or atomic get/set transactions.

Run the [commented listener configuration example](examples/socket_options.rs) with `cargo run -p socketry-executor --example socket_options --features tokio`.

The optional `SocketMessages` capability provides `socket_sendmsg`/`socket_recvmsg` with owned scatter/gather buffers, IP addresses, portable flags, and receive truncation metadata. See the [commented UDP message example](examples/socket_messages.rs). Ancillary data remains a separate extension.


### Cancellation and shutdown

Pre-cancelled file/socket operations do not start. Readiness waits stop on cancellation. io\_uring requests cancellation but waits for the original completion before returning the buffer; successful progress wins a racing cancellation. Blocking jobs check cancellation again on their pool worker, before execution; a running syscall returns its actual result.

Dropping a read/write future abandons its result. A submitted operation may still consume or transmit bytes. Ownership of the buffer and file/socket continues until kernel access ends. Await a result when the byte count matters.

The io\_uring selector probes required opcodes and completion-overflow support. It returns initialization errors rather than silently switching backend. Cancellation requests and original completions have distinct identifiers; only an original terminal completion releases the operation's resources. Socketry shutdown outside a worker joins tasks and drains its ring. Dropping the scheduler on a worker requests cancellation without blocking that worker.

Readiness resources use a process-wide reactor, which is not shut down with an individual Socketry scheduler. An already-started blocking file operation can continue after its waiting task is cancelled. The Tokio adapter's shutdown joins its owned tasks, not the external runtime or its blocking-operation pool.

An unexpected io\_uring selector failure after submission cannot return memory whose kernel lifetime is unknown. That exceptional path retains the affected resources and panics the waiting operation; it does not free in-flight buffers.

### Costs and remaining work

Readiness registrations and buffers are reused. io\_uring currently uses a dedicated selector thread, a command channel, one boxed request for its concrete buffer type, and one completion channel per operation; it does not pool operation records or register buffers. The Tokio file fallback retains an Arc/Mutex buffer owner to return the buffer even when a queued blocking job is cancelled during runtime shutdown. Neither fallback copies the buffer bytes just to transfer ownership.

Native overlapped Windows file operations, raw descriptor import beyond std TCP, socket message/address APIs, operation and buffer pools, and scheduler integration with `socketry-timers` remain future work. There is no thread-local non-Send task facility. No I/O throughput claim has been established by benchmarks.

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
