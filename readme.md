# `socketry`

Foundational concurrency APIs for Rust projects in the Socketry ecosystem.

The `socketry` package re-exports `socketry-executor`: owned asynchronous tasks and a futures executor with multiple workers and work stealing. Tasks use ordinary future polling and have no private coroutine stacks.

## Usage

```sh
cargo add socketry
```

```rust
use socketry::{Scheduler, yield_now};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scheduler = Scheduler::with_workers(4)?;
    let task = scheduler.spawn(async {
        yield_now().await;
        42
    })?;

    let answer = scheduler.block_on(task)?;
    assert_eq!(answer, 42);
    Ok(())
}
```

Workers start when the scheduler is constructed. `spawn` accepts `Send + 'static` futures and returns an owning task handle. Moving it transfers ownership; dropping it requests task destruction after any active poll returns. Dropping the scheduler requests cancellation and joins workers when called outside a worker.

Schedulers and their handles implement the primitive `Spawn` contract. Await individual task handles to observe completion, results, and panics. Task groups can be built above dispatch; the executor does not provide a group abstraction.

`Scheduler::current()` and `Task::current()` provide contextual lookup while tasks run. Use `.await` to suspend; synchronous blocking calls occupy a worker.

See the [executor package](crates/executor/readme.md) for scheduling, ownership, cancellation and allocation details. An executable example is:

```sh
cargo run --package socketry-executor --example work_stealing
```

`AddressResolution` adds explicit hostname lookup through the scheduler, with numeric or named services, native resolver hints, typed resolver errors, and owned address/type/protocol candidates. Unspecified transports can include raw sockets and native socket values. Native and Tokio use bounded blocking execution; the [echo client](examples/echo/client.rs) resolves `localhost` before connecting.

## Cooperative cancellation

`Cancellation` signals a shutdown request without destroying futures. Clones share the request; `child()` creates a boundary that receives parent cancellation without cancelling its parent or siblings. Use one root for application shutdown and children for independently stoppable services. Dropping a signal does not cancel it.

Work can await `signal.cancelled()` or use `signal.check()` at explicit cancellation points, returning `Cancelled`. Waiting uses wakers and does not busy-wait. `Cancellation::never()` supplies an allocation-free input for work that cannot be cancelled through its signal.

`defer_cancel(&signal, future, on_cancel)` calls the synchronous callback once when cancellation is observed and continues awaiting the future's normal output. The callback requests graceful shutdown; the future performs asynchronous draining. These primitives work with standard futures on Socketry or Tokio, without a runtime dependency.

```rust
use socketry::{Cancellation, Scheduler, defer_cancel, yield_now};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scheduler = Scheduler::with_workers(2)?;
    let shutdown = Cancellation::new();
    let server_shutdown = shutdown.child();
    let server = scheduler.spawn(async move {
        let drain = Cancellation::new();
        defer_cancel(&server_shutdown, async {
            drain.cancelled().await;
            // Finish accepted work while the scheduler remains available.
            yield_now().await;
            42
        }, || { drain.cancel(); }).await
    })?;

    shutdown.cancel(); // Request shutdown after receiving a process signal.
    assert_eq!(scheduler.block_on(server)?, 42);
    scheduler.shutdown();
    Ok(())
}
```

Cancellation expresses intent; task handles confirm completion. Keep the scheduler and I/O services alive while draining. Existing task cancellation and scheduler shutdown still destroy futures; `defer_cancel` cannot protect a future from destruction. File and socket operations take explicit cancellation inputs; clock cancellation remains proposed. There is no signal escalation policy: an external supervisor can enforce SIGTERM followed by SIGKILL.

The optional `signals` feature adds Unix `Signals::signal_notify(&signals, target)` to both scheduler implementations. Targets implement `Notify<Value>`: pass a `Cancellation` for shutdown or an unbounded channel sender for queued events such as reload requests. Keep the returned `SignalRegistration` alive and drop it to unsubscribe; already-admitted callbacks may finish. Callbacks run outside raw signal handlers. See the executor's [signal notification contract](crates/executor/readme.md#signal-notifications) for examples, lifetime rules, and process-wide signal disposition behavior.

## Portable I/O and runtime selection

Generic code can accept `SocketScheduler`, `FileScheduler`, `Clock`, and `Spawn` capabilities. Socketry and the optional Tokio adapter implement these contracts with concrete future and resource types. File consumers can require individual operations such as `FileRead + FileWrite`; import those operation traits for method calls on concrete schedulers.

`file_close` waits for OS completion; `file_close_submit` waits only for bounded admission. Both consume an owned file handle and require prior operations to have finished. The [commented file-close example](crates/executor/examples/file_close.rs) demonstrates requesting cancellation, awaiting completion, and then closing safely.

`socket_shutdown(&socket, how)` supports directional shutdown while retaining the descriptor. `socket_close(socket)` consumes it after outstanding work has finished. Both have admission-only `_submit` forms. The [commented half-close example](crates/executor/examples/socket_shutdown.rs) sends a request, shuts down writes, receives the response, then closes.

`SocketOptions<O>` adds typed, cancellable option get/set. Require `SocketReuseAddress`, `SocketKeepalive`, or `TcpNoDelay` as needed; the option selects its value type and backend capability. The [listener configuration example](crates/executor/examples/socket_options.rs) shows configuration before binding.

`FileStat::file_stat` queries an open descriptor and returns owned, native-backed metadata through portable `FileMetadata` accessors. The result remains usable after close; the operation leaves the cursor unchanged. See the [commented metadata example](crates/executor/examples/file_stat.rs).

The optional `SocketMessages` capability provides `socket_sendmsg`/`socket_recvmsg` with owned scatter/gather buffers, IP addresses, portable flags, and receive truncation metadata. See the [commented UDP message example](crates/executor/examples/socket_messages.rs). Ancillary data remains a separate extension.


`FileAllocate` adds optional range allocation with `AllocationMode::Extend` or `KeepSize`. Native and Tokio implementations currently provide it on Linux; unsupported targets omit the trait implementation. See the [executor I/O documentation](crates/executor/readme.md#io-and-selectors) for allocation and filesystem guarantees.

`FileScheduler` aggregates the core file capabilities; `FileOperation::File` supplies their shared associated resource type. Generic code can create and use files without importing OS resource types:

```rust
use socketry::{Cancellation, FileScheduler, FileOptions, OperationResult};
use std::path::Path;

async fn read_prefix<S: FileScheduler>(scheduler: &S, path: &Path, cancellation: &Cancellation) -> OperationResult<Vec<u8>> {
    let file = scheduler.file_open(path, FileOptions::READ, cancellation).await?;
    let (result, mut buffer) = scheduler.file_read_at(&file, vec![0; 4096], 0, cancellation).await;
    buffer.truncate(result?);
    Ok(buffer)
}
```

| Cargo configuration | Implementation |
| --- | --- |
| Default (`native`) | Socketry tasks; epoll, kqueue or Windows IOCP/AFD socket readiness through async-io. |
| `features = ["io-uring"]` on Linux | Native completion reads/writes through an io_uring selector. |
| `features = ["tokio"]` | Also expose `socketry::scheduler::tokio::Scheduler`, adapting an existing Tokio runtime. |
| `default-features = false, features = ["tokio"]` | Tokio adapter without Socketry's native I/O dependencies. |
| `default-features = false` | Task executor and portable contracts, without I/O implementations. |

Socket registrations persist across operations and worker migration. Reads and writes accept owned `IoBuf`/`IoBufMut` storage and return `(OperationResult<usize>, B)`, preserving buffers on cancellation and errors. `Vec<u8>`, `Box<[u8]>`, and owned slice views are supported. Reads fill the buffer's existing length; allocate it with `vec![0; capacity]`. Operations may transfer fewer bytes than requested. Dropping a future can abandon an operation that has already consumed or transmitted bytes.

Run the same TCP exchange with either runtime:

```sh
cargo run -p socketry-executor --example portable_io
cargo run -p socketry-executor --example portable_io --no-default-features --features tokio
# Linux native completion:
cargo run -p socketry-executor --example portable_io --features io-uring
```

The Tokio adapter needs a live runtime with I/O and time enabled. Its task handles preserve transferable execution ownership, and its asynchronous `shutdown().await` joins task destruction. Passing its handle explicitly selects Tokio; Socketry's `Scheduler::current()` continues to identify Socketry execution.

## Current scope

The commented [echo client and server](examples/echo/readme.md) demonstrate TCP I/O, explicit socket open/connect/listen steps, owned buffer reuse, and cooperative SIGINT shutdown using the public scheduler interface.

Nonblocking IPv4/IPv6 stream and datagram socket creation, bind, listen, connect, accept, read/write, and readiness are implemented, along with positioned file reads/writes and sleep. Regular files use blocking pools except for Linux io\_uring. Windows socket readiness uses IOCP/AFD; native overlapped file operations are not implemented. Socketry currently uses async-io's shared readiness reactor and timers. The independent [socketry-timers package](crates/timers/readme.md), re-exported as `socketry::timers`, provides io-event's batched timer queue plus optional worker assistance and lazy sleep futures; integration with the scheduler clock remains future work.

The io\_uring selector owns a dedicated thread, retains buffers and descriptors until terminal completions, and drains cancellation during shutdown. Socket setup, bind, listen, connect, accept, and readiness waits still use async-io. Kernel support is probed when the selector is first needed; failures are returned without silently falling back. Message/address socket APIs, registered buffers, arbitrary descriptor APIs, and a local `!Send` task executor remain future work.

The former coroutine implementation is preserved on branch `coroutine`, at commit `b520f3d`. Its native sources, stack allocation, nested synchronous `wait` and task transfer are absent from the future executor.

## Releasing

Prepare a release with `cargo bake cargo:version:patch` (or `minor`, `major`, or `bump --version X.Y.Z`), then run `cargo bake cargo:release` and open a pull request. After review and merge, GitHub Actions publishes the release when the configured `crates-io` environment approves it, then creates or updates the matching GitHub Release from `releases.md`. See the shared [Releasing skill](https://github.com/socketry/socketry-project-rust/blob/main/context/releasing.md) for the standard release process.

## Releases

<!-- bake-readme:releases:start -->

See [releases.md](releases.md) for the full release history.

### v0.2.0

- Rename the positioned file I/O trait from `FileIO` to `FileIo` in `socketry` and `socketry-executor`, including the public `scheduler` module. Update imports and trait bounds.

### v0.1.5

- Cover io\_uring initialization failures, readiness retries, cancellation, and shutdown while making request ownership invariants explicit.
- Expose the conventional `FileIO` name with `FileIo` compatibility aliases, extract portable scheduler contracts, and require coverage for every supported platform and feature implementation.

### v0.1.4

- Adopt `socketry-project` 0.3.7 for shared project tasks and Markdown normalization.
- Require the aggregate test and coverage result for pull request merges.

<!-- bake-readme:releases:end -->

## See Also

- [`socketry-executor`](https://github.com/socketry/socketry-rust/tree/main/crates/executor).

## Contributing

Please open an issue or pull request on [GitHub](https://github.com/socketry/socketry-rust).

### Agent Context

Run `cargo bake agent:context:install` to install shared context and skills. Read `.agents/context/index.md` to find relevant guides, follow `agents.md` if present, and apply skills under `.agents/skills/`. The installer preserves repository-owned `agents.md`; it does not create or regenerate that file.

[Agent Context guide]: https://github.com/socketry/bake-agent-context-rust/blob/main/context/agent-context.md

The crate publishes [implementation](context/implementation.md) and [design](context/design.md) guides for its architecture and development.
