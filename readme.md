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

Workers start when the scheduler is constructed. `spawn` accepts `Send + 'static` futures and returns an awaitable result handle. Dropping that handle leaves the task owned by its scheduler. Dropping the scheduler requests cancellation and joins workers when called outside a worker.

Schedulers and their handles implement the primitive `Spawn` contract. Await individual task handles to observe completion, results, and panics. Task groups can be built above dispatch; the executor does not provide a group abstraction.

`Scheduler::current()` and `Task::current()` provide contextual lookup while tasks run. Use `.await` to suspend; synchronous blocking calls occupy a worker.

See the [executor package](crates/executor/readme.md) for scheduling, ownership, cancellation and allocation details. An executable example is:

```sh
cargo run --package socketry-executor --example work_stealing
```

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

Cancellation expresses intent; task handles confirm completion. Keep the scheduler and I/O services alive while draining. Existing task cancellation and scheduler shutdown still destroy futures; `defer_cancel` cannot protect a future from destruction. Signal handling and cancellation inputs on I/O operations are not yet integrated. There is no signal escalation policy: an external supervisor can enforce SIGTERM followed by SIGKILL.

## Portable I/O and runtime selection

Generic code can accept `Socket`, `File`, `Clock`, and `Spawn` capabilities. Socketry and the optional Tokio adapter implement these contracts with concrete future and resource types. Import the traits to call their methods.

`File` is the scheduler capability trait; `std::fs::File` is the file resource. Alias the resource when using both names:

```rust
use socketry::File;
use std::{fs::File as StdFile, io, sync::Arc};

async fn read_prefix<S: File>(scheduler: &S, file: Arc<StdFile>) -> io::Result<Vec<u8>> {
    let (result, mut buffer) = scheduler.file_read_at(file, vec![0; 4096], 0).await;
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

Socket registrations persist across operations and worker migration. Reads and writes take a reusable owned `Vec<u8>` and return `(io::Result<usize>, Vec<u8>)`. Reads fill the buffer's existing length; allocate it with `vec![0; capacity]`. Operations may transfer fewer bytes than requested. Dropping a future can abandon an operation that has already consumed or transmitted bytes.

Run the same TCP exchange with either runtime:

```sh
cargo run -p socketry-executor --example portable_io
cargo run -p socketry-executor --example portable_io --no-default-features --features tokio
# Linux native completion:
cargo run -p socketry-executor --example portable_io --features io-uring
```

The Tokio adapter needs a live runtime with I/O and time enabled. It preserves scheduler task ownership, and its asynchronous `shutdown().await` joins task destruction. Passing its handle explicitly selects Tokio; Socketry's `Scheduler::current()` continues to identify Socketry execution.

## Current scope

TCP connect/accept/read/write/readiness, positioned file reads/writes, and sleep are implemented. Regular files use blocking pools except for Linux io\_uring. Windows socket readiness uses IOCP/AFD; native overlapped file operations are not implemented. Socketry currently uses async-io's shared readiness reactor and timers. The independent [socketry-timers package](crates/timers/readme.md), re-exported as `socketry::timers`, provides io-event's batched timer queue plus optional worker assistance and lazy sleep futures; integration with the scheduler clock remains future work.

The io\_uring selector owns a dedicated thread, retains buffers until terminal completions, and drains cancellation during shutdown. Connection setup and readiness waits still use async-io. Kernel support is probed when the selector is first needed; failures are returned without silently falling back. Operation pooling, registered buffers, UDP, arbitrary descriptor APIs, and a local `!Send` task executor remain future work.

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
