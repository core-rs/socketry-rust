# Releases

## Unreleased

- Add shared-cursor `File::file_read` and `file_write` with owned buffers and cancellation across native, Tokio, io\_uring, and simulated files. Implementers of `File` must now provide both methods; callers await operations sequentially when cursor ordering matters.

- Add `Blocking::spawn_blocking` with cancellable bounded admission and owning completion handles. Native and Tokio schedulers use configurable dedicated pools; queued work can be cancelled, while started closures retain their inputs and finish independently.

- Extend `Clock` with monotonic `now`, cancellable `sleep_until`, and relative `sleep(duration, cancellation)`. Preserve lazy waits and Tokio virtual time; report deadline overflow as `InvalidInput`.

- Add commented echo client and server examples, demonstrating owned buffers, partial transfers, and cooperative SIGINT shutdown.

- Make task handles own execution lifetimes: moving a handle transfers ownership, while dropping it requests destruction after any active poll. Await explicitly for completion. Signal guards retain their subscription task handles.

- Add associated file resources, `file_open`, portable `FileOptions`, TCP listener creation and address inspection. Move OS socket importing to `SocketRegistration`. Add `IoBuf`, `IoBufMut`, owned `BufferSlice` views and explicit file/socket cancellation with `OperationError`; return buffers only after backend access ends. The generic example now uses the public traits across native, Tokio, and simulated I/O.

- Document the complete timer lifecycle comparison with Tokio across cancellation rates in the timer crate readme.

- Add an executable scheduler-interface experiment with generic file-copy and echo-server functions running on native, Tokio, and in-memory backends. Record resource creation, buffer-range, blocking execution, and backend cleanup requirements in the executor design.

- Add `Notify<Value>` and the `Signals` scheduler capability with associated signal and registration types. The optional Unix `signals` feature supports cancellation and queued notifications on Socketry and Tokio; dropping the returned guard removes its subscription without waiting for callbacks already in progress.

- Add the independent `socketry-timers` crate and expose it as `socketry::timers`. Include reusable batched queues, optional worker assistance and lazy sleep futures, plus the experimental inbox driver and Tokio lifecycle benchmarks.

- Remove `Barrier`, scheduler barrier factories, and `SpawnError::OwnerClosed` from `socketry-executor` and the facade. Use primitive `spawn(future)` dispatch and await task handles; higher-level task groups are deferred.

- Rename the portable socket operations trait from `Network` to `Socket` in `socketry`, `socketry-executor`, and their public `scheduler` module. Update imports and trait bounds.

- Rename the portable file operations trait from `FileIo` to `File` in `socketry`, `socketry-executor`, and their public `scheduler` module. Update imports and trait bounds; alias `std::fs::File` when both names are used.

- Add runtime-independent `Cancellation`, `Cancelled`, and `defer_cancel` for cooperative service shutdown and asynchronous draining. Export them through `socketry-executor` and `socketry`.

- Let Cargo select the dependency version in the installation example.

## v0.2.0

- Rename the positioned file I/O trait from `FileIO` to `FileIo` in `socketry` and `socketry-executor`, including the public `scheduler` module. Update imports and trait bounds.

## v0.1.5

- Cover io\_uring initialization failures, readiness retries, cancellation, and shutdown while making request ownership invariants explicit.
- Expose the conventional `FileIO` name with `FileIo` compatibility aliases, extract portable scheduler contracts, and require coverage for every supported platform and feature implementation.

## v0.1.4

- Adopt `socketry-project` 0.3.7 for shared project tasks and Markdown normalization.
- Require the aggregate test and coverage result for pull request merges.

## v0.1.3

- Use the shared Socketry Project tasks and update agent context setup guidance.

## v0.1.2

- Use the shared `socketry-project` Releasing skill for the standard release process and remove references to the duplicate Bake Cargo publishing context.

## v0.1.1

- Create or update GitHub Releases after successful crates.io publication.
- Move implementation and design guidance into the package's public context.
- Add Bake Agent Context tasks to the repository's development workspace.
