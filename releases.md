# Releases

## Unreleased

- Split socket capabilities into `SocketOperation` for the shared resource type and individual core operation traits, with `SocketScheduler` as an automatic aggregate. Separate message send/receive and typed option get/set, retaining `SocketMessages` and `SocketOptions<O>` as automatic aggregates. Optional operations and registration depend only on `SocketOperation`. Implementors migrate to the individual traits; concrete callers import operation traits, qualified resource paths use `SocketOperation::Socket`, and extension consumers explicitly require their setup and cleanup capabilities. Preserve backend behavior and update examples, migration guidance, and conformance tests.

- Split file capabilities into `FileOperation` for the shared resource type and individual operation traits, with `FileScheduler` as a blanket convenience aggregate. Rename the broad `File` and `Socket` traits to `FileScheduler` and `SocketScheduler`. `FileStat` owns the metadata type; optional `FileAllocate` and `FileAdvice` no longer require all file operations. Implementors must migrate `impl File` to the individual traits and rename `impl Socket` to `impl SocketScheduler`; concrete file callers must import the operation traits. Fully qualified associated types move to `FileOperation::File`, `FileStat::Metadata`, and `SocketScheduler::Socket`. Update native, Tokio, simulated implementations, examples, and capability-focused contracts.

- Add optional Linux `FileAdvice` with six `AdviceMode` hints and completion/admission-only `file_advise` forms. Native, Tokio, and io_uring schedulers use bounded cleanup admission, retain descriptors through execution, and drain accepted jobs during graceful shutdown. Include simulator contracts and a commented example.

- Add required `File::file_stat` and associated `File::Metadata: FileMetadata`, returning owned status with portable optional accessors. Readiness/Tokio retain standard metadata; io_uring requires `Statx` support and retains native output inside its request allocation. Add shared contracts, completion-lifetime tests, and a commented example. External `File` implementations must add the associated type and operation.

- Add optional `AddressResolution` with hostname/service inputs, native resolver hints, typed resolver errors, and owned address/type/protocol candidates. Support unspecified transports, raw sockets, and platform-specific socket values. Native and Tokio share their bounded blocking pools; started lookups preserve their actual result after cancellation. Add simulated fixtures, shared conformance/lifetime tests, and hostname resolution in the echo client.

- Add optional `SocketMessages` with owned scatter/gather send/receive, IP addresses, peeking, routing flags, and truncation metadata. Native readiness, Tokio, and io_uring share the buffer/cancellation contract; include conformance tests and a commented UDP example.

- Add typed `SocketOptions<O>` get/set capabilities with `SocketReuseAddress`, `SocketKeepalive`, and `TcpNoDelay`. Native, Tokio, io_uring, and simulated backends support lazy cancellable configuration; generic consumers require each option at compile time. Add shared contract tests and a commented listener configuration example.

- Add required `Socket::socket_shutdown`, `socket_shutdown_submit`, `socket_close`, and `socket_close_submit` across native, Tokio, io_uring, and simulated sockets. Directional shutdown borrows the descriptor; close consumes exclusive ownership. Accepted cleanup shares bounded admission and graceful draining with file close. Add a commented half-close example and conformance/lifetime tests. External `Socket` implementations must supply all four operations.

- Add required `File::file_close` and `file_close_submit`, owned non-`Clone` native/Tokio `FileDescriptor` resources, bounded cleanup admission, and graceful cleanup draining. Finish outstanding operations before explicit close; abandoned backend use causes a panic rather than implicit cancellation or joining. Add a commented cancellation/close example and release io_uring file references before completion wakeups. External `File` implementations must add both methods; concrete callers using `Arc<std::fs::File>` must migrate to `FileDescriptor`.

- Add optional `FileAllocate` with `AllocationMode::Extend`/`KeepSize`, cancellation, and cursor preservation. Linux native and Tokio backends reserve space with `fallocate`; io\_uring uses native requests when supported. Other targets omit the capability, and filesystem errors remain observable.

- Replace separate TCP listener resources and combined listen/connect helpers with one associated socket descriptor and syscall-shaped `socket_open`, `socket_bind`, `socket_listen`, `socket_connect`, and `socket_accept` operations. Add IPv4/IPv6 stream and datagram selection with optional TCP/UDP protocol selection. `Socket` trait implementors must migrate to the new resource and operation methods.

- Add required `File::file_truncate` to resize writable files with cancellation, zero-filled extension, and cursor preservation. Use native io\_uring truncation when supported, otherwise blocking file pools. External `File` implementations must provide this method.

- Add required `File::file_sync` with `SyncMode::All`/`Data`, cancellation, and completion/error reporting. io\_uring uses native synchronization; readiness and Tokio use blocking file pools. External `File` implementations must provide this method.

- Add required `File::file_seek` with `std::io::SeekFrom`, explicit cancellation, and the resulting byte offset across native, Tokio, io\_uring, and simulated files. External `File` implementations must provide this method.

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
