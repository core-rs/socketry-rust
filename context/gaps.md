# Scheduler Gaps

Socketry has a working future executor, owning task handles, portable I/O capabilities, and a Tokio adapter. These four gaps describe the remaining architectural work toward a scheduler inspired by [Zig's `std.Io` model](https://ziglang.org/download/0.16.0/release-notes.html#I-O-as-an-Interface), while retaining ordinary Rust futures and `.await`.

## 1. Graceful shutdown requires explicit coordination

Task cancellation currently drops the future before its next poll. An active poll must return first. Synchronous destructors run, but the remainder of the async body, including asynchronous cleanup, does not execute.

`Cancellation` and `defer_cancel` now provide runtime-independent cooperative requests and deferred cleanup. Signals can be shared or linked through child signals to shut down the application or an individual service. They signal intent; task handles still confirm completion.

File and socket I/O operations now accept these signals. Explicit file/socket close omits cancellation, rejects outstanding use, and uses bounded cleanup admission; socket shutdown retains the borrowed descriptor. Graceful scheduler shutdown drains accepted cleanup requests. Applications explicitly await tasks they want to finish; abandoning an owning task handle requests destruction. Graceful shutdown must keep workers, I/O services, and the scheduler available until draining completes. Cooperative cancellation has no automatic forced abort or internal escalation; a process supervisor handles SIGTERM followed by SIGKILL. Existing task cancellation and scheduler shutdown still destroy futures, so callers must request cooperative cancellation and await work first.

## 2. Higher-level task grouping is deferred

`Spawn::spawn(future)` is primitive dispatch shared by schedulers and their handles. It registers a task and returns its owning handle. Moving that handle transfers its lifetime; destroying a task drops handles it still owns; callers await individual handles when completion matters.

The executor does not expose a Barrier or require a group/owner argument. Libraries can eventually provide groups above this primitive, coordinating cooperative cancellation and completion without adding an implicit hierarchy to dispatch. A portable higher-level group lifecycle remains unspecified and is outside the executor for now.

## 3. The I/O boundary is still narrow

`Socket` uses one associated resource type for unconnected, bound, listening, connected, and datagram sockets. It exposes `socket_open`, `socket_bind`, syscall-like `socket_listen`, `socket_connect`, `socket_accept`, address inspection, reads, writes, and readiness. The initial API supports IPv4/IPv6 and stream/datagram sockets with optional TCP/UDP protocol selection. OS-resource import is a separate `SocketRegistration` capability. Socket close/shutdown and their admission-only variants are implemented; typed option get/set supports address reuse, keepalive, and TCP no-delay; `SocketMessages` implements owned scatter/gather payloads, addresses, peeking, routing flags, and receive truncation; ancillary data and further options remain future work. `File` opens associated resources and borrows them for seeking, truncation, synchronization, shared-cursor and positioned I/O. Explicit close/submission now consumes an exclusively owned handle, and optional `FileAllocate` supplies Linux allocation. Metadata hooks remain to implement. File/socket operations support cancellation and owned buffer views. `Clock` supplies monotonic time and cancellable absolute and relative waits.

`Blocking` now adds bounded asynchronous admission for owned synchronous closures, implemented through dedicated native and Tokio pools. `AddressResolution` uses the same bounded pools for system hostname lookup and returns owned addresses with a numeric port and optional family filter; the simulator uses an explicit table. Started system lookups finish independently of cancellation. Existing file fallbacks still use backend pools. Ancillary data, further socket options, randomness, and processes remain outside the portable boundary. Deadline policies can now compose `Clock` and cancellation; higher-level timeout helpers remain separate. Ordinary synchronization uses futures and wakers above scheduler primitives. Expand capabilities as real consumers need them, keeping the application's implementation choice explicit. A broader boundary would also allow more operating-system behavior to be substituted in deterministic tests.

## 4. The backends establish behavior before performance

The default selector delegates readiness and timers to async-io's shared reactor. Linux io\_uring uses a dedicated selector thread and retains buffers until original operations complete; cancellation completions alone do not release them.

The io\_uring implementation currently uses an unbounded command channel, a boxed typed request, and a completion channel per operation. Operation records are not pooled, and buffers are not registered with the kernel. Add submission backpressure and measure allocation, contention, throughput, and retained memory before choosing optimizations or making performance claims.

See the [design guide](design.md) and [implementation guide](implementation.md) for the current contracts and implementation sequence.
