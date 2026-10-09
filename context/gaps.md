# Scheduler Gaps

Socketry has a working future executor, scheduler-owned tasks, portable I/O capabilities, and a Tokio adapter. These four gaps describe the remaining architectural work toward a scheduler inspired by [Zig's `std.Io` model](https://ziglang.org/download/0.16.0/release-notes.html#I-O-as-an-Interface), while retaining ordinary Rust futures and `.await`.

## 1. Graceful cancellation needs runtime integration

Task cancellation currently drops the future before its next poll. An active poll must return first. Synchronous destructors run, but the remainder of the async body, including asynchronous cleanup, does not execute.

`Cancellation` and `defer_cancel` now provide runtime-independent cooperative requests and deferred cleanup. Signals can be shared or linked through child signals to shut down the application or an individual service. They signal intent; task handles still confirm completion.

Integrating these primitives into scheduler shutdown and cancellation-aware I/O remains pending. Graceful shutdown must keep workers, I/O services, and the scheduler available until draining completes. The intended model has no default forced abort or internal escalation; a process supervisor handles SIGTERM followed by SIGKILL. Existing task cancellation and scheduler shutdown still destroy futures, so callers must request cooperative cancellation and await work first.

## 2. Higher-level task grouping is deferred

`Spawn::spawn(future)` is primitive dispatch shared by schedulers and their handles. It registers a scheduler-owned task and returns a handle. Completing or cancelling the spawning task does not cancel independently spawned work; callers await individual handles when completion matters.

The executor does not expose a Barrier or require a group/owner argument. Libraries can eventually provide groups above this primitive, coordinating cooperative cancellation and completion without adding an implicit hierarchy to dispatch. A portable higher-level group lifecycle remains unspecified and is outside the executor for now.

## 3. The I/O boundary is still narrow

`Socket` covers TCP registration, connect, accept, reads, writes, and readiness. Listener binding happens through `std::net`. `File` accepts an already-open `std::fs::File`, and `Clock` only supplies relative sleep.

Opening resources, DNS, deadlines, synchronization, randomness, and processes remain outside the portable boundary. Expand capabilities as real consumers need them, keeping the application's implementation choice explicit. A broader boundary would also allow more operating-system behavior to be substituted in deterministic tests.

## 4. The backends establish behavior before performance

The default selector delegates readiness and timers to async-io's shared reactor. Linux io\_uring uses a dedicated selector thread and retains buffers until original operations complete; cancellation completions alone do not release them.

The io\_uring implementation currently uses an unbounded command channel and a completion channel per operation. Operation records are not pooled, and buffers are not registered with the kernel. Add submission backpressure and measure allocation, contention, throughput, and retained memory before choosing optimizations or making performance claims.

See the [design guide](design.md) and [implementation guide](implementation.md) for the current contracts and implementation sequence.
