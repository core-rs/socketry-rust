# Scheduler Gaps

Socketry has a working future executor, explicit task ownership, portable I/O capabilities, and a Tokio adapter. These four gaps describe the remaining architectural work toward a scheduler inspired by [Zig's `std.Io` model](https://ziglang.org/download/0.16.0/release-notes.html#I-O-as-an-Interface), while retaining ordinary Rust futures and `.await`.

## 1. Graceful cancellation needs runtime integration

Task cancellation currently drops the future before its next poll. An active poll must return first. Synchronous destructors run, but the remainder of the async body, including asynchronous cleanup, does not execute.

`Cancellation` and `defer_cancel` now provide runtime-independent cooperative requests and deferred cleanup. Signals can be shared or linked through child signals to shut down the application or an individual service. They signal intent; task handles and barriers still confirm completion.

Integrating these primitives into scheduler shutdown and cancellation-aware I/O remains pending. Graceful shutdown must keep workers, I/O services, and task owners available until draining completes. The intended model has no default forced abort or internal escalation; a process supervisor handles SIGTERM followed by SIGKILL. Existing task cancellation and scheduler shutdown still destroy futures, so callers must request cooperative cancellation and await work first.

## 2. Ownership currently joins direct children

Barriers own and join their direct children. Cancelling a parent can drop its barriers and request child cancellation, but parent completion does not automatically wait for descendants to finish. Callers must explicitly await their barriers when joined child cleanup is required.

`Spawn` makes task submission portable, while creating and controlling barriers still uses runtime-specific APIs. A generic library can spawn children, but cannot express the full group lifecycle through a shared contract. Define portable group creation, admission closure, waiting, and stopping, and decide whether parent completion should include descendant draining.

## 3. The I/O boundary is still narrow

`Socket` covers TCP registration, connect, accept, reads, writes, and readiness. Listener binding happens through `std::net`. `File` accepts an already-open `std::fs::File`, and `Clock` only supplies relative sleep.

Opening resources, DNS, deadlines, synchronization, randomness, and processes remain outside the portable boundary. Expand capabilities as real consumers need them, keeping the application's implementation choice explicit. A broader boundary would also allow more operating-system behavior to be substituted in deterministic tests.

## 4. The backends establish behavior before performance

The default selector delegates readiness and timers to async-io's shared reactor. Linux io\_uring uses a dedicated selector thread and retains buffers until original operations complete; cancellation completions alone do not release them.

The io\_uring implementation currently uses an unbounded command channel and a completion channel per operation. Operation records are not pooled, and buffers are not registered with the kernel. Add submission backpressure and measure allocation, contention, throughput, and retained memory before choosing optimizations or making performance claims.

See the [design guide](design.md) and [implementation guide](implementation.md) for the current contracts and implementation sequence.
