# Timer benchmarks

Reusable queue slots eliminate per-timer allocations. Adaptive batch expiry also avoids repeated heap repair when many timers are due. With reserved capacity, the measured registration, registered-cancellation and bulk-expiry batches are faster than Tokio; strictly ordered individual expiry remains slower. These results measure the independent queue, before integration with a scheduler clock.

## Results

Measured on 2026-10-12, macOS 27.0.1, `aarch64-apple-darwin`, Rust 1.98.1 (Homebrew, LLVM 22.1.8), optimized Cargo bench profile, Tokio 1.53.2 from the workspace lockfile. Each workload uses 10,000 initial timers, 21 samples and eight complete batches per sample. Timings below are median nanoseconds per input timer for the entire batch lifetime, including queue construction, reservation when applicable and destruction. Lower is better.

| Workload | Socketry growing | Socketry reserved | Tokio |
| --- | ---: | ---: | ---: |
| Register ascending | 22.9 | 20.8 | 26.9 |
| Register shuffled | 24.6 | 22.2 | 26.5 |
| Cancel before registration/flush | 22.6 | 20.4 | 6.5 |
| Register, cancel and reclaim | 24.1 | 22.0 | 27.4 |
| Replace a 75% cancelled batch | 22.9 | 21.4 | 26.7 |
| Twenty cancellation batches with live timers | 25.3 | 24.2 | 26.0 |
| Register, individually expire and deliver | 57.8 | 55.4 | 41.9 |
| Register, bulk expire and deliver | 22.5 | 20.2 | 41.8 |

The [initial implementation](initial-timers.csv) took 50.8 ns/timer for ascending registration, 52.6 for registered cancellation and reclamation, and 106.2 for ordered expiry. Reusable slots reduced these times substantially; the [slot-only snapshot](slots-timers.csv) preserves the measurements before batch expiry was added. In the current run, reserved bulk expiry takes 20.2 ns/input timer for the complete lifecycle, compared with 55.4 for individual pops and 41.8 for Tokio: approximately 2.7× and 2.1× faster, respectively. Absolute timings and Tokio's expiry result vary between runs. Batch results have unspecified delivery order, while individual Socketry pops preserve exact deadline order.

The original design allocated a shared registration and protected its payload with a mutex for every timer. The new design stores values in a reusable slot arena and deadlines directly in queue entries, sharing one queue mutex. This removes per-timer allocation and repeated registration-lock acquisition during heap operations. The combined measurements demonstrate an improvement, without isolating allocation, synchronization and locality costs individually.

Individual expiry repairs the binary heap for every returned value and acquires the shared lock for each `pop_expired` call. Batch expiry acquires the lock and flushes once, counts due entries up to an estimated crossover, then chooses individual heap pops or filters the heap and heapifies survivors once. Counting prunes future subtrees and uses only O(log n) stack space. The crossover compares roughly k × log2(n) repairs with an O(n) rebuild; this is a cost heuristic rather than a measured universal threshold. When all timers expire, there are no survivors to heapify. Tokio's cancellation before first polling remains much cheaper because an unpolled sleep has not registered with its timer service.

## Sparse and dense expiry

The separate expiry-only benchmark keeps 10,000 timers in the queue, with shuffled nanosecond deadlines, and varies the number due. Setup, output reservation, registration, initial flushing, handle destruction and queue destruction are outside timing. Timing includes expiry and no-op wakes. Values below are median nanoseconds per delivered timer; the denominator differs from the whole-lifecycle table above.

| Due timers | Individual pops | Adaptive batch |
| --- | ---: | ---: |
| 1 (0.01%) | 93.9 | 104.1 |
| 100 (1%) | 60.2 | 40.3 |
| 1,000 (10%) | 62.7 | 34.9 |
| 5,000 (50%) | 55.7 | 8.2 |
| 10,000 (100%) | 46.7 | 4.7 |

Batch expiry improves dense draining by approximately 10× in this run. A single due timer costs about 10 ns more because batch selection and counting add overhead. Sparse batches retain individual heap repair, while sharing one lock acquisition and flush. This comparison combines batching, heap strategy and output-storage costs; it does not isolate their individual contributions. Both variants prepare the same output capacity outside timing, and retain handles until after timing. The smoke run uses fewer timers. Raw results and approximate 95th percentiles are in [expiry results](expiry.csv).

## Allocations and capacity

The separate allocation benchmark measures one 10,000-timer batch, then reuses the reserved queue for expiry:

| Operation | Allocation/reallocation calls | Cumulative requested bytes |
| --- | ---: | ---: |
| Socketry growing registration, cancellation and reclamation | 42 | 4,069,768 |
| Socketry reserved queue setup | 5 | 1,120,200 |
| Socketry reserved cancellation and reclamation | 0 | 0 |
| Socketry reserved reuse and expiry | 0 | 0 |
| Socketry reserved bulk expiry | 0 | 0 |
| Tokio runtime setup | 13 | 8,392 |
| Tokio registration, cancellation and reclamation | 2 | 960,064 |

Requested bytes include reallocation requests and are not peak or retained memory. The [initial allocation snapshot](initial-allocations.csv) recorded 20,016 calls and 1,862,144 requested bytes for Socketry. Growing mode now makes far fewer allocation calls, but requests more cumulative bytes because the larger slot arena and queue buffers grow geometrically. Reserving once avoids that growth.

Zero-allocation lifecycle windows exclude queue setup and the caller's handle vector, which is reserved beforehand (approximately 400 KB for 10,000 handles on this target). Bulk expiry also reserves its output vector outside the allocation-counting window (approximately 160 KB for 10,000 wakers). The growing Socketry and Tokio batch rows include their caller-owned storage. These rows therefore illustrate allocation scope rather than equivalent total-memory footprints. User values, wakers and their destructors can allocate independently; this benchmark uses a no-op waker.

`with_capacity` and `reserve` reserve the arena, pending buffer and heap and initialize the shared lock. Within that capacity, scheduling, cancellation, flushing, compaction, expiry, clearing, handle cloning and slot reuse make no allocation calls. An allocation-counting regression test exercises these paths repeatedly. `try_schedule` never grows queue storage and returns the supplied value when full; `schedule` grows when necessary.

Capacity counts occupied registrations, including completed timers with surviving handles and cancelled entries awaiting reclamation. A slot becomes reusable only after its queue entry and last handle are gone. This keeps status queries valid and prevents stale cancellation after reuse, at the cost of retaining slots for outstanding handles. `clear` retains buffers; handles keep shared storage alive even after queue destruction.

`pop_expired_batch` appends only within the output vector's spare capacity and never grows it. A full output leaves the queue unchanged. Batch values have unspecified order; when output capacity is limited, earlier due timers may remain queued while later ones are returned. The caller processes returned values after unlocking. All values in a returned batch have already been claimed, so later cancellation cannot stop their delivery.

## Methodology

Both sides use identical precomputed deadline offsets and the same no-op waker. Deadlines start 60 seconds ahead, spaced one millisecond apart. The shuffled workload permutes indices with `(index * 7919) % count`. Deadline offsets and Tokio's current-thread, time-enabled runtime are created outside timing. Sample order rotates between growing Socketry, reserved Socketry and Tokio, after one warmup batch per implementation. The timing executable uses the normal allocator; a separate executable instruments allocation calls.

Socketry schedules each waker, retains its handle, and flushes the batch. The reserved variant reserves the number of input timers before scheduling: 10,000 for ordinary cases, 20,000 for replacement and 30,000 for churn. This setup occurs inside timing, so the timing rows include initial allocations even though reuse within reserved capacity is allocation-free. Tokio constructs `Sleep` values, pins the complete slice once, and polls each sleep to register it. There is no box per Tokio sleep. The comparison includes the APIs' different storage and ownership costs. All batch state is destroyed within timing. Socketry cancellation also flushes the queue, charging for deferred reclamation. The unregistered cancellation workload deliberately omits polling/flushing until cancellation.

Replacement cancels 75% of the registered initial batch, then registers a full batch with earlier deadlines, triggering Socketry compaction. Results are divided by 20,000 input timers. Churn keeps 10,000 live timers while registering and cancelling twenty later batches of 1,000 timers in the same queue/runtime. Results are divided by 30,000 input timers; this exercises incremental insertion, retained cancellations, compaction and eventual destruction.

Expiry explicitly supplies elapsed time to Socketry and wakes every returned value. The bulk lifecycle case creates its output vector inside timing, so the added caller storage is charged to the batch API. Tokio uses paused time, advances beyond every deadline, drives its timer service with a zero-duration sleep, and polls every original sleep to confirm completion; the two expiry rows use the same Tokio operation and measure it separately. The public driver boundary is included for Tokio. Real wall-clock waiting is excluded. Assertions confirm registration and completion instead of merely timing future construction. Debug all-target test runs execute a small correctness check of all three benchmark executables.

The queue preserves `Instant` precision. These workloads use millisecond deadlines compatible with both APIs. This benchmark measures timer bookkeeping rather than wakeup latency, contention, application throughput or a completed Socketry `Clock::sleep` implementation. It was run on one development machine without CPU isolation; rerun on deployment hardware before relying on absolute timings.

## Reproduction

From the workspace root:

```sh
cargo bench -p socketry-timers --bench timers --locked
cargo bench -p socketry-timers --bench allocations --locked
cargo bench -p socketry-timers --bench expiry --locked
```

Keep the lockfile and run optimized timing measurements without other builds or heavy workloads in parallel. The timing executables print medians and approximate 95th percentiles. Raw lifecycle and allocation results are stored in [timer results](timers.csv) and [allocation results](allocations.csv); the `initial-*` files retain the original baseline and `slots-timers.csv` records reusable slots before batch expiry.

Verification passed timer tests, timer doctests, workspace Clippy with the Tokio feature and warnings denied, and formatting. The crate's Bake coverage gate passed all 565 measured library source regions (100%) and ran doctests and benchmark smoke checks. Workspace tests and doctests also passed before the batch-expiry addition. The code uses portable standard-library Rust; local execution verifies macOS AArch64.
