// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Concurrent queue bookkeeping, followed by a separate parked-driver latency
//! experiment. No scheduler integration or Tokio contention comparison implied.
use socketry_timers::driver::{DriverHandle, TimerDriver, TimerProducer};
use socketry_timers::{SharedTimerProducer, TimerHandle, Timers};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

trait Handle {
    fn cancel(&self) -> bool;
}
impl Handle for TimerHandle<Instant> {
    fn cancel(&self) -> bool {
        self.cancel()
    }
}
impl Handle for DriverHandle<Instant> {
    fn cancel(&self) -> bool {
        self.cancel()
    }
}
trait Producer: Send {
    type Handle: Handle;
    fn schedule(&mut self, deadline: Instant) -> Self::Handle;
}
impl Producer for SharedTimerProducer<Instant> {
    type Handle = TimerHandle<Instant>;
    fn schedule(&mut self, deadline: Instant) -> Self::Handle {
        self.try_schedule(deadline, deadline).unwrap()
    }
}
impl Producer for TimerProducer<Instant> {
    type Handle = DriverHandle<Instant>;
    fn schedule(&mut self, deadline: Instant) -> Self::Handle {
        self.try_schedule(deadline, deadline).unwrap()
    }
}
trait Queue {
    type Producer: Producer;
    fn producer(&mut self, capacity: usize) -> Self::Producer;
    fn poll(&mut self, now: Instant, output: &mut Vec<Instant>) -> usize;
    fn park(&mut self);
}
impl Queue for Timers<Instant> {
    fn park(&mut self) {
        match self.next_deadline() {
            Some(deadline) => {
                thread::park_timeout(deadline.saturating_duration_since(Instant::now()))
            }
            None => thread::park(),
        }
    }
    type Producer = SharedTimerProducer<Instant>;
    fn producer(&mut self, _: usize) -> Self::Producer {
        self.shared_producer()
    }
    fn poll(&mut self, now: Instant, output: &mut Vec<Instant>) -> usize {
        self.pop_expired_batch(now, output)
    }
}
impl Queue for TimerDriver<Instant> {
    fn park(&mut self) {
        self.park();
    }
    type Producer = TimerProducer<Instant>;
    fn producer(&mut self, capacity: usize) -> Self::Producer {
        self.producer(capacity)
    }
    fn poll(&mut self, now: Instant, output: &mut Vec<Instant>) -> usize {
        self.poll(now, output)
    }
}

fn concurrent(
    mut queue: impl Queue,
    workers: usize,
    count: usize,
    cancel: bool,
    paced: bool,
) -> Duration {
    let producers: Vec<_> = (0..workers).map(|_| queue.producer(count)).collect();
    let mut output = Vec::with_capacity(workers * count);
    let ready = Barrier::new(workers + 1);
    let done = AtomicUsize::new(0);
    let base = Instant::now();
    let deadline = base + Duration::from_secs(30);
    // Queue/pool setup, OS thread creation and output reservation are excluded.
    thread::scope(|scope| {
        for mut producer in producers {
            let ready = &ready;
            let done = &done;
            scope.spawn(move || {
                ready.wait();
                for index in 0..count {
                    let handle = producer.schedule(deadline);
                    if cancel && index % 2 == 0 {
                        assert!(handle.cancel());
                    }
                    drop(handle);
                    if paced && index % 64 == 63 {
                        thread::sleep(Duration::from_micros(100));
                    }
                }
                done.fetch_add(1, Ordering::Release);
            });
        }
        let start = Instant::now();
        ready.wait();
        let mut delivered = 0;
        while done.load(Ordering::Acquire) != workers {
            // Cancellation runs before expiry, so its outcome is deterministic.
            delivered += queue.poll(if cancel { base } else { deadline }, &mut output);
            black_box(&output);
            output.clear();
            thread::yield_now();
        }
        delivered += queue.poll(deadline, &mut output);
        let elapsed = start.elapsed();
        assert_eq!(delivered, workers * if cancel { count / 2 } else { count });
        elapsed
    })
}

struct Wake {
    pending: AtomicBool,
    owner: thread::Thread,
}
impl Wake {
    fn send(&self) {
        if !self.pending.swap(true, Ordering::AcqRel) {
            self.owner.unpark();
        }
    }
}

fn parked_latency(
    mut driver: impl Queue,
    workers: usize,
    count: usize,
    burst: bool,
    shared: bool,
) -> (Vec<Duration>, Duration, usize) {
    let producers: Vec<_> = (0..workers).map(|_| driver.producer(count)).collect();
    let mut output = Vec::with_capacity(workers * count);
    let mut lateness = Vec::with_capacity(workers * count);
    let ready = Arc::new(Barrier::new(workers + 1));
    let submissions = Mutex::new(Vec::with_capacity(workers));
    let mut parks = 0;
    // The baseline queue has no driver notification API. Give it the same
    // per-producer coalescing/unpark policy in this benchmark adapter.
    let wakes: Vec<_> = (0..workers)
        .map(|_| {
            Arc::new(Wake {
                pending: AtomicBool::new(false),
                owner: thread::current(),
            })
        })
        .collect();
    thread::scope(|scope| {
        for (index, mut producer) in producers.into_iter().enumerate() {
            let ready = Arc::clone(&ready);
            let wake = Arc::clone(&wakes[index]);
            let submissions = &submissions;
            scope.spawn(move || {
                ready.wait();
                let started = Instant::now();
                for index in 0..count {
                    // Deadline is relative to actual submission, not thread setup.
                    let deadline = Instant::now() + Duration::from_millis(2);
                    drop(producer.schedule(deadline));
                    if shared {
                        wake.send();
                    }
                    if !burst && index + 1 < count {
                        thread::sleep(Duration::from_millis(1));
                    }
                }
                submissions.lock().unwrap().push((started, Instant::now()));
            });
        }
        ready.wait();
        while lateness.len() < workers * count {
            driver.poll(Instant::now(), &mut output);
            let delivered = Instant::now();
            lateness.extend(
                output
                    .drain(..)
                    .map(|deadline| delivered.saturating_duration_since(deadline)),
            );
            if lateness.len() < workers * count {
                if shared {
                    for wake in &wakes {
                        wake.pending.swap(false, Ordering::AcqRel);
                    }
                }
                parks += 1;
                driver.park();
            }
        }
    });
    lateness.sort_unstable();
    let submissions = submissions.into_inner().unwrap();
    let started = submissions.iter().map(|(start, _)| *start).min().unwrap();
    let finished = submissions.iter().map(|(_, end)| *end).max().unwrap();
    (lateness, finished.duration_since(started), parks)
}

fn main() {
    let smoke = cfg!(debug_assertions) || std::env::args().any(|arg| arg == "--test");
    let (count, samples) = if smoke { (64, 1) } else { (10_000, 11) };
    println!(
        "{count} timers/producer, {samples} samples; setup excluded; total concurrent lifecycle ns/input timer"
    );
    println!("workload,producers,shared_median_ns,driver_median_ns,shared_p95_ns,driver_p95_ns");
    for (name, cancel, paced) in [
        ("burst", false, false),
        ("cancel_half", true, false),
        ("paced", false, true),
    ] {
        for workers in [1, 2, 4, 8] {
            let mut shared = Vec::new();
            let mut driver = Vec::new();
            for iteration in 0..samples {
                let mut run_shared = || {
                    shared.push(concurrent(
                        Timers::with_capacity(workers * count),
                        workers,
                        count,
                        cancel,
                        paced,
                    ))
                };
                let mut run_driver = || {
                    driver.push(concurrent(
                        TimerDriver::new(),
                        workers,
                        count,
                        cancel,
                        paced,
                    ))
                };
                if iteration % 2 == 0 {
                    run_shared();
                    run_driver();
                } else {
                    run_driver();
                    run_shared();
                }
            }
            shared.sort_unstable();
            driver.sort_unstable();
            let divisor = (workers * count) as f64;
            let p95 = (samples - 1) * 95 / 100;
            println!(
                "{name},{workers},{:.1},{:.1},{:.1},{:.1}",
                shared[samples / 2].as_nanos() as f64 / divisor,
                driver[samples / 2].as_nanos() as f64 / divisor,
                shared[p95].as_nanos() as f64 / divisor,
                driver[p95].as_nanos() as f64 / divisor
            );
        }
    }
    println!(
        "parked_workload,implementation,producers,timers,p50_late_us,p95_late_us,p99_late_us,max_late_us,median_submission_span_us,median_park_calls"
    );
    for burst in [false, true] {
        for workers in [1, 2, 4, 8] {
            let count = if smoke {
                4
            } else if burst {
                1000
            } else {
                100
            };
            let trials = if smoke { 1 } else { 3 };
            for shared in [true, false] {
                let mut times = Vec::with_capacity(workers * count * trials);
                let mut spans = Vec::with_capacity(trials);
                let mut parks = Vec::with_capacity(trials);
                for _ in 0..trials {
                    let (mut trial_times, span, calls) = if shared {
                        parked_latency(
                            Timers::with_capacity(workers * count),
                            workers,
                            count,
                            burst,
                            true,
                        )
                    } else {
                        parked_latency(TimerDriver::new(), workers, count, burst, false)
                    };
                    times.append(&mut trial_times);
                    spans.push(span);
                    parks.push(calls);
                }
                times.sort_unstable();
                spans.sort_unstable();
                parks.sort_unstable();
                let percentile =
                    |percent: usize| times[(times.len() - 1) * percent / 100].as_secs_f64() * 1e6;
                println!(
                    "{},{},{workers},{},{:.1},{:.1},{:.1},{:.1},{:.1},{}",
                    if burst { "burst" } else { "sparse" },
                    if shared { "shared" } else { "driver" },
                    times.len(),
                    percentile(50),
                    percentile(95),
                    percentile(99),
                    percentile(100),
                    spans[trials / 2].as_secs_f64() * 1e6,
                    parks[trials / 2]
                );
            }
        }
    }
}
