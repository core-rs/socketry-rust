// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use socketry_timers::driver::{DriverHandle, TimerDriver, TimerProducer};
use socketry_timers::sharded::{TimerWaiter, WorkerTimers};
use socketry_timers::{SharedTimerProducer, TimerHandle, Timers};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Barrier, mpsc};
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
}
impl Queue for Timers<Instant> {
    type Producer = SharedTimerProducer<Instant>;
    fn producer(&mut self, _: usize) -> Self::Producer {
        self.shared_producer()
    }
    fn poll(&mut self, now: Instant, output: &mut Vec<Instant>) -> usize {
        self.pop_expired_batch(now, output)
    }
}
impl Queue for TimerDriver<Instant> {
    type Producer = TimerProducer<Instant>;
    fn producer(&mut self, capacity: usize) -> Self::Producer {
        self.producer(capacity)
    }
    fn poll(&mut self, now: Instant, output: &mut Vec<Instant>) -> usize {
        self.poll(now, output)
    }
}

fn expected(count: usize, cancel: bool) -> usize {
    if cancel { count / 100 } else { count }
}

fn global(mut queue: impl Queue, workers: usize, count: usize, cancel: bool) -> Duration {
    let producers: Vec<_> = (0..workers).map(|_| queue.producer(count)).collect();
    let mut output = Vec::with_capacity(workers * count);
    let ready = Barrier::new(workers + 1);
    let done = AtomicUsize::new(0);
    let base = Instant::now();
    let deadline = base + Duration::from_secs(30);
    thread::scope(|scope| {
        for mut producer in producers {
            let ready = &ready;
            let done = &done;
            scope.spawn(move || {
                ready.wait();
                for index in 0..count {
                    let handle = producer.schedule(deadline);
                    if cancel && index % 100 != 99 {
                        assert!(handle.cancel());
                    }
                    drop(handle);
                }
                done.fetch_add(1, Ordering::Release);
            });
        }
        let start = Instant::now();
        ready.wait();
        let mut delivered = 0;
        while done.load(Ordering::Acquire) != workers {
            delivered += queue.poll(if cancel { base } else { deadline }, &mut output);
            black_box(&output);
            output.clear();
            thread::yield_now();
        }
        delivered += queue.poll(deadline, &mut output);
        let elapsed = start.elapsed();
        assert_eq!(delivered, workers * expected(count, cancel));
        elapsed
    })
}

fn local(workers: usize, count: usize, cancel: bool, watched: bool) -> Duration {
    let (views_tx, views_rx) = mpsc::channel();
    let ready = Barrier::new(workers + 1);
    let done = AtomicUsize::new(0);
    let base = Instant::now();
    let deadline = base + Duration::from_secs(30);
    thread::scope(|scope| {
        let mut threads = Vec::with_capacity(workers);
        for _ in 0..workers {
            let ready = &ready;
            let done = &done;
            let views_tx = views_tx.clone();
            threads.push(scope.spawn(move || {
                // Construct worker-owned storage on its worker, outside timing.
                let mut owner = WorkerTimers::with_capacity(count);
                let mut output = Vec::with_capacity(count); // Outside startup barrier.
                views_tx.send(owner.assistance()).unwrap();
                ready.wait();
                let mut delivered = 0;
                for index in 0..count {
                    let handle = owner.try_schedule(deadline, deadline).unwrap();
                    if cancel && index % 100 != 99 {
                        assert!(handle.cancel());
                    }
                    drop(handle);
                    if index % 64 == 63 {
                        delivered += owner.poll(if cancel { base } else { deadline }, &mut output);
                        black_box(&output);
                        output.clear();
                    }
                }
                delivered += owner.poll(deadline, &mut output);
                assert_eq!(delivered, expected(count, cancel));
                done.fetch_add(1, Ordering::Release);
                (owner, output)
            }));
        }
        let peers: Vec<_> = (0..workers).map(|_| views_rx.recv().unwrap()).collect();
        // Measure one registered observer's notification cost as well. Healthy
        // owners still service their queues; this observer adds no lock polling.
        let _waiter = watched.then(|| TimerWaiter::new(&peers));
        let start = Instant::now();
        ready.wait();
        while done.load(Ordering::Acquire) != workers {
            thread::yield_now();
        }
        let elapsed = start.elapsed();
        for worker in threads {
            drop(worker.join().unwrap());
        }
        elapsed
    })
}

fn stalled(count: usize, assist: bool) -> Vec<Duration> {
    let mut owner = WorkerTimers::with_capacity(count);
    let peer = owner.assistance();
    let waiter = TimerWaiter::new(std::slice::from_ref(&peer));
    let ready = Barrier::new(2);
    let owner_done = AtomicBool::new(false);
    let mut output = Vec::with_capacity(count);
    let mut lateness = Vec::with_capacity(count);
    thread::scope(|scope| {
        let ready_ref = &ready;
        let owner_done_ref = &owner_done;
        let parked_owner = scope.spawn(move || {
            let mut output = Vec::with_capacity(count);
            let mut times = Vec::with_capacity(count);
            ready_ref.wait();
            let deadline = Instant::now() + Duration::from_millis(2);
            for _ in 0..count {
                owner.try_schedule(deadline, deadline).unwrap();
            }
            // Deliberately stall outside all queue operations, without flushing.
            thread::sleep(Duration::from_millis(20));
            owner.poll(Instant::now(), &mut output);
            let delivered = Instant::now();
            times.extend(
                output
                    .into_iter()
                    .map(|deadline| delivered.saturating_duration_since(deadline)),
            );
            owner_done_ref.store(true, Ordering::Release);
            times
        });
        ready.wait();
        if assist {
            while lateness.len() < count {
                peer.try_poll(Instant::now(), &mut output);
                let delivered = Instant::now();
                lateness.extend(
                    output
                        .drain(..)
                        .map(|deadline| delivered.saturating_duration_since(deadline)),
                );
                if owner_done.load(Ordering::Acquire) {
                    break;
                }
                if lateness.len() < count {
                    waiter.park();
                }
            }
        }
        lateness.extend(parked_owner.join().unwrap());
    });
    assert_eq!(lateness.len(), count);
    lateness
}

fn main() {
    let smoke = cfg!(debug_assertions) || std::env::args().any(|arg| arg == "--test");
    let (count, samples) = if smoke { (100, 1) } else { (100_000, 11) };
    println!("{count} timers/worker, {samples} samples; setup excluded; wall ns/input timer");
    println!(
        "workload,workers,shared_ns,driver_ns,sharded_ns,sharded_watched_ns,shared_p95_ns,driver_p95_ns,sharded_p95_ns,sharded_watched_p95_ns"
    );
    for cancel in [false, true] {
        for workers in [1, 2, 4, 8] {
            let mut times = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
            for sample in 0..samples {
                for offset in 0..times.len() {
                    let implementation = (sample + offset) % times.len();
                    let elapsed = match implementation {
                        0 => global(
                            Timers::with_capacity(workers * count),
                            workers,
                            count,
                            cancel,
                        ),
                        1 => global(TimerDriver::new(), workers, count, cancel),
                        _ => local(workers, count, cancel, implementation == 3),
                    };
                    times[implementation].push(elapsed);
                }
            }
            for values in &mut times {
                values.sort_unstable();
            }
            let divisor = (workers * count) as f64;
            print!(
                "{},{workers},{:.1},{:.1},{:.1},{:.1}",
                if cancel { "cancel_99" } else { "burst" },
                times[0][samples / 2].as_nanos() as f64 / divisor,
                times[1][samples / 2].as_nanos() as f64 / divisor,
                times[2][samples / 2].as_nanos() as f64 / divisor,
                times[3][samples / 2].as_nanos() as f64 / divisor
            );
            for values in &times {
                print!(
                    ",{:.1}",
                    values[(samples - 1) * 95 / 100].as_nanos() as f64 / divisor
                );
            }
            println!();
        }
    }
    println!("stalled_owner,assisted,timers,p50_late_us,p99_late_us,max_late_us");
    for assist in [false, true] {
        let mut times = Vec::new();
        for _ in 0..if smoke { 1 } else { 3 } {
            times.extend(stalled(if smoke { 4 } else { 1000 }, assist));
        }
        times.sort_unstable();
        let percentile =
            |percent: usize| times[(times.len() - 1) * percent / 100].as_secs_f64() * 1e6;
        println!(
            "20ms,{assist},{},{:.1},{:.1},{:.1}",
            times.len(),
            percentile(50),
            percentile(99),
            percentile(100)
        );
    }
}
