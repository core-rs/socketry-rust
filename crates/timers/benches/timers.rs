// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Public-API batch benchmark. Run with `cargo bench -p socketry-timers`.

use socketry_timers::Timers;
use std::future::Future;
use std::hint::black_box;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};
use tokio::runtime::{Builder, Runtime};
use tokio::time::Sleep;

#[derive(Clone, Copy)]
enum Case {
    Schedule,
    Shuffled,
    CancelPending,
    CancelRegistered,
    ReplaceCancelled,
    Expire,
    ExpireBatch,
    Churn,
}

impl Case {
    fn name(self) -> &'static str {
        match self {
            Self::Schedule => "register ascending",
            Self::Shuffled => "register shuffled",
            Self::CancelPending => "cancel before registration/flush",
            Self::CancelRegistered => "register + cancel + reclaim",
            Self::ReplaceCancelled => "replace 75% cancelled batch",
            Self::Expire => "register + expire + deliver",
            Self::ExpireBatch => "register + bulk expire + deliver",
            Self::Churn => "20 cancellation batches with live timers",
        }
    }

    fn inputs(self, count: usize) -> usize {
        match self {
            Self::ReplaceCancelled => count * 2,
            Self::Churn => count + 20 * (count / 10),
            _ => count,
        }
    }
}

fn offsets(count: usize, shuffled: bool) -> Vec<Duration> {
    (0..count)
        .map(|index| {
            let slot = if shuffled {
                (index * 7919) % count
            } else {
                index
            };
            Duration::from_secs(60) + Duration::from_millis(slot as u64)
        })
        .collect()
}

fn socketry(case: Case, offsets: &[Duration], waker: &Waker, reserve: bool) {
    let now = Instant::now();
    let count = offsets.len();
    let mut timers = if reserve {
        Timers::with_capacity(case.inputs(count))
    } else {
        Timers::new()
    };
    let handles: Vec<_> = offsets
        .iter()
        .map(|offset| timers.schedule(now + *offset, waker.clone()))
        .collect();
    if !matches!(case, Case::CancelPending) {
        timers.flush();
    }
    match case {
        Case::Schedule | Case::Shuffled => {
            black_box(timers.next_deadline());
        }
        Case::CancelPending | Case::CancelRegistered => {
            for handle in &handles {
                black_box(handle.cancel());
            }
            timers.flush();
            assert!(timers.is_empty());
        }
        Case::ReplaceCancelled => {
            for handle in &handles[..count * 3 / 4] {
                handle.cancel();
            }
            for offset in offsets {
                timers.schedule(now + *offset - Duration::from_secs(30), waker.clone());
            }
            timers.flush();
            assert_eq!(timers.len(), count + count / 4);
        }
        Case::Expire => {
            let mut fired = 0;
            while let Some(waker) = timers.pop_expired(now + expiry_interval(count)) {
                waker.wake();
                fired += 1;
            }
            assert_eq!(fired, count);
        }
        Case::ExpireBatch => {
            let mut output = Vec::with_capacity(count);
            assert_eq!(
                timers.pop_expired_batch(now + expiry_interval(count), &mut output),
                count
            );
            for waker in output {
                waker.wake();
            }
        }
        Case::Churn => {
            for _ in 0..20 {
                let batch: Vec<_> = offsets[..count / 10]
                    .iter()
                    .map(|offset| {
                        timers.schedule(now + *offset + Duration::from_secs(60), waker.clone())
                    })
                    .collect();
                timers.flush();
                for handle in batch {
                    handle.cancel();
                }
                timers.flush();
            }
            assert_eq!(timers.len(), count);
        }
    }
    black_box(&handles);
    black_box(&timers);
}

// Pin the entire slice once, avoiding a Box allocation for each Tokio Sleep.
// Nothing resizes the slice or moves a Sleep after its first poll.
struct TokioTimers(Pin<Box<[Option<Sleep>]>>);

impl TokioTimers {
    fn new(
        offsets: &[Duration],
        base: tokio::time::Instant,
        register: bool,
        waker: &Waker,
    ) -> Self {
        let sleeps: Box<[_]> = offsets
            .iter()
            .map(|offset| Some(tokio::time::sleep_until(base + *offset)))
            .collect();
        let mut timers = Self(Box::into_pin(sleeps));
        if register {
            timers.poll(waker, false);
        }
        timers
    }

    fn entry(&mut self, index: usize) -> Pin<&mut Option<Sleep>> {
        // SAFETY: the boxed slice remains pinned and has a fixed length. We
        // only expose a pinned element; set(None) runs its pinned destructor.
        unsafe { self.0.as_mut().map_unchecked_mut(|slice| &mut slice[index]) }
    }

    fn poll(&mut self, waker: &Waker, ready: bool) {
        let mut context = Context::from_waker(waker);
        for index in 0..self.0.len() {
            let result = self.entry(index).as_pin_mut().unwrap().poll(&mut context);
            assert_eq!(
                result,
                if ready {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            );
        }
    }

    fn cancel(&mut self, count: usize) {
        for index in 0..count {
            self.entry(index).set(None);
        }
    }
}

fn tokio(runtime: &Runtime, case: Case, offsets: &[Duration], waker: &Waker) {
    let count = offsets.len();
    let now = tokio::time::Instant::now();
    let mut timers = TokioTimers::new(offsets, now, !matches!(case, Case::CancelPending), waker);
    match case {
        Case::Schedule | Case::Shuffled => {
            black_box(&timers);
        }
        Case::CancelPending | Case::CancelRegistered => timers.cancel(count),
        Case::ReplaceCancelled => {
            timers.cancel(count * 3 / 4);
            // The earlier replacement batch is still safely in the future.
            black_box(TokioTimers::new(
                offsets,
                now - Duration::from_secs(30),
                true,
                waker,
            ));
        }
        Case::Expire | Case::ExpireBatch => {
            runtime.block_on(async {
                tokio::time::advance(expiry_interval(count)).await;
                // Drive the public timer service to a known completed deadline.
                tokio::time::sleep(Duration::ZERO).await;
            });
            timers.poll(waker, true);
        }
        Case::Churn => {
            for _ in 0..20 {
                let mut batch = TokioTimers::new(
                    &offsets[..count / 10],
                    now + Duration::from_secs(60),
                    true,
                    waker,
                );
                batch.cancel(count / 10);
            }
        }
    }
    black_box(&timers);
}

fn elapsed(mut operation: impl FnMut(), repeats: usize) -> Duration {
    let start = Instant::now();
    for _ in 0..repeats {
        operation();
    }
    start.elapsed()
}

fn expiry_interval(count: usize) -> Duration {
    Duration::from_secs(62) + Duration::from_millis(count as u64)
}

fn main() {
    // Cargo's all-target test run gets a short correctness check of every case.
    let smoke = cfg!(debug_assertions) || std::env::args().any(|argument| argument == "--test");
    let (count, samples, repeats) = if smoke { (40, 1, 1) } else { (10_000, 21, 8) };
    let runtime = Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();
    let _scope = runtime.enter();
    let waker = Waker::noop();
    println!(
        "{count} timers, {samples} samples, {repeats} batches/sample; median ns/input timer (whole batch lifetime)"
    );
    println!(
        "case,socketry_ns,socketry_reserved_ns,tokio_ns,socketry_p95_ns,socketry_reserved_p95_ns,tokio_p95_ns"
    );
    for case in [
        Case::Schedule,
        Case::Shuffled,
        Case::CancelPending,
        Case::CancelRegistered,
        Case::ReplaceCancelled,
        Case::Expire,
        Case::ExpireBatch,
        Case::Churn,
    ] {
        let offsets = offsets(count, matches!(case, Case::Shuffled));
        socketry(case, &offsets, waker, false);
        socketry(case, &offsets, waker, true);
        tokio(&runtime, case, &offsets, waker);
        let mut socketry_samples = Vec::new();
        let mut reserved_samples = Vec::new();
        let mut tokio_samples = Vec::new();
        for sample in 0..samples {
            let mut measure_socketry = || {
                socketry_samples.push(elapsed(|| socketry(case, &offsets, waker, false), repeats))
            };
            let mut measure_reserved = || {
                reserved_samples.push(elapsed(|| socketry(case, &offsets, waker, true), repeats))
            };
            let mut measure_tokio =
                || tokio_samples.push(elapsed(|| tokio(&runtime, case, &offsets, waker), repeats));
            match sample % 3 {
                0 => {
                    measure_socketry();
                    measure_reserved();
                    measure_tokio();
                }
                1 => {
                    measure_reserved();
                    measure_tokio();
                    measure_socketry();
                }
                _ => {
                    measure_tokio();
                    measure_socketry();
                    measure_reserved();
                }
            }
        }
        socketry_samples.sort_unstable();
        reserved_samples.sort_unstable();
        tokio_samples.sort_unstable();
        let divisor = (case.inputs(count) * repeats) as f64;
        let socketry_median = socketry_samples[samples / 2].as_nanos() as f64 / divisor;
        let reserved_median = reserved_samples[samples / 2].as_nanos() as f64 / divisor;
        let tokio_median = tokio_samples[samples / 2].as_nanos() as f64 / divisor;
        let percentile = (samples - 1) * 95 / 100;
        println!(
            "{},{:.1},{:.1},{:.1},{:.1},{:.1},{:.1}",
            case.name(),
            socketry_median,
            reserved_median,
            tokio_median,
            socketry_samples[percentile].as_nanos() as f64 / divisor,
            reserved_samples[percentile].as_nanos() as f64 / divisor,
            tokio_samples[percentile].as_nanos() as f64 / divisor
        );
    }
}
