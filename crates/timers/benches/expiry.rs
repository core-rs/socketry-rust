// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Expiry-only benchmark across sparse and dense batches. Setup and teardown
//! are outside timing; the public-API lifecycle benchmark includes both.

use socketry_timers::Timers;
use std::hint::black_box;
use std::task::Waker;
use std::time::{Duration, Instant};

fn expiry(count: usize, due: usize, batch: bool) -> Duration {
    let now = Instant::now();
    let mut timers = Timers::with_capacity(count);
    let handles: Vec<_> = (0..count)
        .map(|index| {
            let offset = (index * 7919) % count;
            timers.schedule(
                now + Duration::from_nanos(offset as u64),
                Waker::noop().clone(),
            )
        })
        .collect();
    timers.flush();
    let cutoff = now + Duration::from_nanos((due - 1) as u64);
    let mut output = Vec::with_capacity(count);
    let start = Instant::now();
    let fired = if batch {
        let fired = timers.pop_expired_batch(cutoff, &mut output);
        for waker in output.drain(..) {
            waker.wake();
        }
        fired
    } else {
        let mut fired = 0;
        while let Some(waker) = timers.pop_expired(cutoff) {
            waker.wake();
            fired += 1;
        }
        fired
    };
    let elapsed = start.elapsed();
    assert_eq!(fired, due);
    assert_eq!(timers.len(), count - due);
    assert_eq!(
        timers.next_deadline(),
        (due < count).then(|| now + Duration::from_nanos(due as u64))
    );
    black_box(&handles);
    black_box(&output);
    elapsed
}

fn sample(count: usize, due: usize, batch: bool, repeats: usize) -> Duration {
    (0..repeats).map(|_| expiry(count, due, batch)).sum()
}

fn main() {
    let smoke = cfg!(debug_assertions) || std::env::args().any(|argument| argument == "--test");
    let (count, samples, repeats) = if smoke { (40, 1, 1) } else { (10_000, 21, 8) };
    println!(
        "{count} timers, {samples} samples, {repeats} batches/sample; expiry-only ns/delivered timer, setup and teardown excluded"
    );
    println!("expired,individual_ns,batch_ns,individual_p95_ns,batch_p95_ns");
    for due in [1, (count / 100).max(1), count / 10, count / 2, count] {
        expiry(count, due, false);
        expiry(count, due, true);
        let mut individual = Vec::new();
        let mut batch = Vec::new();
        for iteration in 0..samples {
            if iteration % 2 == 0 {
                individual.push(sample(count, due, false, repeats));
                batch.push(sample(count, due, true, repeats));
            } else {
                batch.push(sample(count, due, true, repeats));
                individual.push(sample(count, due, false, repeats));
            }
        }
        individual.sort_unstable();
        batch.sort_unstable();
        let divisor = (due * repeats) as f64;
        let percentile = (samples - 1) * 95 / 100;
        println!(
            "{due},{:.1},{:.1},{:.1},{:.1}",
            individual[samples / 2].as_nanos() as f64 / divisor,
            batch[samples / 2].as_nanos() as f64 / divisor,
            individual[percentile].as_nanos() as f64 / divisor,
            batch[percentile].as_nanos() as f64 / divisor
        );
    }
}
