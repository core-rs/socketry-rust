// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Cancellation churn with an earlier, resident heap, plus a no-cancellation
//! lifecycle control and cancellation sweep. These owner-thread fixtures
//! measure timer bookkeeping; they do not measure worker contention.
use socketry_timers::driver::TimerDriver;
use socketry_timers::{TimerHandle, Timers};
use std::future::Future;
use std::hint::black_box;
use std::pin::Pin;
use std::task::{Context, Waker};
use std::time::{Duration, Instant};
use tokio::runtime::{Builder, Runtime};
use tokio::time::Sleep;

trait Handle {
    fn cancel(&self) -> bool;
}
impl Handle for TimerHandle<Instant> {
    fn cancel(&self) -> bool {
        self.cancel()
    }
}
impl Handle for socketry_timers::driver::DriverHandle<Instant> {
    fn cancel(&self) -> bool {
        self.cancel()
    }
}

trait Queue {
    type Handle: Handle;
    fn schedule(&mut self, deadline: Instant) -> Self::Handle;
    fn flush(&mut self);
    fn expire(&mut self, now: Instant, output: &mut Vec<Instant>) -> usize;
}

impl Queue for Timers<Instant> {
    type Handle = TimerHandle<Instant>;
    fn schedule(&mut self, deadline: Instant) -> Self::Handle {
        self.try_schedule(deadline, deadline).unwrap()
    }
    fn flush(&mut self) {
        self.next_deadline();
    }
    fn expire(&mut self, now: Instant, output: &mut Vec<Instant>) -> usize {
        self.pop_expired_batch(now, output)
    }
}

#[cfg(feature = "sharded")]
impl Queue for socketry_timers::sharded::WorkerTimers<Instant> {
    type Handle = TimerHandle<Instant>;
    fn schedule(&mut self, deadline: Instant) -> Self::Handle {
        self.try_schedule(deadline, deadline).unwrap()
    }
    fn flush(&mut self) {
        self.next_deadline();
    }
    fn expire(&mut self, now: Instant, output: &mut Vec<Instant>) -> usize {
        self.poll(now, output)
    }
}

impl Queue
    for (
        TimerDriver<Instant>,
        socketry_timers::driver::TimerProducer<Instant>,
    )
{
    type Handle = socketry_timers::driver::DriverHandle<Instant>;
    fn schedule(&mut self, deadline: Instant) -> Self::Handle {
        self.1.try_schedule(deadline, deadline).unwrap()
    }
    fn flush(&mut self) {
        self.0.next_deadline();
    }
    fn expire(&mut self, now: Instant, output: &mut Vec<Instant>) -> usize {
        self.0.poll(now, output)
    }
}

fn churn(
    mut queue: impl Queue,
    resident: usize,
    batch: usize,
    rounds: usize,
    percent: usize,
    admitted: bool,
) -> Duration {
    let now = Instant::now();
    let deadline = now + Duration::from_secs(120);
    for _ in 0..resident {
        drop(queue.schedule(now + Duration::from_secs(60)));
    }
    queue.flush();
    let mut handles = Vec::with_capacity(batch);
    let mut output = Vec::with_capacity(resident + batch * rounds);
    let cancelled = batch * percent / 100;
    let start = Instant::now();
    for _ in 0..rounds {
        for _ in 0..batch {
            handles.push(queue.schedule(deadline));
        }
        if admitted {
            queue.flush();
        }
        for handle in &handles[..cancelled] {
            assert!(handle.cancel());
        }
        handles.clear();
        queue.flush();
    }
    let delivered = queue.expire(deadline, &mut output);
    black_box(&output);
    let elapsed = start.elapsed();
    assert_eq!(delivered, resident + (batch - cancelled) * rounds);
    assert!(output.iter().all(|value| *value <= deadline));
    elapsed
}

struct PinnedSleeps(Pin<Box<[Option<Sleep>]>>);

impl PinnedSleeps {
    fn new(capacity: usize) -> Self {
        Self(Box::into_pin((0..capacity).map(|_| None).collect()))
    }

    fn entry(&mut self, index: usize) -> Pin<&mut Option<Sleep>> {
        // SAFETY: the fixed-length slice stays pinned until destruction. Only
        // pinned elements are exposed, and set drops a Sleep in place.
        unsafe { self.0.as_mut().map_unchecked_mut(|slice| &mut slice[index]) }
    }

    fn register(&mut self, index: usize, context: &mut Context<'_>) {
        assert!(
            self.entry(index)
                .as_pin_mut()
                .unwrap()
                .poll(context)
                .is_pending()
        );
    }
}

fn tokio_churn(
    runtime: &Runtime,
    resident: usize,
    batch: usize,
    rounds: usize,
    percent: usize,
    admitted: bool,
) -> Duration {
    let now = tokio::time::Instant::now();
    let deadline = now + Duration::from_secs(120);
    let mut timers = PinnedSleeps::new(resident + batch * rounds);
    let mut live = Vec::with_capacity(resident + batch * rounds);
    let mut output = Vec::with_capacity(resident + batch * rounds);
    let mut context = Context::from_waker(Waker::noop());
    for index in 0..resident {
        timers.entry(index).set(Some(tokio::time::sleep_until(
            now + Duration::from_secs(60),
        )));
        timers.register(index, &mut context);
        live.push(index);
    }
    let cancelled = batch * percent / 100;
    let start = Instant::now();
    for round in 0..rounds {
        let base = resident + round * batch;
        for index in base..base + batch {
            timers
                .entry(index)
                .set(Some(tokio::time::sleep_until(deadline)));
        }
        if admitted {
            for index in base..base + batch {
                timers.register(index, &mut context);
            }
        }
        for index in base..base + cancelled {
            timers.entry(index).set(None); // Pinned destruction deregisters.
        }
        for index in base + cancelled..base + batch {
            if !admitted {
                timers.register(index, &mut context);
            }
            live.push(index);
        }
    }
    runtime.block_on(async {
        // Tokio rounds deadlines to milliseconds. Advance beyond that rounding
        // and drive its public timer service before polling survivors.
        tokio::time::advance(Duration::from_secs(120) + Duration::from_millis(1)).await;
        tokio::time::sleep(Duration::ZERO).await;
    });
    for &index in &live {
        let mut sleep = timers.entry(index).as_pin_mut().unwrap();
        assert!(sleep.as_mut().poll(&mut context).is_ready());
        output.push(sleep.deadline().into_std());
    }
    if resident == 0 {
        // Fixtures without resident timers measure complete future lifecycles,
        // including deregistration and destruction of every surviving sleep.
        for &index in &live {
            timers.entry(index).set(None);
        }
    }
    black_box(&output);
    let elapsed = start.elapsed();
    assert_eq!(output.len(), resident + (batch - cancelled) * rounds);
    assert!(output.iter().all(|value| *value <= deadline.into_std()));
    elapsed
}

#[cfg(feature = "sharded")]
fn lazy_churn(
    resident: usize,
    batch: usize,
    rounds: usize,
    percent: usize,
    admitted: bool,
) -> Duration {
    use socketry_timers::sharded::{SleepWake, WorkerTimers};
    let capacity = resident + batch * rounds;
    let mut owner = WorkerTimers::<SleepWake>::with_capacity(capacity);
    let clock = owner.assistance();
    // Sleep is Unpin; fixed storage and caller buffers are prepared outside timing.
    let mut sleeps: Vec<_> = (0..capacity).map(|_| None).collect();
    let mut live = Vec::with_capacity(capacity);
    let mut ready = Vec::with_capacity(capacity);
    let mut output = Vec::with_capacity(capacity);
    let mut context = Context::from_waker(Waker::noop());
    let now = Instant::now();
    let deadline = now + Duration::from_secs(120);
    for (index, entry) in sleeps.iter_mut().enumerate().take(resident) {
        let mut sleep = clock.sleep_until(now + Duration::from_secs(60));
        assert!(Pin::new(&mut sleep).poll(&mut context).is_pending());
        *entry = Some(sleep);
        live.push(index);
    }
    owner.next_deadline();
    let cancelled = batch * percent / 100;
    let start = Instant::now();
    for round in 0..rounds {
        let base = resident + round * batch;
        for entry in &mut sleeps[base..base + batch] {
            *entry = Some(clock.sleep_until(deadline));
        }
        if admitted {
            for entry in &mut sleeps[base..base + batch] {
                assert!(
                    Pin::new(entry.as_mut().unwrap())
                        .poll(&mut context)
                        .is_pending()
                );
            }
            owner.next_deadline();
        }
        for entry in &mut sleeps[base..base + cancelled] {
            *entry = None;
        }
        for (offset, entry) in sleeps[base + cancelled..base + batch]
            .iter_mut()
            .enumerate()
        {
            if !admitted {
                assert!(
                    Pin::new(entry.as_mut().unwrap())
                        .poll(&mut context)
                        .is_pending()
                );
            }
            live.push(base + cancelled + offset);
        }
        owner.next_deadline();
    }
    assert_eq!(owner.poll(deadline, &mut ready), live.len());
    ready.clear(); // Actual no-op wakeups after queue collection.
    for &index in &live {
        let sleep = sleeps[index].as_mut().unwrap();
        assert_eq!(
            Pin::new(&mut *sleep).poll(&mut context),
            std::task::Poll::Ready(Ok(()))
        );
        output.push(sleep.deadline());
    }
    if resident == 0 {
        // Match Tokio's completed-future destruction inside the timing window.
        sleeps.clear();
    }
    black_box(&output);
    let elapsed = start.elapsed();
    assert_eq!(output.len(), resident + (batch - cancelled) * rounds);
    assert!(output.iter().all(|value| *value <= deadline));
    elapsed
}

fn main() {
    let smoke = cfg!(debug_assertions) || std::env::args().any(|arg| arg == "--test");
    let (resident, batch, rounds, samples) = if smoke {
        (128, 100, 4, 1)
    } else {
        (10_000, 1_000, 64, 11)
    };
    let no_cancel_only = std::env::args().any(|arg| arg == "--no-cancel");
    let sweep = std::env::args().any(|arg| arg == "--cancellation-sweep");
    let resident = if no_cancel_only || sweep { 0 } else { resident };
    let runtime = Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();
    let _scope = runtime.enter();
    println!(
        "{resident} resident timers, {rounds} rounds of {batch}, {samples} samples; setup excluded; ns/churn timer including final delivery"
    );
    println!(
        "0% control and cancellation sweep: no resident timers; completed future destruction included"
    );
    println!(
        "cancel_percent,phase,shared_median_ns,driver_median_ns,tokio_median_ns{}",
        if cfg!(feature = "sharded") {
            ",sharded_median_ns,lazy_sleep_median_ns"
        } else {
            ""
        }
    );
    let percentages: &[usize] = if sweep {
        &[0, 10, 50, 90, 100]
    } else {
        &[0, 95, 99]
    };
    for &percent in percentages {
        if no_cancel_only && percent != 0 {
            continue;
        }
        let resident = if percent == 0 || sweep { 0 } else { resident };
        for admitted in [false, true] {
            if (percent == 0 || sweep) && !admitted {
                continue;
            }
            #[cfg(not(feature = "sharded"))]
            let mut times = [Vec::new(), Vec::new(), Vec::new()];
            #[cfg(feature = "sharded")]
            let mut times = [Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()];
            for sample in 0..samples {
                for offset in 0..times.len() {
                    let implementation = (sample + offset) % times.len();
                    let capacity = resident + rounds * batch;
                    let elapsed = if implementation == 0 {
                        churn(
                            Timers::with_capacity(capacity),
                            resident,
                            batch,
                            rounds,
                            percent,
                            admitted,
                        )
                    } else if implementation == 1 {
                        let mut driver = TimerDriver::new();
                        let producer = driver.producer(capacity);
                        churn(
                            (driver, producer),
                            resident,
                            batch,
                            rounds,
                            percent,
                            admitted,
                        )
                    } else if implementation == 2 {
                        tokio_churn(&runtime, resident, batch, rounds, percent, admitted)
                    } else {
                        #[cfg(feature = "sharded")]
                        {
                            if implementation == 3 {
                                churn(
                                    socketry_timers::sharded::WorkerTimers::with_capacity(capacity),
                                    resident,
                                    batch,
                                    rounds,
                                    percent,
                                    admitted,
                                )
                            } else {
                                lazy_churn(resident, batch, rounds, percent, admitted)
                            }
                        }
                        #[cfg(not(feature = "sharded"))]
                        {
                            unreachable!("three implementations without the sharded feature")
                        }
                    };
                    times[implementation].push(elapsed);
                }
            }
            for values in &mut times {
                values.sort_unstable();
            }
            let divisor = (rounds * batch) as f64;
            print!(
                "{percent},{}",
                if admitted {
                    "after_admission"
                } else {
                    "before_admission"
                }
            );
            for values in &times {
                print!(",{:.1}", values[samples / 2].as_nanos() as f64 / divisor);
            }
            println!();
        }
    }
}
