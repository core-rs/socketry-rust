// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Cancellation, Notify, Spawn};
use signal_hook::iterator::{backend::SignalDelivery, exfiltrator::SignalOnly};
use std::ffi::c_int;
use std::future::{Future, poll_fn};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::pin::pin;
use std::task::Poll;

/// Owns a Unix signal subscription created by `Signals::signal_notify`.
///
/// Dropping this guard disables further dispatch and wakes the subscription's
/// scheduler task to release its resources. An already-admitted notification
/// may finish; the task owns its destination throughout the callback. Drop
/// does not join the task or wait for user code.
///
/// Keep the guard, scheduler, and I/O services alive during graceful draining.
/// Scheduler shutdown also ends delivery. A panicking destination unwinds its
/// own task using the scheduler's usual panic handling.
///
/// Removing a subscription does not restore the process's original signal
/// disposition, including after partial setup failure. Coordinate ownership
/// with other libraries; existing handlers may still run. No escalation or
/// default termination policy is installed.
#[must_use = "dropping the registration removes the signal subscription"]
pub struct SignalRegistration {
    stopped: Cancellation,
}

impl std::fmt::Debug for SignalRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SignalRegistration")
            .finish_non_exhaustive()
    }
}

/// The backend's asynchronous self-pipe reader. Never block a polling worker.
pub(crate) trait SignalReader: AsRawFd + Send + Sync + 'static {
    fn read_signal(&mut self) -> impl Future<Output = io::Result<usize>> + Send;
}

impl SignalRegistration {
    pub(crate) fn new<Reader: SignalReader>(
        scheduler: &impl Spawn,
        signals: &[c_int],
        target: impl Notify<c_int> + 'static,
        register: impl FnOnce(UnixStream) -> io::Result<Reader>,
    ) -> io::Result<Self> {
        validate(signals)?;
        let delivery = prepare(signals, UnixStream::pair(), register)?;
        Self::start(scheduler, delivery, target)
    }

    fn start<Reader: SignalReader>(
        scheduler: &impl Spawn,
        delivery: SignalDelivery<Reader, SignalOnly>,
        target: impl Notify<c_int> + 'static,
    ) -> io::Result<Self> {
        let registration = Self {
            stopped: Cancellation::new(),
        };
        // Spawn retains the task when its result handle is dropped.
        drop(
            scheduler
                .spawn(dispatch(delivery, target, registration.stopped.clone()))
                .map_err(io::Error::other)?,
        );
        Ok(registration)
    }
}

fn prepare<Reader: SignalReader>(
    signals: &[c_int],
    pipe: io::Result<(UnixStream, UnixStream)>,
    register: impl FnOnce(UnixStream) -> io::Result<Reader>,
) -> io::Result<SignalDelivery<Reader, SignalOnly>> {
    let (read, write) = pipe?;
    let read = register(read)?;
    SignalDelivery::with_pipe(read, write, SignalOnly, signals)
}

async fn dispatch<Reader: SignalReader>(
    mut delivery: SignalDelivery<Reader, SignalOnly>,
    target: impl Notify<c_int>,
    stopped: Cancellation,
) {
    loop {
        let result = {
            let mut stop = pin!(stopped.cancelled());
            let mut read = pin!(delivery.get_read_mut().read_signal());
            poll_fn(|context| {
                if stop.as_mut().poll(context).is_ready() {
                    return Poll::Ready(None);
                }
                read.as_mut().poll(context).map(Some)
            })
            .await
        };
        match result {
            Some(Ok(1..)) => {
                for signal in delivery.pending() {
                    // Passing this check admits one notification. Removal may
                    // race the callback, but cannot destroy its owned target.
                    if stopped.is_cancelled() {
                        return;
                    }
                    target.notify(signal);
                }
            }
            Some(Err(error)) if error.kind() == io::ErrorKind::Interrupted => {}
            _ => return,
        }
        // A continuously readable pipe must not monopolize a polling worker.
        crate::yield_now().await;
    }
}

impl Drop for SignalRegistration {
    fn drop(&mut self) {
        self.stopped.cancel();
    }
}

fn validate(signals: &[c_int]) -> io::Result<()> {
    if signals.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "empty signal set",
        ));
    }
    // Check the entire set for values that would panic inside signal-hook.
    // The backend reports any further platform-specific registration errors.
    for &signal in signals {
        // signal-hook's iterator indexes a table of 128 entries. Reject its
        // forbidden signals and out-of-range values instead of panicking.
        if !(1..128).contains(&signal) || signal_hook::consts::FORBIDDEN.contains(&signal) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid or forbidden signal",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
