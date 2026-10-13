// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::{Scheduler, SchedulerHandle};
use crate::signal_registration::SignalReader;
use crate::{Notify, SignalRegistration, Signals};
use std::ffi::c_int;
use std::io;
use std::io::Read;
use std::os::unix::net::UnixStream;
use std::sync::atomic::Ordering;

impl Signals for SchedulerHandle {
    type Signal = c_int;
    type Registration = SignalRegistration;

    fn signal_notify<Target>(
        &self,
        signals: &[c_int],
        target: Target,
    ) -> io::Result<SignalRegistration>
    where
        Target: Notify<c_int> + 'static,
    {
        if self.shared.closed.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "scheduler is closed",
            ));
        }
        SignalRegistration::new(self, signals, target, async_io::Async::new)
    }
}

impl Signals for Scheduler {
    type Signal = c_int;
    type Registration = SignalRegistration;

    fn signal_notify<Target>(
        &self,
        signals: &[c_int],
        target: Target,
    ) -> io::Result<SignalRegistration>
    where
        Target: Notify<c_int> + 'static,
    {
        self.handle().signal_notify(signals, target)
    }
}

impl SignalReader for async_io::Async<UnixStream> {
    async fn read_signal(&mut self) -> io::Result<usize> {
        self.read_with(|mut stream| stream.read(&mut [0])).await
    }
}
