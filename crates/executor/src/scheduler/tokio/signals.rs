// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::{Scheduler, SchedulerHandle};
use crate::signal_registration::SignalReader;
use crate::{Notify, SignalRegistration, Signals};
use ::tokio::io::AsyncReadExt;
use std::ffi::c_int;
use std::io;

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
        self.shared.check_open()?;
        SignalRegistration::new(self, signals, target, |stream| {
            register_read(&self.shared.runtime, stream, |stream| {
                stream.set_nonblocking(true)
            })
        })
    }
}

fn register_read(
    runtime: &::tokio::runtime::Handle,
    stream: std::os::unix::net::UnixStream,
    set_nonblocking: impl FnOnce(&std::os::unix::net::UnixStream) -> io::Result<()>,
) -> io::Result<::tokio::net::UnixStream> {
    set_nonblocking(&stream)?;
    let _scope = runtime.enter();
    ::tokio::net::UnixStream::from_std(stream)
}

#[cfg(test)]
mod tests;

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
        self.handle.signal_notify(signals, target)
    }
}

impl SignalReader for ::tokio::net::UnixStream {
    async fn read_signal(&mut self) -> io::Result<usize> {
        self.read(&mut [0]).await
    }
}
