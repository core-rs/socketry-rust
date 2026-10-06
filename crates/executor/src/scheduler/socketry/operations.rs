// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Native selector operations exposed by Socketry schedulers and handles.
use super::{Scheduler, SchedulerHandle};
use crate::scheduler::selector::DefaultSelector;
use crate::scheduler::{BufferResult, Clock, File, Interest, Socket};
use std::fs::File as StdFile;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;

fn check_open(closed: bool) -> io::Result<()> {
    if closed {
        Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "scheduler is closed",
        ))
    } else {
        Ok(())
    }
}

fn copy_selector_error(error: &io::Error) -> io::Error {
    io::Error::new(error.kind(), error.to_string())
}

#[cfg(all(feature = "io-uring", target_os = "linux"))]
fn check_initialized_open(closed: bool, selector: &DefaultSelector) -> io::Result<()> {
    if closed {
        selector.close();
    }
    check_open(closed)
}

#[cfg(not(all(feature = "io-uring", target_os = "linux")))]
fn check_initialized_open(closed: bool, _selector: &DefaultSelector) -> io::Result<()> {
    check_open(closed)
}

impl SchedulerHandle {
    /// Lazily initialize this scheduler's compile-time selected I/O selector.
    pub fn selector(&self) -> io::Result<&DefaultSelector> {
        self.selector_with(DefaultSelector::new)
    }

    fn selector_with(
        &self,
        initialize: impl FnOnce() -> io::Result<DefaultSelector>,
    ) -> io::Result<&DefaultSelector> {
        check_open(
            self.shared
                .closed
                .load(std::sync::atomic::Ordering::Acquire),
        )?;
        let selector = self.shared.selector.get_or_init(initialize);
        self.initialized_selector(selector)
    }

    fn initialized_selector<'a>(
        &self,
        selector: &'a io::Result<DefaultSelector>,
    ) -> io::Result<&'a DefaultSelector> {
        let selector = selector.as_ref().map_err(copy_selector_error)?;
        // Shutdown may have raced lazy initialization. Do not leave a newly
        // constructed completion selector running after admission closes.
        check_initialized_open(
            self.shared
                .closed
                .load(std::sync::atomic::Ordering::Acquire),
            selector,
        )?;
        Ok(selector)
    }
}

impl Socket for SchedulerHandle {
    type Socket = <DefaultSelector as Socket>::Socket;
    type Listener = <DefaultSelector as Socket>::Listener;

    fn register_socket(&self, socket: TcpStream) -> io::Result<Self::Socket> {
        self.selector()?.register_socket(socket)
    }

    fn register_listener(&self, listener: TcpListener) -> io::Result<Self::Listener> {
        self.selector()?.register_listener(listener)
    }

    async fn connect(&self, address: SocketAddr) -> io::Result<Self::Socket> {
        self.selector()?.connect(address).await
    }

    async fn accept(&self, listener: &Self::Listener) -> io::Result<(Self::Socket, SocketAddr)> {
        self.selector()?.accept(listener).await
    }

    async fn io_read(&self, socket: &Self::Socket, buffer: Vec<u8>) -> BufferResult {
        match self.selector() {
            Ok(selector) => selector.io_read(socket, buffer).await,
            Err(error) => (Err(error), buffer),
        }
    }

    async fn io_write(&self, socket: &Self::Socket, buffer: Vec<u8>) -> BufferResult {
        match self.selector() {
            Ok(selector) => selector.io_write(socket, buffer).await,
            Err(error) => (Err(error), buffer),
        }
    }

    async fn io_wait(&self, socket: &Self::Socket, interest: Interest) -> io::Result<()> {
        self.selector()?.io_wait(socket, interest).await
    }
}

impl File for SchedulerHandle {
    async fn file_read_at(&self, file: Arc<StdFile>, buffer: Vec<u8>, offset: u64) -> BufferResult {
        match self.selector() {
            Ok(selector) => selector.file_read_at(file, buffer, offset).await,
            Err(error) => (Err(error), buffer),
        }
    }

    async fn file_write_at(
        &self,
        file: Arc<StdFile>,
        buffer: Vec<u8>,
        offset: u64,
    ) -> BufferResult {
        match self.selector() {
            Ok(selector) => selector.file_write_at(file, buffer, offset).await,
            Err(error) => (Err(error), buffer),
        }
    }
}

impl Clock for SchedulerHandle {
    async fn sleep(&self, duration: Duration) {
        // Timer progress is independent of selector initialization and ring support.
        async_io::Timer::after(duration).await;
    }
}

impl Socket for Scheduler {
    type Socket = <SchedulerHandle as Socket>::Socket;
    type Listener = <SchedulerHandle as Socket>::Listener;

    fn register_socket(&self, socket: TcpStream) -> io::Result<Self::Socket> {
        self.handle.register_socket(socket)
    }

    fn register_listener(&self, listener: TcpListener) -> io::Result<Self::Listener> {
        self.handle.register_listener(listener)
    }

    async fn connect(&self, address: SocketAddr) -> io::Result<Self::Socket> {
        self.handle.connect(address).await
    }

    async fn accept(&self, listener: &Self::Listener) -> io::Result<(Self::Socket, SocketAddr)> {
        self.handle.accept(listener).await
    }

    async fn io_read(&self, socket: &Self::Socket, buffer: Vec<u8>) -> BufferResult {
        self.handle.io_read(socket, buffer).await
    }

    async fn io_write(&self, socket: &Self::Socket, buffer: Vec<u8>) -> BufferResult {
        self.handle.io_write(socket, buffer).await
    }

    async fn io_wait(&self, socket: &Self::Socket, interest: Interest) -> io::Result<()> {
        self.handle.io_wait(socket, interest).await
    }
}

impl File for Scheduler {
    async fn file_read_at(&self, file: Arc<StdFile>, buffer: Vec<u8>, offset: u64) -> BufferResult {
        self.handle.file_read_at(file, buffer, offset).await
    }

    async fn file_write_at(
        &self,
        file: Arc<StdFile>,
        buffer: Vec<u8>,
        offset: u64,
    ) -> BufferResult {
        self.handle.file_write_at(file, buffer, offset).await
    }
}

impl Clock for Scheduler {
    async fn sleep(&self, duration: Duration) {
        self.handle.sleep(duration).await;
    }
}

#[cfg(test)]
mod tests;
