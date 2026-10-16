// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Native selector operations exposed by Socketry schedulers and handles.
use super::{Scheduler, SchedulerHandle};
use crate::scheduler::selector::DefaultSelector;
use crate::scheduler::{BufferResult, Clock, File, Interest, Socket};
use crate::{Cancellation, FileOptions, IoBuf, IoBufMut, OperationResult, SocketRegistration};
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::time::Instant;

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

impl SocketRegistration for SchedulerHandle {
    fn register_socket(&self, socket: TcpStream) -> io::Result<Self::Socket> {
        self.selector()?.register_socket(socket)
    }
    fn register_listener(&self, listener: TcpListener) -> io::Result<Self::Listener> {
        self.selector()?.register_listener(listener)
    }
}
impl Socket for SchedulerHandle {
    type Socket = <DefaultSelector as Socket>::Socket;
    type Listener = <DefaultSelector as Socket>::Listener;
    async fn socket_listen(
        &self,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Listener> {
        self.selector()?.socket_listen(address, cancellation).await
    }
    fn listener_addr(&self, listener: &Self::Listener) -> io::Result<SocketAddr> {
        self.selector()?.listener_addr(listener)
    }
    fn socket_local_addr(&self, socket: &Self::Socket) -> io::Result<SocketAddr> {
        self.selector()?.socket_local_addr(socket)
    }
    fn socket_peer_addr(&self, socket: &Self::Socket) -> io::Result<SocketAddr> {
        self.selector()?.socket_peer_addr(socket)
    }
    async fn connect(
        &self,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Socket> {
        self.selector()?.connect(address, cancellation).await
    }
    async fn accept(
        &self,
        listener: &Self::Listener,
        cancellation: &Cancellation,
    ) -> OperationResult<(Self::Socket, SocketAddr)> {
        self.selector()?.accept(listener, cancellation).await
    }
    async fn io_read<B: IoBufMut>(
        &self,
        socket: &Self::Socket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        match self.selector() {
            Ok(selector) => selector.io_read(socket, buffer, cancellation).await,
            Err(error) => (Err(error.into()), buffer),
        }
    }
    async fn io_write<B: IoBuf>(
        &self,
        socket: &Self::Socket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        match self.selector() {
            Ok(selector) => selector.io_write(socket, buffer, cancellation).await,
            Err(error) => (Err(error.into()), buffer),
        }
    }
    async fn io_wait(
        &self,
        socket: &Self::Socket,
        interest: Interest,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.selector()?
            .io_wait(socket, interest, cancellation)
            .await
    }
}
impl File for SchedulerHandle {
    type File = <DefaultSelector as File>::File;
    async fn file_open(
        &self,
        path: &Path,
        options: FileOptions,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::File> {
        self.selector()?
            .file_open(path, options, cancellation)
            .await
    }
    async fn file_read_at<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        match self.selector() {
            Ok(selector) => {
                selector
                    .file_read_at(file, buffer, offset, cancellation)
                    .await
            }
            Err(error) => (Err(error.into()), buffer),
        }
    }
    async fn file_write_at<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        match self.selector() {
            Ok(selector) => {
                selector
                    .file_write_at(file, buffer, offset, cancellation)
                    .await
            }
            Err(error) => (Err(error.into()), buffer),
        }
    }
}
impl Clock for SchedulerHandle {
    fn now(&self) -> Instant {
        Instant::now()
    }
    async fn sleep_until(
        &self,
        deadline: Instant,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        // Timer progress is independent of selector initialization and ring support.
        crate::scheduler::clock::native_sleep_until(deadline, cancellation).await
    }
}
impl SocketRegistration for Scheduler {
    fn register_socket(&self, socket: TcpStream) -> io::Result<Self::Socket> {
        self.handle.register_socket(socket)
    }
    fn register_listener(&self, listener: TcpListener) -> io::Result<Self::Listener> {
        self.handle.register_listener(listener)
    }
}
impl Socket for Scheduler {
    type Socket = <SchedulerHandle as Socket>::Socket;
    type Listener = <SchedulerHandle as Socket>::Listener;
    async fn socket_listen(
        &self,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Listener> {
        self.handle.socket_listen(address, cancellation).await
    }
    fn listener_addr(&self, listener: &Self::Listener) -> io::Result<SocketAddr> {
        self.handle.listener_addr(listener)
    }
    fn socket_local_addr(&self, socket: &Self::Socket) -> io::Result<SocketAddr> {
        self.handle.socket_local_addr(socket)
    }
    fn socket_peer_addr(&self, socket: &Self::Socket) -> io::Result<SocketAddr> {
        self.handle.socket_peer_addr(socket)
    }
    async fn connect(
        &self,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Socket> {
        self.handle.connect(address, cancellation).await
    }
    async fn accept(
        &self,
        listener: &Self::Listener,
        cancellation: &Cancellation,
    ) -> OperationResult<(Self::Socket, SocketAddr)> {
        self.handle.accept(listener, cancellation).await
    }
    async fn io_read<B: IoBufMut>(
        &self,
        socket: &Self::Socket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.handle.io_read(socket, buffer, cancellation).await
    }
    async fn io_write<B: IoBuf>(
        &self,
        socket: &Self::Socket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.handle.io_write(socket, buffer, cancellation).await
    }
    async fn io_wait(
        &self,
        socket: &Self::Socket,
        interest: Interest,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.handle.io_wait(socket, interest, cancellation).await
    }
}
impl File for Scheduler {
    type File = <SchedulerHandle as File>::File;
    async fn file_open(
        &self,
        path: &Path,
        options: FileOptions,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::File> {
        self.handle.file_open(path, options, cancellation).await
    }
    async fn file_read_at<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.handle
            .file_read_at(file, buffer, offset, cancellation)
            .await
    }
    async fn file_write_at<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.handle
            .file_write_at(file, buffer, offset, cancellation)
            .await
    }
}
impl Clock for Scheduler {
    fn now(&self) -> Instant {
        self.handle.now()
    }
    async fn sleep_until(
        &self,
        deadline: Instant,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.handle.sleep_until(deadline, cancellation).await
    }
}
#[cfg(test)]
mod tests;
