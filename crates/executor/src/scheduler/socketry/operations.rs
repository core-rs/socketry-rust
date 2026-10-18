// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Native selector operations exposed by Socketry schedulers and handles.
use super::{Scheduler, SchedulerHandle};
use crate::SyncMode;
use crate::scheduler::selector::DefaultSelector;
use crate::scheduler::{BufferResult, Clock, Interest, Socket};
#[cfg(target_os = "linux")]
use crate::{AllocationMode, FileAllocate};
use crate::{Cancellation, FileOptions, IoBuf, IoBufMut, OperationResult, SocketRegistration};
use crate::{
    FileClose, FileOpen, FileOperation, FileRead, FileReadAt, FileSeek, FileStat, FileSync,
    FileTruncate, FileWrite, FileWriteAt,
};
use crate::{SocketMessage, SocketMessages, SocketRecvFlags, SocketRecvResult, SocketSendFlags};
use std::io::{self, SeekFrom};
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

impl<O: crate::SocketOption> crate::SocketOptions<O> for SchedulerHandle
where
    DefaultSelector: crate::SocketOptions<O>,
{
    async fn socket_get_option(
        &self,
        socket: &Self::Socket,
        option: O,
        cancellation: &Cancellation,
    ) -> OperationResult<O::Value> {
        self.selector()?
            .socket_get_option(socket, option, cancellation)
            .await
    }

    async fn socket_set_option(
        &self,
        socket: &Self::Socket,
        option: O,
        value: O::Value,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.selector()?
            .socket_set_option(socket, option, value, cancellation)
            .await
    }
}

impl SocketRegistration for SchedulerHandle {
    fn register_socket(&self, socket: TcpStream) -> io::Result<Self::Socket> {
        self.selector()?.register_socket(socket)
    }
    fn register_listener(&self, listener: TcpListener) -> io::Result<Self::Socket> {
        self.selector()?.register_listener(listener)
    }
}
impl Socket for SchedulerHandle {
    type Socket = <DefaultSelector as Socket>::Socket;

    async fn socket_close(&self, socket: Self::Socket) -> io::Result<()> {
        check_open(
            self.shared
                .closed
                .load(std::sync::atomic::Ordering::Acquire),
        )?;
        crate::scheduler::cleanup::run(&self.shared.cleanup, socket.into_close()).await
    }

    async fn socket_close_submit(&self, socket: Self::Socket) -> io::Result<()> {
        check_open(
            self.shared
                .closed
                .load(std::sync::atomic::Ordering::Acquire),
        )?;
        crate::scheduler::cleanup::submit_only(&self.shared.cleanup, socket.into_close()).await
    }

    async fn socket_shutdown(
        &self,
        socket: &Self::Socket,
        how: std::net::Shutdown,
    ) -> io::Result<()> {
        check_open(
            self.shared
                .closed
                .load(std::sync::atomic::Ordering::Acquire),
        )?;
        let socket = socket.retain();
        crate::scheduler::cleanup::run(&self.shared.cleanup, move || socket.get_ref().shutdown(how))
            .await
    }

    async fn socket_shutdown_submit(
        &self,
        socket: &Self::Socket,
        how: std::net::Shutdown,
    ) -> io::Result<()> {
        check_open(
            self.shared
                .closed
                .load(std::sync::atomic::Ordering::Acquire),
        )?;
        let socket = socket.retain();
        crate::scheduler::cleanup::submit_only(&self.shared.cleanup, move || {
            socket.get_ref().shutdown(how)
        })
        .await
    }

    async fn socket_open(
        &self,
        domain: crate::SocketDomain,
        kind: crate::SocketType,
        protocol: Option<crate::SocketProtocol>,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Socket> {
        self.selector()?
            .socket_open(domain, kind, protocol, cancellation)
            .await
    }
    async fn socket_bind(
        &self,
        socket: &Self::Socket,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.selector()?
            .socket_bind(socket, address, cancellation)
            .await
    }
    async fn socket_listen(
        &self,
        socket: &Self::Socket,
        backlog: i32,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.selector()?
            .socket_listen(socket, backlog, cancellation)
            .await
    }
    fn socket_local_addr(&self, socket: &Self::Socket) -> io::Result<SocketAddr> {
        self.selector()?.socket_local_addr(socket)
    }
    fn socket_peer_addr(&self, socket: &Self::Socket) -> io::Result<SocketAddr> {
        self.selector()?.socket_peer_addr(socket)
    }
    async fn socket_connect(
        &self,
        socket: &Self::Socket,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.selector()?
            .socket_connect(socket, address, cancellation)
            .await
    }
    async fn socket_accept(
        &self,
        socket: &Self::Socket,
        cancellation: &Cancellation,
    ) -> OperationResult<(Self::Socket, SocketAddr)> {
        self.selector()?.socket_accept(socket, cancellation).await
    }
    async fn socket_read<B: IoBufMut>(
        &self,
        socket: &Self::Socket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        match self.selector() {
            Ok(selector) => selector.socket_read(socket, buffer, cancellation).await,
            Err(error) => (Err(error.into()), buffer),
        }
    }
    async fn socket_write<B: IoBuf>(
        &self,
        socket: &Self::Socket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        match self.selector() {
            Ok(selector) => selector.socket_write(socket, buffer, cancellation).await,
            Err(error) => (Err(error.into()), buffer),
        }
    }
    async fn socket_wait(
        &self,
        socket: &Self::Socket,
        interest: Interest,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.selector()?
            .socket_wait(socket, interest, cancellation)
            .await
    }
}
#[cfg(target_os = "linux")]
impl FileAllocate for SchedulerHandle {
    async fn file_allocate(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        mode: AllocationMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.selector()?
            .file_allocate(file, offset, length, mode, cancellation)
            .await
    }
}

#[cfg(target_os = "linux")]
impl crate::FileAdvice for SchedulerHandle {
    async fn file_advise(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        advice: crate::AdviceMode,
    ) -> io::Result<()> {
        check_open(
            self.shared
                .closed
                .load(std::sync::atomic::Ordering::Acquire),
        )?;
        crate::scheduler::file_advice_io::run(&self.shared.cleanup, file, offset, length, advice)
            .await
    }

    async fn file_advise_submit(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        advice: crate::AdviceMode,
    ) -> io::Result<()> {
        check_open(
            self.shared
                .closed
                .load(std::sync::atomic::Ordering::Acquire),
        )?;
        crate::scheduler::file_advice_io::submit_only(
            &self.shared.cleanup,
            file,
            offset,
            length,
            advice,
        )
        .await
    }
}

impl FileOperation for SchedulerHandle {
    type File = <DefaultSelector as FileOperation>::File;
}

impl FileClose for SchedulerHandle {
    async fn file_close(&self, file: Self::File) -> io::Result<()> {
        check_open(
            self.shared
                .closed
                .load(std::sync::atomic::Ordering::Acquire),
        )?;
        crate::scheduler::file_close_io::close(&self.shared.cleanup, file).await
    }

    async fn file_close_submit(&self, file: Self::File) -> io::Result<()> {
        check_open(
            self.shared
                .closed
                .load(std::sync::atomic::Ordering::Acquire),
        )?;
        crate::scheduler::file_close_io::submit_only(&self.shared.cleanup, file).await
    }
}

impl FileTruncate for SchedulerHandle {
    async fn file_truncate(
        &self,
        file: &Self::File,
        length: u64,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.selector()?
            .file_truncate(file, length, cancellation)
            .await
    }
}

impl FileStat for SchedulerHandle {
    type Metadata = <DefaultSelector as FileStat>::Metadata;

    async fn file_stat(
        &self,
        file: &Self::File,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Metadata> {
        self.selector()?.file_stat(file, cancellation).await
    }
}

impl FileSync for SchedulerHandle {
    async fn file_sync(
        &self,
        file: &Self::File,
        mode: SyncMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.selector()?.file_sync(file, mode, cancellation).await
    }
}

impl FileSeek for SchedulerHandle {
    async fn file_seek(
        &self,
        file: &Self::File,
        position: SeekFrom,
        cancellation: &Cancellation,
    ) -> OperationResult<u64> {
        self.selector()?
            .file_seek(file, position, cancellation)
            .await
    }
}

impl FileOpen for SchedulerHandle {
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
}

impl FileRead for SchedulerHandle {
    async fn file_read<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        match self.selector() {
            Ok(selector) => selector.file_read(file, buffer, cancellation).await,
            Err(error) => (Err(error.into()), buffer),
        }
    }
}

impl FileWrite for SchedulerHandle {
    async fn file_write<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        match self.selector() {
            Ok(selector) => selector.file_write(file, buffer, cancellation).await,
            Err(error) => (Err(error.into()), buffer),
        }
    }
}

impl FileReadAt for SchedulerHandle {
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
}

impl FileWriteAt for SchedulerHandle {
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
impl<O: crate::SocketOption> crate::SocketOptions<O> for Scheduler
where
    SchedulerHandle: crate::SocketOptions<O>,
{
    async fn socket_get_option(
        &self,
        socket: &Self::Socket,
        option: O,
        cancellation: &Cancellation,
    ) -> OperationResult<O::Value> {
        self.handle
            .socket_get_option(socket, option, cancellation)
            .await
    }

    async fn socket_set_option(
        &self,
        socket: &Self::Socket,
        option: O,
        value: O::Value,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.handle
            .socket_set_option(socket, option, value, cancellation)
            .await
    }
}

impl SocketRegistration for Scheduler {
    fn register_socket(&self, socket: TcpStream) -> io::Result<Self::Socket> {
        self.handle.register_socket(socket)
    }
    fn register_listener(&self, listener: TcpListener) -> io::Result<Self::Socket> {
        self.handle.register_listener(listener)
    }
}
impl Socket for Scheduler {
    type Socket = <SchedulerHandle as Socket>::Socket;

    async fn socket_close(&self, socket: Self::Socket) -> io::Result<()> {
        self.handle.socket_close(socket).await
    }

    async fn socket_close_submit(&self, socket: Self::Socket) -> io::Result<()> {
        self.handle.socket_close_submit(socket).await
    }

    async fn socket_shutdown(
        &self,
        socket: &Self::Socket,
        how: std::net::Shutdown,
    ) -> io::Result<()> {
        self.handle.socket_shutdown(socket, how).await
    }

    async fn socket_shutdown_submit(
        &self,
        socket: &Self::Socket,
        how: std::net::Shutdown,
    ) -> io::Result<()> {
        self.handle.socket_shutdown_submit(socket, how).await
    }

    async fn socket_open(
        &self,
        domain: crate::SocketDomain,
        kind: crate::SocketType,
        protocol: Option<crate::SocketProtocol>,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Socket> {
        self.handle
            .socket_open(domain, kind, protocol, cancellation)
            .await
    }
    async fn socket_bind(
        &self,
        socket: &Self::Socket,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.handle.socket_bind(socket, address, cancellation).await
    }
    async fn socket_listen(
        &self,
        socket: &Self::Socket,
        backlog: i32,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.handle
            .socket_listen(socket, backlog, cancellation)
            .await
    }
    fn socket_local_addr(&self, socket: &Self::Socket) -> io::Result<SocketAddr> {
        self.handle.socket_local_addr(socket)
    }
    fn socket_peer_addr(&self, socket: &Self::Socket) -> io::Result<SocketAddr> {
        self.handle.socket_peer_addr(socket)
    }
    async fn socket_connect(
        &self,
        socket: &Self::Socket,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.handle
            .socket_connect(socket, address, cancellation)
            .await
    }
    async fn socket_accept(
        &self,
        socket: &Self::Socket,
        cancellation: &Cancellation,
    ) -> OperationResult<(Self::Socket, SocketAddr)> {
        self.handle.socket_accept(socket, cancellation).await
    }
    async fn socket_read<B: IoBufMut>(
        &self,
        socket: &Self::Socket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.handle.socket_read(socket, buffer, cancellation).await
    }
    async fn socket_write<B: IoBuf>(
        &self,
        socket: &Self::Socket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.handle.socket_write(socket, buffer, cancellation).await
    }
    async fn socket_wait(
        &self,
        socket: &Self::Socket,
        interest: Interest,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.handle
            .socket_wait(socket, interest, cancellation)
            .await
    }
}
#[cfg(target_os = "linux")]
impl FileAllocate for Scheduler {
    async fn file_allocate(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        mode: AllocationMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.handle
            .file_allocate(file, offset, length, mode, cancellation)
            .await
    }
}

#[cfg(target_os = "linux")]
impl crate::FileAdvice for Scheduler {
    async fn file_advise(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        advice: crate::AdviceMode,
    ) -> io::Result<()> {
        crate::FileAdvice::file_advise(&self.handle, file, offset, length, advice).await
    }

    async fn file_advise_submit(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        advice: crate::AdviceMode,
    ) -> io::Result<()> {
        crate::FileAdvice::file_advise_submit(&self.handle, file, offset, length, advice).await
    }
}

impl FileOperation for Scheduler {
    type File = <SchedulerHandle as FileOperation>::File;
}

impl FileClose for Scheduler {
    async fn file_close(&self, file: Self::File) -> io::Result<()> {
        self.handle.file_close(file).await
    }

    async fn file_close_submit(&self, file: Self::File) -> io::Result<()> {
        self.handle.file_close_submit(file).await
    }
}

impl FileTruncate for Scheduler {
    async fn file_truncate(
        &self,
        file: &Self::File,
        length: u64,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.handle.file_truncate(file, length, cancellation).await
    }
}

impl FileStat for Scheduler {
    type Metadata = <SchedulerHandle as FileStat>::Metadata;

    async fn file_stat(
        &self,
        file: &Self::File,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Metadata> {
        self.handle.file_stat(file, cancellation).await
    }
}

impl FileSync for Scheduler {
    async fn file_sync(
        &self,
        file: &Self::File,
        mode: SyncMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.handle.file_sync(file, mode, cancellation).await
    }
}

impl FileSeek for Scheduler {
    async fn file_seek(
        &self,
        file: &Self::File,
        position: SeekFrom,
        cancellation: &Cancellation,
    ) -> OperationResult<u64> {
        self.handle.file_seek(file, position, cancellation).await
    }
}

impl FileOpen for Scheduler {
    async fn file_open(
        &self,
        path: &Path,
        options: FileOptions,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::File> {
        self.handle.file_open(path, options, cancellation).await
    }
}

impl FileRead for Scheduler {
    async fn file_read<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.handle.file_read(file, buffer, cancellation).await
    }
}

impl FileWrite for Scheduler {
    async fn file_write<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.handle.file_write(file, buffer, cancellation).await
    }
}

impl FileReadAt for Scheduler {
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
}

impl FileWriteAt for Scheduler {
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

impl SocketMessages for SchedulerHandle {
    async fn socket_sendmsg<B: IoBuf>(
        &self,
        socket: &Self::Socket,
        message: SocketMessage<B>,
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<usize>, SocketMessage<B>) {
        match self.selector() {
            Ok(selector) => {
                selector
                    .socket_sendmsg(socket, message, flags, cancellation)
                    .await
            }
            Err(error) => (Err(error.into()), message),
        }
    }
    async fn socket_recvmsg<B: IoBufMut>(
        &self,
        socket: &Self::Socket,
        buffers: Vec<B>,
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<SocketRecvResult>, Vec<B>) {
        match self.selector() {
            Ok(selector) => {
                selector
                    .socket_recvmsg(socket, buffers, flags, cancellation)
                    .await
            }
            Err(error) => (Err(error.into()), buffers),
        }
    }
}

impl SocketMessages for Scheduler {
    async fn socket_sendmsg<B: IoBuf>(
        &self,
        socket: &Self::Socket,
        message: SocketMessage<B>,
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<usize>, SocketMessage<B>) {
        self.handle
            .socket_sendmsg(socket, message, flags, cancellation)
            .await
    }
    async fn socket_recvmsg<B: IoBufMut>(
        &self,
        socket: &Self::Socket,
        buffers: Vec<B>,
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<SocketRecvResult>, Vec<B>) {
        self.handle
            .socket_recvmsg(socket, buffers, flags, cancellation)
            .await
    }
}

impl crate::AddressResolution for SchedulerHandle {
    async fn address_resolve(
        &self,
        name: Option<&str>,
        service: crate::AddressService<'_>,
        hints: crate::AddressHints,
        cancellation: &Cancellation,
    ) -> OperationResult<Vec<crate::AddressInfo>> {
        crate::scheduler::address_resolution::system::resolve(
            &self.shared.blocking,
            name,
            service,
            hints,
            cancellation,
        )
        .await
    }
}

impl crate::AddressResolution for Scheduler {
    async fn address_resolve(
        &self,
        name: Option<&str>,
        service: crate::AddressService<'_>,
        hints: crate::AddressHints,
        cancellation: &Cancellation,
    ) -> OperationResult<Vec<crate::AddressInfo>> {
        self.handle
            .address_resolve(name, service, hints, cancellation)
            .await
    }
}
