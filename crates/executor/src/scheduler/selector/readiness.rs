// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Shared readiness operations backed by `async-io`'s persistent registrations.
//!
//! Its process-wide reactor drives OS events independently of our executor.
//! It remains available while registered resources are alive; Socketry task
//! shutdown does not shut down that shared reactor. Futures contain no private
//! coroutine stacks and may be polled on different workers.
use crate::scheduler::file_io::{read_at, write_at};
use crate::scheduler::operation::cancellable;
use crate::scheduler::{BufferResult, Clock, File, Interest, Socket as SocketOperations};
#[cfg(target_os = "linux")]
use crate::{AllocationMode, FileAllocate};
use crate::{Cancellation, FileOptions, IoBuf, IoBufMut, OperationResult, SocketRegistration};
use crate::{FileDescriptor, SyncMode};
use std::io::{self, SeekFrom};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::time::Instant;

mod socket_descriptor;
pub use socket_descriptor::SocketDescriptor;

/// Compile-time platform selection with a shared readiness reactor. All instances share registrations.
#[derive(Clone, Copy, Default)]
pub struct Selector;

impl Selector {
    pub fn new() -> io::Result<Self> {
        Ok(Self)
    }
}

impl SocketRegistration for Selector {
    fn register_socket(&self, socket: TcpStream) -> io::Result<SocketDescriptor> {
        SocketDescriptor::new(socket.into())
    }
    fn register_listener(&self, listener: TcpListener) -> io::Result<SocketDescriptor> {
        SocketDescriptor::new(listener.into())
    }
}

impl SocketOperations for Selector {
    type Socket = SocketDescriptor;
    async fn socket_open(
        &self,
        domain: crate::SocketDomain,
        kind: crate::SocketType,
        protocol: Option<crate::SocketProtocol>,
        cancellation: &Cancellation,
    ) -> OperationResult<SocketDescriptor> {
        cancellation.check()?;
        Ok(SocketDescriptor::new(crate::scheduler::socket_io::open(
            domain, kind, protocol,
        )?)?)
    }
    async fn socket_bind(
        &self,
        socket: &SocketDescriptor,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        Ok(socket.get_ref().bind(&address.into())?)
    }
    async fn socket_listen(
        &self,
        socket: &SocketDescriptor,
        backlog: i32,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        Ok(socket.get_ref().listen(backlog)?)
    }
    fn socket_local_addr(&self, socket: &SocketDescriptor) -> io::Result<SocketAddr> {
        socket.local_addr()
    }
    fn socket_peer_addr(&self, socket: &SocketDescriptor) -> io::Result<SocketAddr> {
        socket.peer_addr()
    }
    async fn socket_connect(
        &self,
        socket: &SocketDescriptor,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        match socket.get_ref().connect(&address.into()) {
            Ok(()) => Ok(()),
            Err(error) if crate::scheduler::socket_io::connect_pending(&error) => {
                cancellable(
                    cancellation,
                    socket
                        .inner
                        .write_with(crate::scheduler::socket_io::connected),
                )
                .await
            }
            Err(error) => Err(error.into()),
        }
    }
    async fn socket_accept(
        &self,
        socket: &SocketDescriptor,
        cancellation: &Cancellation,
    ) -> OperationResult<(SocketDescriptor, SocketAddr)> {
        cancellable(cancellation, async {
            let (accepted, address) = socket.inner.read_with(|socket| socket.accept()).await?;
            Ok((
                SocketDescriptor::new(accepted)?,
                crate::scheduler::socket_io::address(address),
            ))
        })
        .await
    }
    async fn socket_read<B: IoBufMut>(
        &self,
        socket: &SocketDescriptor,
        mut buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let result = cancellable(
            cancellation,
            socket
                .inner
                .read_with(|socket| crate::scheduler::socket_io::recv(socket, buffer.as_mut())),
        )
        .await;
        (result, buffer)
    }
    async fn socket_write<B: IoBuf>(
        &self,
        socket: &SocketDescriptor,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let send_buffer = crate::scheduler::socket_io::SendBuffer::new(&buffer);
        let result = cancellable(
            cancellation,
            socket
                .inner
                .write_with(move |socket| send_buffer.send(socket)),
        )
        .await;
        (result, buffer)
    }
    async fn socket_wait(
        &self,
        socket: &SocketDescriptor,
        interest: Interest,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellable(cancellation, async {
            match interest {
                Interest::Readable => socket.inner.readable().await,
                Interest::Writable => socket.inner.writable().await,
            }
        })
        .await
    }
}
#[cfg(target_os = "linux")]
impl FileAllocate for Selector {
    async fn file_allocate(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        mode: AllocationMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        let file = file.retain();
        let cancellation = cancellation.clone();
        blocking::unblock(move || {
            crate::scheduler::blocking::run(&cancellation, || {
                crate::scheduler::file_io::allocate(&file, offset, length, mode)
            })
        })
        .await
    }
}

impl File for Selector {
    type File = FileDescriptor;
    async fn file_close(&self, file: Self::File) -> io::Result<()> {
        crate::scheduler::file_close::close(crate::scheduler::file_close::standalone_pool(), file)
            .await
    }

    async fn file_close_submit(&self, file: Self::File) -> io::Result<()> {
        crate::scheduler::file_close::submit_only(
            crate::scheduler::file_close::standalone_pool(),
            file,
        )
        .await
    }

    async fn file_truncate(
        &self,
        file: &Self::File,
        length: u64,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        let file = file.retain();
        let cancellation = cancellation.clone();
        blocking::unblock(move || {
            crate::scheduler::blocking::run(&cancellation, || {
                crate::scheduler::file_io::truncate(&file, length)
            })
        })
        .await
    }

    async fn file_sync(
        &self,
        file: &Self::File,
        mode: SyncMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        let file = file.retain();
        let cancellation = cancellation.clone();
        blocking::unblock(move || {
            crate::scheduler::blocking::run(&cancellation, || {
                crate::scheduler::file_io::sync(&file, mode)
            })
        })
        .await
    }

    async fn file_seek(
        &self,
        file: &Self::File,
        position: SeekFrom,
        cancellation: &Cancellation,
    ) -> OperationResult<u64> {
        cancellation.check()?;
        let file = file.retain();
        let cancellation = cancellation.clone();
        blocking::unblock(move || {
            crate::scheduler::blocking::run(&cancellation, || {
                crate::scheduler::file_io::seek(&file, position)
            })
        })
        .await
    }

    async fn file_open(
        &self,
        path: &Path,
        options: FileOptions,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::File> {
        cancellation.check()?;
        let path = path.to_owned();
        let cancellation = cancellation.clone();
        blocking::unblock(move || {
            crate::scheduler::blocking::run(&cancellation, || {
                options.open(&path).map(FileDescriptor::from)
            })
        })
        .await
    }
    async fn file_read<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let file = file.retain();
        file_operation(buffer, cancellation, move |buffer| {
            crate::scheduler::file_io::read(&file, buffer.as_mut())
        })
        .await
    }
    async fn file_write<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let file = file.retain();
        file_operation(buffer, cancellation, move |buffer| {
            crate::scheduler::file_io::write(&file, buffer.as_ref())
        })
        .await
    }
    async fn file_read_at<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let file = file.retain();
        file_operation(buffer, cancellation, move |buffer| {
            read_at(&file, buffer.as_mut(), offset)
        })
        .await
    }
    async fn file_write_at<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let file = file.retain();
        file_operation(buffer, cancellation, move |buffer| {
            write_at(&file, buffer.as_ref(), offset)
        })
        .await
    }
}

async fn file_operation<B: IoBuf>(
    mut buffer: B,
    cancellation: &Cancellation,
    operation: impl FnOnce(&mut B) -> io::Result<usize> + Send + 'static,
) -> BufferResult<B> {
    if let Err(error) = cancellation.check() {
        return (Err(error.into()), buffer);
    }
    let cancellation = cancellation.clone();
    blocking::unblock(move || {
        let result = crate::scheduler::blocking::run(&cancellation, || operation(&mut buffer));
        (result, buffer)
    })
    .await
}

impl Clock for Selector {
    fn now(&self) -> Instant {
        Instant::now()
    }
    async fn sleep_until(
        &self,
        deadline: Instant,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        crate::scheduler::clock::native_sleep_until(deadline, cancellation).await
    }
}

#[cfg(test)]
mod tests;
