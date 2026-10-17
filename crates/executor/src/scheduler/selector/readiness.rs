// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Shared readiness operations backed by `async-io`'s persistent registrations.
//!
//! Its process-wide reactor drives OS events independently of our executor.
//! It remains available while registered resources are alive; Socketry task
//! shutdown does not shut down that shared reactor. Futures contain no private
//! coroutine stacks and may be polled on different workers.
use crate::SyncMode;
use crate::scheduler::file_io::{read_at, write_at};
use crate::scheduler::operation::cancellable;
use crate::scheduler::{BufferResult, Clock, File, Interest, Socket as SocketOperations};
use crate::{Cancellation, FileOptions, IoBuf, IoBufMut, OperationResult, SocketRegistration};
use async_io::Async;
use std::fs::File as StdFile;
use std::io::{self, Read, SeekFrom, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

/// A reusable socket registration. Clones share one underlying registration.
#[derive(Clone)]
pub struct Socket(pub(crate) Arc<Async<TcpStream>>);

impl Socket {
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.0.get_ref().local_addr()
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.0.get_ref().peer_addr()
    }
}

/// A reusable listening socket registration.
pub struct Listener(Async<TcpListener>);

impl Listener {
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.0.get_ref().local_addr()
    }
}

/// Compile-time platform selection with a shared readiness reactor.
/// All instances share the reactor and may use each other's registrations.
#[derive(Clone, Copy, Default)]
pub struct Selector;

impl Selector {
    pub fn new() -> io::Result<Self> {
        Ok(Self)
    }
}

fn wrap_socket(socket: io::Result<Async<TcpStream>>) -> io::Result<Socket> {
    socket.map(|socket| Socket(Arc::new(socket)))
}

fn wrap_listener(listener: io::Result<Async<TcpListener>>) -> io::Result<Listener> {
    listener.map(Listener)
}

fn wrap_accepted(
    accepted: io::Result<(Async<TcpStream>, SocketAddr)>,
) -> io::Result<(Socket, SocketAddr)> {
    accepted.map(|(socket, address)| (Socket(Arc::new(socket)), address))
}

impl SocketRegistration for Selector {
    fn register_socket(&self, socket: TcpStream) -> io::Result<Socket> {
        wrap_socket(Async::new(socket))
    }

    fn register_listener(&self, listener: TcpListener) -> io::Result<Listener> {
        wrap_listener(Async::new(listener))
    }
}

impl SocketOperations for Selector {
    type Socket = Socket;
    type Listener = Listener;
    async fn socket_listen(
        &self,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<Listener> {
        cancellation.check()?;
        self.register_listener(TcpListener::bind(address)?)
            .map_err(Into::into)
    }
    fn listener_addr(&self, listener: &Listener) -> io::Result<SocketAddr> {
        listener.local_addr()
    }
    fn socket_local_addr(&self, socket: &Socket) -> io::Result<SocketAddr> {
        socket.local_addr()
    }
    fn socket_peer_addr(&self, socket: &Socket) -> io::Result<SocketAddr> {
        socket.peer_addr()
    }
    async fn connect(
        &self,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<Socket> {
        cancellable(cancellation, async {
            wrap_socket(Async::<TcpStream>::connect(address).await)
        })
        .await
    }
    async fn accept(
        &self,
        listener: &Listener,
        cancellation: &Cancellation,
    ) -> OperationResult<(Socket, SocketAddr)> {
        cancellable(cancellation, async {
            wrap_accepted(listener.0.accept().await)
        })
        .await
    }
    async fn io_read<B: IoBufMut>(
        &self,
        socket: &Socket,
        mut buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let result = cancellable(
            cancellation,
            socket
                .0
                .read_with(|mut socket| socket.read(buffer.as_mut())),
        )
        .await;
        (result, buffer)
    }
    async fn io_write<B: IoBuf>(
        &self,
        socket: &Socket,
        mut buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let owned = &mut buffer;
        let result = cancellable(
            cancellation,
            socket
                .0
                .write_with(move |mut socket| socket.write(owned.as_ref())),
        )
        .await;
        (result, buffer)
    }
    async fn io_wait(
        &self,
        socket: &Socket,
        interest: Interest,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellable(cancellation, async {
            match interest {
                Interest::Readable => socket.0.readable().await,
                Interest::Writable => socket.0.writable().await,
            }
        })
        .await
    }
}

impl File for Selector {
    type File = Arc<StdFile>;
    async fn file_sync(
        &self,
        file: &Self::File,
        mode: SyncMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        let file = Arc::clone(file);
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
        let file = Arc::clone(file);
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
            crate::scheduler::blocking::run(&cancellation, || options.open(&path).map(Arc::new))
        })
        .await
    }
    async fn file_read<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let file = Arc::clone(file);
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
        let file = Arc::clone(file);
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
        let file = Arc::clone(file);
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
        let file = Arc::clone(file);
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
