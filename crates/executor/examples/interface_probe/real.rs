// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Bridges to existing backends. The Arc requirement stays inside this module.
use super::{File, Open, Socket};
use socketry_executor::{BufferResult, File as CurrentFile, Socket as CurrentSocket};
use std::{fs::File as StdFile, io, net::SocketAddr, path::Path, sync::Arc};

fn open(path: &Path, mode: Open) -> io::Result<Arc<StdFile>> {
    match mode {
        Open::Read => StdFile::open(path),
        Open::Create => StdFile::create(path),
    }
    .map(Arc::new)
}

#[cfg(feature = "native")]
pub struct Native {
    pub scheduler: socketry_executor::SchedulerHandle,
}

#[cfg(feature = "native")]
impl Native {
    async fn open(&self, path: &Path, mode: Open) -> io::Result<Arc<StdFile>> {
        let path = path.to_owned();
        blocking::unblock(move || open(&path, mode)).await
    }
}

#[cfg(feature = "tokio")]
pub struct Tokio {
    pub scheduler: socketry_executor::scheduler::tokio::SchedulerHandle,
    pub runtime: tokio::runtime::Handle,
}

#[cfg(feature = "tokio")]
impl Tokio {
    async fn open(&self, path: &Path, mode: Open) -> io::Result<Arc<StdFile>> {
        let path = path.to_owned();
        self.runtime
            .spawn_blocking(move || open(&path, mode))
            .await
            .map_err(io::Error::other)?
    }
}

// These adapters intentionally reuse production reads/writes and registration.
// Resource creation is the missing capability being prototyped here. The
// selected backend chooses its pool; application code does not select one.
macro_rules! capabilities {
    ($backend:ty, $scheduler:ty) => {
        impl File for $backend {
            type File = Arc<StdFile>;
            async fn file_open(&self, path: &Path, mode: Open) -> io::Result<Self::File> {
                self.open(path, mode).await
            }
            async fn file_read_at(
                &self,
                file: &Self::File,
                buffer: Vec<u8>,
                offset: u64,
            ) -> BufferResult {
                self.scheduler
                    .file_read_at(Arc::clone(file), buffer, offset)
                    .await
            }
            async fn file_write_at(
                &self,
                file: &Self::File,
                buffer: Vec<u8>,
                offset: u64,
            ) -> BufferResult {
                self.scheduler
                    .file_write_at(Arc::clone(file), buffer, offset)
                    .await
            }
        }
        impl Socket for $backend {
            type Socket = <$scheduler as CurrentSocket>::Socket;
            type Listener = <$scheduler as CurrentSocket>::Listener;
            async fn socket_listen(&self, address: SocketAddr) -> io::Result<Self::Listener> {
                self.scheduler
                    .register_listener(std::net::TcpListener::bind(address)?)
            }
            fn listener_addr(&self, listener: &Self::Listener) -> io::Result<SocketAddr> {
                listener.local_addr()
            }
            async fn socket_accept(&self, listener: &Self::Listener) -> io::Result<Self::Socket> {
                self.scheduler
                    .accept(listener)
                    .await
                    .map(|(socket, _)| socket)
            }
            async fn socket_connect(&self, address: SocketAddr) -> io::Result<Self::Socket> {
                self.scheduler.connect(address).await
            }
            async fn socket_recv(&self, socket: &Self::Socket, buffer: Vec<u8>) -> BufferResult {
                self.scheduler.io_read(socket, buffer).await
            }
            async fn socket_send(&self, socket: &Self::Socket, buffer: Vec<u8>) -> BufferResult {
                self.scheduler.io_write(socket, buffer).await
            }
        }
    };
}

#[cfg(feature = "native")]
capabilities!(Native, socketry_executor::SchedulerHandle);
#[cfg(feature = "tokio")]
capabilities!(Tokio, socketry_executor::scheduler::tokio::SchedulerHandle);
