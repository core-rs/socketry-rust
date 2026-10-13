// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use socketry_executor::BufferResult;
use std::{future::Future, io, net::SocketAddr};

/// TCP-only experiment. socket_listen combines create/bind/listen so we can
/// validate ownership without first deciding raw socket options and flags.
pub trait Socket: Send + Sync {
    type Socket: Send + Sync;
    type Listener: Send + Sync;

    fn socket_listen(
        &self,
        address: SocketAddr,
    ) -> impl Future<Output = io::Result<Self::Listener>> + Send;
    fn listener_addr(&self, listener: &Self::Listener) -> io::Result<SocketAddr>;
    fn socket_accept(
        &self,
        listener: &Self::Listener,
    ) -> impl Future<Output = io::Result<Self::Socket>> + Send;
    fn socket_connect(
        &self,
        address: SocketAddr,
    ) -> impl Future<Output = io::Result<Self::Socket>> + Send;
    fn socket_recv(
        &self,
        socket: &Self::Socket,
        buffer: Vec<u8>,
    ) -> impl Future<Output = BufferResult> + Send;
    fn socket_send(
        &self,
        socket: &Self::Socket,
        buffer: Vec<u8>,
    ) -> impl Future<Output = BufferResult> + Send;
}
