// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::SocketOperation;
use std::{future::Future, io, net::Shutdown};

/// Shut down one or both directions of a socket through the scheduler.
pub trait SocketShutdown: SocketOperation {
    /// Shut down reads, writes, or both, awaiting bounded admission and OS completion. The descriptor remains owned by the caller. Finish preceding writes before `Shutdown::Write`; reads may continue. Success confirms local shutdown, not peer receipt. Repeated shutdown and later I/O preserve platform-specific results.
    ///
    /// This lazy cleanup operation has no cancellation input. After admission it retains the socket and executes even if its waiter is dropped. Await its completion before explicitly closing the socket.
    fn socket_shutdown(
        &self,
        socket: &Self::Socket,
        how: Shutdown,
    ) -> impl Future<Output = io::Result<()>> + Send;

    /// Request shutdown and await bounded admission only. Acceptance does not establish completion or ordering with later operations, including close. The backend retains the socket until execution finishes; graceful scheduler shutdown drains accepted requests. Use `socket_shutdown` when subsequent work requires completion or errors.
    fn socket_shutdown_submit(
        &self,
        socket: &Self::Socket,
        how: Shutdown,
    ) -> impl Future<Output = io::Result<()>> + Send;
}
