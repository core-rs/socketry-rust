// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::SocketOperation;
use std::{future::Future, io};

/// Consume and close an exclusively owned socket through the scheduler.
pub trait SocketClose: SocketOperation {
    /// Consume and close an exclusively owned socket, awaiting bounded admission and OS completion. Finish all other operations first: close never cancels or joins them, and retained backend use may panic. Dropping an unpolled close releases the owned descriptor through its ordinary destructor.
    fn socket_close(&self, socket: Self::Socket) -> impl Future<Output = io::Result<()>> + Send;

    /// Consume the socket and await bounded cleanup admission only. Accepted close jobs survive waiter abandonment and are drained during graceful scheduler shutdown. Execution errors are discarded; use `socket_close` to observe them. The same exclusive-ownership requirement applies.
    fn socket_close_submit(
        &self,
        socket: Self::Socket,
    ) -> impl Future<Output = io::Result<()>> + Send;
}
