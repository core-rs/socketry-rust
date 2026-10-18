// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::StatxMetadata;
use std::cell::UnsafeCell;

struct StatRequest {
    identifier: u64,
    file: Option<Arc<StdFile>>,
    // Shared request access must permit kernel writes here. No Rust read or exclusive request borrow occurs between publication and original completion.
    metadata: UnsafeCell<StatxMetadata>,
    reply: Cell<Option<oneshot::Sender<io::Result<StatxMetadata>>>>,
    cancelling: Cell<bool>,
}

impl PendingRequest for StatRequest {
    fn identifier(&self) -> u64 {
        self.identifier
    }

    fn entry(&mut self) -> squeue::Entry {
        let descriptor = self
            .file
            .as_ref()
            .expect("request retains its file")
            .as_raw_fd();
        // The request is boxed before entry construction and stays at that address until the original terminal CQE. Both its inline output and retained file survive cancellation or waiter destruction; the empty pathname has static storage.
        opcode::Statx::new(
            types::Fd(descriptor),
            c"".as_ptr(),
            self.metadata.get().cast(),
        )
        .flags(libc::AT_EMPTY_PATH)
        .mask(libc::STATX_BASIC_STATS | libc::STATX_BTIME)
        .build()
        .user_data(self.identifier)
    }

    fn finish(mut self: Box<Self>, result: io::Result<usize>) {
        let reply = self
            .reply
            .take()
            .expect("unfinished request retains its sender");
        let metadata = *self.metadata.get_mut();
        // Release backend ownership before waking a caller that may immediately close the file. Only original completion permits reading and returning the output.
        drop(self);
        let _ = reply.send(result.map(|_| metadata));
    }

    fn cancelling(&self) -> bool {
        self.cancelling.get()
    }

    fn set_cancelling(&self) {
        self.cancelling.set(true);
    }

    fn take_reply(&self) -> Option<Box<dyn Send>> {
        self.reply
            .take()
            .map(|reply| Box::new(reply) as Box<dyn Send>)
    }
}

impl Drop for StatRequest {
    fn drop(&mut self) {
        drop(self.file.take());
        // A reply remains only for requests discarded before submission. InFlight retains requests whose kernel completion is unknown.
        if let Some(reply) = self.reply.take() {
            let _ = reply.send(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "selector closed before submission",
            )));
        }
    }
}

impl Selector {
    pub(super) async fn stat(
        &self,
        file: &FileDescriptor,
        cancellation: &Cancellation,
    ) -> OperationResult<StatxMetadata> {
        cancellation.check()?;
        self.check_open()?;
        let identifier = self.next_identifier()?;
        let (reply, result) = oneshot::channel();
        let request = StatRequest {
            identifier,
            file: Some(file.retain()),
            metadata: UnsafeCell::new(StatxMetadata::empty()),
            reply: Cell::new(Some(reply)),
            cancelling: Cell::new(false),
        };
        operation_result(
            self.submit(Box::new(request), result, cancellation).await,
            cancellation,
        )
    }
}

#[cfg(test)]
mod tests;
