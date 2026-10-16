// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{BlockingError, Cancellation, TaskError};
use std::future::Future;

/// Optional scheduler capability for owned synchronous work on blocking threads.
pub trait Blocking {
    type Handle<Output>: Future<Output = Result<Output, TaskError>> + Send + 'static
    where
        Output: Send + 'static;

    /// Wait asynchronously for capacity and submit a closure, returning its owning completion handle.
    ///
    /// Admission is lazy: an unpolled future submits nothing. Cancellation before admission returns `BlockingError::Cancelled`. A worker checks cancellation again before starting; cancellation after that boundary does not replace the closure's result. The closure may capture a cancellation clone to cooperate while running.
    ///
    /// Moving the handle transfers ownership. Dropping it requests that queued work be skipped, but cannot interrupt a running closure. Queued cancellation need not complete until a worker reaches the job. Await the handle before shutting down services the closure needs.
    ///
    /// Implementations bound queued plus running jobs; waiting admission futures remain caller-owned. A running job retains its capacity until completion even if its handle is dropped. Job panics are reported as `TaskError::Panicked`.
    fn spawn_blocking<F, Output>(
        &self,
        operation: F,
        cancellation: &Cancellation,
    ) -> impl Future<Output = Result<Self::Handle<Output>, BlockingError>> + Send
    where
        F: FnOnce() -> Output + Send + 'static,
        Output: Send + 'static;
}
