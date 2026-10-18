// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::blocking_pool::BlockingPool;
use crate::{
    AddressHints, AddressInfo, AddressService, BlockingError, Cancellation, Cancelled,
    OperationResult, TaskError,
};
use std::io;
mod native;

pub(crate) async fn resolve(
    pool: &BlockingPool,
    name: Option<&str>,
    service: AddressService<'_>,
    hints: AddressHints,
    cancellation: &Cancellation,
) -> OperationResult<Vec<AddressInfo>> {
    resolve_with(pool, name, service, hints, cancellation, native::lookup).await
}

// Keep the lookup boundary injectable so cancellation and ownership can be tested without timing external DNS servers or blocking the test indefinitely.
async fn resolve_with(
    pool: &BlockingPool,
    name: Option<&str>,
    service: AddressService<'_>,
    hints: AddressHints,
    cancellation: &Cancellation,
    lookup: impl FnOnce(Option<&str>, &str, AddressHints) -> io::Result<Vec<AddressInfo>>
    + Send
    + 'static,
) -> OperationResult<Vec<AddressInfo>> {
    cancellation.check()?;
    if name.is_some_and(|name| name.is_empty() || name.contains('\0')) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected a nonempty hostname without NUL bytes",
        )
        .into());
    }
    let service = match service {
        AddressService::Port(port) => port.to_string(),
        AddressService::Name(name) => {
            if name.is_empty() || name.contains('\0') {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "expected a nonempty service without NUL bytes",
                )
                .into());
            }
            name.to_owned()
        }
    };
    let name = name.map(str::to_owned);
    let task = pool
        .spawn(
            move || lookup(name.as_deref(), &service, hints),
            cancellation,
        )
        .await
        .map_err(|error| match error {
            BlockingError::Cancelled => Cancelled.into(),
            BlockingError::SchedulerClosed => closed().into(),
            BlockingError::Io(error) => crate::OperationError::Io(error),
        })?;
    match task.await {
        Ok(result) => result.map_err(Into::into),
        Err(TaskError::Cancelled) => {
            // The pool also discards queued work during shutdown. Distinguish that from a cooperative request when possible.
            cancellation.check()?;
            Err(closed().into())
        }
        Err(TaskError::Panicked(payload)) => std::panic::resume_unwind(payload),
    }
}

fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "resolver scheduler is closed")
}

#[cfg(test)]
mod tests;
