// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::blocking_pool::BlockingPool;
use crate::{
    AddressResolveOptions, BlockingError, Cancellation, Cancelled, OperationResult, SocketDomain,
    TaskError,
};
use std::{
    io,
    net::{SocketAddr, ToSocketAddrs},
};

pub(crate) async fn resolve(
    pool: &BlockingPool,
    name: &str,
    options: AddressResolveOptions,
    cancellation: &Cancellation,
) -> OperationResult<Vec<SocketAddr>> {
    resolve_with(pool, name, options, cancellation, |name, port| {
        (name, port).to_socket_addrs().map(Iterator::collect)
    })
    .await
}

// Keep the lookup boundary injectable so cancellation and ownership can be tested without timing external DNS servers or blocking the test indefinitely.
async fn resolve_with(
    pool: &BlockingPool,
    name: &str,
    options: AddressResolveOptions,
    cancellation: &Cancellation,
    lookup: impl FnOnce(&str, u16) -> io::Result<Vec<SocketAddr>> + Send + 'static,
) -> OperationResult<Vec<SocketAddr>> {
    cancellation.check()?;
    if name.is_empty() || name.contains('\0') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected a nonempty hostname without NUL bytes",
        )
        .into());
    }
    let name = name.to_owned();
    let task = pool
        .spawn(
            move || -> io::Result<Vec<SocketAddr>> {
                let mut addresses = lookup(&name, options.port)?;
                addresses.retain(|address| {
                    options
                        .domain
                        .is_none_or(|domain| SocketDomain::from(*address) == domain)
                });
                Ok(addresses)
            },
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
