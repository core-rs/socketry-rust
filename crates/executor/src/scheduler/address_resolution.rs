// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{AddressHints, AddressInfo, AddressService, Cancellation, OperationResult};
use std::future::Future;

#[cfg(any(feature = "native", feature = "tokio"))]
pub(crate) mod system;

/// Optional IP endpoint resolution, independent of socket creation and connection. Implementations may use a system resolver, an asynchronous resolver, or a deterministic table.
///
/// ```compile_fail
/// use socketry_executor::{AddressResolution, AddressHints, AddressService, Cancellation, SocketScheduler};
/// async fn resolve<S: SocketScheduler>(scheduler: &S) {
///     scheduler.address_resolve(Some("localhost"), AddressService::Port(80), AddressHints::default(), &Cancellation::never()).await;
/// }
/// ```
pub trait AddressResolution: Send + Sync {
    /// Resolve a hostname and service to owned endpoints and socket creation parameters using typed resolver hints.
    ///
    /// Names contain no port. An absent name selects wildcard addresses with `passive`, or loopback without it. Empty names/services and embedded NUL bytes return `InvalidInput`. Numeric ports avoid named-service lookup. `numeric_host` prevents hostname lookup. Family, type, protocol, and address-configuration hints apply during resolution. With unspecified type/protocol the resolver can return multiple transports, including raw sockets. Preserve candidate ordering, duplicates, scope identifiers, and native type/protocol values. Native non-IP results are rejected with `InvalidData` because this capability returns IP endpoints. Successful results may be empty; no matching family or transport may instead be a resolver error, depending on the backend.
    ///
    /// Resolver failures are carried by `AddressResolveError` inside `OperationError::Io`, retaining a portable category and any native resolver code. Lookup and service databases are implementation-specific; there is no implicit connection policy, Socketry cache, canonical-name query, or IDNA normalization guarantee.
    ///
    /// Futures are lazy; pre-cancellation wins over validation. Cancellation can prevent admission or queued execution. Started blocking lookups return their actual result, and cancellation does not promise prompt completion. Dropping a future may abandon a running lookup, whose worker retains both inputs and its bounded capacity until completion. Await lookups before scheduler shutdown for graceful completion.
    fn address_resolve(
        &self,
        name: Option<&str>,
        service: AddressService<'_>,
        hints: AddressHints,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<Vec<AddressInfo>>> + Send;
}
