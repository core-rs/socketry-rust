// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{AddressResolveOptions, Cancellation, OperationResult};
use std::{future::Future, net::SocketAddr};

#[cfg(any(feature = "native", feature = "tokio"))]
pub(crate) mod system;

/// Optional hostname resolution, independent of socket creation and connection.
///
/// Consumers require this capability explicitly; implementing `Socket` does not require a resolver. Implementations may use a system resolver, an asynchronous resolver, or a deterministic table. Resolution policy and address ordering belong to the implementation.
///
/// ```compile_fail
/// use socketry_executor::{AddressResolution, AddressResolveOptions, Cancellation, Socket};
/// async fn resolve<S: Socket>(scheduler: &S) {
///     scheduler.address_resolve("localhost", AddressResolveOptions::default(), &Cancellation::never()).await;
/// }
/// ```
pub trait AddressResolution: Send + Sync {
    /// Resolve a hostname or numeric IP literal to owned socket addresses.
    ///
    /// `name` contains no port; IPv6 literals use the unbracketed `IpAddr` syntax. Empty names and embedded NUL bytes return `InvalidInput`. Other name syntax and resolver errors are implementation-specific. No implicit IDNA conversion, service-name lookup, caching, sorting, deduplication, or connection attempts are required. Results may be empty, including when the family filter removes every address. Every result has the requested port and family; duplicates and resolver order are preserved.
    ///
    /// Futures are lazy. Pre-cancellation wins over validation. Cancellation may prevent queued work from starting; implementations using a blocking system resolver await its actual result once started. A cancellation request does not promise prompt completion. Dropping a future may abandon a running lookup, which retains its inputs and bounded capacity until completion. Await lookups before shutting down their scheduler for graceful completion.
    fn address_resolve(
        &self,
        name: &str,
        options: AddressResolveOptions,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<Vec<SocketAddr>>> + Send;
}
