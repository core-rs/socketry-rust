// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Portable classification of resolver failures. Native error codes remain available separately and are not errno values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AddressResolveErrorKind {
    TemporaryFailure,
    NameNotFound,
    ServiceUnavailable,
    InvalidHints,
    PermanentFailure,
    OutOfMemory,
    System,
    Other,
}
