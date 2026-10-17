// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::AddressResolveErrorKind;
use std::{fmt, io};

/// Resolver-specific details stored inside `OperationError::Io`. Recover this value with `io_error.get_ref().and_then(|error| error.downcast_ref::<AddressResolveError>())`.
#[derive(Debug)]
pub struct AddressResolveError {
    pub kind: AddressResolveErrorKind,
    /// A backend-specific resolver code (`EAI_*` or Winsock). Simulators may omit it. Do not interpret it as errno or carry it between platforms.
    pub code: Option<i32>,
    pub message: String,
    /// An underlying system failure, such as errno captured immediately after `EAI_SYSTEM`.
    pub system_error: Option<io::Error>,
}

impl fmt::Display for AddressResolveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(formatter)
    }
}

impl std::error::Error for AddressResolveError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.system_error.as_ref().map(|error| error as _)
    }
}

impl From<AddressResolveError> for io::Error {
    fn from(error: AddressResolveError) -> Self {
        use AddressResolveErrorKind::*;
        let kind = match error.kind {
            TemporaryFailure => io::ErrorKind::WouldBlock,
            NameNotFound | ServiceUnavailable => io::ErrorKind::NotFound,
            InvalidHints => io::ErrorKind::InvalidInput,
            OutOfMemory => io::ErrorKind::OutOfMemory,
            System => error
                .system_error
                .as_ref()
                .map_or(io::ErrorKind::Other, io::Error::kind),
            PermanentFailure | Other => io::ErrorKind::Other,
        };
        Self::new(kind, error)
    }
}
