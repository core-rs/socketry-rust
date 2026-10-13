// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::Cancelled;
use std::{fmt, io};

/// Cooperative cancellation is distinct from an operating-system error.
#[derive(Debug)]
pub enum OperationError {
    Cancelled(Cancelled),
    Io(io::Error),
}

pub type OperationResult<T> = Result<T, OperationError>;

impl OperationError {
    /// I/O-compatible classification; match `Cancelled` to distinguish a
    /// cooperative request from an OS interruption.
    pub fn kind(&self) -> io::ErrorKind {
        match self {
            Self::Cancelled(_) => io::ErrorKind::Interrupted,
            Self::Io(error) => error.kind(),
        }
    }

    pub fn raw_os_error(&self) -> Option<i32> {
        match self {
            Self::Cancelled(_) => None,
            Self::Io(error) => error.raw_os_error(),
        }
    }
}

impl fmt::Display for OperationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled(error) => error.fmt(formatter),
            Self::Io(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for OperationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Cancelled(error) => error,
            Self::Io(error) => error,
        })
    }
}

impl From<io::Error> for OperationError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<Cancelled> for OperationError {
    fn from(error: Cancelled) -> Self {
        Self::Cancelled(error)
    }
}

impl From<OperationError> for io::Error {
    fn from(error: OperationError) -> Self {
        match error {
            OperationError::Io(error) => error,
            OperationError::Cancelled(error) => Self::new(io::ErrorKind::Interrupted, error),
        }
    }
}
