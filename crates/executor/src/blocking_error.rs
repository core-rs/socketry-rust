// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::{fmt, io};

/// Blocking work could not be admitted. Job completion separately reports `TaskError`.
#[derive(Debug)]
pub enum BlockingError {
    Cancelled,
    SchedulerClosed,
    /// The dedicated pool could not start its worker threads.
    Io(io::Error),
}

impl fmt::Display for BlockingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => formatter.write_str("blocking admission cancelled"),
            Self::SchedulerClosed => formatter.write_str("the scheduler is closed"),
            Self::Io(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for BlockingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}
