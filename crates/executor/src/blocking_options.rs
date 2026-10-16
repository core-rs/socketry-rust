// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::io;

/// Limits for a scheduler's dedicated blocking pool. Threads start on first admission.
#[derive(Clone, Copy, Debug)]
pub struct BlockingOptions {
    /// Maximum number of concurrently executing closures. Must be nonzero.
    pub threads: usize,
    /// Maximum number of admitted jobs, including both queued and running work. Must be nonzero.
    pub capacity: usize,
}

impl Default for BlockingOptions {
    fn default() -> Self {
        Self {
            threads: 4,
            capacity: 64,
        }
    }
}

impl BlockingOptions {
    pub(crate) fn validate(self) -> io::Result<Self> {
        if self.threads == 0 || self.capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "blocking thread count and capacity must be nonzero",
            ));
        }
        Ok(self)
    }
}
