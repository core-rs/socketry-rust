// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::SpawnError;

#[test]
fn spawn_errors_have_descriptive_messages() {
    assert_eq!(
        SpawnError::SchedulerClosed.to_string(),
        "the scheduler is closed"
    );
    assert_eq!(
        SpawnError::IdentifiersExhausted.to_string(),
        "task identifiers are exhausted"
    );
    assert!(std::error::Error::source(&SpawnError::SchedulerClosed).is_none());
}
