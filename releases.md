# Releases

## Unreleased

- Document the complete timer lifecycle comparison with Tokio across cancellation rates in the timer crate readme.

- Add the independent `socketry-timers` crate and expose it as `socketry::timers`. Include reusable batched queues, optional worker assistance and lazy sleep futures, plus the experimental inbox driver and Tokio lifecycle benchmarks.
- Rename the portable socket operations trait from `Network` to `Socket` in `socketry`, `socketry-executor`, and their public `scheduler` module. Update imports and trait bounds.
- Rename the portable file operations trait from `FileIo` to `File` in `socketry`, `socketry-executor`, and their public `scheduler` module. Update imports and trait bounds; alias `std::fs::File` when both names are used.
- Add runtime-independent `Cancellation`, `Cancelled`, and `defer_cancel` for cooperative service shutdown and asynchronous draining. Export them through `socketry-executor` and `socketry`.
- Let Cargo select the dependency version in the installation example.

## v0.2.0

- Rename the positioned file I/O trait from `FileIO` to `FileIo` in `socketry` and `socketry-executor`, including the public `scheduler` module. Update imports and trait bounds.

## v0.1.5

- Cover io\_uring initialization failures, readiness retries, cancellation, and shutdown while making request ownership invariants explicit.
- Expose the conventional `FileIO` name with `FileIo` compatibility aliases, extract portable scheduler contracts, and require coverage for every supported platform and feature implementation.

## v0.1.4

- Adopt `socketry-project` 0.3.7 for shared project tasks and Markdown normalization.
- Require the aggregate test and coverage result for pull request merges.

## v0.1.3

- Use the shared Socketry Project tasks and update agent context setup guidance.

## v0.1.2

- Use the shared `socketry-project` Releasing skill for the standard release process and remove references to the duplicate Bake Cargo publishing context.

## v0.1.1

- Create or update GitHub Releases after successful crates.io publication.
- Move implementation and design guidance into the package's public context.
- Add Bake Agent Context tasks to the repository's development workspace.
