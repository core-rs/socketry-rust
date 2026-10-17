// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// How file-space allocation affects the logical file length.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AllocationMode {
    /// Reserve the range and extend the file to its end if necessary. Existing bytes are preserved and newly exposed bytes read as zero. The file is never shrunk.
    #[default]
    Extend,
    /// Reserve the range without changing the logical file length, including when allocating beyond EOF.
    KeepSize,
}
