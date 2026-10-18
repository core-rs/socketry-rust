// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// A file access or cache-management hint. Advice does not change the file's contents, size, or cursor, and the operating system may ignore it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AdviceMode {
    /// Restore the default access policy.
    #[default]
    Normal,
    /// Expect sequential access; the OS may increase readahead.
    Sequential,
    /// Expect random access; the OS may disable readahead.
    Random,
    /// Hint that the range will be accessed soon. Completion does not guarantee that it is resident in memory.
    WillNeed,
    /// Hint that cached pages in the range are no longer needed. This does not guarantee eviction or write dirty pages durably; partial pages may be ignored.
    DontNeed,
    /// Hint that the range is unlikely to be reused. The effect depends on the OS and kernel version.
    NoReuse,
}
