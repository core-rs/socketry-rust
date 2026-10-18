// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// The portable classification of a filesystem object. Platform metadata can distinguish additional kinds, such as Unix devices, sockets, and FIFOs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileType {
    File,
    Directory,
    Symlink,
    Other,
}
