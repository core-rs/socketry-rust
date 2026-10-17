// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Blocking shared-cursor and positioned file operations shared by selector adapters.
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};

pub(crate) fn seek(mut file: &File, position: SeekFrom) -> io::Result<u64> {
    if matches!(position, SeekFrom::Start(offset) if offset > i64::MAX as u64) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file offset exceeds i64::MAX",
        ));
    }
    retry_interrupted(|| file.seek(position))
}

pub(crate) fn read(mut file: &File, buffer: &mut [u8]) -> io::Result<usize> {
    retry_interrupted(|| file.read(buffer))
}

pub(crate) fn write(mut file: &File, buffer: &[u8]) -> io::Result<usize> {
    retry_interrupted(|| file.write(buffer))
}

pub(crate) fn read_at(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    if offset > i64::MAX as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file offset exceeds i64::MAX",
        ));
    }
    let operation = || {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.read_at(buffer, offset)
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            file.seek_read(buffer, offset)
        }
    };
    retry_interrupted(operation)
}

pub(crate) fn write_at(file: &File, buffer: &[u8], offset: u64) -> io::Result<usize> {
    if offset > i64::MAX as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file offset exceeds i64::MAX",
        ));
    }
    let operation = || {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.write_at(buffer, offset)
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            file.seek_write(buffer, offset)
        }
    };
    retry_interrupted(operation)
}

fn retry_interrupted<T>(mut operation: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    loop {
        match operation() {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}

#[cfg(test)]
mod tests;
