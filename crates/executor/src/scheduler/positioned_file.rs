// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Blocking positioned file operations shared by selector adapters.
use std::fs::File;
use std::io;

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

fn retry_interrupted(mut operation: impl FnMut() -> io::Result<usize>) -> io::Result<usize> {
    loop {
        match operation() {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}

#[cfg(test)]
mod tests;
