// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::{read_at, retry_interrupted, write_at};
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

#[test]
fn read_and_write_reject_offsets_outside_the_platform_range() {
    let file = File::open(std::env::current_exe().unwrap()).unwrap();

    assert_eq!(
        read_at(&file, &mut [0; 1], u64::MAX).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        write_at(&file, &[0], u64::MAX).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn positioned_file_operations_read_and_write_at_the_requested_offset() {
    static NEXT_FILE: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "socketry-file-{}-{}",
        std::process::id(),
        NEXT_FILE.fetch_add(1, Ordering::Relaxed)
    ));
    let _cleanup = Cleanup(path.clone());
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();

    assert_eq!(write_at(&file, b"data", 9).unwrap(), 4);
    let mut buffer = [0; 4];
    assert_eq!(read_at(&file, &mut buffer, 9).unwrap(), 4);
    assert_eq!(&buffer, b"data");
}

#[test]
fn retries_interrupted_file_operations() {
    let mut attempts = 0;
    let operation = || {
        attempts += 1;
        if attempts == 1 {
            Err(io::ErrorKind::Interrupted.into())
        } else {
            Ok(7)
        }
    };
    let result = retry_interrupted(operation);

    assert_eq!(result.unwrap(), 7);
    assert_eq!(attempts, 2);
}

struct Cleanup(std::path::PathBuf);

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
