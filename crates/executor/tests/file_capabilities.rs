// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use futures_lite::future::block_on;
use socketry_executor::{
    BufferResult, Cancellation, FileOperation, FileRead, IoBufMut, OperationError,
};
use std::sync::Mutex;

// This implementation has no OS resources and supplies no open, write, close, or metadata operations.
struct ReadOnly;

impl FileOperation for ReadOnly {
    type File = Mutex<&'static [u8]>;
}

impl FileRead for ReadOnly {
    async fn file_read<B: IoBufMut>(
        &self,
        file: &Self::File,
        mut buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        if let Err(error) = cancellation.check() {
            return (Err(error.into()), buffer);
        }
        let mut remaining = file.lock().unwrap();
        let count = remaining.len().min(buffer.as_mut().len());
        buffer.as_mut()[..count].copy_from_slice(&remaining[..count]);
        *remaining = &remaining[count..];
        (Ok(count), buffer)
    }
}

async fn read_chunk<S: FileRead>(
    scheduler: &S,
    file: &S::File,
    buffer: Vec<u8>,
    cancellation: &Cancellation,
) -> BufferResult {
    scheduler.file_read(file, buffer, cancellation).await
}

#[test]
fn read_only_implementation_satisfies_a_read_only_consumer() {
    let file = Mutex::new(b"abc".as_slice());
    let buffer = vec![0; 2];
    let allocation = buffer.as_ptr();
    let never = Cancellation::never();
    let (result, buffer) = block_on(read_chunk(&ReadOnly, &file, buffer, &never));
    assert_eq!(result.unwrap(), 2);
    assert_eq!(buffer, b"ab");
    assert_eq!(buffer.as_ptr(), allocation);

    let cancelled = Cancellation::new();
    cancelled.cancel();
    let (result, buffer) = block_on(read_chunk(&ReadOnly, &file, buffer, &cancelled));
    assert!(matches!(result, Err(OperationError::Cancelled(_))));
    assert_eq!(buffer, b"ab");
    assert_eq!(buffer.as_ptr(), allocation);

    let (result, buffer) = block_on(read_chunk(&ReadOnly, &file, buffer, &never));
    assert_eq!(result.unwrap(), 1);
    assert_eq!(buffer, b"cb");
    let (result, buffer) = block_on(read_chunk(&ReadOnly, &file, buffer, &never));
    assert_eq!(result.unwrap(), 0);
    assert_eq!(buffer.as_ptr(), allocation);
}
