// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

// Socket and IoBuf are traits: importing them makes their methods available.
use socketry::{Cancellation, IoBuf, OperationResult, Scheduler, Socket};
use std::io;

// This function works with any scheduler implementing the Socket trait.
// `&S` and `&Cancellation` borrow values owned by main().
// The Vec is owned by this future and returned with the echoed bytes.
async fn echo<S: Socket>(
    scheduler: &S,
    mut buffer: Vec<u8>,
    cancellation: &Cancellation,
) -> OperationResult<Vec<u8>> {
    // This is the address used by server.rs.
    // `.await` suspends the future until connecting finishes.
    let socket = scheduler
        .connect("127.0.0.1:9000".parse().unwrap(), cancellation)
        .await?;

    let length = buffer.len();
    let mut offset = 0;

    while offset < length {
        // Send up to 8192 bytes, then read their echo before sending more.
        // This lets both ends make progress even for a large message.
        let end = (offset + 8192).min(length);

        // `slice` moves the buffer into an owned range view without copying.
        // The operation returns (result, view), even when it fails.
        let (result, returned) = scheduler
            .io_write(&socket, buffer.slice(offset..end), cancellation)
            .await;
        buffer = returned.into_inner();

        // `?` extracts the byte count or returns the operation's error.
        // A write may send only part of the requested range.
        let written = result?;
        if written == 0 {
            return Err(io::Error::from(io::ErrorKind::WriteZero).into());
        }

        // TCP is a byte stream: one write need not correspond to one read.
        // Read exactly the number of bytes this write actually sent.
        let end = offset + written;
        let mut received = offset;
        while received < end {
            // Reuse the part of the buffer we have already sent, replacing
            // it with the reply. Bytes not yet sent remain untouched.
            let (result, returned) = scheduler
                .io_read(&socket, buffer.slice(received..end), cancellation)
                .await;
            buffer = returned.into_inner();

            let count = result?;
            if count == 0 {
                // EOF before the expected reply means the server closed early.
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
            }
            received += count;
        }

        offset = end;
    }

    // We know the expected reply length, so we do not wait for server EOF.
    // Returning releases the local socket and gives the buffer to main().
    Ok(buffer)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The first argument is the message. Quote it if it contains spaces.
    // `nth(1)` skips the executable name; the closure supplies a default
    // only when no message argument was given.
    let message = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "Hello, Socketry!".to_owned());

    let scheduler = Scheduler::with_workers(1)?;

    // This client completes one exchange without cooperative cancellation.
    // Unlike the server, it does not install a signal subscription.
    let cancellation = Cancellation::never();

    // Copy the message's UTF-8 bytes into owned storage for the I/O operations.
    // Keep the original string so we can verify the reply below.
    // `block_on` drives this future to completion, blocking the main thread.
    let reply = scheduler.block_on(echo(&scheduler, message.as_bytes().to_vec(), &cancellation))?;

    if reply != message.as_bytes() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "echo reply differs").into());
    }

    // Convert the returned bytes into a String, checking that they are UTF-8.
    println!("{}", String::from_utf8(reply)?);
    Ok(())
}
