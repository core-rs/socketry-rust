// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use futures_lite::future::block_on;
use socketry_executor::{
    BufferResult, Cancellation, IoBuf, IoBufMut, OperationError, SocketOperation, SocketRead,
    SocketWrite,
};
use std::sync::Mutex;

// A protocol can run against existing in-memory connections without socket creation, listening, options, messages, or explicit close.
struct Memory;

struct Connection {
    incoming: &'static [u8],
    outgoing: Vec<u8>,
}

impl SocketOperation for Memory {
    type Socket = Mutex<Connection>;
}

impl SocketRead for Memory {
    async fn socket_read<B: IoBufMut>(
        &self,
        socket: &Self::Socket,
        mut buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        if let Err(error) = cancellation.check() {
            return (Err(error.into()), buffer);
        }
        let mut connection = socket.lock().unwrap();
        let count = connection.incoming.len().min(buffer.as_mut().len()).min(2);
        buffer.as_mut()[..count].copy_from_slice(&connection.incoming[..count]);
        connection.incoming = &connection.incoming[count..];
        (Ok(count), buffer)
    }
}

impl SocketWrite for Memory {
    async fn socket_write<B: IoBuf>(
        &self,
        socket: &Self::Socket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        if let Err(error) = cancellation.check() {
            return (Err(error.into()), buffer);
        }
        let count = buffer.as_ref().len().min(1);
        socket
            .lock()
            .unwrap()
            .outgoing
            .extend_from_slice(&buffer.as_ref()[..count]);
        (Ok(count), buffer)
    }
}

async fn echo_chunk<S: SocketRead + SocketWrite>(
    scheduler: &S,
    socket: &S::Socket,
    buffer: Vec<u8>,
    cancellation: &Cancellation,
) -> BufferResult {
    let (result, mut buffer) = scheduler.socket_read(socket, buffer, cancellation).await;
    let count = match result {
        Ok(count) => count,
        Err(error) => return (Err(error), buffer),
    };
    let mut offset = 0;
    while offset < count {
        let (result, view) = scheduler
            .socket_write(socket, buffer.slice(offset..count), cancellation)
            .await;
        buffer = view.into_inner();
        match result {
            Ok(0) => {
                return (
                    Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into()),
                    buffer,
                );
            }
            Ok(written) => offset += written,
            Err(error) => return (Err(error), buffer),
        }
    }
    (Ok(count), buffer)
}

#[test]
fn protocol_needs_only_reads_and_writes_on_one_shared_resource_type() {
    let socket = Mutex::new(Connection {
        incoming: b"abc",
        outgoing: Vec::new(),
    });
    let buffer = vec![0; 8];
    let allocation = buffer.as_ptr();
    let never = Cancellation::never();
    let (result, buffer) = block_on(echo_chunk(&Memory, &socket, buffer, &never));
    assert_eq!(result.unwrap(), 2);
    assert_eq!(buffer.as_ptr(), allocation);
    assert_eq!(socket.lock().unwrap().outgoing, b"ab");

    let cancelled = Cancellation::new();
    cancelled.cancel();
    let (result, buffer) = block_on(echo_chunk(&Memory, &socket, buffer, &cancelled));
    assert!(matches!(result, Err(OperationError::Cancelled(_))));
    assert_eq!(buffer.as_ptr(), allocation);
    assert_eq!(socket.lock().unwrap().incoming, b"c");

    let (result, buffer) = block_on(echo_chunk(&Memory, &socket, buffer, &never));
    assert_eq!(result.unwrap(), 1);
    let (result, buffer) = block_on(echo_chunk(&Memory, &socket, buffer, &never));
    assert_eq!(result.unwrap(), 0);
    assert_eq!(buffer.as_ptr(), allocation);
    assert_eq!(socket.lock().unwrap().outgoing, b"abc");
}
