// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! A small loopback request/response exchange framed by TCP half-close. Run with `cargo run -p socketry-executor --example socket_shutdown --features tokio` to exercise both native and Tokio backends.

#[cfg(any(feature = "native", feature = "tokio"))]
use socketry_executor::{Cancellation, IoBuf, Socket, SocketDomain, SocketType};
use std::io;
#[cfg(any(feature = "native", feature = "tokio"))]
use std::net::Shutdown;

#[cfg(any(feature = "native", feature = "tokio"))]
async fn send_all<S: Socket>(
    scheduler: &S,
    socket: &S::Socket,
    mut buffer: Vec<u8>,
    cancellation: &Cancellation,
) -> io::Result<()> {
    let mut offset = 0;
    while offset < buffer.len() {
        // A syscall can write only part of the requested bytes. The owned slice keeps the original allocation and returns it when the operation completes.
        let (result, remaining) = scheduler
            .socket_write(socket, buffer.slice(offset..), cancellation)
            .await;
        buffer = remaining.into_inner();
        let written = result?;
        if written == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        offset += written;
    }
    Ok(())
}

#[cfg(any(feature = "native", feature = "tokio"))]
async fn receive_all<S: Socket>(
    scheduler: &S,
    socket: &S::Socket,
    cancellation: &Cancellation,
) -> io::Result<Vec<u8>> {
    let mut received = Vec::new();
    let mut buffer = vec![0; 128];
    loop {
        let (result, returned) = scheduler.socket_read(socket, buffer, cancellation).await;
        buffer = returned;
        let count = result?;
        if count == 0 {
            // A nonempty TCP receive buffer returning zero means the peer has finished sending.
            return Ok(received);
        }
        received.extend_from_slice(&buffer[..count]);
    }
}

#[cfg(any(feature = "native", feature = "tokio"))]
async fn exchange<S: Socket>(scheduler: &S) -> io::Result<()> {
    // This example deliberately runs to completion. An application can pass its cooperative shutdown signal to reads and writes instead.
    let cancellation = Cancellation::never();
    let listener = scheduler
        .socket_open(SocketDomain::Ipv4, SocketType::Stream, None, &cancellation)
        .await?;
    scheduler
        .socket_bind(&listener, "127.0.0.1:0".parse().unwrap(), &cancellation)
        .await?;
    scheduler
        .socket_listen(&listener, 16, &cancellation)
        .await?;
    let client = scheduler
        .socket_open(SocketDomain::Ipv4, SocketType::Stream, None, &cancellation)
        .await?;
    scheduler
        .socket_connect(
            &client,
            scheduler.socket_local_addr(&listener)?,
            &cancellation,
        )
        .await?;
    let (server, _) = scheduler.socket_accept(&listener, &cancellation).await?;
    scheduler.socket_close(listener).await?;

    // Both peers live in this one future for clarity. The small request fits in the socket buffer; real servers normally handle connections in separate tasks.
    send_all(
        scheduler,
        &client,
        b"Hello, half-close!".to_vec(),
        &cancellation,
    )
    .await?;
    // Finish the write operation first, then announce EOF in the client-to-server direction. This borrows `client`: it remains usable for receiving the response.
    scheduler.socket_shutdown(&client, Shutdown::Write).await?;
    let request = receive_all(scheduler, &server, &cancellation).await?;

    // Receiving EOF did not close the server's sending direction. Echo the completed request and announce the end of the response.
    send_all(scheduler, &server, request, &cancellation).await?;
    scheduler.socket_shutdown(&server, Shutdown::Write).await?;
    let response = receive_all(scheduler, &client, &cancellation).await?;
    assert_eq!(response, b"Hello, half-close!");

    // Every operation above has completed, so both descriptors can now be moved into close. Close consumes ownership; shutdown only disables the requested directions.
    // If cancelling an operation, request cancellation and keep awaiting it before closing. Dropping its future can leave kernel work in flight, and explicit close may then panic.
    scheduler.socket_close(client).await?;
    scheduler.socket_close(server).await?;
    // Use `socket_close_submit(socket).await` when only acceptance matters. Do not replace the shutdowns above with `_submit`: the next step depends on their completion.
    Ok(())
}

fn main() -> io::Result<()> {
    #[cfg(feature = "native")]
    {
        let scheduler = socketry_executor::Scheduler::with_workers(1)?;
        // block_on drives this root future until the exchange completes. The same generic function works with either scheduler implementation.
        scheduler.block_on(exchange(&scheduler))?;
    }
    #[cfg(feature = "tokio")]
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let scheduler =
            socketry_executor::scheduler::tokio::Scheduler::new(runtime.handle().clone());
        runtime.block_on(exchange(&scheduler))?;
        runtime.block_on(scheduler.shutdown());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn half_close_exchange_completes_on_enabled_backends() {
        super::main().unwrap();
    }
}
