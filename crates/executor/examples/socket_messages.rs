// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Send one UDP message from several owned buffers, then receive its payload and sender address. Run with `cargo run -p socketry-executor --example socket_messages --features tokio` to exercise both backends.

use std::io;

#[cfg(any(feature = "native", feature = "tokio"))]
async fn exchange<
    S: socketry_executor::SocketMessages
        + socketry_executor::SocketOpen
        + socketry_executor::SocketBind
        + socketry_executor::SocketLocalAddr
        + socketry_executor::SocketClose,
>(
    scheduler: &S,
) -> io::Result<()> {
    use socketry_executor::{
        Cancellation, SocketDomain, SocketMessage, SocketRecvFlags, SocketSendFlags, SocketType,
    };

    // This example completes a single exchange. A server can pass its service cancellation signal here instead.
    let cancellation = Cancellation::never();
    let receiver = scheduler
        .socket_open(
            SocketDomain::Ipv4,
            SocketType::Datagram,
            None,
            &cancellation,
        )
        .await?;
    let sender = scheduler
        .socket_open(
            SocketDomain::Ipv4,
            SocketType::Datagram,
            None,
            &cancellation,
        )
        .await?;
    scheduler
        .socket_bind(&receiver, "127.0.0.1:0".parse().unwrap(), &cancellation)
        .await?;
    let address = scheduler.socket_local_addr(&receiver)?;

    // These two allocations form one datagram. The backend builds the native iovec array without concatenating or copying their contents. An address selects the destination without connecting the socket; None would use an existing connected peer.
    let message = SocketMessage {
        buffers: vec![b"hello ".to_vec(), b"world".to_vec()],
        address: Some(address),
    };
    let (sent, message) = scheduler
        .socket_sendmsg(&sender, message, SocketSendFlags::default(), &cancellation)
        .await;
    assert_eq!(sent?, 11);
    assert_eq!(message.buffers[0], b"hello ");

    // Ownership of each receive buffer moves into the future and comes back with the result, including on error or cancellation. Capacity comes from the initialized lengths, not Vec::capacity(). Nonempty storage also lets us distinguish a received empty datagram from OS-specific zero-capacity behavior.
    let (received, buffers) = scheduler
        .socket_recvmsg(
            &receiver,
            vec![vec![0; 6], vec![0; 32]],
            SocketRecvFlags::default(),
            &cancellation,
        )
        .await;
    let received = received?;
    assert_eq!(received.bytes, 11);
    assert!(!received.truncated);
    assert_eq!(&buffers[0], b"hello ");
    assert_eq!(&buffers[1][..5], b"world");
    println!(
        "Received {} bytes from {:?}",
        received.bytes, received.address
    );

    // A small destination reports truncated = true and discards the remaining datagram bytes. Peeking with SocketRecvFlags { peek: true } observes a message without consuming it, but does not reserve it against other readers.
    // Both operations have completed, so no kernel access remains. Close can now consume the descriptors. For cancellation, request it and await the operation before closing; dropping a future alone does not establish completion on io_uring.
    scheduler.socket_close(sender).await?;
    scheduler.socket_close(receiver).await?;
    Ok(())
}

fn main() -> io::Result<()> {
    #[cfg(feature = "native")]
    {
        let scheduler = socketry_executor::Scheduler::with_workers(1)?;
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
    fn message_exchange_on_enabled_backends() {
        super::main().unwrap();
    }
}
