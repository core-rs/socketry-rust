// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Shared message contracts for backends implementing the optional capability.

use socketry_executor::{
    Cancellation, IoBuf, IoBufMut, OperationError, SocketDomain, SocketMessage, SocketMessages,
    SocketRecvFlags, SocketScheduler, SocketSendFlags, SocketType,
};
use std::{
    future::{Future, poll_fn},
    net::SocketAddr,
    task::Poll,
};

const NEVER: Cancellation = Cancellation::never();

async fn bound<S: SocketScheduler>(
    scheduler: &S,
    address: SocketAddr,
    kind: SocketType,
) -> S::Socket {
    let socket = scheduler
        .socket_open(SocketDomain::from(address), kind, None, &NEVER)
        .await
        .unwrap();
    scheduler
        .socket_bind(&socket, address, &NEVER)
        .await
        .unwrap();
    socket
}

async fn pending(future: std::pin::Pin<&mut impl Future>) {
    let mut future = future;
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

pub async fn check<S: SocketMessages>(scheduler: &S) {
    for local in ["127.0.0.1:0", "[::1]:0"] {
        let receiver = bound(scheduler, local.parse().unwrap(), SocketType::Datagram).await;
        let sender = bound(scheduler, local.parse().unwrap(), SocketType::Datagram).await;
        let address = scheduler.socket_local_addr(&receiver).unwrap();
        let source = scheduler.socket_local_addr(&sender).unwrap();

        // An unpolled send has no effect. Receive cancellation also returns the original owners.
        drop(scheduler.socket_sendmsg(
            &sender,
            SocketMessage {
                buffers: vec![b"never".to_vec()],
                address: Some(address),
            },
            SocketSendFlags::default(),
            &NEVER,
        ));
        let stopped = Cancellation::new();
        let buffers = vec![vec![7; 8]];
        let allocation = buffers[0].as_ptr();
        let mut receiving = Box::pin(scheduler.socket_recvmsg(
            &receiver,
            buffers,
            SocketRecvFlags::default(),
            &stopped,
        ));
        pending(receiving.as_mut()).await;
        stopped.cancel();
        let (result, buffers) = receiving.await;
        assert!(matches!(result, Err(OperationError::Cancelled(_))));
        assert_eq!(buffers[0].as_ptr(), allocation);
        assert_eq!(buffers, vec![vec![7; 8]]);

        let stopping = Cancellation::new();
        let message = SocketMessage {
            buffers,
            address: Some(address),
        };
        let sending =
            scheduler.socket_sendmsg(&sender, message, SocketSendFlags::default(), &stopping);
        stopping.cancel();
        let (result, message) = sending.await;
        assert!(matches!(result, Err(OperationError::Cancelled(_))));
        assert_eq!(message.buffers[0].as_ptr(), allocation);
        let (result, buffers) = scheduler
            .socket_recvmsg(
                &receiver,
                message.buffers,
                SocketRecvFlags::default(),
                &stopping,
            )
            .await;
        assert!(matches!(result, Err(OperationError::Cancelled(_))));
        assert_eq!(buffers[0].as_ptr(), allocation);

        // Scatter/gather is one datagram. Peek reports truncation without consuming it.
        let message = SocketMessage {
            buffers: vec![b"he".to_vec(), vec![], b"llo".to_vec()],
            address: Some(address),
        };
        let allocation = message.buffers[0].as_ptr();
        let (sent, message) = scheduler
            .socket_sendmsg(
                &sender,
                message,
                SocketSendFlags { dont_route: true },
                &NEVER,
            )
            .await;
        assert_eq!(sent.unwrap(), 5);
        assert_eq!(message.buffers[0].as_ptr(), allocation);
        assert_eq!(message.address, Some(address));
        let (received, buffers) = scheduler
            .socket_recvmsg(
                &receiver,
                vec![vec![0; 1], vec![0; 2]],
                SocketRecvFlags { peek: true },
                &NEVER,
            )
            .await;
        let received = received.unwrap();
        assert_eq!(received.bytes, 3);
        assert_eq!(received.address, Some(source));
        assert!(received.truncated);
        assert!(!received.control_truncated);
        assert_eq!(buffers, vec![b"h".to_vec(), b"el".to_vec()]);
        let buffers = vec![
            vec![9; 2].into_boxed_slice().slice(..),
            vec![9; 6].into_boxed_slice().slice(1..5),
        ];
        let allocation = buffers[0].as_ref().as_ptr();
        let (received, buffers) = scheduler
            .socket_recvmsg(&receiver, buffers, SocketRecvFlags::default(), &NEVER)
            .await;
        let received = received.unwrap();
        assert_eq!(received.bytes, 5);
        assert_eq!(received.address, Some(source));
        assert!(!received.truncated);
        assert!(!received.control_truncated);
        assert_eq!(buffers[0].as_ref(), b"he");
        assert_eq!(buffers[0].as_ref().as_ptr(), allocation);
        assert_eq!(buffers[1].as_ref(), b"llo\x09");
        assert_eq!(
            &*buffers.into_iter().nth(1).unwrap().into_inner(),
            b"\x09llo\x09\x09"
        );

        scheduler
            .socket_connect(&sender, address, &NEVER)
            .await
            .unwrap();
        for buffers in [vec![], vec![vec![]]] {
            let length: usize = buffers.iter().map(Vec::len).sum();
            let (sent, _) = scheduler
                .socket_sendmsg(
                    &sender,
                    SocketMessage {
                        buffers,
                        address: None,
                    },
                    SocketSendFlags::default(),
                    &NEVER,
                )
                .await;
            assert_eq!(sent.unwrap(), length);
            let (received, buffers) = scheduler
                .socket_recvmsg::<Vec<u8>>(
                    &receiver,
                    vec![vec![0; 1]],
                    SocketRecvFlags::default(),
                    &NEVER,
                )
                .await;
            let received = received.unwrap();
            assert_eq!(received.bytes, 0);
            assert_eq!(received.address, Some(source));
            assert_eq!(received.truncated, length > 0);
            assert_eq!(buffers, vec![vec![0; 1]]);
        }

        // Truncation consumes one datagram and leaves the next message intact. IoBuf deliberately does not require Sync.
        let (sent, _) = scheduler
            .socket_sendmsg(
                &sender,
                SocketMessage {
                    buffers: vec![LocalBuffer {
                        bytes: b"abcdef".to_vec(),
                        marker: std::cell::Cell::new(()),
                    }],
                    address: None,
                },
                SocketSendFlags::default(),
                &NEVER,
            )
            .await;
        assert_eq!(sent.unwrap(), 6);
        // A nonempty peek establishes that the message is queued. Some kernels return immediately from a zero-capacity receive even when no data has arrived.
        let (peeked, _) = scheduler
            .socket_recvmsg(
                &receiver,
                vec![vec![0; 1]],
                SocketRecvFlags { peek: true },
                &NEVER,
            )
            .await;
        assert_eq!(peeked.unwrap().bytes, 1);
        let (peeked, empty) = scheduler
            .socket_recvmsg::<Vec<u8>>(&receiver, vec![], SocketRecvFlags { peek: true }, &NEVER)
            .await;
        let peeked = peeked.unwrap();
        assert_eq!(peeked.bytes, 0);
        assert!(peeked.truncated);
        assert!(empty.is_empty());
        let (received, _) = scheduler
            .socket_recvmsg(
                &receiver,
                vec![vec![0; 2]],
                SocketRecvFlags::default(),
                &NEVER,
            )
            .await;
        assert!(received.unwrap().truncated);
        let stopped = Cancellation::new();
        let mut receiving = Box::pin(scheduler.socket_recvmsg(
            &receiver,
            vec![LocalBuffer {
                bytes: vec![0; 2],
                marker: std::cell::Cell::new(()),
            }],
            SocketRecvFlags::default(),
            &stopped,
        ));
        pending(receiving.as_mut()).await;
        stopped.cancel();
        assert!(matches!(
            receiving.await.0,
            Err(OperationError::Cancelled(_))
        ));
        scheduler.socket_close(sender).await.unwrap();
        scheduler.socket_close(receiver).await.unwrap();
    }
    stream(scheduler).await;
}

async fn stream<S: SocketMessages>(scheduler: &S) {
    let listener = bound(
        scheduler,
        "127.0.0.1:0".parse().unwrap(),
        SocketType::Stream,
    )
    .await;
    scheduler.socket_listen(&listener, 8, &NEVER).await.unwrap();
    let sender = scheduler
        .socket_open(SocketDomain::Ipv4, SocketType::Stream, None, &NEVER)
        .await
        .unwrap();
    scheduler
        .socket_connect(
            &sender,
            scheduler.socket_local_addr(&listener).unwrap(),
            &NEVER,
        )
        .await
        .unwrap();
    let (receiver, _) = scheduler.socket_accept(&listener, &NEVER).await.unwrap();
    let (sent, _) = scheduler
        .socket_sendmsg(
            &sender,
            SocketMessage {
                buffers: vec![b"ab".to_vec(), b"cd".to_vec()],
                address: None,
            },
            SocketSendFlags::default(),
            &NEVER,
        )
        .await;
    assert_eq!(sent.unwrap(), 4);
    for expected in [b"ab", b"cd"] {
        let (received, buffers) = scheduler
            .socket_recvmsg(
                &receiver,
                vec![vec![0; 1], vec![0; 1]],
                SocketRecvFlags::default(),
                &NEVER,
            )
            .await;
        let received = received.unwrap();
        assert_eq!(received.bytes, 2);
        assert!(!received.truncated);
        assert_eq!(buffers.concat(), expected);
    }
    scheduler
        .socket_shutdown(&sender, std::net::Shutdown::Write)
        .await
        .unwrap();
    let (received, buffers) = scheduler
        .socket_recvmsg(
            &receiver,
            vec![vec![9; 1]],
            SocketRecvFlags::default(),
            &NEVER,
        )
        .await;
    assert_eq!(received.unwrap().bytes, 0);
    assert_eq!(buffers, vec![vec![9; 1]]);
    scheduler.socket_close(sender).await.unwrap();
    scheduler.socket_close(receiver).await.unwrap();
    scheduler.socket_close(listener).await.unwrap();
}

struct LocalBuffer {
    bytes: Vec<u8>,
    marker: std::cell::Cell<()>,
}
impl AsRef<[u8]> for LocalBuffer {
    fn as_ref(&self) -> &[u8] {
        self.marker.get();
        &self.bytes
    }
}
impl AsMut<[u8]> for LocalBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}
// SAFETY: The owned Vec allocation is stable and exclusively owned throughout each operation. The Cell proves that no Sync bound is required.
unsafe impl IoBuf for LocalBuffer {}
unsafe impl IoBufMut for LocalBuffer {}
