// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::scheduler::message_io::{self, Message};
use crate::{SocketMessage, SocketMessages, SocketRecvFlags, SocketRecvResult, SocketSendFlags};

#[derive(Clone, Copy)]
enum MessageOperation {
    Send(SocketSendFlags),
    Receive(SocketRecvFlags),
}

struct MessageRequest<B: IoBuf> {
    identifier: u64,
    socket: Option<SocketDescriptor>,
    message: Option<Message<B>>,
    operation: MessageOperation,
    reply: Cell<Option<oneshot::Sender<RawBufferResult<Message<B>>>>>,
    cancelling: Cell<bool>,
}

impl<B: IoBuf> PendingRequest for MessageRequest<B> {
    fn identifier(&self) -> u64 {
        self.identifier
    }

    fn entry(&mut self) -> squeue::Entry {
        let descriptor = types::Fd(
            self.socket
                .as_ref()
                .expect("request retains its socket")
                .get_ref()
                .as_raw_fd(),
        );
        let message = self.message.as_mut().expect("request retains its message");
        match self.operation {
            MessageOperation::Send(flags) => {
                opcode::SendMsg::new(descriptor, message.send_header())
                    .flags(message_io::send_flags(flags) as _)
                    .build()
            }
            MessageOperation::Receive(flags) => {
                opcode::RecvMsg::new(descriptor, message.recv_header())
                    .flags(message_io::recv_flags(flags) as _)
                    .build()
            }
        }
        .user_data(self.identifier)
    }

    fn finish(mut self: Box<Self>, result: io::Result<usize>) {
        let reply = self
            .reply
            .take()
            .expect("unfinished request retains its sender");
        let message = self
            .message
            .take()
            .expect("request returns its message once");
        // Release the descriptor before notification so the caller may immediately close it. The original terminal CQE establishes that metadata and payload can be returned.
        drop(self);
        let _ = reply.send((result, message));
    }

    fn cancelling(&self) -> bool {
        self.cancelling.get()
    }
    fn set_cancelling(&self) {
        self.cancelling.set(true);
    }
    fn take_reply(&self) -> Option<Box<dyn Send>> {
        self.reply
            .take()
            .map(|reply| Box::new(reply) as Box<dyn Send>)
    }
}

impl<B: IoBuf> Drop for MessageRequest<B> {
    fn drop(&mut self) {
        drop(self.socket.take());
        // Only unsubmitted requests can reach this destructor with a reply. InFlight deliberately retains requests whose kernel completion is unknown.
        if let Some(reply) = self.reply.take() {
            let _ = reply.send((
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "selector closed before submission",
                )),
                self.message
                    .take()
                    .expect("request returns its message once"),
            ));
        }
    }
}

impl Selector {
    async fn message_operation<B: IoBuf>(
        &self,
        socket: &SocketDescriptor,
        message: Message<B>,
        operation: MessageOperation,
        cancellation: &Cancellation,
    ) -> RawBufferResult<Message<B>> {
        let identifier = match self.check_open().and_then(|()| self.next_identifier()) {
            Ok(identifier) => identifier,
            Err(error) => return (Err(error), message),
        };
        let (reply, result) = oneshot::channel();
        let request = MessageRequest {
            identifier,
            socket: Some(socket.retain()),
            message: Some(message),
            operation,
            reply: Cell::new(Some(reply)),
            cancelling: Cell::new(false),
        };
        self.submit(Box::new(request), result, cancellation).await
    }
}

impl SocketMessages for Selector {
    async fn socket_sendmsg<B: IoBuf>(
        &self,
        socket: &Self::Socket,
        message: SocketMessage<B>,
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<usize>, SocketMessage<B>) {
        if let Err(error) = cancellation
            .check()
            .map_err(crate::OperationError::from)
            .and_then(|()| message_io::validate(&message.buffers).map_err(Into::into))
        {
            return (Err(error), message);
        }
        let address = message.address;
        let message = Message::sending(message.buffers, address);
        let (result, message) = retry_operation(
            message,
            |message| {
                self.message_operation(socket, message, MessageOperation::Send(flags), cancellation)
            },
            || async {
                readiness::Selector
                    .socket_wait(socket, Interest::Writable, cancellation)
                    .await
                    .map_err(Into::into)
            },
            cancellation,
        )
        .await;
        (
            operation_result(result, cancellation),
            SocketMessage {
                buffers: message.into_buffers(),
                address,
            },
        )
    }

    async fn socket_recvmsg<B: IoBufMut>(
        &self,
        socket: &Self::Socket,
        buffers: Vec<B>,
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<SocketRecvResult>, Vec<B>) {
        if let Err(error) = cancellation
            .check()
            .map_err(crate::OperationError::from)
            .and_then(|()| message_io::validate(&buffers).map_err(Into::into))
        {
            return (Err(error), buffers);
        }
        let message = Message::receiving(buffers);
        let (result, message) = retry_operation(
            message,
            |message| {
                self.message_operation(
                    socket,
                    message,
                    MessageOperation::Receive(flags),
                    cancellation,
                )
            },
            || async {
                readiness::Selector
                    .socket_wait(socket, Interest::Readable, cancellation)
                    .await
                    .map_err(Into::into)
            },
            cancellation,
        )
        .await;
        let result = operation_result(result, cancellation).map(|bytes| message.received(bytes));
        (result, message.into_buffers())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::future::{block_on, poll_once};

    fn assert_cancelled<T: std::fmt::Debug>(result: OperationResult<T>) {
        assert_eq!(
            std::mem::discriminant(&result.unwrap_err()),
            std::mem::discriminant(&crate::OperationError::Cancelled(crate::Cancelled)),
        );
    }

    fn harness() -> (Selector, mpsc::Receiver<Command>, UnixStream) {
        let (commands, incoming) = mpsc::channel();
        let (notification, peer) = UnixStream::pair().unwrap();
        (
            Selector {
                supports_file_truncate: true,
                supports_file_allocate: true,
                controller: Arc::new(Controller {
                    cleanup: crate::blocking_pool::BlockingPool::new(
                        crate::BlockingOptions::default(),
                    ),
                    commands,
                    notification,
                    next_identifier: AtomicU64::new(0),
                    closed: AtomicBool::new(false),
                    shutdown: Arc::new(Shutdown {
                        finished: AtomicBool::new(false),
                        event: CompletionEvent::new(),
                    }),
                }),
            },
            incoming,
            peer,
        )
    }

    fn socket() -> SocketDescriptor {
        SocketDescriptor::new(
            socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::DGRAM, None).unwrap(),
        )
        .unwrap()
    }

    async fn run(
        selector: &Selector,
        socket: &SocketDescriptor,
        send: bool,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        if send {
            selector
                .socket_sendmsg(
                    socket,
                    SocketMessage {
                        buffers: vec![vec![7; 8]],
                        address: Some("127.0.0.1:9".parse().unwrap()),
                    },
                    SocketSendFlags::default(),
                    cancellation,
                )
                .await
                .0
        } else {
            selector
                .socket_recvmsg(
                    socket,
                    vec![vec![7; 8]],
                    SocketRecvFlags::default(),
                    cancellation,
                )
                .await
                .0
                .map(|result| result.bytes)
        }
    }

    #[test]
    fn message_lifetimes_follow_original_completion_and_release_before_notification() {
        block_on(async {
            for send in [false, true] {
                for abandon in [false, true] {
                    for success in [false, true] {
                        let (selector, incoming, _peer) = harness();
                        let socket = socket();
                        let cancellation = Cancellation::new();
                        drop(run(&selector, &socket, send, &cancellation));
                        assert!(incoming.try_recv().is_err());
                        assert_eq!(Arc::strong_count(&socket.inner), 1);
                        let mut operation = Box::pin(run(&selector, &socket, send, &cancellation));
                        assert!(poll_once(&mut operation).await.is_none());
                        let Command::Submit(request) = incoming.try_recv().unwrap() else {
                            unreachable!(
                                "the first poll can only submit: this harness has not cancelled or closed the selector"
                            )
                        };
                        assert!(!request.cancelling());
                        request.set_cancelling();
                        assert!(request.cancelling());
                        cancellation.cancel();
                        assert!(poll_once(&mut operation).await.is_none());
                        assert!(
                            matches!(incoming.try_recv().unwrap(), Command::Cancel(id) if id == request.identifier())
                        );
                        assert_eq!(Arc::strong_count(&socket.inner), 2);
                        let completion = if success {
                            Ok(0)
                        } else {
                            Err(io::Error::from_raw_os_error(libc::ECANCELED))
                        };
                        if abandon {
                            drop(operation);
                            assert_eq!(Arc::strong_count(&socket.inner), 2);
                            request.finish(completion);
                        } else {
                            request.finish(completion);
                            // The descriptor reference is gone before the waiter is polled again.
                            assert_eq!(Arc::strong_count(&socket.inner), 1);
                            let result = operation.await;
                            if success {
                                assert_eq!(result.unwrap(), 0);
                            } else {
                                assert_cancelled(result);
                            }
                        }
                        assert_eq!(Arc::strong_count(&socket.inner), 1);
                    }
                }
            }
        });
    }

    #[test]
    fn message_would_block_retries_readiness_and_remains_cancellable() {
        block_on(async {
            for send in [false, true] {
                for cancel in [false, true] {
                    let (selector, incoming, _peer) = harness();
                    let socket = socket();
                    socket
                        .get_ref()
                        .bind(&"127.0.0.1:0".parse::<SocketAddr>().unwrap().into())
                        .unwrap();
                    let cancellation = Cancellation::new();
                    let mut operation = Box::pin(run(&selector, &socket, send, &cancellation));
                    assert!(poll_once(&mut operation).await.is_none());
                    let Command::Submit(request) = incoming.try_recv().unwrap() else {
                        unreachable!(
                            "the first poll can only submit: this harness has not cancelled or closed the selector"
                        )
                    };
                    request.finish(Err(io::ErrorKind::WouldBlock.into()));
                    if cancel {
                        cancellation.cancel();
                        assert_cancelled(operation.await);
                    } else {
                        if !send {
                            let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
                            sender
                                .send_to(b"ready", socket.local_addr().unwrap())
                                .unwrap();
                        }
                        let request = poll_fn(|cx| {
                            assert!(operation.as_mut().poll(cx).is_pending());
                            match incoming.try_recv() {
                                Ok(Command::Submit(request)) => Poll::Ready(request),
                                Err(mpsc::TryRecvError::Empty) => Poll::Pending,
                                _ => unreachable!("the live harness only resubmits after EAGAIN; it has not cancelled or closed the selector"),
                            }
                        })
                        .await;
                        request.finish(Ok(0));
                        assert_eq!(operation.await.unwrap(), 0);
                    }
                    assert_eq!(Arc::strong_count(&socket.inner), 1);
                }
            }
        });
    }

    #[test]
    fn queued_messages_return_owners_and_unknown_completions_disconnect_waiters() {
        block_on(async {
            for lost_completion in [false, true] {
                let (selector, incoming, _peer) = harness();
                let socket = socket();
                let never = Cancellation::never();
                let buffers = vec![vec![7; 8]];
                let allocation = buffers[0].as_ptr();
                let mut operation = Box::pin(selector.socket_recvmsg(
                    &socket,
                    buffers,
                    SocketRecvFlags::default(),
                    &never,
                ));
                assert!(poll_once(&mut operation).await.is_none());
                let Command::Submit(request) = incoming.try_recv().unwrap() else {
                    unreachable!(
                        "the first poll can only submit: this harness has not cancelled or closed the selector"
                    )
                };
                if lost_completion {
                    drop(request.take_reply());
                    assert!(request.take_reply().is_none());
                    // No kernel request was published by this harness, so releasing this test request is safe. The actual InFlight failure path deliberately retains it.
                    drop(request);
                    assert!(
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(
                            operation
                        )))
                        .is_err()
                    );
                } else {
                    drop(request);
                    assert_eq!(Arc::strong_count(&socket.inner), 1);
                    let (result, buffers) = operation.await;
                    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
                    assert_eq!(buffers[0].as_ptr(), allocation);
                    assert_eq!(buffers, vec![vec![7; 8]]);
                }
            }
        });
    }

    #[test]
    fn messages_return_errors_when_submission_is_disconnected_or_identifiers_exhausted() {
        block_on(async {
            let (selector, incoming, _peer) = harness();
            drop(incoming);
            let socket = socket();
            for send in [false, true] {
                assert_eq!(
                    run(&selector, &socket, send, &Cancellation::never())
                        .await
                        .unwrap_err()
                        .kind(),
                    io::ErrorKind::BrokenPipe
                );
            }
            selector
                .controller
                .next_identifier
                .store(CANCEL_TAG - 1, Ordering::Relaxed);
            for send in [false, true] {
                assert!(
                    run(&selector, &socket, send, &Cancellation::never())
                        .await
                        .unwrap_err()
                        .to_string()
                        .contains("identifiers exhausted")
                );
            }
        });
    }
}
