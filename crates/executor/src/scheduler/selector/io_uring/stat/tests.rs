// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::FileMetadata;
use futures_lite::future::{block_on, poll_once};

#[test]
fn native_statx_queries_the_open_descriptor_after_unlink() {
    use std::os::unix::fs::MetadataExt;
    block_on(async {
        let selector = Selector::new().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("unlinked");
        let never = Cancellation::never();
        let file = selector
            .file_open(&path, FileOptions::CREATE, &never)
            .await
            .unwrap();
        selector.file_truncate(&file, 37, &never).await.unwrap();
        std::fs::remove_file(&path).unwrap();
        let expected = file.as_ref().metadata().unwrap();
        let metadata = selector.file_stat(&file, &never).await.unwrap();
        selector.file_close(file).await.unwrap();
        assert!(!path.exists());
        assert_eq!(metadata.size(), Some(37));
        assert_eq!(metadata.file_type(), Some(crate::FileType::File));
        assert_eq!(metadata.modified(), expected.modified().ok());
        assert_eq!(metadata.accessed(), expected.accessed().ok());
        // Filesystems may omit birth time; assert the conversion only when the native validity mask promises it.
        if metadata.as_statx().stx_mask & libc::STATX_BTIME != 0 {
            assert_eq!(metadata.created(), expected.created().ok());
        }
        assert_ne!(metadata.as_statx().stx_mask & libc::STATX_INO, 0);
        assert_eq!(metadata.as_statx().stx_ino, expected.ino());
        assert_eq!(metadata.as_statx().stx_nlink, 0);
        selector.shutdown().await;
    });
}

fn harness() -> (Selector, mpsc::Receiver<Command>, UnixStream) {
    let (commands, incoming) = mpsc::channel();
    let (notification, peer) = UnixStream::pair().unwrap();
    (
        Selector {
            supports_file_truncate: true,
            supports_file_allocate: true,
            controller: Arc::new(Controller {
                cleanup: crate::blocking_pool::BlockingPool::new(crate::BlockingOptions::default()),
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

#[test]
fn metadata_requests_retain_files_until_original_completion() {
    block_on(async {
        for outcome in 0..5 {
            let (selector, incoming, _peer) = harness();
            let file = FileDescriptor::from(tempfile::tempfile().unwrap());
            let observer = Arc::downgrade(&file.retain());
            let cancellation = Cancellation::new();
            drop(selector.file_stat(&file, &cancellation));
            assert!(incoming.try_recv().is_err());
            let mut operation = Box::pin(selector.file_stat(&file, &cancellation));
            assert!(poll_once(&mut operation).await.is_none());
            let Command::Submit(mut request) = incoming.try_recv().unwrap() else {
                unreachable!("the first poll submits to an open, uncancelled selector")
            };
            let entry = request.entry();
            assert_eq!(entry.get_opcode(), u32::from(opcode::Statx::CODE));
            assert_eq!(entry.get_user_data(), request.identifier());
            assert_eq!(observer.strong_count(), 2);
            if outcome >= 2 && outcome != 4 {
                cancellation.cancel();
                assert!(poll_once(&mut operation).await.is_none());
                assert!(
                    matches!(incoming.try_recv().unwrap(), Command::Cancel(id) if id == request.identifier())
                );
                assert!(!request.cancelling());
                request.set_cancelling();
                assert!(request.cancelling());
            }
            let result = match outcome {
                1 => Err(io::ErrorKind::PermissionDenied.into()),
                3 => Err(io::Error::from_raw_os_error(libc::ECANCELED)),
                _ => Ok(0),
            };
            if outcome == 4 {
                drop(operation);
                assert!(
                    matches!(incoming.try_recv().unwrap(), Command::Cancel(id) if id == request.identifier())
                );
                assert_eq!(observer.strong_count(), 2);
                request.finish(result);
            } else {
                request.finish(result);
                assert_eq!(observer.strong_count(), 1);
                let result = operation.await;
                match outcome {
                    1 => assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied),
                    3 => assert!(matches!(result, Err(crate::OperationError::Cancelled(_)))),
                    // This harness supplies no metadata fields; successful original completion still wins concurrent cancellation.
                    _ => assert_eq!(result.unwrap().size(), None),
                }
            }
            assert_eq!(observer.strong_count(), 1);
        }
    });
}

#[test]
fn queued_and_lost_completions_follow_the_shared_failure_contract() {
    block_on(async {
        for lost in [false, true] {
            let (selector, incoming, _peer) = harness();
            let file = FileDescriptor::from(tempfile::tempfile().unwrap());
            let cancellation = Cancellation::never();
            let mut operation = Box::pin(selector.file_stat(&file, &cancellation));
            assert!(poll_once(&mut operation).await.is_none());
            let Command::Submit(request) = incoming.try_recv().unwrap() else {
                unreachable!("the first poll only submits in this harness")
            };
            if lost {
                drop(request.take_reply());
                assert!(request.take_reply().is_none());
                // This harness never publishes to the kernel. Actual InFlight failure deliberately retains the request and its output.
                drop(request);
                assert!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(operation)))
                        .is_err()
                );
            } else {
                drop(request);
                assert_eq!(
                    operation.await.unwrap_err().kind(),
                    io::ErrorKind::BrokenPipe
                );
            }
        }
    });
}

#[test]
fn metadata_submission_reports_closed_and_exhausted_selectors() {
    block_on(async {
        let (selector, incoming, _peer) = harness();
        let file = FileDescriptor::from(tempfile::tempfile().unwrap());
        let never = Cancellation::never();
        drop(incoming);
        assert_eq!(
            selector.file_stat(&file, &never).await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        selector
            .controller
            .next_identifier
            .store(CANCEL_TAG - 1, Ordering::Relaxed);
        assert!(
            selector
                .file_stat(&file, &never)
                .await
                .unwrap_err()
                .to_string()
                .contains("identifiers exhausted")
        );
        selector.close();
        assert_eq!(
            selector.file_stat(&file, &never).await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    });
}

struct ExclusiveOnWake(std::sync::Weak<StdFile>);

impl std::task::Wake for ExclusiveOnWake {
    fn wake(self: Arc<Self>) {
        assert_eq!(
            self.0.strong_count(),
            1,
            "metadata completion woke the caller before releasing the file"
        );
    }
}

#[test]
fn metadata_completion_releases_file_before_notifying_waiters() {
    for discarded in [false, true] {
        let file = Arc::new(tempfile::tempfile().unwrap());
        let (reply, mut result) = oneshot::channel();
        let mut request = Box::new(StatRequest {
            identifier: 1,
            file: Some(Arc::clone(&file)),
            metadata: UnsafeCell::new(StatxMetadata::empty()),
            reply: Cell::new(Some(reply)),
            cancelling: Cell::new(false),
        });
        request.metadata.get_mut().0.stx_mask = libc::STATX_SIZE;
        request.metadata.get_mut().0.stx_size = 17;
        let waker = std::task::Waker::from(Arc::new(ExclusiveOnWake(Arc::downgrade(&file))));
        let mut context = std::task::Context::from_waker(&waker);
        assert!(
            std::pin::Pin::new(&mut result)
                .poll(&mut context)
                .is_pending()
        );
        if discarded {
            drop(request);
            assert_eq!(
                block_on(result).unwrap().unwrap_err().kind(),
                io::ErrorKind::BrokenPipe
            );
        } else {
            request.finish(Ok(0));
            assert_eq!(block_on(result).unwrap().unwrap().size(), Some(17));
        }
    }
}
