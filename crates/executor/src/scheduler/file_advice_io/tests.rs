// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::{BlockingOptions, FileAdvice};
use futures_lite::future::{block_on, poll_once};
use std::{
    sync::{Arc, mpsc},
    time::Duration,
};

const TIMEOUT: Duration = Duration::from_secs(10);

async fn block_worker(pool: &BlockingPool) -> mpsc::Sender<()> {
    let (started, starting) = mpsc::channel();
    let (release, released) = mpsc::channel();
    drop(
        pool.submit(move || {
            started.send(()).unwrap();
            released.recv_timeout(TIMEOUT).unwrap();
        })
        .await
        .unwrap(),
    );
    starting.recv_timeout(TIMEOUT).unwrap();
    release
}

// Each caller configures one worker and capacity two. A blocked first job makes accepted advice retain its descriptor until shutdown drains it.
pub(crate) async fn accepted<S: FileAdvice<File = FileDescriptor>>(
    scheduler: &S,
    pool: &BlockingPool,
    submit_only: bool,
) {
    let release = block_worker(pool).await;
    let file = FileDescriptor::from(tempfile::tempfile().unwrap());
    let retained = Arc::downgrade(&file.0);
    if submit_only {
        scheduler
            .file_advise_submit(&file, 0, 0, AdviceMode::Normal)
            .await
            .unwrap();
    } else {
        let mut waiting = Box::pin(scheduler.file_advise(&file, 0, 0, AdviceMode::Normal));
        assert!(poll_once(&mut waiting).await.is_none());
        drop(waiting);
    }
    drop(file);
    assert_eq!(retained.strong_count(), 1);
    pool.close();
    let mut draining = Box::pin(pool.wait_submitted());
    assert!(poll_once(&mut draining).await.is_none());
    assert_eq!(retained.strong_count(), 1);
    release.send(()).unwrap();
    draining.await;
    assert_eq!(retained.strong_count(), 0);
}

// Capacity one is consumed by the blocked worker. Neither form can admit until capacity is released; abandoned admission releases the retained descriptor.
pub(crate) async fn saturated<S: FileAdvice<File = FileDescriptor>>(
    scheduler: &S,
    pool: &BlockingPool,
) {
    let release = block_worker(pool).await;
    let file = FileDescriptor::from(tempfile::tempfile().unwrap());
    let retained = Arc::downgrade(&file.0);
    let unpolled = scheduler.file_advise(&file, 0, 0, AdviceMode::Normal);
    assert_eq!(retained.strong_count(), 1);
    drop(unpolled);
    let unpolled = scheduler.file_advise_submit(&file, 0, 0, AdviceMode::Normal);
    assert_eq!(retained.strong_count(), 1);
    drop(unpolled);
    let mut waiting = Box::pin(scheduler.file_advise(&file, 0, 0, AdviceMode::Normal));
    assert!(poll_once(&mut waiting).await.is_none());
    assert_eq!(retained.strong_count(), 2);
    drop(waiting);
    assert_eq!(retained.strong_count(), 1);
    let mut waiting = Box::pin(scheduler.file_advise_submit(&file, 0, 0, AdviceMode::Normal));
    assert!(poll_once(&mut waiting).await.is_none());
    assert_eq!(retained.strong_count(), 2);
    // Closing admission wakes capacity waiters and returns an error, without accepting their jobs.
    pool.close();
    assert_eq!(waiting.await.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(retained.strong_count(), 1);
    release.send(()).unwrap();
    pool.wait_submitted().await;
}

pub(crate) async fn closed<S: FileAdvice<File = FileDescriptor>>(scheduler: &S) {
    let file = tempfile::tempfile().unwrap().into();
    assert_eq!(
        scheduler
            .file_advise(&file, 0, 0, AdviceMode::Normal)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        scheduler
            .file_advise_submit(&file, 0, 0, AdviceMode::Normal)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[test]
fn advice_reports_syscall_errors_and_submission_reports_only_admission() {
    use std::{fs::File, os::fd::FromRawFd};
    let mut descriptors = [-1; 2];
    // SAFETY: The output array has room for both descriptors. On success each descriptor is immediately given one owning File.
    assert_eq!(
        unsafe { libc::pipe2(descriptors.as_mut_ptr(), libc::O_CLOEXEC) },
        0
    );
    // SAFETY: pipe2 succeeded; each fresh descriptor receives exactly one owning File.
    let (read, _write) = unsafe {
        (
            File::from_raw_fd(descriptors[0]),
            File::from_raw_fd(descriptors[1]),
        )
    };
    let file = FileDescriptor::from(read);
    let pool = BlockingPool::new(BlockingOptions::default());
    block_on(async {
        assert_eq!(
            run(&pool, &file, 0, 0, AdviceMode::Normal)
                .await
                .unwrap_err()
                .raw_os_error(),
            Some(libc::ESPIPE)
        );
        submit_only(&pool, &file, 0, 0, AdviceMode::Normal)
            .await
            .unwrap();
        pool.wait_submitted().await;
        assert_eq!(Arc::strong_count(&file.0), 1);
    });
}

#[test]
fn advice_reports_startup_failure_and_closed_admission() {
    let mut pool = BlockingPool::new(BlockingOptions::default());
    pool.fail_start_at = Some(0);
    let file = tempfile::tempfile().unwrap().into();
    assert_eq!(
        block_on(run(&pool, &file, 0, 0, AdviceMode::Normal))
            .unwrap_err()
            .to_string(),
        "injected thread startup failure"
    );
    assert_eq!(
        block_on(submit_only(&pool, &file, 0, 0, AdviceMode::Normal))
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[test]
fn advice_releases_the_descriptor_before_waking_completion() {
    use std::{
        future::Future,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Wake, Waker},
    };
    struct Observe {
        file: std::sync::Weak<std::fs::File>,
        references: AtomicUsize,
    }
    impl Wake for Observe {
        fn wake(self: Arc<Self>) {
            self.references
                .store(self.file.strong_count(), Ordering::SeqCst);
        }
    }
    block_on(async {
        let pool = BlockingPool::new(BlockingOptions {
            threads: 1,
            capacity: 2,
        });
        let release = block_worker(&pool).await;
        let file = FileDescriptor::from(tempfile::tempfile().unwrap());
        let mut completion = Box::pin(
            submit(&pool, &file, 0, 0, AdviceMode::Normal)
                .await
                .unwrap(),
        );
        let observer = Arc::new(Observe {
            file: Arc::downgrade(&file.0),
            references: AtomicUsize::new(usize::MAX),
        });
        let waker = Waker::from(Arc::clone(&observer));
        assert!(
            completion
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        release.send(()).unwrap();
        pool.wait_submitted().await;
        assert_eq!(observer.references.load(Ordering::SeqCst), 1);
        completion.await.unwrap().unwrap().unwrap();
    });
}
