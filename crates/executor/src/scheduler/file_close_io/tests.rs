// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::{BlockingOptions, TaskError};
use futures_channel::oneshot;
use futures_lite::future::{block_on, poll_once};
use std::{panic::AssertUnwindSafe, sync::Arc};

#[test]
fn exclusive_close_is_lazy_and_reports_closed_admission() {
    let pool = BlockingPool::new(BlockingOptions::default());
    let file = FileDescriptor::from(tempfile::tempfile().unwrap());
    let retained = Arc::downgrade(&file.0);
    let close = submit(&pool, file);
    assert_eq!(retained.strong_count(), 1);
    drop(close);
    assert_eq!(retained.strong_count(), 0);

    pool.close();
    assert_eq!(
        block_on(submit(&pool, tempfile::tempfile().unwrap().into()))
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        block_on(super::close(&pool, tempfile::tempfile().unwrap().into()))
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        block_on(submit_only(&pool, tempfile::tempfile().unwrap().into()))
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[test]
fn outstanding_use_panics_without_closing_the_retained_file() {
    let pool = BlockingPool::new(BlockingOptions::default());
    let file = FileDescriptor::from(tempfile::tempfile().unwrap());
    let retained = file.retain();
    let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| block_on(submit(&pool, file))));
    assert!(outcome.is_err());
    assert_eq!(Arc::strong_count(&retained), 1);
    assert!(retained.metadata().is_ok());
}

#[test]
fn close_admission_reports_thread_startup_failure() {
    let mut pool = BlockingPool::new(BlockingOptions::default());
    pool.fail_start_at = Some(0);
    assert_eq!(
        block_on(submit(&pool, tempfile::tempfile().unwrap().into()))
            .unwrap_err()
            .to_string(),
        "injected thread startup failure"
    );
}

#[test]
fn close_waits_for_execution_and_reports_os_or_worker_errors() {
    block_on(async {
        let (sender, receiver) = oneshot::channel();
        let mut waiting = Box::pin(wait(receiver));
        assert!(poll_once(&mut waiting).await.is_none());
        sender
            .send(Ok(Err(io::Error::from(io::ErrorKind::PermissionDenied))))
            .unwrap();
        assert_eq!(
            waiting.await.unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );

        let (sender, receiver) = oneshot::channel();
        sender
            .send(Err(TaskError::Panicked(Box::new("worker panic"))))
            .unwrap();
        assert_eq!(
            wait(receiver).await.unwrap_err().to_string(),
            "task panicked"
        );
        let (sender, receiver) = oneshot::channel();
        drop(sender);
        assert!(wait(receiver).await.is_err());

        let pool = BlockingPool::new(BlockingOptions::default());
        wait(
            submit(&pool, tempfile::tempfile().unwrap().into())
                .await
                .unwrap(),
        )
        .await
        .unwrap();
    });
}
