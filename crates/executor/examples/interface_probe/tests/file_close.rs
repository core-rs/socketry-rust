// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use socketry_executor::{Cancellation, FileOptions, OperationError};
use socketry_executor::{FileClose, FileOpen, FileRead, FileWrite};
use std::path::Path;

pub async fn check<S: FileClose + FileOpen + FileRead + FileWrite>(scheduler: &S, root: &Path) {
    let never = Cancellation::never();
    let path = root.join("close");
    let file = scheduler
        .file_open(&path, FileOptions::CREATE, &never)
        .await
        .unwrap();
    let (result, _) = scheduler.file_write(&file, b"data".to_vec(), &never).await;
    assert!(result.unwrap() > 0);
    scheduler.file_close(file).await.unwrap();

    let file = scheduler
        .file_open(&path, FileOptions::READ, &never)
        .await
        .unwrap();
    let cancelled = Cancellation::new();
    cancelled.cancel();
    let (result, returned) = scheduler.file_read(&file, vec![0; 4], &cancelled).await;
    assert!(matches!(result, Err(OperationError::Cancelled(_))));
    assert_eq!(returned, vec![0; 4]);
    // A cancellation result is a completion boundary, so explicit close is now valid.
    scheduler.file_close(file).await.unwrap();

    let file = scheduler
        .file_open(&path, FileOptions::READ, &never)
        .await
        .unwrap();
    scheduler.file_close_submit(file).await.unwrap();
    // Admission is the only guarantee here. The owning scheduler's graceful shutdown drains this job.
}
