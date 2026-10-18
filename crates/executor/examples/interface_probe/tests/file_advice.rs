// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use socketry_executor::{AdviceMode, Cancellation, FileAdvice, FileOptions};
use std::{
    io::{self, SeekFrom},
    path::Path,
};

pub async fn check<S: FileAdvice>(scheduler: &S, root: &Path) {
    let never = Cancellation::never();
    let path = root.join("advice");
    let file = scheduler
        .file_open(&path, FileOptions::CREATE, &never)
        .await
        .unwrap();
    assert_eq!(
        scheduler
            .file_write(&file, vec![42], &never)
            .await
            .0
            .unwrap(),
        1
    );
    scheduler
        .file_seek(&file, SeekFrom::Start(3), &never)
        .await
        .unwrap();
    assert_eq!(AdviceMode::default(), AdviceMode::Normal);
    for advice in [
        AdviceMode::Normal,
        AdviceMode::Sequential,
        AdviceMode::Random,
        AdviceMode::WillNeed,
        AdviceMode::DontNeed,
        AdviceMode::NoReuse,
    ] {
        // Hints are not guarantees of cache state. Check observable file semantics, including ranges beyond EOF and zero length through EOF.
        for (offset, length) in [
            (0, 0),
            (0, 1),
            (512, 0),
            (4096, 8192),
            (u32::MAX as u64 + 1, 1),
        ] {
            scheduler
                .file_advise(&file, offset, length, advice)
                .await
                .unwrap();
        }
        assert_eq!(
            scheduler
                .file_seek(&file, SeekFrom::Current(0), &never)
                .await
                .unwrap(),
            3
        );
        let reader = scheduler
            .file_open(&path, FileOptions::READ, &never)
            .await
            .unwrap();
        scheduler.file_advise(&reader, 0, 0, advice).await.unwrap();
        let (result, buffer) = scheduler.file_read(&reader, vec![0], &never).await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(buffer, [42]);
        assert_eq!(
            scheduler
                .file_read(&reader, vec![0], &never)
                .await
                .0
                .unwrap(),
            0
        );
        scheduler.file_close(reader).await.unwrap();
        let submitted = scheduler
            .file_open(&path, FileOptions::READ, &never)
            .await
            .unwrap();
        scheduler
            .file_advise_submit(&submitted, 0, 0, advice)
            .await
            .unwrap();
        // Admission is not completion; ordinary drop lets the backend retain this descriptor while its job finishes.
        drop(submitted);
    }
    for (offset, length) in [
        (u64::MAX, 0),
        (0, u64::MAX),
        (i64::MAX as u64, 1),
        (u64::MAX, 1),
    ] {
        assert_eq!(
            scheduler
                .file_advise(&file, offset, length, AdviceMode::Normal)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            scheduler
                .file_advise_submit(&file, offset, length, AdviceMode::Normal)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
    drop(scheduler.file_advise(&file, 0, 0, AdviceMode::Normal));
    drop(scheduler.file_advise_submit(&file, 0, 0, AdviceMode::Normal));
    // Completed ordinary advice releases backend ownership before returning, permitting immediate explicit close.
    scheduler.file_close(file).await.unwrap();
}
