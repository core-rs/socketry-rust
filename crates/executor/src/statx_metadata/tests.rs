// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;

#[test]
fn native_layout_and_validity_masks_are_preserved() {
    assert_eq!(
        std::mem::size_of::<StatxMetadata>(),
        std::mem::size_of::<libc::statx>()
    );
    assert_eq!(
        std::mem::align_of::<StatxMetadata>(),
        std::mem::align_of::<libc::statx>()
    );
    let mut metadata = StatxMetadata::empty();
    // Dummy values must not become valid observations when their mask bits are absent.
    metadata.0.stx_size = 123;
    metadata.0.stx_mode = libc::S_IFREG as u16;
    assert_eq!(metadata.size(), None);
    assert_eq!(metadata.file_type(), None);
    assert_eq!(metadata.accessed(), None);
    assert_eq!(metadata.modified(), None);
    assert_eq!(metadata.created(), None);
    metadata.0.stx_mask = libc::STATX_SIZE | libc::STATX_TYPE;
    assert_eq!(metadata.size(), Some(123));
    metadata.0.stx_size = 0;
    assert_eq!(metadata.size(), Some(0));
    for (mode, kind) in [
        (libc::S_IFREG, FileType::File),
        (libc::S_IFDIR, FileType::Directory),
        (libc::S_IFLNK, FileType::Symlink),
        (libc::S_IFCHR, FileType::Other),
        (libc::S_IFIFO, FileType::Other),
    ] {
        metadata.0.stx_mode = mode as u16 | 0o640;
        assert_eq!(metadata.file_type(), Some(kind));
        assert_eq!(u32::from(metadata.as_statx().stx_mode), mode | 0o640);
    }
    assert!(format!("{metadata:?}").contains("StatxMetadata"));
}

#[test]
fn timestamps_convert_only_available_fields_without_conflating_creation_and_change() {
    let mut metadata = StatxMetadata::empty();
    metadata.0.stx_mask = libc::STATX_ATIME | libc::STATX_MTIME | libc::STATX_CTIME;
    metadata.0.stx_atime.tv_sec = -1;
    metadata.0.stx_atime.tv_nsec = 500_000_000;
    metadata.0.stx_mtime.tv_sec = 7;
    metadata.0.stx_mtime.tv_nsec = 123;
    metadata.0.stx_ctime.tv_sec = 19;
    metadata.0.stx_btime.tv_sec = 5;
    assert_eq!(
        metadata.accessed(),
        UNIX_EPOCH.checked_sub(Duration::from_millis(500))
    );
    assert_eq!(
        metadata.modified(),
        UNIX_EPOCH.checked_add(Duration::new(7, 123))
    );
    assert_eq!(metadata.created(), None);
    metadata.0.stx_mask |= libc::STATX_BTIME;
    assert_eq!(
        metadata.created(),
        UNIX_EPOCH.checked_add(Duration::from_secs(5))
    );
    assert_eq!(metadata.as_statx().stx_ctime.tv_sec, 19);
    assert_eq!(timestamp(0, 0), Some(UNIX_EPOCH));
    assert_eq!(
        timestamp(-1, 0),
        UNIX_EPOCH.checked_sub(Duration::from_secs(1))
    );
    assert_eq!(timestamp(1, 1_000_000_000), None);
    assert_eq!(
        timestamp(i64::MAX, 999_999_999),
        UNIX_EPOCH.checked_add(Duration::new(i64::MAX as u64, 999_999_999))
    );
    assert_eq!(
        timestamp(i64::MIN, 0),
        UNIX_EPOCH.checked_sub(Duration::from_secs(1 << 63))
    );
}
