// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;

#[test]
fn preserves_native_metadata_and_exposes_portable_observations() {
    let file = tempfile::tempfile().unwrap();
    file.set_len(37).unwrap();
    let original = file.metadata().unwrap();
    let result = SystemFileMetadata::from(original.clone());
    assert_eq!(result.file_type(), Some(FileType::File));
    assert_eq!(result.size(), Some(original.len()));
    assert_eq!(result.accessed(), original.accessed().ok());
    assert_eq!(result.modified(), original.modified().ok());
    assert_eq!(result.created(), original.created().ok());
    assert_eq!(result.as_std().len(), 37);
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(result.as_std().ino(), original.ino());
        assert_eq!(result.as_std().mode(), original.mode());
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        assert_eq!(
            result.as_std().file_attributes(),
            original.file_attributes()
        );
    }
    let directory = tempfile::tempdir().unwrap();
    let result = SystemFileMetadata::from(directory.path().metadata().unwrap());
    assert_eq!(result.file_type(), Some(FileType::Directory));
}

#[test]
fn classification_preserves_special_and_unknown_file_kinds() {
    // `std::fs::FileType` has no public constructor. Cover every valid classification, including special file kinds that cannot be created portably on all CI hosts.
    assert_eq!(classify(true, false, false), FileType::File);
    assert_eq!(classify(false, true, false), FileType::Directory);
    assert_eq!(classify(false, false, true), FileType::Symlink);
    assert_eq!(classify(false, false, false), FileType::Other);
}

#[cfg(unix)]
#[test]
fn classifies_real_symlinks_and_special_files() {
    let directory = tempfile::tempdir().unwrap();
    let link = directory.path().join("link");
    std::os::unix::fs::symlink("missing", &link).unwrap();
    let metadata = SystemFileMetadata::from(std::fs::symlink_metadata(&link).unwrap());
    assert_eq!(metadata.file_type(), Some(FileType::Symlink));
    assert_eq!(metadata.size(), Some(7));
    let socket = directory.path().join("socket");
    let _listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    assert_eq!(
        SystemFileMetadata::from(socket.metadata().unwrap()).file_type(),
        Some(FileType::Other)
    );
}
