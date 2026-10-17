// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::{fs::File, sync::Arc};

/// An owned file for the native and Tokio backends. Operations borrow it; explicit close consumes it. It is deliberately not `Clone`.
///
/// The backend privately retains the file while an operation uses it. Dropping an operation future does not establish completion; request cancellation and await the operation before explicitly closing the file. Ordinary handle destruction releases ownership without waiting or reporting close errors.
///
/// ```compile_fail
/// use socketry_executor::FileHandle;
/// fn duplicate(file: &FileHandle) -> FileHandle {
///     file.clone() // Cloning this reference cannot produce another owned handle.
/// }
/// ```
#[derive(Debug)]
pub struct FileHandle(pub(crate) Arc<File>);

impl From<File> for FileHandle {
    fn from(file: File) -> Self {
        Self(Arc::new(file))
    }
}

impl AsRef<File> for FileHandle {
    fn as_ref(&self) -> &File {
        &self.0
    }
}

#[cfg(any(feature = "native", feature = "tokio"))]
impl FileHandle {
    pub(crate) fn retain(&self) -> Arc<File> {
        Arc::clone(&self.0)
    }

    pub(crate) fn into_file(self) -> File {
        Arc::try_unwrap(self.0)
            .expect("cannot close a file with outstanding operations; cancel and await them first")
    }
}
