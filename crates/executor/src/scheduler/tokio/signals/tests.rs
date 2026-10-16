// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use std::os::unix::net::UnixStream;

#[test]
fn nonblocking_setup_failure_is_returned_before_runtime_registration() {
    let runtime = ::tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let (read, _write) = UnixStream::pair().unwrap();
    let error = register_read(runtime.handle(), read, |_| {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "setup denied",
        ))
    })
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
}
