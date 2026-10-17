// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{AddressResolveError, AddressResolveErrorKind as Kind};
pub(super) use libc as sys;
pub(super) use libc::{addrinfo as AddrInfo, freeaddrinfo, getaddrinfo};
use std::{
    ffi::{CStr, CString},
    io,
};

pub(super) fn string(value: &str) -> CString {
    CString::new(value).expect("resolver inputs were checked for NUL before admission")
}

pub(super) fn error(code: i32) -> AddressResolveError {
    let system_error = (code == sys::EAI_SYSTEM).then(io::Error::last_os_error);
    let kind = match code {
        sys::EAI_AGAIN => Kind::TemporaryFailure,
        sys::EAI_NONAME => Kind::NameNotFound,
        #[cfg(any(
            target_os = "linux",
            target_os = "android",
            target_vendor = "apple",
            target_os = "netbsd",
            target_os = "openbsd"
        ))]
        sys::EAI_NODATA => Kind::NameNotFound,
        sys::EAI_SERVICE => Kind::ServiceUnavailable,
        sys::EAI_BADFLAGS | sys::EAI_FAMILY | sys::EAI_SOCKTYPE => Kind::InvalidHints,
        sys::EAI_FAIL => Kind::PermanentFailure,
        sys::EAI_MEMORY => Kind::OutOfMemory,
        sys::EAI_SYSTEM => Kind::System,
        _ => Kind::Other,
    };
    // SAFETY: gai_strerror returns a terminated static message for any resolver code, including unknown codes.
    let message = unsafe { CStr::from_ptr(sys::gai_strerror(code)) }
        .to_string_lossy()
        .into_owned();
    AddressResolveError {
        kind,
        code: Some(code),
        message,
        system_error,
    }
}
