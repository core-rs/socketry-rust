// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use socketry_executor::{AddressResolveError, AddressResolveErrorKind as Kind, AddressService};
use std::{error::Error, io};

#[test]
fn resolver_errors_retain_category_code_message_and_source() {
    for (kind, expected) in [
        (Kind::TemporaryFailure, io::ErrorKind::WouldBlock),
        (Kind::NameNotFound, io::ErrorKind::NotFound),
        (Kind::ServiceUnavailable, io::ErrorKind::NotFound),
        (Kind::InvalidHints, io::ErrorKind::InvalidInput),
        (Kind::PermanentFailure, io::ErrorKind::Other),
        (Kind::OutOfMemory, io::ErrorKind::OutOfMemory),
        (Kind::System, io::ErrorKind::Other),
        (Kind::Other, io::ErrorKind::Other),
    ] {
        let error = AddressResolveError {
            kind,
            code: Some(123),
            message: "resolver failure".into(),
            system_error: None,
        };
        assert_eq!(error.to_string(), "resolver failure");
        assert!(error.source().is_none());
        assert!(format!("{error:?}").contains("resolver failure"));
        let error: io::Error = error.into();
        assert_eq!(error.kind(), expected);
        assert_eq!(error.raw_os_error(), None);
        let detail = error
            .get_ref()
            .unwrap()
            .downcast_ref::<AddressResolveError>()
            .unwrap();
        assert_eq!(detail.kind, kind);
        assert_eq!(detail.code, Some(123));
    }
    let error = AddressResolveError {
        kind: Kind::System,
        code: None,
        message: "system failure".into(),
        system_error: Some(io::Error::from(io::ErrorKind::PermissionDenied)),
    };
    assert_eq!(
        error
            .source()
            .unwrap()
            .downcast_ref::<io::Error>()
            .unwrap()
            .kind(),
        io::ErrorKind::PermissionDenied
    );
    let error: io::Error = error.into();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    for service in [AddressService::Port(80), AddressService::Name("http")] {
        assert_eq!(service, service);
        assert!(!format!("{service:?}").is_empty());
    }
}
