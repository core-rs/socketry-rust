// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use futures_lite::future::block_on;
use socketry_executor::{IoBufMut, SocketDomain, SocketProtocol, SocketType, SyncMode};
use std::future::Future;
use std::pin::pin;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Wake, Waker};

async fn open_listener(
    scheduler: &simulated::Scheduler,
    address: std::net::SocketAddr,
) -> OperationResult<simulated::SocketDescriptor> {
    let socket = scheduler
        .socket_open(
            SocketDomain::Ipv4,
            SocketType::Stream,
            Some(SocketProtocol::Tcp),
            &NEVER,
        )
        .await?;
    scheduler.socket_bind(&socket, address, &NEVER).await?;
    scheduler.socket_listen(&socket, 128, &NEVER).await?;
    Ok(socket)
}

async fn open_client(
    scheduler: &simulated::Scheduler,
    address: std::net::SocketAddr,
) -> OperationResult<simulated::SocketDescriptor> {
    let socket = scheduler
        .socket_open(
            SocketDomain::Ipv4,
            SocketType::Stream,
            Some(SocketProtocol::Tcp),
            &NEVER,
        )
        .await?;
    scheduler.socket_connect(&socket, address, &NEVER).await?;
    Ok(socket)
}

#[path = "tests/stateful_file.rs"]
mod stateful_file;

#[path = "tests/file_sync.rs"]
mod file_sync;

#[path = "tests/file_truncate.rs"]
mod file_truncate;

#[path = "tests/file_allocate.rs"]
mod file_allocate;

#[path = "tests/file_close.rs"]
mod file_close;

#[test]
fn stateful_operations_forward_and_preserve_short_transfers() {
    let scheduler = FailingFiles {
        scheduler: simulated::Scheduler::new(2, 1),
        failure: Failure::None,
    };
    // Exercise forwarding through the wrapper with failure injection disabled.
    block_on(stateful_file::check(&scheduler, Path::new("stateful")));
    block_on(file_sync::check(&scheduler, Path::new("sync")));
    block_on(file_truncate::check(&scheduler, Path::new("truncate")));
    block_on(file_close::check(&scheduler, Path::new("close")));
    assert_eq!(scheduler.scheduler.open_files(), 0);
}

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

enum Failure {
    None,
    Create,
    Read,
    Write,
    WriteZero,
}

struct FailingFiles {
    scheduler: simulated::Scheduler,
    failure: Failure,
}

impl File for FailingFiles {
    type File = <simulated::Scheduler as File>::File;
    async fn file_close(&self, file: Self::File) -> io::Result<()> {
        self.scheduler.file_close(file).await
    }
    async fn file_close_submit(&self, file: Self::File) -> io::Result<()> {
        self.scheduler.file_close_submit(file).await
    }

    async fn file_truncate(
        &self,
        file: &Self::File,
        length: u64,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.scheduler
            .file_truncate(file, length, cancellation)
            .await
    }

    async fn file_sync(
        &self,
        file: &Self::File,
        mode: SyncMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.scheduler.file_sync(file, mode, cancellation).await
    }

    async fn file_seek(
        &self,
        file: &Self::File,
        position: std::io::SeekFrom,
        cancellation: &Cancellation,
    ) -> OperationResult<u64> {
        self.scheduler.file_seek(file, position, cancellation).await
    }

    async fn file_open(
        &self,
        path: &Path,
        mode: FileOptions,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::File> {
        if matches!(self.failure, Failure::Create) && mode.create {
            return Err(io::Error::from(io::ErrorKind::PermissionDenied).into());
        }
        self.scheduler.file_open(path, mode, cancellation).await
    }

    async fn file_read<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> socketry_executor::BufferResult<B> {
        self.scheduler.file_read(file, buffer, cancellation).await
    }

    async fn file_write<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> socketry_executor::BufferResult<B> {
        self.scheduler.file_write(file, buffer, cancellation).await
    }

    async fn file_read_at<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> socketry_executor::BufferResult<B> {
        if matches!(self.failure, Failure::Read) {
            return (
                Err(io::Error::from(io::ErrorKind::PermissionDenied).into()),
                buffer,
            );
        }
        self.scheduler
            .file_read_at(file, buffer, offset, cancellation)
            .await
    }

    async fn file_write_at<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> socketry_executor::BufferResult<B> {
        // First make real progress, then fail. The already-written prefix must
        // remain in the destination even though copy returns an error.
        if offset > 0 {
            match self.failure {
                Failure::Write => {
                    return (
                        Err(io::Error::from(io::ErrorKind::PermissionDenied).into()),
                        buffer,
                    );
                }
                Failure::WriteZero => return (Ok(0), buffer),
                _ => {}
            }
        }
        self.scheduler
            .file_write_at(file, buffer, offset, cancellation)
            .await
    }
}

#[test]
fn failed_copies_release_handles_and_keep_completed_writes() {
    block_on(async {
        for failure in [
            Failure::Create,
            Failure::Read,
            Failure::Write,
            Failure::WriteZero,
        ] {
            let scheduler = FailingFiles {
                scheduler: simulated::Scheduler::new(3, 1),
                failure,
            };
            scheduler.scheduler.insert(Path::new("source"), PAYLOAD);
            let error = copy(
                &scheduler,
                Path::new("source"),
                Path::new("destination"),
                &NEVER,
            )
            .await
            .unwrap_err();
            let expected = if matches!(scheduler.failure, Failure::WriteZero) {
                io::ErrorKind::WriteZero
            } else {
                io::ErrorKind::PermissionDenied
            };
            assert_eq!(error.kind(), expected);
            assert_eq!(scheduler.scheduler.open_files(), 0);
            if matches!(scheduler.failure, Failure::Write | Failure::WriteZero) {
                assert_eq!(
                    scheduler.scheduler.contents(Path::new("destination")),
                    &PAYLOAD[..1]
                );
            }
        }
    });
}

#[test]
fn same_application_runs_on_every_enabled_backend() {
    main().unwrap();
}

#[test]
fn copy_handles_empty_short_and_multiple_buffers_and_releases_files() {
    block_on(async {
        for limit in [1, 3, BUFFER_SIZE, 1024] {
            let scheduler = simulated::Scheduler::new(limit, 1);
            for size in [0, 1, BUFFER_SIZE - 1, BUFFER_SIZE, BUFFER_SIZE + 1, 99] {
                let expected: Vec<u8> = (0..size as u8).collect();
                scheduler.insert(Path::new("source"), &expected);
                scheduler.insert(Path::new("destination"), &[255; 200]);
                assert_eq!(
                    copy(
                        &scheduler,
                        Path::new("source"),
                        Path::new("destination"),
                        &NEVER
                    )
                    .await
                    .unwrap(),
                    size as u64
                );
                assert_eq!(scheduler.contents(Path::new("destination")), expected);
                assert_eq!(scheduler.open_files(), 0);
            }
            let error = copy(
                &scheduler,
                Path::new("missing"),
                Path::new("destination"),
                &NEVER,
            )
            .await
            .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::NotFound);
            assert_eq!(scheduler.open_files(), 0);
        }
    });
}

#[test]
fn file_results_preserve_allocations_on_partial_reads_eof_and_errors() {
    block_on(async {
        let scheduler = simulated::Scheduler::new(2, 2);
        scheduler.insert(Path::new("source"), b"hello");
        let source = scheduler
            .file_open(Path::new("source"), FileOptions::READ, &NEVER)
            .await
            .unwrap();
        let buffer = vec![0; 8];
        let allocation = buffer.as_ptr();
        let (result, buffer) = scheduler.file_read_at(&source, buffer, 1, &NEVER).await;
        assert_eq!(result.unwrap(), 2);
        assert_eq!(&buffer[..2], b"el");
        assert_eq!(buffer.len(), 8);
        assert_eq!(buffer.as_ptr(), allocation);
        let (result, buffer) = scheduler.file_read_at(&source, buffer, 100, &NEVER).await;
        assert_eq!(result.unwrap(), 0);
        assert_eq!(buffer.as_ptr(), allocation);
        let (result, buffer) = scheduler.file_write_at(&source, buffer, 0, &NEVER).await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(buffer.as_ptr(), allocation);
        let destination = scheduler
            .file_open(Path::new("destination"), FileOptions::CREATE, &NEVER)
            .await
            .unwrap();
        let (result, buffer) = scheduler
            .file_write_at(&destination, buffer, u64::MAX, &NEVER)
            .await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        assert_eq!(buffer.as_ptr(), allocation);
        let (result, buffer) = scheduler
            .file_write_at(&destination, buffer, 3, &NEVER)
            .await;
        assert_eq!(result.unwrap(), 2);
        assert_eq!(buffer.as_ptr(), allocation);
        assert_eq!(scheduler.contents(Path::new("destination")), b"\0\0\0el");
    });
}

#[test]
fn pending_accept_and_receive_wake_and_drop_produces_eof() {
    block_on(async {
        let wakes = Arc::new(WakeCount::default());
        let waker = Waker::from(wakes.clone());
        let mut context = Context::from_waker(&waker);
        let scheduler = simulated::Scheduler::new(2, 2);
        let listener = open_listener(&scheduler, "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let address = scheduler.socket_local_addr(&listener).unwrap();
        let mut accept = pin!(scheduler.socket_accept(&listener, &NEVER));
        assert!(accept.as_mut().poll(&mut context).is_pending());
        let client = open_client(&scheduler, address).await.unwrap();
        assert!(wakes.0.swap(0, Ordering::Relaxed) > 0);
        let (server, _) = accept.await.unwrap();
        let mut read = pin!(scheduler.socket_read(&server, vec![0; 8], &NEVER));
        assert!(read.as_mut().poll(&mut context).is_pending());
        let (result, _) = scheduler
            .socket_write(&client, b"abc".to_vec(), &NEVER)
            .await;
        assert_eq!(result.unwrap(), 2);
        assert!(wakes.0.swap(0, Ordering::Relaxed) > 0);
        let (result, buffer) = read.await;
        assert_eq!(result.unwrap(), 2);
        assert_eq!(&buffer[..2], b"ab");
        let mut read = pin!(scheduler.socket_read(&server, buffer, &NEVER));
        assert!(read.as_mut().poll(&mut context).is_pending());
        drop(client);
        assert!(wakes.0.swap(0, Ordering::Relaxed) > 0);
        let (result, buffer) = read.await;
        assert_eq!(result.unwrap(), 0);
        let allocation = buffer.as_ptr();
        let (result, buffer) = scheduler.socket_write(&server, buffer, &NEVER).await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(buffer.as_ptr(), allocation);
    });
}

#[test]
fn listener_drop_releases_address_and_pending_connections() {
    block_on(async {
        let scheduler = simulated::Scheduler::new(2, 2);
        let listener = open_listener(&scheduler, "127.0.0.1:12345".parse().unwrap())
            .await
            .unwrap();
        let address = scheduler.socket_local_addr(&listener).unwrap();
        let duplicate = scheduler
            .socket_open(
                SocketDomain::Ipv4,
                SocketType::Stream,
                Some(SocketProtocol::Tcp),
                &NEVER,
            )
            .await
            .unwrap();
        assert!(
            matches!(scheduler.socket_bind(&duplicate, address, &NEVER).await, Err(error) if error.kind() == io::ErrorKind::AddrInUse)
        );
        let client = open_client(&scheduler, address).await.unwrap();
        drop(listener);
        assert!(
            matches!(open_client(&scheduler, address).await, Err(error) if error.kind() == io::ErrorKind::ConnectionRefused)
        );
        let (result, _) = scheduler.socket_read(&client, vec![0; 1], &NEVER).await;
        assert_eq!(result.unwrap(), 0);
        let listener = open_listener(&scheduler, address).await.unwrap();
        assert_eq!(scheduler.socket_local_addr(&listener).unwrap(), address);
    });
}

#[test]
fn simulated_truncate_rejects_excessive_storage_without_changing_file_or_cursor() {
    block_on(async {
        let scheduler = simulated::Scheduler::new(2, 2);
        let path = Path::new("bounded-truncate");
        let file = scheduler
            .file_open(path, FileOptions::CREATE, &NEVER)
            .await
            .unwrap();
        scheduler
            .file_seek(&file, std::io::SeekFrom::Start(5), &NEVER)
            .await
            .unwrap();
        scheduler
            .file_truncate(&file, 1024 * 1024, &NEVER)
            .await
            .unwrap();
        assert_eq!(
            scheduler
                .file_truncate(&file, 1024 * 1024 + 1, &NEVER)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::FileTooLarge
        );
        assert_eq!(scheduler.contents(path), vec![0; 1024 * 1024]);
        assert_eq!(
            scheduler
                .file_seek(&file, std::io::SeekFrom::Current(0), &NEVER)
                .await
                .unwrap(),
            5
        );
        scheduler.file_truncate(&file, 0, &NEVER).await.unwrap();
        assert!(scheduler.contents(path).is_empty());
    });
}

#[test]
fn simulated_allocation_reserves_space_and_preserves_file_contracts() {
    use socketry_executor::{AllocationMode, FileAllocate};
    let scheduler = simulated::Scheduler::new(2, 1);
    block_on(async {
        file_allocate::check(&scheduler, Path::new("fixture")).await;
        let file = scheduler
            .file_open(Path::new("bounded-allocation"), FileOptions::CREATE, &NEVER)
            .await
            .unwrap();
        for mode in [AllocationMode::Extend, AllocationMode::KeepSize] {
            assert_eq!(
                scheduler
                    .file_allocate(&file, 0, 1024 * 1024 + 1, mode, &NEVER)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::FileTooLarge
            );
        }
        assert_eq!(scheduler.contents(Path::new("bounded-allocation")), b"");
        scheduler
            .file_allocate(&file, 0, 1024 * 1024, AllocationMode::KeepSize, &NEVER)
            .await
            .unwrap();
        assert_eq!(scheduler.contents(Path::new("bounded-allocation")), b"");
    });
    assert_eq!(scheduler.open_files(), 0);
}

#[path = "tests/address_resolution.rs"]
mod address_resolution;

#[test]
fn simulated_resolution_contract_and_explicit_names() {
    use socketry_executor::{AddressHints, AddressResolution, AddressService, SocketType};
    let scheduler = simulated::Scheduler::new(2, 2);
    block_on(async {
        address_resolution::check(&scheduler).await;
        scheduler.insert_addresses("loopback.test", vec!["127.0.0.1".parse().unwrap()]);
        address_resolution::connection(&scheduler, "loopback.test").await;
        let options = AddressHints {
            socket_type: Some(SocketType::Stream),
            ..AddressHints::default()
        };
        assert_eq!(
            scheduler
                .address_resolve(
                    Some("service.test"),
                    AddressService::Port(80),
                    options,
                    &NEVER
                )
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );
        let addresses = vec![
            "127.0.0.2".parse().unwrap(),
            "::1".parse().unwrap(),
            "127.0.0.2".parse().unwrap(),
        ];
        let unpolled = scheduler.address_resolve(
            Some("service.test"),
            AddressService::Port(80),
            options,
            &NEVER,
        );
        scheduler.insert_addresses("service.test", addresses);
        assert_eq!(
            unpolled
                .await
                .unwrap()
                .into_iter()
                .map(|candidate| candidate.address)
                .collect::<Vec<_>>(),
            vec![
                "127.0.0.2:80".parse::<std::net::SocketAddr>().unwrap(),
                "[::1]:80".parse().unwrap(),
                "127.0.0.2:80".parse().unwrap()
            ]
        );
        scheduler.insert_addresses("service.test", vec![]);
        assert!(
            scheduler
                .address_resolve(
                    Some("service.test"),
                    AddressService::Port(80),
                    options,
                    &NEVER
                )
                .await
                .is_err()
        );
    });
}

#[test]
fn simulated_services_transports_and_configured_families() {
    use socketry_executor::{
        AddressHints, AddressResolution, AddressResolveError, AddressResolveErrorKind as Kind,
        AddressService, SocketProtocol, SocketType,
    };
    let scheduler = simulated::Scheduler::new(2, 2);
    block_on(async {
        for (service, count, port) in [("http", 1, 80), ("domain", 2, 53)] {
            let result = scheduler
                .address_resolve(
                    Some("127.0.0.1"),
                    AddressService::Name(service),
                    AddressHints {
                        address_configured: true,
                        ..AddressHints::default()
                    },
                    &NEVER,
                )
                .await
                .unwrap();
            assert_eq!(result.len(), count);
            assert!(result.iter().all(|info| info.address.port() == port));
        }
        let raw = scheduler
            .address_resolve(
                Some("127.0.0.1"),
                AddressService::Port(0),
                AddressHints {
                    socket_type: Some(SocketType::Raw),
                    ..AddressHints::default()
                },
                &NEVER,
            )
            .await
            .unwrap();
        assert_eq!(raw[0].socket_type, SocketType::Raw);
        assert_eq!(raw[0].protocol, None);
        for (service, hints, kind) in [
            (
                AddressService::Name("unknown"),
                AddressHints::default(),
                Kind::ServiceUnavailable,
            ),
            (
                AddressService::Name("http"),
                AddressHints {
                    socket_type: Some(SocketType::Datagram),
                    ..AddressHints::default()
                },
                Kind::ServiceUnavailable,
            ),
            (
                AddressService::Port(80),
                AddressHints {
                    socket_type: Some(SocketType::Other(12345)),
                    ..AddressHints::default()
                },
                Kind::InvalidHints,
            ),
            (
                AddressService::Port(80),
                AddressHints {
                    socket_type: Some(SocketType::Stream),
                    protocol: Some(SocketProtocol::Udp),
                    ..AddressHints::default()
                },
                Kind::InvalidHints,
            ),
        ] {
            let error = scheduler
                .address_resolve(Some("127.0.0.1"), service, hints, &NEVER)
                .await
                .unwrap_err();
            let socketry_executor::OperationError::Io(error) = error else {
                panic!("expected resolver failure")
            };
            let detail = error
                .get_ref()
                .unwrap()
                .downcast_ref::<AddressResolveError>()
                .unwrap();
            assert_eq!(detail.kind, kind);
            assert_eq!(detail.code, None);
        }
    });
}
