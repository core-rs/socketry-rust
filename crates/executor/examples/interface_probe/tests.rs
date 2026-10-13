// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use futures_lite::future::block_on;
use std::future::Future;
use std::pin::pin;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Wake, Waker};

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

enum Failure {
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

    async fn file_open(&self, path: &Path, mode: Open) -> io::Result<Self::File> {
        if matches!((&self.failure, mode), (Failure::Create, Open::Create)) {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
        self.scheduler.file_open(path, mode).await
    }

    async fn file_read_at(
        &self,
        file: &Self::File,
        buffer: Vec<u8>,
        offset: u64,
    ) -> socketry_executor::BufferResult {
        if matches!(self.failure, Failure::Read) {
            return (Err(io::ErrorKind::PermissionDenied.into()), buffer);
        }
        self.scheduler.file_read_at(file, buffer, offset).await
    }

    async fn file_write_at(
        &self,
        file: &Self::File,
        buffer: Vec<u8>,
        offset: u64,
    ) -> socketry_executor::BufferResult {
        // First make real progress, then fail. The already-written prefix must
        // remain in the destination even though copy returns an error.
        if offset > 0 {
            match self.failure {
                Failure::Write => return (Err(io::ErrorKind::PermissionDenied.into()), buffer),
                Failure::WriteZero => return (Ok(0), buffer),
                _ => {}
            }
        }
        self.scheduler.file_write_at(file, buffer, offset).await
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
            let error = copy(&scheduler, Path::new("source"), Path::new("destination"))
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
                    copy(&scheduler, Path::new("source"), Path::new("destination"))
                        .await
                        .unwrap(),
                    size as u64
                );
                assert_eq!(scheduler.contents(Path::new("destination")), expected);
                assert_eq!(scheduler.open_files(), 0);
            }
            let error = copy(&scheduler, Path::new("missing"), Path::new("destination"))
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
            .file_open(Path::new("source"), Open::Read)
            .await
            .unwrap();
        let buffer = vec![0; 8];
        let allocation = buffer.as_ptr();
        let (result, buffer) = scheduler.file_read_at(&source, buffer, 1).await;
        assert_eq!(result.unwrap(), 2);
        assert_eq!(&buffer[..2], b"el");
        assert_eq!(buffer.len(), 8);
        assert_eq!(buffer.as_ptr(), allocation);
        let (result, buffer) = scheduler.file_read_at(&source, buffer, 100).await;
        assert_eq!(result.unwrap(), 0);
        assert_eq!(buffer.as_ptr(), allocation);
        let (result, buffer) = scheduler.file_write_at(&source, buffer, 0).await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(buffer.as_ptr(), allocation);
        let destination = scheduler
            .file_open(Path::new("destination"), Open::Create)
            .await
            .unwrap();
        let (result, buffer) = scheduler
            .file_write_at(&destination, buffer, u64::MAX)
            .await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        assert_eq!(buffer.as_ptr(), allocation);
        let (result, buffer) = scheduler.file_write_at(&destination, buffer, 3).await;
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
        let listener = scheduler
            .socket_listen("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let address = scheduler.listener_addr(&listener).unwrap();
        let mut accept = pin!(scheduler.socket_accept(&listener));
        assert!(accept.as_mut().poll(&mut context).is_pending());
        let client = scheduler.socket_connect(address).await.unwrap();
        assert!(wakes.0.swap(0, Ordering::Relaxed) > 0);
        let server = accept.await.unwrap();
        let mut read = pin!(scheduler.socket_recv(&server, vec![0; 8]));
        assert!(read.as_mut().poll(&mut context).is_pending());
        let (result, _) = scheduler.socket_send(&client, b"abc".to_vec()).await;
        assert_eq!(result.unwrap(), 2);
        assert!(wakes.0.swap(0, Ordering::Relaxed) > 0);
        let (result, buffer) = read.await;
        assert_eq!(result.unwrap(), 2);
        assert_eq!(&buffer[..2], b"ab");
        let mut read = pin!(scheduler.socket_recv(&server, buffer));
        assert!(read.as_mut().poll(&mut context).is_pending());
        drop(client);
        assert!(wakes.0.swap(0, Ordering::Relaxed) > 0);
        let (result, buffer) = read.await;
        assert_eq!(result.unwrap(), 0);
        let allocation = buffer.as_ptr();
        let (result, buffer) = scheduler.socket_send(&server, buffer).await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(buffer.as_ptr(), allocation);
    });
}

#[test]
fn listener_drop_releases_address_and_pending_connections() {
    block_on(async {
        let scheduler = simulated::Scheduler::new(2, 2);
        let listener = scheduler
            .socket_listen("127.0.0.1:12345".parse().unwrap())
            .await
            .unwrap();
        let address = scheduler.listener_addr(&listener).unwrap();
        assert!(
            matches!(scheduler.socket_listen(address).await, Err(error) if error.kind() == io::ErrorKind::AddrInUse)
        );
        let client = scheduler.socket_connect(address).await.unwrap();
        drop(listener);
        assert!(
            matches!(scheduler.socket_connect(address).await, Err(error) if error.kind() == io::ErrorKind::ConnectionRefused)
        );
        let (result, _) = scheduler.socket_recv(&client, vec![0; 1]).await;
        assert_eq!(result.unwrap(), 0);
        scheduler.socket_listen(address).await.unwrap();
    });
}
