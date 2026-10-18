// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::{BlockingOptions, BlockingTask, OperationError, SocketProtocol, SocketType};
use futures_lite::future::{block_on, poll_once};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};

static NEVER: Cancellation = Cancellation::never();
fn hints() -> AddressHints {
    AddressHints {
        socket_type: Some(SocketType::Stream),
        ..AddressHints::default()
    }
}
fn candidate() -> AddressInfo {
    AddressInfo {
        address: "127.0.0.1:80".parse().unwrap(),
        socket_type: SocketType::Stream,
        protocol: Some(SocketProtocol::Tcp),
    }
}
const TIMEOUT: Duration = Duration::from_secs(10);

fn pool(capacity: usize) -> BlockingPool {
    BlockingPool::new(BlockingOptions {
        threads: 1,
        capacity,
    })
}

struct Gate(mpsc::Sender<()>);
impl Drop for Gate {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

fn occupy(pool: &BlockingPool) -> (BlockingTask<()>, Gate) {
    let (started, starting) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let task = block_on(pool.spawn(
        move || {
            started.send(()).unwrap();
            released.recv_timeout(TIMEOUT).unwrap();
        },
        &NEVER,
    ))
    .unwrap();
    starting.recv_timeout(TIMEOUT).unwrap();
    (task, Gate(release))
}

#[test]
fn lazy_admission_and_queued_cancellation_do_not_lookup() {
    let pool = pool(2);
    let calls = Arc::new(AtomicUsize::new(0));
    let lookup = {
        let calls = calls.clone();
        move |_: Option<&str>, _: &str, _: AddressHints| {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(vec![])
        }
    };
    drop(resolve_with(
        &pool,
        Some("example.test"),
        AddressService::Port(80),
        hints(),
        &NEVER,
        lookup.clone(),
    ));
    let (running, gate) = occupy(&pool);
    let cancellation = Cancellation::new();
    let mut queued = Box::pin(resolve_with(
        &pool,
        Some("example.test"),
        AddressService::Port(80),
        hints(),
        &cancellation,
        lookup.clone(),
    ));
    assert!(block_on(poll_once(&mut queued)).is_none());
    cancellation.cancel();
    drop(gate);
    block_on(running).unwrap();
    assert!(matches!(
        block_on(queued),
        Err(OperationError::Cancelled(_))
    ));

    let (running, gate) = occupy(&pool);
    let mut abandoned = Box::pin(resolve_with(
        &pool,
        Some("example.test"),
        AddressService::Port(80),
        hints(),
        &NEVER,
        lookup,
    ));
    assert!(block_on(poll_once(&mut abandoned)).is_none());
    drop(abandoned);
    drop(gate);
    block_on(running).unwrap();
    block_on(resolve(
        &pool,
        Some("127.0.0.1"),
        AddressService::Port(80),
        hints(),
        &NEVER,
    ))
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn running_lookup_preserves_success_and_failure_after_cancellation() {
    for fail in [false, true] {
        // A fresh pool guarantees that the first poll admits this lookup. Completion notification can precede release of a previous job's capacity, so reusing one here could instead park admission before the synchronous start handshake.
        let pool = pool(1);
        let (started, starting) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let gate = Gate(release);
        let cancellation = Cancellation::new();
        let mut lookup = Box::pin(resolve_with(
            &pool,
            Some("example.test"),
            AddressService::Port(80),
            hints(),
            &cancellation,
            move |name, service, _| {
                started.send(()).unwrap();
                released.recv_timeout(TIMEOUT).unwrap();
                assert_eq!(name, Some("example.test"));
                assert_eq!(service, "80");
                if fail {
                    Err(io::Error::from(io::ErrorKind::PermissionDenied))
                } else {
                    Ok(vec![candidate()])
                }
            },
        ));
        assert!(block_on(poll_once(&mut lookup)).is_none());
        starting.recv_timeout(TIMEOUT).unwrap();
        cancellation.cancel();
        assert!(block_on(poll_once(&mut lookup)).is_none());
        drop(gate);
        let result = block_on(lookup);
        if fail {
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
        } else {
            assert_eq!(result.unwrap(), vec![candidate()]);
        }
    }
}

#[test]
fn abandonment_keeps_owned_name_and_capacity_until_lookup_finishes() {
    let pool = pool(1);
    let name = String::from("example.test");
    let service = String::from("http");
    let (started, starting) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let (finished, finishing) = mpsc::channel();
    let gate = Gate(release);
    let mut lookup = Box::pin(resolve_with(
        &pool,
        Some(&name),
        AddressService::Name(&service),
        hints(),
        &NEVER,
        move |name, service, _| {
            started.send(()).unwrap();
            released.recv_timeout(TIMEOUT).unwrap();
            finished
                .send((name.unwrap().to_owned(), service.to_owned()))
                .unwrap();
            Ok(vec![])
        },
    ));
    assert!(block_on(poll_once(&mut lookup)).is_none());
    starting.recv_timeout(TIMEOUT).unwrap();
    drop(lookup);
    drop(name);
    drop(service);
    let mut next = Box::pin(resolve(
        &pool,
        Some("127.0.0.1"),
        AddressService::Port(80),
        hints(),
        &NEVER,
    ));
    assert!(block_on(poll_once(&mut next)).is_none());
    drop(gate);
    assert_eq!(
        finishing.recv_timeout(TIMEOUT).unwrap(),
        ("example.test".to_owned(), "http".to_owned())
    );
    assert_eq!(block_on(next).unwrap().len(), 1);
}

#[test]
fn shutdown_rejects_admission_and_discards_queued_lookups() {
    let pool = pool(2);
    let (running, gate) = occupy(&pool);
    let mut queued = Box::pin(resolve(
        &pool,
        Some("127.0.0.1"),
        AddressService::Port(80),
        hints(),
        &NEVER,
    ));
    assert!(block_on(poll_once(&mut queued)).is_none());
    pool.close();
    assert_eq!(
        block_on(queued).unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        block_on(resolve(
            &pool,
            Some("127.0.0.1"),
            AddressService::Port(80),
            hints(),
            &NEVER
        ))
        .unwrap_err()
        .kind(),
        io::ErrorKind::BrokenPipe
    );
    drop(gate);
    block_on(running).unwrap();
}

#[test]
fn startup_failure_is_reported_and_worker_panics_resume_at_the_caller() {
    let mut broken = pool(1);
    broken.fail_start_at = Some(0);
    assert_eq!(
        block_on(resolve(
            &broken,
            Some("127.0.0.1"),
            AddressService::Port(80),
            hints(),
            &NEVER
        ))
        .unwrap_err()
        .to_string(),
        "injected thread startup failure"
    );
    let pool = pool(1);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        block_on(resolve_with(
            &pool,
            Some("example.test"),
            AddressService::Port(80),
            hints(),
            &NEVER,
            |_, _, _| panic!("resolver panic"),
        ))
    }))
    .unwrap_err();
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"resolver panic"));
    assert_eq!(
        block_on(resolve(
            &pool,
            Some("127.0.0.1"),
            AddressService::Port(80),
            hints(),
            &NEVER
        ))
        .unwrap()
        .len(),
        1
    );
}

#[test]
fn hints_reach_lookup_and_results_keep_order_and_duplicates() {
    let pool = pool(1);
    let expected = vec![
        candidate(),
        AddressInfo {
            address: "[::1]:80".parse().unwrap(),
            socket_type: SocketType::Other(12345),
            protocol: Some(SocketProtocol::Other(12345)),
        },
        candidate(),
    ];
    let input = expected.clone();
    let hints = AddressHints {
        passive: true,
        numeric_host: true,
        address_configured: true,
        ..hints()
    };
    let result = block_on(resolve_with(
        &pool,
        None,
        AddressService::Name("http"),
        hints,
        &NEVER,
        move |name, service, received| {
            assert_eq!(name, None);
            assert_eq!(service, "http");
            assert_eq!(received, hints);
            Ok(input)
        },
    ))
    .unwrap();
    assert_eq!(result, expected);
}
