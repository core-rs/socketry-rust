// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::{BlockingOptions, BlockingTask, OperationError};
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
const OPTIONS: AddressResolveOptions = AddressResolveOptions {
    port: 80,
    domain: None,
};
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
        move |_: &str, _: u16| {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(vec![])
        }
    };
    drop(resolve_with(
        &pool,
        "example.test",
        OPTIONS,
        &NEVER,
        lookup.clone(),
    ));
    let (running, gate) = occupy(&pool);
    let cancellation = Cancellation::new();
    let mut queued = Box::pin(resolve_with(
        &pool,
        "example.test",
        OPTIONS,
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
    let mut abandoned = Box::pin(resolve_with(&pool, "example.test", OPTIONS, &NEVER, lookup));
    assert!(block_on(poll_once(&mut abandoned)).is_none());
    drop(abandoned);
    drop(gate);
    block_on(running).unwrap();
    block_on(resolve(&pool, "127.0.0.1", OPTIONS, &NEVER)).unwrap();
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
            "example.test",
            OPTIONS,
            &cancellation,
            move |name, port| {
                started.send(()).unwrap();
                released.recv_timeout(TIMEOUT).unwrap();
                assert_eq!(name, "example.test");
                assert_eq!(port, 80);
                if fail {
                    Err(io::Error::from(io::ErrorKind::PermissionDenied))
                } else {
                    Ok(vec![SocketAddr::from(([127, 0, 0, 1], port))])
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
            assert_eq!(
                result.unwrap(),
                vec![SocketAddr::from(([127, 0, 0, 1], 80))]
            );
        }
    }
}

#[test]
fn abandonment_keeps_owned_name_and_capacity_until_lookup_finishes() {
    let pool = pool(1);
    let name = String::from("example.test");
    let (started, starting) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let (finished, finishing) = mpsc::channel();
    let gate = Gate(release);
    let mut lookup = Box::pin(resolve_with(
        &pool,
        &name,
        OPTIONS,
        &NEVER,
        move |name, port| {
            started.send(()).unwrap();
            released.recv_timeout(TIMEOUT).unwrap();
            finished.send((name.to_owned(), port)).unwrap();
            Ok(vec![])
        },
    ));
    assert!(block_on(poll_once(&mut lookup)).is_none());
    starting.recv_timeout(TIMEOUT).unwrap();
    drop(lookup);
    drop(name);
    let mut next = Box::pin(resolve(&pool, "127.0.0.1", OPTIONS, &NEVER));
    assert!(block_on(poll_once(&mut next)).is_none());
    drop(gate);
    assert_eq!(
        finishing.recv_timeout(TIMEOUT).unwrap(),
        ("example.test".to_owned(), 80)
    );
    assert_eq!(block_on(next).unwrap().len(), 1);
}

#[test]
fn shutdown_rejects_admission_and_discards_queued_lookups() {
    let pool = pool(2);
    let (running, gate) = occupy(&pool);
    let mut queued = Box::pin(resolve(&pool, "127.0.0.1", OPTIONS, &NEVER));
    assert!(block_on(poll_once(&mut queued)).is_none());
    pool.close();
    assert_eq!(
        block_on(queued).unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        block_on(resolve(&pool, "127.0.0.1", OPTIONS, &NEVER))
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
        block_on(resolve(&broken, "127.0.0.1", OPTIONS, &NEVER))
            .unwrap_err()
            .to_string(),
        "injected thread startup failure"
    );
    let pool = pool(1);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        block_on(resolve_with(
            &pool,
            "example.test",
            OPTIONS,
            &NEVER,
            |_, _| panic!("resolver panic"),
        ))
    }))
    .unwrap_err();
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"resolver panic"));
    assert_eq!(
        block_on(resolve(&pool, "127.0.0.1", OPTIONS, &NEVER))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn filtering_preserves_order_and_duplicates() {
    let pool = pool(1);
    let expected: Vec<SocketAddr> = ["127.0.0.2:80", "127.0.0.1:80", "127.0.0.2:80"]
        .map(|address| address.parse().unwrap())
        .into();
    let mut input = expected.clone();
    input.insert(1, "[::1]:80".parse().unwrap());
    let result = block_on(resolve_with(
        &pool,
        "example.test",
        AddressResolveOptions {
            domain: Some(SocketDomain::Ipv4),
            ..OPTIONS
        },
        &NEVER,
        move |_, _| Ok(input),
    ))
    .unwrap();
    assert_eq!(result, expected);
}
