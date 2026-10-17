// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![cfg(any(feature = "native", feature = "tokio"))]

#[path = "../examples/interface_probe/tests/address_resolution.rs"]
mod contracts;
mod support;

use futures_lite::future::{block_on, poll_once};
use socketry_executor::{
    AddressHints, AddressResolution, AddressService, Blocking, BlockingOptions, Cancellation,
    OperationError,
};
use std::{io, sync::mpsc};

static NEVER: Cancellation = Cancellation::never();

async fn system_names(scheduler: &impl AddressResolution) {
    // localhost exercises the system resolver without depending on public DNS or a particular address ordering.
    let addresses = scheduler
        .address_resolve(
            Some("localhost"),
            AddressService::Port(1234),
            AddressHints::default(),
            &NEVER,
        )
        .await
        .unwrap();
    assert!(!addresses.is_empty());
    assert!(
        addresses
            .iter()
            .all(|address| address.address.port() == 1234)
    );
}

fn bounded(scheduler: &(impl AddressResolution + Blocking)) {
    let (started, starting) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let running = block_on(scheduler.spawn_blocking(
        move || {
            started.send(()).unwrap();
            support::receive(&released);
        },
        &NEVER,
    ))
    .unwrap();
    support::receive(&starting);
    let cancellation = Cancellation::new();
    let mut lookup = Box::pin(scheduler.address_resolve(
        Some("localhost"),
        AddressService::Port(1234),
        AddressHints::default(),
        &cancellation,
    ));
    assert!(block_on(poll_once(&mut lookup)).is_none());
    cancellation.cancel();
    assert!(matches!(
        block_on(lookup),
        Err(OperationError::Cancelled(_))
    ));
    release.send(()).unwrap();
    block_on(running).unwrap();
}

fn closed(scheduler: &impl AddressResolution) {
    assert_eq!(
        block_on(scheduler.address_resolve(
            Some("127.0.0.1"),
            AddressService::Port(0),
            AddressHints::default(),
            &NEVER
        ))
        .unwrap_err()
        .kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[cfg(feature = "native")]
#[test]
fn native_resolution_contract() {
    let scheduler = socketry_executor::Scheduler::with_workers_and_blocking(
        1,
        BlockingOptions {
            threads: 1,
            capacity: 1,
        },
    )
    .unwrap();
    let handle = scheduler.handle();
    scheduler.block_on(async {
        contracts::check(&scheduler).await;
        contracts::check(&handle).await;
        system_names(&scheduler).await;
    });
    scheduler.block_on(contracts::connection(&scheduler, "localhost"));
    bounded(&handle);
    scheduler.shutdown();
    closed(&handle);
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_resolution_contract_without_driving_a_runtime() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let scheduler = socketry_executor::scheduler::tokio::Scheduler::with_blocking(
        runtime.handle().clone(),
        BlockingOptions {
            threads: 1,
            capacity: 1,
        },
    )
    .unwrap();
    let handle = scheduler.handle();
    runtime.block_on(contracts::connection(&scheduler, "localhost"));
    // Lookup is supplied by the adapter's own pool and does not require a Tokio I/O or time driver.
    drop(runtime);
    block_on(async {
        contracts::check(&scheduler).await;
        contracts::check(&handle).await;
        system_names(&scheduler).await;
    });
    bounded(&handle);
    block_on(scheduler.shutdown());
    closed(&handle);
}
