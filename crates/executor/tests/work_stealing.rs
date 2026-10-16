// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

mod support;

use socketry_executor::Scheduler;
use std::cell::{Cell, RefCell};
use std::future::{Future, poll_fn};
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll, Waker};
use std::thread::{self, ThreadId};
use std::time::Instant;
use support::{TIMEOUT, receive};

#[test]
fn idle_worker_steals_work_spawned_on_a_busy_worker() {
    let scheduler = Scheduler::with_workers(2).unwrap();
    let parent = scheduler
        .spawn(async {
            let parent_thread = thread::current().id();
            let (sender, receiver) = mpsc::channel();
            let child = Scheduler::current()
                .unwrap()
                .spawn(async move {
                    sender.send(thread::current().id()).unwrap();
                })
                .unwrap();
            // Deliberately occupy the owner: the child must be stolen from its
            // local queue by the other worker, rather than run on its parent.
            let child_thread = receive(&receiver);
            assert_ne!(parent_thread, child_thread);
            child.await.unwrap();
        })
        .unwrap();
    scheduler.block_on(parent).unwrap();
}

struct MigrationFuture {
    original: Cell<Option<ThreadId>>,
    address: Cell<usize>,
    waker_sender: mpsc::Sender<Waker>,
    blocker_started: mpsc::Sender<ThreadId>,
    blocker_release: RefCell<Option<mpsc::Receiver<()>>>,
    release_sender: mpsc::Sender<()>,
    blocker: RefCell<Option<socketry_executor::TaskHandle<()>>>,
    _pinned: PhantomPinned,
}

impl Future for MigrationFuture {
    type Output = (ThreadId, ThreadId);

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_ref().get_ref();
        let address = std::ptr::from_ref(this) as usize;
        if let Some(original) = this.original.get() {
            assert_eq!(this.address.get(), address, "pinned future moved in memory");
            this.release_sender.send(()).unwrap();
            return Poll::Ready((original, thread::current().id()));
        }
        this.address.set(address);
        this.original.set(Some(thread::current().id()));
        let started = this.blocker_started.clone();
        let release = this.blocker_release.borrow_mut().take().unwrap();
        let blocker = Scheduler::current()
            .unwrap()
            .spawn(async move {
                started.send(thread::current().id()).unwrap();
                receive(&release);
            })
            .unwrap();
        *this.blocker.borrow_mut() = Some(blocker);
        this.waker_sender.send(context.waker().clone()).unwrap();
        Poll::Pending
    }
}

#[test]
fn pending_future_migrates_without_moving_its_pinned_storage() {
    let scheduler = Scheduler::with_workers(2).unwrap();
    let (busy_sender, busy_receiver) = mpsc::channel();
    let (free_sender, free_receiver) = mpsc::channel();
    let busy = scheduler
        .spawn(async move {
            busy_sender.send(thread::current().id()).unwrap();
            receive(&free_receiver);
        })
        .unwrap();
    let busy_thread = receive(&busy_receiver);

    let (waker_sender, waker_receiver) = mpsc::channel();
    let (blocker_started, blocker_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let migrating = scheduler
        .spawn(MigrationFuture {
            original: Cell::new(None),
            address: Cell::new(0),
            waker_sender,
            blocker_started,
            blocker_release: RefCell::new(Some(release_receiver)),
            release_sender,
            blocker: RefCell::new(None),
            _pinned: PhantomPinned,
        })
        .unwrap();
    let waker = receive(&waker_receiver);
    let original_thread = receive(&blocker_receiver);
    assert_ne!(busy_thread, original_thread);

    // Both workers are occupied. Wake the pending future into its original
    // worker's inbox, then free only the other worker to steal that inbox.
    waker.wake();
    free_sender.send(()).unwrap();
    let (original, resumed) = scheduler.block_on(migrating).unwrap();
    assert_eq!(original, original_thread);
    assert_eq!(resumed, busy_thread);
    scheduler.block_on(busy).unwrap();
    scheduler.run();
}

#[test]
fn external_work_is_not_starved_by_a_self_waking_local_task() {
    let scheduler = Scheduler::with_workers(1).unwrap();
    let complete = Arc::new(AtomicBool::new(false));
    let task_complete = Arc::clone(&complete);
    let (started_sender, started_receiver) = mpsc::channel();
    let mut started = false;
    let deadline = Instant::now() + TIMEOUT;
    let busy = scheduler
        .spawn(poll_fn(move |context| {
            if !started {
                started = true;
                started_sender.send(()).unwrap();
            }
            if task_complete.load(Ordering::Acquire) {
                Poll::Ready(())
            } else {
                assert!(Instant::now() < deadline, "external work starved");
                context.waker().wake_by_ref();
                Poll::Pending
            }
        }))
        .unwrap();
    receive(&started_receiver);
    let external = scheduler
        .spawn(async move {
            complete.store(true, Ordering::Release);
        })
        .unwrap();
    scheduler.block_on(busy).unwrap();
    scheduler.block_on(external).unwrap();
}
