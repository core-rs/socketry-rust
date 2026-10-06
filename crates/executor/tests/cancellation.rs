// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

mod support;

use socketry_executor::{Cancellation, Cancelled, Scheduler, defer_cancel, yield_now};
use std::cell::Cell;
use std::future::{Future, pending, poll_fn};
use std::marker::PhantomPinned;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use support::{CountDrop, receive};

#[derive(Default)]
struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn poll<FutureType: Future>(
    future: Pin<&mut FutureType>,
    waker: &Waker,
) -> Poll<FutureType::Output> {
    future.poll(&mut Context::from_waker(waker))
}

#[test]
fn clones_share_persistent_cancellation_and_a_descriptive_error() {
    fn thread_safe<Value: Send + Sync>() {}
    fn send<Value: Send>(_: Value) {}
    thread_safe::<Cancellation>();
    thread_safe::<Cancelled>();

    let token = Cancellation::default();
    send(token.cancelled());
    let clone = token.clone();
    assert_eq!(clone.check(), Ok(()));
    assert!(token.cancel());
    assert!(clone.is_cancelled());
    assert!(!clone.cancel());
    assert_eq!(clone.check(), Err(Cancelled));
    assert_eq!(Cancelled.to_string(), "operation cancelled");
    let error: &dyn std::error::Error = &Cancelled;
    assert!(error.source().is_none());

    let mut notification = pin!(clone.cancelled());
    assert!(poll(notification.as_mut(), Waker::noop()).is_ready());
}

#[test]
fn independent_and_child_signals_have_separate_cancellation_boundaries() {
    let root = Cancellation::new();
    let child = root.child();
    let grandchild = child.child();
    let sibling = root.child();
    let independent = Cancellation::new();
    assert!(child.cancel());
    assert!(grandchild.is_cancelled());
    assert!(!root.is_cancelled());
    assert!(!sibling.is_cancelled());
    assert!(root.cancel());
    assert!(sibling.is_cancelled());
    assert!(!independent.is_cancelled());
    assert!(!child.cancel());
    assert!(!grandchild.cancel());
    assert!(child.child().child().is_cancelled());
}

#[test]
fn ancestor_cancellation_reaches_children_after_intermediate_handles_are_dropped() {
    let root = Cancellation::new();
    let intermediate = root.child();
    let child = intermediate.child();
    drop(intermediate);
    assert!(!child.is_cancelled());
    root.cancel();
    assert!(child.is_cancelled());
}

#[test]
fn dropping_all_parent_handles_does_not_cancel_surviving_children() {
    let root = Cancellation::new();
    let child = root.child();
    drop(root.clone());
    drop(root);
    assert!(!child.is_cancelled());
    assert!(child.cancel());
}

#[test]
fn never_cancelled_tokens_remain_pending_and_can_create_independent_children() {
    let token = Cancellation::never();
    let counter = Arc::new(WakeCounter::default());
    let waker = Waker::from(Arc::clone(&counter));
    let mut notification = pin!(token.cancelled());
    assert!(poll(notification.as_mut(), &waker).is_pending());
    assert!(!token.cancel());
    assert!(!token.clone().cancel());
    assert!(!token.is_cancelled());
    assert_eq!(token.check(), Ok(()));
    assert_eq!(counter.0.load(Ordering::SeqCst), 0);
    let child = token.child();
    assert!(child.cancel());
    assert!(!token.is_cancelled());
    assert!(poll(notification.as_mut(), &waker).is_pending());
}

#[test]
fn cancellation_wakes_every_registered_waiter_including_descendants() {
    let root = Cancellation::new();
    let tokens = [root.clone(), root.clone(), root.child()];
    let counters: Vec<_> = tokens
        .iter()
        .map(|_| Arc::new(WakeCounter::default()))
        .collect();
    let wakers: Vec<_> = counters
        .iter()
        .map(|counter| Waker::from(Arc::clone(counter)))
        .collect();
    let mut notifications: Vec<_> = tokens
        .iter()
        .map(|token| Box::pin(token.cancelled()))
        .collect();
    for (notification, waker) in notifications.iter_mut().zip(&wakers) {
        assert!(poll(notification.as_mut(), waker).is_pending());
    }
    assert!(root.cancel());
    for ((notification, waker), counter) in notifications.iter_mut().zip(&wakers).zip(&counters) {
        assert_eq!(counter.0.load(Ordering::SeqCst), 1);
        assert!(poll(notification.as_mut(), waker).is_ready());
    }
    assert!(!root.cancel());
    assert!(
        counters
            .iter()
            .all(|counter| counter.0.load(Ordering::SeqCst) == 1)
    );
}

#[test]
fn dropping_a_wait_unregisters_it_without_affecting_other_waiters() {
    let token = Cancellation::new();
    let abandoned = Arc::new(WakeCounter::default());
    let retained = Arc::new(WakeCounter::default());
    let mut first = Box::pin(token.cancelled());
    let mut second = pin!(token.cancelled());
    let retained_waker = Waker::from(Arc::clone(&retained));
    assert!(poll(first.as_mut(), &Waker::from(Arc::clone(&abandoned))).is_pending());
    assert!(poll(second.as_mut(), &retained_waker).is_pending());
    drop(first);
    assert!(!token.is_cancelled());
    token.cancel();
    assert_eq!(abandoned.0.load(Ordering::SeqCst), 0);
    assert_eq!(retained.0.load(Ordering::SeqCst), 1);
    assert!(poll(second.as_mut(), &retained_waker).is_ready());
}

#[test]
fn a_wait_uses_the_waker_from_its_latest_poll() {
    let token = Cancellation::new();
    let old = Arc::new(WakeCounter::default());
    let new = Arc::new(WakeCounter::default());
    let mut notification = pin!(token.cancelled());
    let new_waker = Waker::from(Arc::clone(&new));
    assert!(poll(notification.as_mut(), &Waker::from(Arc::clone(&old))).is_pending());
    assert!(poll(notification.as_mut(), &new_waker).is_pending());
    token.cancel();
    assert_eq!(old.0.load(Ordering::SeqCst), 0);
    assert_eq!(new.0.load(Ordering::SeqCst), 1);
    assert!(poll(notification.as_mut(), &new_waker).is_ready());
}

#[test]
fn cancellation_wakers_can_reenter_token_operations() {
    struct ReentrantWake {
        token: Cancellation,
        count: AtomicUsize,
    }
    impl Wake for ReentrantWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            assert!(!self.token.cancel());
            assert!(self.token.child().is_cancelled());
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    let token = Cancellation::new();
    let counter = Arc::new(ReentrantWake {
        token: token.clone(),
        count: AtomicUsize::new(0),
    });
    let waker = Waker::from(Arc::clone(&counter));
    let mut notification = pin!(token.cancelled());
    assert!(poll(notification.as_mut(), &waker).is_pending());
    let (sender, receiver) = mpsc::channel();
    let cancellation = token.clone();
    let thread = std::thread::spawn(move || sender.send(cancellation.cancel()).unwrap());
    assert!(receive(&receiver));
    thread.join().unwrap();
    assert_eq!(counter.count.load(Ordering::SeqCst), 1);
    assert!(poll(notification.as_mut(), &waker).is_ready());
}

#[test]
fn cancellation_racing_child_registration_cannot_escape_propagation() {
    for _ in 0..64 {
        let root = Cancellation::new();
        let start = Arc::new(Barrier::new(2));
        let parent = root.clone();
        let child_start = Arc::clone(&start);
        let child = std::thread::spawn(move || {
            child_start.wait();
            parent.child().child()
        });
        start.wait();
        root.cancel();
        assert!(child.join().unwrap().is_cancelled());
    }
}

#[test]
fn cancellation_racing_waiter_registration_preserves_wakeups() {
    for _ in 0..64 {
        let token = Cancellation::new();
        let cancellation = token.clone();
        let start = Arc::new(Barrier::new(2));
        let cancellation_start = Arc::clone(&start);
        let thread = std::thread::spawn(move || {
            cancellation_start.wait();
            cancellation.cancel();
        });
        let counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&counter));
        let mut notification = pin!(token.cancelled());
        start.wait();
        let first = poll(notification.as_mut(), &waker);
        thread.join().unwrap();
        if first.is_pending() {
            assert_eq!(counter.0.load(Ordering::SeqCst), 1);
            assert!(poll(notification.as_mut(), &waker).is_ready());
        }
    }
}

#[test]
fn concurrent_cancellation_requests_have_one_winner() {
    let root = Cancellation::new();
    let child = root.child();
    let start = Arc::new(Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let token = root.clone();
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                token.cancel()
            })
        })
        .collect();
    assert_eq!(
        threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .filter(|won| *won)
            .count(),
        1
    );
    assert!(child.is_cancelled());
}

#[test]
fn cancellation_traverses_a_deep_chain_without_recursion() {
    let root = Cancellation::new();
    let mut leaf = root.clone();
    for _ in 0..10_000 {
        leaf = leaf.child();
    }
    assert!(root.cancel());
    assert!(leaf.is_cancelled());
}

#[test]
fn deferred_work_completes_normally_without_a_cancellation_callback() {
    let scheduler = Scheduler::with_workers(1).unwrap();
    let token = Cancellation::new();
    let called = Rc::new(Cell::new(false));
    let callback_called = Rc::clone(&called);
    let callback_dropped = Arc::new(AtomicUsize::new(0));
    let guard = CountDrop(Arc::clone(&callback_dropped));
    let value = scheduler.block_on(defer_cancel(
        &token,
        async {
            yield_now().await;
            42
        },
        move || {
            let _guard = guard;
            callback_called.set(true);
        },
    ));
    assert_eq!(value, 42);
    assert!(!called.get());
    assert_eq!(callback_dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn already_cancelled_tokens_notify_before_the_first_protected_poll() {
    let scheduler = Scheduler::with_workers(1).unwrap();
    let token = Cancellation::new();
    token.cancel();
    let called = Cell::new(false);
    let value = scheduler.block_on(defer_cancel(
        &token,
        async {
            assert!(called.get());
            42
        },
        || called.set(true),
    ));
    assert_eq!(value, 42);
}

#[test]
fn deferred_cancellation_keeps_pending_work_alive_and_calls_a_consuming_callback_once() {
    let token = Cancellation::new();
    let callback_count = Cell::new(0);
    let released = Cell::new(false);
    let dropped = Arc::new(AtomicUsize::new(0));
    let guard = CountDrop(Arc::clone(&dropped));
    let future = poll_fn(|_| {
        let _ = &guard;
        if released.get() {
            Poll::Ready(42)
        } else {
            Poll::Pending
        }
    });
    let payload = String::from("shutdown");
    let counter = Arc::new(WakeCounter::default());
    let waker = Waker::from(Arc::clone(&counter));
    let mut wrapper = pin!(defer_cancel(&token, future, || {
        assert_eq!(payload, "shutdown");
        drop(payload);
        callback_count.set(callback_count.get() + 1);
    }));
    assert!(poll(wrapper.as_mut(), &waker).is_pending());
    assert!(token.cancel());
    assert_eq!(counter.0.load(Ordering::SeqCst), 1);
    assert!(poll(wrapper.as_mut(), &waker).is_pending());
    assert_eq!(callback_count.get(), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert!(!token.cancel());
    assert!(poll(wrapper.as_mut(), &waker).is_pending());
    assert_eq!(callback_count.get(), 1);
    released.set(true);
    assert_eq!(poll(wrapper.as_mut(), &waker), Poll::Ready(42));
}

#[test]
fn the_callback_can_make_protected_work_ready_in_the_same_poll() {
    let token = Cancellation::new();
    let ready = Cell::new(false);
    let future = poll_fn(|_| {
        if ready.get() {
            Poll::Ready(42)
        } else {
            Poll::Pending
        }
    });
    let mut wrapper = pin!(defer_cancel(&token, future, || ready.set(true)));
    assert!(poll(wrapper.as_mut(), Waker::noop()).is_pending());
    token.cancel();
    assert_eq!(poll(wrapper.as_mut(), Waker::noop()), Poll::Ready(42));
}

#[test]
fn nested_wrappers_observe_their_own_tokens_and_keep_draining() {
    let outer = Cancellation::new();
    let inner = Cancellation::new();
    let outer_count = Cell::new(0);
    let inner_count = Cell::new(0);
    let ready = Cell::new(false);
    let future = poll_fn(|_| {
        if ready.get() {
            Poll::Ready(42)
        } else {
            Poll::Pending
        }
    });
    let mut wrapper = pin!(defer_cancel(
        &outer,
        defer_cancel(&inner, future, || inner_count.set(1)),
        || outer_count.set(1)
    ));
    assert!(poll(wrapper.as_mut(), Waker::noop()).is_pending());
    outer.cancel();
    assert!(poll(wrapper.as_mut(), Waker::noop()).is_pending());
    assert_eq!(outer_count.get(), 1);
    assert_eq!(inner_count.get(), 0);
    inner.cancel();
    assert!(poll(wrapper.as_mut(), Waker::noop()).is_pending());
    assert_eq!(inner_count.get(), 1);
    ready.set(true);
    assert_eq!(poll(wrapper.as_mut(), Waker::noop()), Poll::Ready(42));
}

#[test]
fn dropping_a_wrapper_drops_its_work_and_does_not_call_on_cancel() {
    let token = Cancellation::new();
    let count = Cell::new(0);
    let dropped = Arc::new(AtomicUsize::new(0));
    let guard = CountDrop(Arc::clone(&dropped));
    let future = async move {
        let _guard = guard;
        pending::<()>().await;
    };
    let mut wrapper = Box::pin(defer_cancel(&token, future, || count.set(1)));
    assert!(poll(wrapper.as_mut(), Waker::noop()).is_pending());
    drop(wrapper);
    token.cancel();
    assert_eq!(count.get(), 0);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn callback_panics_propagate_and_the_wrapper_can_be_destroyed() {
    let token = Cancellation::new();
    token.cancel();
    let dropped = Arc::new(AtomicUsize::new(0));
    let guard = CountDrop(Arc::clone(&dropped));
    let mut wrapper = Box::pin(defer_cancel(
        &token,
        async move {
            let _guard = guard;
            pending::<()>().await;
        },
        || panic!("shutdown callback failed"),
    ));
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        poll(wrapper.as_mut(), Waker::noop())
    }));
    assert!(panic.is_err());
    drop(wrapper);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn protected_futures_need_not_be_unpin() {
    struct PinnedFuture {
        address: Cell<usize>,
        _pin: PhantomPinned,
    }
    impl Future for PinnedFuture {
        type Output = usize;
        fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<usize> {
            let this = self.as_ref().get_ref();
            let address = this as *const Self as usize;
            if this.address.get() == 0 {
                this.address.set(address);
                context.waker().wake_by_ref();
                Poll::Pending
            } else {
                assert_eq!(this.address.get(), address);
                Poll::Ready(address)
            }
        }
    }
    let scheduler = Scheduler::with_workers(1).unwrap();
    let address = scheduler.block_on(defer_cancel(
        &Cancellation::never(),
        PinnedFuture {
            address: Cell::new(0),
            _pin: PhantomPinned,
        },
        || panic!("never-cancelled token invoked callback"),
    ));
    assert_ne!(address, 0);
}

#[test]
fn socketry_tasks_can_finish_async_cleanup_after_token_cancellation() {
    let scheduler = Scheduler::with_workers(2).unwrap();
    let shutdown = Cancellation::new();
    let token = shutdown.clone();
    let (started, waiting) = mpsc::channel();
    let task = scheduler
        .spawn(async move {
            let drain = Cancellation::new();
            let callback_drain = drain.clone();
            defer_cancel(
                &token,
                async {
                    started.send(()).unwrap();
                    drain.cancelled().await;
                    yield_now().await;
                    42
                },
                move || {
                    callback_drain.cancel();
                },
            )
            .await
        })
        .unwrap();
    receive(&waiting);
    shutdown.cancel();
    assert_eq!(scheduler.block_on(task).unwrap(), 42);
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_can_drive_the_same_token_and_deferred_cleanup() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let shutdown = Cancellation::new();
    let token = shutdown.clone();
    let task = runtime.spawn(async move {
        let drain = Cancellation::new();
        let callback_drain = drain.clone();
        defer_cancel(
            &token,
            async {
                drain.cancelled().await;
                tokio::task::yield_now().await;
                42
            },
            move || {
                callback_drain.cancel();
            },
        )
        .await
    });
    let value = runtime.block_on(async {
        tokio::task::yield_now().await;
        shutdown.cancel();
        task.await.unwrap()
    });
    assert_eq!(value, 42);
}
