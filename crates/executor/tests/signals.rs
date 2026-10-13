// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![cfg(all(unix, feature = "signals", any(feature = "native", feature = "tokio")))]

mod support;

use futures_lite::StreamExt;
use socketry_executor::{Cancellation, Notify, SignalRegistration, Signals, signal::*};
use std::ffi::c_int;
use std::future::Future;
use std::io;
use std::pin::pin;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};
use support::{TIMEOUT, receive};

// Signal dispositions are process-global. Exercise them only in a child
// process, and impose a deadline so a delivery or teardown bug cannot hang CI.
#[test]
fn signal_subscriptions_in_an_isolated_process() {
    const CHILD: &str = "SOCKETRY_SIGNAL_TEST_CHILD";
    if std::env::var_os(CHILD).is_some() {
        exercise_backends();
        return;
    }
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "signal_subscriptions_in_an_isolated_process",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + TIMEOUT * 6;
    loop {
        if child.try_wait().unwrap().is_some() {
            let output = child.wait_with_output().unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            break;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!(
                "signal tests timed out: {}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn emit(signal: c_int) {
    signal_hook::low_level::raise(signal).unwrap();
}

struct WakeChannel(mpsc::Sender<()>);

impl Wake for WakeChannel {
    fn wake(self: Arc<Self>) {
        let _ = self.0.send(());
    }
}

fn awaited_signal<S: Signals<Signal = c_int>>(scheduler: &S) {
    let cancellation = Cancellation::new();
    let registration = scheduler
        .signal_notify(&[SIGUSR1, SIGUSR2], cancellation.clone())
        .unwrap();
    let (wake, woke) = mpsc::channel();
    let waker = Waker::from(Arc::new(WakeChannel(wake)));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(cancellation.cancelled());
    assert!(future.as_mut().poll(&mut context).is_pending());
    emit(SIGUSR1);
    receive(&woke);
    assert!(future.as_mut().poll(&mut context).is_ready());
    drop(registration);

    let (sender, mut receiver) = futures_channel::mpsc::unbounded();
    let registration = scheduler.signal_notify(&[SIGUSR2], sender).unwrap();
    for before_wait in [true, false] {
        if before_wait {
            emit(SIGUSR2);
        }
        let mut next = pin!(receiver.next());
        if !before_wait {
            assert!(next.as_mut().poll(&mut context).is_pending());
            emit(SIGUSR2);
        }
        loop {
            if let Poll::Ready(value) = next.as_mut().poll(&mut context) {
                assert_eq!(value, Some(SIGUSR2));
                break;
            }
            receive(&woke);
        }
    }
    drop(registration);
}

struct Callback {
    entered: mpsc::Sender<c_int>,
    resume: Mutex<mpsc::Receiver<()>>,
    dropped: mpsc::Sender<()>,
    panic: bool,
}

impl Notify<c_int> for Callback {
    fn notify(&self, signal: c_int) {
        self.entered.send(signal).unwrap();
        receive(&self.resume.lock().unwrap());
        assert!(!self.panic, "intentional notification panic");
    }
}

impl Drop for Callback {
    fn drop(&mut self) {
        self.dropped.send(()).unwrap();
    }
}

fn removal_and_panic<S: Signals<Signal = c_int>>(scheduler: &S, panic: bool) {
    let (observer, observed) = mpsc::channel();
    let observer_guard = scheduler
        .signal_notify(&[SIGUSR1, SIGUSR2], observer)
        .unwrap();
    let (entered, entering) = mpsc::channel();
    let (resume, resuming) = mpsc::channel();
    let (dropped, dropping) = mpsc::channel();
    let guard = scheduler
        .signal_notify(
            &[SIGUSR1, SIGUSR2],
            Callback {
                entered,
                resume: Mutex::new(resuming),
                dropped,
                panic,
            },
        )
        .unwrap();
    emit(SIGUSR1);
    assert_eq!(receive(&entering), SIGUSR1);
    // Queue another signal while user code is still executing.
    emit(SIGUSR2);
    if !panic {
        drop(guard);
        assert!(dropping.try_recv().is_err());
        resume.send(()).unwrap();
        receive(&dropping);
    } else {
        resume.send(()).unwrap();
        receive(&dropping);
        drop(guard);
    }
    assert!(entering.recv_timeout(TIMEOUT).is_err());
    let mut values = [receive(&observed), receive(&observed)];
    values.sort();
    let mut expected = [SIGUSR1, SIGUSR2];
    expected.sort();
    assert_eq!(values, expected);
    emit(SIGUSR1);
    assert_eq!(receive(&observed), SIGUSR1);
    drop(observer_guard);
}

fn validation<S: Signals<Signal = c_int>>(scheduler: &S) {
    for signals in [
        vec![],
        vec![0],
        vec![-1],
        vec![i32::MAX],
        vec![SIGUSR1, SIGKILL],
        vec![SIGSTOP],
        vec![SIGSEGV],
        vec![128],
    ] {
        let (sender, receiver) = mpsc::channel::<c_int>();
        assert!(scheduler.signal_notify(&signals, sender).is_err());
        assert!(matches!(
            receiver.recv_timeout(TIMEOUT),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ));
    }
    let (sender, receiver) = mpsc::channel();
    let guard = scheduler
        .signal_notify(&[SIGUSR1, SIGUSR1], sender)
        .unwrap();
    emit(SIGUSR1);
    assert_eq!(receive(&receiver), SIGUSR1);
    drop(guard);
    assert!(matches!(
        receiver.recv_timeout(TIMEOUT),
        Err(mpsc::RecvTimeoutError::Disconnected)
    ));
}

fn subscriptions<S: Signals<Signal = c_int>>(scheduler: &S) {
    awaited_signal(scheduler);
    removal_and_panic(scheduler, false);
    removal_and_panic(scheduler, true);
    validation(scheduler);
}

fn closed<S: Signals<Signal = c_int, Registration = SignalRegistration>>(scheduler: &S) {
    let error = scheduler
        .signal_notify(&[SIGUSR1], Cancellation::new())
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
}

struct ExecutionContext(mpsc::Sender<(bool, bool)>);

impl Notify<c_int> for ExecutionContext {
    fn notify(&self, _: c_int) {
        #[cfg(feature = "tokio")]
        let tokio = tokio::runtime::Handle::try_current().is_ok();
        #[cfg(not(feature = "tokio"))]
        let tokio = false;
        self.0
            .send((socketry_executor::Task::current().is_some(), tokio))
            .unwrap();
    }
}

fn execution_context<S: Signals<Signal = c_int>>(scheduler: &S, native: bool) {
    let (sender, receiver) = mpsc::channel();
    let guard = scheduler
        .signal_notify(&[SIGUSR1], ExecutionContext(sender))
        .unwrap();
    emit(SIGUSR1);
    assert_eq!(receive(&receiver), (native, !native));
    drop(guard);
}

fn exercise_backends() {
    #[cfg(feature = "native")]
    {
        let scheduler = socketry_executor::Scheduler::with_workers(1).unwrap();
        subscriptions(&scheduler);
        subscriptions(&scheduler.handle());
        execution_context(&scheduler, true);
        let (sender, receiver) = mpsc::channel();
        let guard = scheduler.signal_notify(&[SIGUSR1], sender).unwrap();
        assert!(format!("{guard:?}").contains("SignalRegistration"));
        let handle = scheduler.handle();
        drop(scheduler);
        closed(&handle);
        // Scheduler shutdown releases the subscription even with a live guard.
        assert!(matches!(
            receiver.recv_timeout(TIMEOUT),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ));
        drop(guard);
    }

    #[cfg(feature = "tokio")]
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let scheduler =
            socketry_executor::scheduler::tokio::Scheduler::new(runtime.handle().clone());
        let stop = Cancellation::new();
        let stopping = stop.clone();
        let driver = thread::spawn(move || {
            runtime.block_on(stopping.cancelled());
            runtime
        });
        subscriptions(&scheduler);
        subscriptions(&scheduler.handle());
        execution_context(&scheduler, false);
        let (sender, receiver) = mpsc::channel();
        let guard = scheduler.signal_notify(&[SIGUSR1], sender).unwrap();
        let handle = scheduler.handle();
        drop(scheduler);
        closed(&handle);
        assert!(matches!(
            receiver.recv_timeout(TIMEOUT),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ));
        drop(guard);
        stop.cancel();
        drop(driver.join().unwrap());
    }
}
