// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::{SpawnError, TaskError};
use std::collections::VecDeque;
use std::future::Ready;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};

struct Reader {
    stream: UnixStream,
    results: VecDeque<io::Result<usize>>,
    polls: Arc<AtomicUsize>,
}

impl AsRawFd for Reader {
    fn as_raw_fd(&self) -> RawFd {
        self.stream.as_raw_fd()
    }
}

impl SignalReader for Reader {
    async fn read_signal(&mut self) -> io::Result<usize> {
        self.polls.fetch_add(1, Ordering::Relaxed);
        self.results
            .pop_front()
            .expect("read polled again after termination")
    }
}

fn reader(stream: UnixStream) -> io::Result<Reader> {
    Ok(Reader {
        stream,
        results: VecDeque::from([Ok(0)]),
        polls: Arc::default(),
    })
}

struct RejectSpawn;

impl Spawn for RejectSpawn {
    type Handle<Output>
        = Ready<Result<Output, TaskError>>
    where
        Output: Send + 'static;

    fn spawn<FutureType>(
        &self,
        _future: FutureType,
    ) -> Result<Self::Handle<FutureType::Output>, SpawnError>
    where
        FutureType: Future + Send + 'static,
        FutureType::Output: Send + 'static,
    {
        Err(SpawnError::SchedulerClosed)
    }
}

#[test]
fn pipe_and_reactor_failures_release_setup_resources() {
    let error = prepare(&[], Err(io::Error::from_raw_os_error(libc::EMFILE)), reader)
        .err()
        .unwrap();
    assert_eq!(error.raw_os_error(), Some(libc::EMFILE));

    let (sender, receiver) = mpsc::channel::<c_int>();
    let error =
        SignalRegistration::new::<Reader>(&RejectSpawn, &[crate::signal::SIGUSR1], sender, |_| {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "registration denied",
            ))
        })
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(receiver.try_recv(), Err(mpsc::TryRecvError::Disconnected));

    // The backend can fail after validation; preserve its error and drop both
    // pipe ends. An invalid OS number avoids installing real test handlers.
    let error = prepare(&[0], UnixStream::pair(), reader).err().unwrap();
    assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
}

#[test]
fn spawn_failure_drops_the_unpolled_subscription_and_target() {
    let delivery = prepare(&[], UnixStream::pair(), reader).unwrap();
    let (sender, receiver) = mpsc::channel::<c_int>();
    let error = SignalRegistration::start(&RejectSpawn, delivery, sender).unwrap_err();
    assert!(error.get_ref().unwrap().is::<SpawnError>());
    assert_eq!(receiver.try_recv(), Err(mpsc::TryRecvError::Disconnected));
}

#[test]
fn eof_or_read_error_ends_dispatch_without_spinning() {
    for result in [Ok(0), Err(io::Error::from(io::ErrorKind::BrokenPipe))] {
        let mut delivery = prepare(&[], UnixStream::pair(), reader).unwrap();
        let polls = delivery.get_read().polls.clone();
        delivery.get_read_mut().results = VecDeque::from([result]);
        let (sender, receiver) = mpsc::channel::<c_int>();
        futures_lite::future::block_on(dispatch(delivery, sender, Cancellation::new()));
        assert_eq!(polls.load(Ordering::Relaxed), 1);
        assert_eq!(receiver.try_recv(), Err(mpsc::TryRecvError::Disconnected));
    }
}

#[test]
fn interrupted_reads_are_retried_before_dispatch_ends() {
    let mut delivery = prepare(&[], UnixStream::pair(), reader).unwrap();
    let polls = delivery.get_read().polls.clone();
    delivery.get_read_mut().results =
        VecDeque::from([Err(io::Error::from(io::ErrorKind::Interrupted)), Ok(0)]);
    let (sender, receiver) = mpsc::channel::<c_int>();
    futures_lite::future::block_on(dispatch(delivery, sender, Cancellation::new()));
    assert_eq!(polls.load(Ordering::Relaxed), 2);
    assert_eq!(receiver.try_recv(), Err(mpsc::TryRecvError::Disconnected));
}

#[test]
fn removal_before_the_first_poll_releases_without_reading() {
    let delivery = prepare(&[], UnixStream::pair(), reader).unwrap();
    let polls = delivery.get_read().polls.clone();
    let stopped = Cancellation::new();
    stopped.cancel();
    let (sender, receiver) = mpsc::channel::<c_int>();
    futures_lite::future::block_on(dispatch(delivery, sender, stopped));
    assert_eq!(polls.load(Ordering::Relaxed), 0);
    assert_eq!(receiver.try_recv(), Err(mpsc::TryRecvError::Disconnected));
}
