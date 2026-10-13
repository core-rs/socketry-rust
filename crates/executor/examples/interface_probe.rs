// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Executable design experiment, not an exported scheduler API.
//! Run with `--features tokio` for native, Tokio, and simulated I/O.
//! With `--no-default-features`, only the in-memory implementation runs.

#[path = "interface_probe/file.rs"]
mod file;
#[cfg(any(feature = "native", feature = "tokio"))]
#[path = "interface_probe/real.rs"]
mod real;
#[path = "interface_probe/simulated.rs"]
mod simulated;
#[path = "interface_probe/socket.rs"]
mod socket;

use file::{File, Open};
use socket::Socket;
use std::{io, path::Path};

const PAYLOAD: &[u8] = b"Explicit schedulers keep protocol code portable.\n";
const BUFFER_SIZE: usize = 16;

/// Application code only sees the backend's associated resources.
async fn copy<S: File>(scheduler: &S, source: &Path, destination: &Path) -> io::Result<u64> {
    let source = scheduler.file_open(source, Open::Read).await?;
    let destination = scheduler.file_open(destination, Open::Create).await?;
    let mut buffer = vec![0; BUFFER_SIZE];
    let mut offset = 0;
    loop {
        let (result, returned) = scheduler.file_read_at(&source, buffer, offset).await;
        buffer = returned;
        let count = result?;
        if count == 0 {
            return Ok(offset);
        }
        buffer.truncate(count);
        while !buffer.is_empty() {
            let (result, returned) = scheduler.file_write_at(&destination, buffer, offset).await;
            buffer = returned;
            let count = result?;
            if count == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            offset += count as u64;
            // Vec has no owned subrange view. This moves the unwritten suffix
            // within the same allocation; a future IoBuf view should avoid it.
            buffer.drain(..count);
        }
        buffer.resize(BUFFER_SIZE, 0);
    }
}

async fn send_all<S: Socket>(
    scheduler: &S,
    socket: &S::Socket,
    mut buffer: Vec<u8>,
) -> io::Result<()> {
    while !buffer.is_empty() {
        let (result, returned) = scheduler.socket_send(socket, buffer).await;
        buffer = returned;
        let count = result?;
        if count == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        buffer.drain(..count);
    }
    Ok(())
}

/// Serve one connection until peer EOF, echoing each received prefix.
async fn serve<S: Socket>(scheduler: &S, listener: &S::Listener) -> io::Result<usize> {
    let socket = scheduler.socket_accept(listener).await?;
    let mut total = 0;
    loop {
        let (result, mut buffer) = scheduler.socket_recv(&socket, vec![0; BUFFER_SIZE]).await;
        let count = result?;
        if count == 0 {
            return Ok(total);
        }
        total += count;
        buffer.truncate(count);
        send_all(scheduler, &socket, buffer).await?;
    }
}

async fn exchange<S: Socket>(scheduler: &S) -> io::Result<()> {
    let listener = scheduler
        .socket_listen("127.0.0.1:0".parse().unwrap())
        .await?;
    let address = scheduler.listener_addr(&listener)?;
    let client = async {
        let socket = scheduler.socket_connect(address).await?;
        send_all(scheduler, &socket, PAYLOAD.to_vec()).await?;
        let mut received = Vec::new();
        while received.len() < PAYLOAD.len() {
            let (result, buffer) = scheduler.socket_recv(&socket, vec![0; BUFFER_SIZE]).await;
            let count = result?;
            if count == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            received.extend_from_slice(&buffer[..count]);
        }
        assert_eq!(received, PAYLOAD);
        // Ordinary resource Drop closes this connection, allowing serve to end.
        Ok(())
    };
    // Composition needs only Future/Waker, not a scheduler block/unblock API.
    // Spawn is already exercised by portable_io; this probe focuses on I/O.
    let (count, ()) = futures_lite::future::try_zip(serve(scheduler, &listener), client).await?;
    assert_eq!(count, PAYLOAD.len());
    Ok(())
}

async fn exercise<S: File + Socket>(
    scheduler: &S,
    source: &Path,
    destination: &Path,
) -> io::Result<()> {
    assert_eq!(
        copy(scheduler, source, destination).await?,
        PAYLOAD.len() as u64
    );
    exchange(scheduler).await
}

fn main() -> io::Result<()> {
    let simulated = simulated::Scheduler::new(3, 2);
    simulated.insert(Path::new("source"), PAYLOAD);
    futures_lite::future::block_on(exercise(&simulated, Path::new("source"), Path::new("copy")))?;
    assert_eq!(simulated.contents(Path::new("copy")), PAYLOAD);
    println!(
        "Simulated: copied and echoed {} bytes with short transfers",
        PAYLOAD.len()
    );

    #[cfg(any(feature = "native", feature = "tokio"))]
    let directory = tempfile::tempdir()?;
    #[cfg(any(feature = "native", feature = "tokio"))]
    let source = directory.path().join("source");
    #[cfg(any(feature = "native", feature = "tokio"))]
    std::fs::write(&source, PAYLOAD)?;

    #[cfg(feature = "native")]
    {
        let scheduler = socketry_executor::Scheduler::with_workers(2)?;
        let backend = real::Native {
            scheduler: scheduler.handle(),
        };
        let destination = directory.path().join("native-copy");
        scheduler.block_on(exercise(&backend, &source, &destination))?;
        assert_eq!(std::fs::read(destination)?, PAYLOAD);
        println!("Native: copied and echoed {} bytes", PAYLOAD.len());
    }
    #[cfg(feature = "tokio")]
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let scheduler =
            socketry_executor::scheduler::tokio::Scheduler::new(runtime.handle().clone());
        let backend = real::Tokio {
            scheduler: scheduler.handle(),
            runtime: runtime.handle().clone(),
        };
        let destination = directory.path().join("tokio-copy");
        runtime.block_on(exercise(&backend, &source, &destination))?;
        assert_eq!(std::fs::read(destination)?, PAYLOAD);
        runtime.block_on(scheduler.shutdown());
        println!("Tokio: copied and echoed {} bytes", PAYLOAD.len());
    }
    Ok(())
}

#[cfg(test)]
#[path = "interface_probe/tests.rs"]
mod tests;
