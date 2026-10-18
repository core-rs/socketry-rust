// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! The same generic application using the public FileScheduler, SocketScheduler, and AddressResolution capabilities.
//! Run with `--features tokio` for native, Tokio, and simulated I/O.
//! With `--no-default-features`, only the in-memory implementation runs.
#[path = "interface_probe/simulated.rs"]
mod simulated;

use socketry_executor::{
    AddressHints, AddressResolution, AddressService, Cancellation, FileOpen, FileOptions,
    FileReadAt, FileScheduler, FileWriteAt, IoBuf, OperationResult, SocketDomain, SocketProtocol,
    SocketScheduler, SocketType,
};
use socketry_executor::{
    SocketAccept, SocketBind, SocketConnect, SocketListen, SocketLocalAddr, SocketOpen, SocketRead,
    SocketWrite,
};
use std::{io, path::Path};
static NEVER: Cancellation = Cancellation::never();

const PAYLOAD: &[u8] = b"Explicit schedulers keep protocol code portable.\n";
const BUFFER_SIZE: usize = 16;

/// Application code only sees the backend's associated resources.
async fn copy<S: FileOpen + FileReadAt + FileWriteAt>(
    scheduler: &S,
    source: &Path,
    destination: &Path,
    cancellation: &Cancellation,
) -> OperationResult<u64> {
    let source = scheduler
        .file_open(source, FileOptions::READ, cancellation)
        .await?;
    let destination = scheduler
        .file_open(destination, FileOptions::CREATE, cancellation)
        .await?;
    let mut buffer = vec![0; BUFFER_SIZE];
    let mut offset = 0;
    loop {
        let (result, returned) = scheduler
            .file_read_at(&source, buffer, offset, cancellation)
            .await;
        buffer = returned;
        let count = result?;
        if count == 0 {
            return Ok(offset);
        }
        let mut written = 0;
        while written < count {
            let (result, returned) = scheduler
                .file_write_at(
                    &destination,
                    buffer.slice(written..count),
                    offset + written as u64,
                    cancellation,
                )
                .await;
            buffer = returned.into_inner();
            let count = result?;
            if count == 0 {
                return Err(io::Error::from(io::ErrorKind::WriteZero).into());
            }
            written += count;
        }
        offset += count as u64;
    }
}

async fn send_all<S: SocketWrite>(
    scheduler: &S,
    socket: &S::Socket,
    mut buffer: Vec<u8>,
    cancellation: &Cancellation,
) -> OperationResult<()> {
    let mut written = 0;
    while written < buffer.len() {
        let (result, returned) = scheduler
            .socket_write(socket, buffer.slice(written..), cancellation)
            .await;
        buffer = returned.into_inner();
        let count = result?;
        if count == 0 {
            return Err(io::Error::from(io::ErrorKind::WriteZero).into());
        }
        written += count;
    }
    Ok(())
}

/// Serve one connection until peer EOF, echoing each received prefix.
async fn serve<S: SocketAccept + SocketRead + SocketWrite>(
    scheduler: &S,
    listener: &S::Socket,
) -> OperationResult<usize> {
    let (socket, _) = scheduler.socket_accept(listener, &NEVER).await?;
    let mut total = 0;
    loop {
        let (result, mut buffer) = scheduler
            .socket_read(&socket, vec![0; BUFFER_SIZE], &NEVER)
            .await;
        let count = result?;
        if count == 0 {
            return Ok(total);
        }
        total += count;
        buffer.truncate(count);
        send_all(scheduler, &socket, buffer, &NEVER).await?;
    }
}

async fn exchange<
    S: SocketOpen
        + SocketBind
        + SocketListen
        + SocketLocalAddr
        + SocketConnect
        + SocketAccept
        + SocketRead
        + SocketWrite
        + AddressResolution,
>(
    scheduler: &S,
) -> OperationResult<()> {
    let address = "127.0.0.1:0".parse().unwrap();
    let listener = scheduler
        .socket_open(
            SocketDomain::Ipv4,
            SocketType::Stream,
            Some(SocketProtocol::Tcp),
            &NEVER,
        )
        .await?;
    scheduler.socket_bind(&listener, address, &NEVER).await?;
    scheduler.socket_listen(&listener, 128, &NEVER).await?;
    let address = scheduler.socket_local_addr(&listener)?;
    let client = async {
        // The same hostname resolves through the system backend or the simulator's explicit table. Connection policy remains application code.
        let addresses = scheduler
            .address_resolve(
                Some("localhost"),
                AddressService::Port(address.port()),
                AddressHints {
                    domain: Some(SocketDomain::Ipv4),
                    socket_type: Some(SocketType::Stream),
                    ..AddressHints::default()
                },
                &NEVER,
            )
            .await?;
        let address = addresses
            .into_iter()
            .next()
            .ok_or_else(|| io::Error::from(io::ErrorKind::AddrNotAvailable))?;
        let socket = scheduler
            .socket_open(
                SocketDomain::from(address.address),
                address.socket_type,
                address.protocol,
                &NEVER,
            )
            .await?;
        scheduler
            .socket_connect(&socket, address.address, &NEVER)
            .await?;
        send_all(scheduler, &socket, PAYLOAD.to_vec(), &NEVER).await?;
        let mut received = Vec::new();
        while received.len() < PAYLOAD.len() {
            let (result, buffer) = scheduler
                .socket_read(&socket, vec![0; BUFFER_SIZE], &NEVER)
                .await;
            let count = result?;
            if count == 0 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
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

async fn exercise<S: FileScheduler + SocketScheduler + AddressResolution>(
    scheduler: &S,
    source: &Path,
    destination: &Path,
) -> OperationResult<()> {
    assert_eq!(
        copy(scheduler, source, destination, &NEVER).await?,
        PAYLOAD.len() as u64
    );
    exchange(scheduler).await
}

fn main() -> io::Result<()> {
    let simulated = simulated::Scheduler::new(3, 2);
    simulated.insert(Path::new("source"), PAYLOAD);
    simulated.insert_addresses("localhost", vec!["127.0.0.1".parse().unwrap()]);
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
        let backend = scheduler.handle();
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
        let backend = scheduler.handle();
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
