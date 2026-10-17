// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! The same TCP exchange, compiled against Socketry or Tokio.
#[cfg(any(feature = "native", feature = "tokio"))]
static NEVER: socketry_executor::Cancellation = socketry_executor::Cancellation::never();

#[cfg(any(feature = "native", feature = "tokio"))]
use socketry_executor::{Socket, SocketDomain, SocketProtocol, SocketType, Spawn};
#[cfg(any(feature = "native", feature = "tokio"))]
use std::io;

#[cfg(any(feature = "native", feature = "tokio"))]
async fn exchange<SchedulerType>(scheduler: SchedulerType) -> io::Result<u8>
where
    SchedulerType: Socket + Spawn + Clone + 'static,
{
    let listener = scheduler
        .socket_open(
            SocketDomain::Ipv4,
            SocketType::Stream,
            Some(SocketProtocol::Tcp),
            &NEVER,
        )
        .await?;
    scheduler
        .socket_bind(&listener, "127.0.0.1:0".parse().unwrap(), &NEVER)
        .await?;
    scheduler.socket_listen(&listener, 128, &NEVER).await?;
    let address = scheduler.socket_local_addr(&listener)?;
    let server_scheduler = scheduler.clone();
    let server = scheduler
        .spawn(async move {
            let (socket, _) = server_scheduler.socket_accept(&listener, &NEVER).await?;
            let (result, buffer) = server_scheduler
                .socket_read(&socket, vec![0; 1], &NEVER)
                .await;
            if result? != 1 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
            let (result, _) = server_scheduler.socket_write(&socket, buffer, &NEVER).await;
            if result? != 1 {
                return Err(io::Error::from(io::ErrorKind::WriteZero));
            }
            Ok::<_, io::Error>(())
        })
        .map_err(io::Error::other)?;

    let socket = scheduler
        .socket_open(
            SocketDomain::Ipv4,
            SocketType::Stream,
            Some(SocketProtocol::Tcp),
            &NEVER,
        )
        .await?;
    scheduler.socket_connect(&socket, address, &NEVER).await?;
    let (result, buffer) = scheduler.socket_write(&socket, vec![42], &NEVER).await;
    if result? != 1 {
        return Err(io::Error::from(io::ErrorKind::WriteZero));
    }
    let (result, buffer) = scheduler.socket_read(&socket, buffer, &NEVER).await;
    if result? != 1 {
        return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
    }
    server
        .await
        .map_err(|error| io::Error::other(error.to_string()))??;
    Ok(buffer[0])
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "tokio")]
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let scheduler =
            socketry_executor::scheduler::tokio::Scheduler::new(runtime.handle().clone());
        let value = runtime.block_on(exchange(scheduler.handle()))?;
        println!("Tokio received {value}");
        runtime.block_on(scheduler.shutdown());
    }
    #[cfg(all(feature = "native", not(feature = "tokio")))]
    {
        let scheduler = socketry_executor::Scheduler::with_workers(2)?;
        let value = scheduler.block_on(exchange(scheduler.handle()))?;
        println!("Socketry received {value}");
    }
    #[cfg(not(any(feature = "native", feature = "tokio")))]
    {
        Err("enable the native or tokio feature".into())
    }
    #[cfg(any(feature = "native", feature = "tokio"))]
    Ok(())
}
