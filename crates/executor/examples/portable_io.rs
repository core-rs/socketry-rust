// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! The same TCP exchange, compiled against Socketry or Tokio.
#[cfg(any(feature = "native", feature = "tokio"))]
use socketry_executor::{Socket, Spawn};
#[cfg(any(feature = "native", feature = "tokio"))]
use std::{io, net::TcpListener};

#[cfg(any(feature = "native", feature = "tokio"))]
async fn exchange<SchedulerType>(scheduler: SchedulerType) -> io::Result<u8>
where
    SchedulerType: Socket + Spawn + Clone + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let listener = scheduler.register_listener(listener)?;
    let server_scheduler = scheduler.clone();
    let server = scheduler
        .spawn(async move {
            let (socket, _) = server_scheduler.accept(&listener).await?;
            let (result, buffer) = server_scheduler.io_read(&socket, vec![0; 1]).await;
            if result? != 1 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
            let (result, _) = server_scheduler.io_write(&socket, buffer).await;
            if result? != 1 {
                return Err(io::Error::from(io::ErrorKind::WriteZero));
            }
            Ok::<_, io::Error>(())
        })
        .map_err(io::Error::other)?;

    let socket = scheduler.connect(address).await?;
    let (result, buffer) = scheduler.io_write(&socket, vec![42]).await;
    if result? != 1 {
        return Err(io::Error::from(io::ErrorKind::WriteZero));
    }
    let (result, buffer) = scheduler.io_read(&socket, buffer).await;
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
        return Err("enable the native or tokio feature".into());
    }
    #[cfg(any(feature = "native", feature = "tokio"))]
    Ok(())
}
