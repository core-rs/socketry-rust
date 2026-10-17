// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Configure a listener before binding through a generic scheduler. Run with `cargo run -p socketry-executor --example socket_options --features tokio` for both native and Tokio implementations.

use std::io;

#[cfg(any(feature = "native", feature = "tokio"))]
async fn listener<S>(scheduler: &S) -> io::Result<()>
where
    S: socketry_executor::SocketOptions<socketry_executor::SocketReuseAddress>,
{
    use socketry_executor::{Cancellation, SocketDomain, SocketReuseAddress, SocketType};

    let cancellation = Cancellation::never();
    let socket = scheduler
        .socket_open(SocketDomain::Ipv4, SocketType::Stream, None, &cancellation)
        .await?;

    // The option identifier selects both the capability and its value type. SO_REUSEADDR takes a bool, so another value type would not compile.
    // Await configuration before bind. Address-sharing rules remain OS-specific, especially on Windows; choose this policy deliberately for a real server.
    scheduler
        .socket_set_option(&socket, SocketReuseAddress, true, &cancellation)
        .await?;
    assert!(
        scheduler
            .socket_get_option(&socket, SocketReuseAddress, &cancellation)
            .await?
    );

    // Port zero gives this example an available local port. The same descriptor progresses from open to bound to listening.
    scheduler
        .socket_bind(&socket, "127.0.0.1:0".parse().unwrap(), &cancellation)
        .await?;
    scheduler.socket_listen(&socket, 16, &cancellation).await?;
    println!(
        "Configured listener at {}",
        scheduler.socket_local_addr(&socket)?
    );

    // A server could now accept connections and explicitly configure TcpNoDelay or SocketKeepalive on each accepted descriptor. This example stops after setup; every operation is complete before consuming close.
    scheduler.socket_close(socket).await?;
    Ok(())
}

fn main() -> io::Result<()> {
    #[cfg(feature = "native")]
    {
        let scheduler = socketry_executor::Scheduler::with_workers(1)?;
        scheduler.block_on(listener(&scheduler))?;
    }
    #[cfg(feature = "tokio")]
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let scheduler =
            socketry_executor::scheduler::tokio::Scheduler::new(runtime.handle().clone());
        runtime.block_on(listener(&scheduler))?;
        runtime.block_on(scheduler.shutdown());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn configure_listener_on_enabled_backends() {
        super::main().unwrap();
    }
}
