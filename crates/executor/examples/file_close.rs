// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Cancel an operation, await its completion, and explicitly close its file.
//!
//! Run with `cargo run -p socketry-executor --example file_close`. Add `--features tokio` to also exercise the Tokio adapter, or `--features io-uring` on Linux for native completion I/O.

#[cfg(any(feature = "native", feature = "tokio"))]
use socketry_executor::{
    Cancellation, FileClose, FileOpen, FileOptions, FileRead, OperationResult, yield_now,
};
use std::io;
#[cfg(any(feature = "native", feature = "tokio"))]
use std::path::Path;

#[cfg(any(feature = "native", feature = "tokio"))]
async fn read_then_close<S: FileOpen + FileRead + FileClose>(
    scheduler: &S,
    path: &Path,
) -> OperationResult<usize> {
    let file = scheduler
        .file_open(path, FileOptions::READ, &Cancellation::never())
        .await?;
    let shutdown = Cancellation::new();

    // `clone` shares the same cancellation signal. It does not copy a boolean or create a separate task: cancelling either handle wakes operations watching this signal.
    let request_shutdown = shutdown.clone();
    let request = async move {
        // Stand in for a signal handler or another task requesting shutdown. Yield once so the read has an opportunity to start; a fast read can still finish before cancellation.
        yield_now().await;
        request_shutdown.cancel();
    };

    // `zip` polls both futures and waits for BOTH to finish. Requesting cancellation does not drop the read future: we keep awaiting it until the backend has stopped accessing its buffer and file.
    let ((result, buffer), ()) = futures_lite::future::zip(
        scheduler.file_read(&file, vec![0; 4096], &shutdown),
        request,
    )
    .await;

    // The buffer has been returned, and the backend has released its file reference. Completion may have won the cancellation race, so `result` may be a byte count or a cancellation/error result.
    drop(buffer);

    // Move ownership into close only after the read has completed. Handle the read result AFTER closing, so a cancellation or read error does not skip explicit cleanup.
    // `drop(read_future)` abandons completion and may leave backend work running. io_uring requests cancellation on drop, but even then immediately closing could panic because backend access may still be outstanding.
    scheduler.file_close(file).await?;
    result
}

#[cfg(any(feature = "native", feature = "tokio"))]
fn report(backend: &str, result: OperationResult<usize>) -> io::Result<()> {
    match result {
        Ok(count) => println!("{backend}: read {count} bytes, then closed the file"),
        Err(socketry_executor::OperationError::Cancelled(_)) => {
            println!("{backend}: cancellation completed, then closed the file")
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn main() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("example.txt");
    // Synchronous setup runs before entering a scheduler.
    std::fs::write(&path, b"Cancel, await, then close.\n")?;

    #[cfg(feature = "native")]
    {
        let scheduler = socketry_executor::Scheduler::with_workers(1)?;
        report(
            "Native",
            scheduler.block_on(read_then_close(&scheduler, &path)),
        )?;
    }
    #[cfg(feature = "tokio")]
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let scheduler =
            socketry_executor::scheduler::tokio::Scheduler::new(runtime.handle().clone());
        report(
            "Tokio",
            runtime.block_on(read_then_close(&scheduler, &path)),
        )?;
        runtime.block_on(scheduler.shutdown());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn example_completes_on_enabled_backends() {
        super::main().unwrap();
    }

    #[cfg(any(feature = "native", feature = "tokio"))]
    #[test]
    fn reports_both_race_outcomes_and_preserves_io_errors() {
        super::report("test", Ok(3)).unwrap();
        let cancellation = socketry_executor::Cancellation::new();
        cancellation.cancel();
        super::report("test", Err(cancellation.check().unwrap_err().into())).unwrap();
        let error = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        assert_eq!(
            super::report("test", Err(error.into())).unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied
        );
    }
}
