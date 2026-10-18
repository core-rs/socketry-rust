// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Give a file access hints, choosing between completion and submission-only ownership.
//!
//! On Linux, run `cargo run -p socketry-executor --example file_advice`. Add `--features tokio` for the adapter or `--features io-uring` for the native completion backend. Advice currently uses the cleanup pool on all three.

#[cfg(all(target_os = "linux", any(feature = "native", feature = "tokio")))]
async fn advise<
    S: socketry_executor::FileAdvice + socketry_executor::FileOpen + socketry_executor::FileClose,
>(
    scheduler: &S,
    path: &std::path::Path,
) -> socketry_executor::OperationResult<()> {
    use socketry_executor::{AdviceMode, Cancellation, FileOptions};
    let never = Cancellation::never();
    let file = scheduler.file_open(path, FileOptions::READ, &never).await?;
    // Zero length means through EOF. The OS may use a larger readahead window; success is not a promise that data is already resident.
    let result = scheduler
        .file_advise(&file, 0, 0, AdviceMode::Sequential)
        .await;
    // Awaiting advice releases its backend reference, so explicit close is valid even when the hint returned an error.
    scheduler.file_close(file).await?;
    result?;

    let file = scheduler.file_open(path, FileOptions::READ, &never).await?;
    // This await only establishes admission. The scheduler owns the accepted job and discards any execution error.
    scheduler
        .file_advise_submit(&file, 0, 0, AdviceMode::WillNeed)
        .await?;
    // Ordinary drop is safe while the job is pending. Explicit close would require completed work instead; use file_advise when you need that boundary.
    drop(file);
    Ok(())
}

fn main() -> std::io::Result<()> {
    #[cfg(all(target_os = "linux", any(feature = "native", feature = "tokio")))]
    {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("advice.txt");
        std::fs::write(&path, b"Access hints preserve file contents.\n")?;
        #[cfg(feature = "native")]
        {
            let scheduler = socketry_executor::Scheduler::with_workers(1)?;
            scheduler.block_on(advise(&scheduler, &path))?;
            // Graceful shutdown drains accepted advice even though its caller kept no completion handle.
            scheduler.shutdown();
        }
        #[cfg(feature = "tokio")]
        {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let scheduler =
                socketry_executor::scheduler::tokio::Scheduler::new(runtime.handle().clone());
            runtime.block_on(advise(&scheduler, &path))?;
            runtime.block_on(scheduler.shutdown());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn example_completes_on_enabled_backends() {
        super::main().unwrap();
    }
}
