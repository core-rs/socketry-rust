// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Query an open file through the scheduler and keep the metadata after closing it.
//!
//! Run with `cargo run -p socketry-executor --example file_stat`. Add `--features tokio` for the Tokio adapter, or `--features io-uring` on Linux for native `statx` completion.

#[cfg(any(feature = "native", feature = "tokio"))]
use socketry_executor::{
    Cancellation, FileClose, FileMetadata, FileOpen, FileOptions, FileStat, OperationResult,
};
use std::io;

#[cfg(any(feature = "native", feature = "tokio"))]
async fn inspect<S: FileOpen + FileStat + FileClose>(
    scheduler: &S,
    path: &std::path::Path,
) -> OperationResult<()> {
    // `never` is an explicit choice for this short example. A real application can pass its shared shutdown cancellation instead.
    let cancellation = Cancellation::never();
    let file = scheduler
        .file_open(path, FileOptions::READ, &cancellation)
        .await?;

    // Awaiting establishes that backend access has ended. The result owns its metadata; it does not borrow the file or change its cursor.
    let result = scheduler.file_stat(&file, &cancellation).await;
    scheduler.file_close(file).await?;
    let metadata = result?;

    // Importing `FileMetadata` makes the common accessors available on whichever concrete metadata type this scheduler returns. Fields are interpreted on demand, rather than eagerly copied into a portable record.
    println!(
        "type: {:?}, size: {:?}, modified: {:?}",
        metadata.file_type(),
        metadata.size(),
        metadata.modified()
    );
    // `None` means the backend/filesystem did not supply a field or its timestamp could not be represented. Native details are available on concrete results: `as_statx()` for io_uring, or `as_std()` for readiness/Tokio metadata extensions.
    Ok(())
}

fn main() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("example.txt");
    std::fs::write(&path, b"Owned metadata, native storage.\n")?;
    #[cfg(feature = "native")]
    {
        let scheduler = socketry_executor::Scheduler::with_workers(1)?;
        scheduler.block_on(inspect(&scheduler, &path))?;
    }
    #[cfg(feature = "tokio")]
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let scheduler =
            socketry_executor::scheduler::tokio::Scheduler::new(runtime.handle().clone());
        runtime.block_on(inspect(&scheduler, &path))?;
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
}
