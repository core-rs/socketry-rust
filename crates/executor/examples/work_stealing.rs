// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use socketry_executor::{Scheduler, Task, yield_now};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scheduler = Scheduler::with_workers(4)?;
    let mut handles = Vec::new();

    for value in 0..8 {
        handles.push(scheduler.spawn(async move {
            let task = Task::current().expect("running in a task");
            yield_now().await;
            println!(
                "task {} ran on {:?}",
                task.id(),
                std::thread::current().name()
            );
            value * value
        })?);
    }

    let sum = scheduler.block_on(async {
        let mut sum = 0;
        for handle in handles {
            sum += handle.await?;
        }
        Ok::<_, socketry_executor::TaskError>(sum)
    })?;
    println!("sum: {sum}");
    Ok(())
}
