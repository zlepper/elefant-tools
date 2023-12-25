use std::future::Future;
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct ParallelRunner {
    permits: Arc<Semaphore>,
}

impl ParallelRunner {
    pub fn new(max_parallelism: usize) -> Self {
        if max_parallelism < 1 {
            panic!("max_parallelism must be at least 1");
        }

        Self {
            permits: Arc::new(Semaphore::new(max_parallelism))
        }
    }

    pub async fn run<F>(&self, run: F) -> F::Output
        where F: Future + Send + 'static,
              F::Output: Send + 'static,
    {
        let permit = Arc::clone(&self.permits).acquire_owned().await.expect("Failed to acquire semaphore permit to parallel processing. This should never happen...");

        let result = run.await;

        drop(permit);

        result
    }
}

