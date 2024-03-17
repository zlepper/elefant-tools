use std::future::Future;
use futures::stream::FuturesUnordered;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use std::num::NonZeroUsize;
use futures::StreamExt;

/// Provides a way of waiting for multiple futures to complete in parallel.
/// 
/// The futures here needs to return a Result, which is the main difference from the standard
/// FuturesUnordered.
struct JoinHandles<T, E>
    where T: Future,
          T: Future<Output=Result<(), E>>
{
    futures: FuturesUnordered<T>,
}

impl<T, E> JoinHandles<T, E>
    where T: Future,
          T: Future<Output=Result<(), E>>
{
    /// Create a new JoinHandles
    pub fn new() -> Self {
        let futures = FuturesUnordered::new();

        Self {
            futures,
        }
    }

    /// Adds another Future to the queue
    pub fn push(&mut self, future: T) {
        self.futures.push(future);
    }

    /// Executes all futures in parallel and waits for all of them to complete.
    /// If any of the futures returns an error, the error is returned.
    pub async fn join_all(mut self) -> T::Output {
        while let Some(r) = self.futures.next().await {
            r?;
        }

        Ok(())
    }

    /// Waits for one of the futures to complete. If the future returns an error, the error is returned.
    pub async fn wait_one(&mut self) -> Result<(), E> {
        if let Some(r) = self.futures.next().await {
            r
        } else {
            Ok(())
        }
    }
}

/// Executes a given set of futures in parallel, with a maximum number of parallel executions.
pub(crate) struct ParallelRunner<T, E>
    where T: Future,
          T: Future<Output=Result<(), E>>
{
    join_handles: JoinHandles<WaitingFuture<T, E>, E>,
    permits: Arc<Semaphore>,
}

impl<T, E> ParallelRunner<T, E>
    where T: Future,
          T: Future<Output=Result<(), E>>
{
    /// Creates a new ParallelRunner with the specified maximum number of parallel executions.
    pub fn new(max_parallel: NonZeroUsize) -> Self {
        let permits = Arc::new(Semaphore::new(max_parallel.get()));

        Self {
            join_handles: JoinHandles::new(),
            permits,
        }
    }

    /// Enqueues a new future to be executed in parallel. 
    /// If the maximum number of parallel executions has been reached, this function will wait until
    /// one of the futures has completed.
    pub async fn enqueue(&mut self, fut: T) -> Result<(), E>
    {
        loop {
            match Arc::clone(&self.permits).try_acquire_owned() {
                Ok(permit) => {
                    self.join_handles.push(WaitingFuture {
                        inner: Box::pin(fut),
                        _permit: permit,
                    });
                    break;
                }
                Err(TryAcquireError::NoPermits) => {
                    self.join_handles.wait_one().await?;
                }
                Err(_) => {
                    panic!("Failed to acquire semaphore permit to parallel processing. This should never happen...")
                }
            }
        }

        Ok(())
    }

    /// Waits for all remaining futures to complete.
    pub async fn run_remaining(self) -> Result<(), E> {
        self.join_handles.join_all().await
    }
}

struct WaitingFuture<F, E>
    where F: Future,
          F: Future<Output=Result<(), E>>
{
    inner: Pin<Box<F>>,
    _permit: OwnedSemaphorePermit,
}

impl<F, E> Future for WaitingFuture<F, E>
    where F: Future,
          F: Future<Output=Result<(), E>>
{
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use tokio::test;
    use crate::parallel_runner::ParallelRunner;

    #[test]
    async fn runs_in_parallel() {
        let mut runner = ParallelRunner::new(NonZeroUsize::new(10).unwrap());

        let start = std::time::Instant::now();

        for _ in 0..5 {
            runner.enqueue(delay(100)).await.unwrap();
        }

        runner.run_remaining().await.unwrap();

        let end = std::time::Instant::now();

        let took = end - start;

        assert!(took < std::time::Duration::from_millis(200), "Took {:?}", took);
    }

    #[test]
    async fn only_runs_limited_number_of_parallel() {
        let mut runner = ParallelRunner::new(NonZeroUsize::new(10).unwrap());

        let start = std::time::Instant::now();

        for _ in 0..15 {
            runner.enqueue(delay(100)).await.unwrap();
        }

        runner.run_remaining().await.unwrap();

        let end = std::time::Instant::now();

        let took = end - start;

        assert!(took < std::time::Duration::from_millis(400) && took > std::time::Duration::from_millis(200), "Took {:?}", took);
    }

    async fn delay(dur_ms: u64) -> Result<(), &'static str> {
        tokio::time::sleep(std::time::Duration::from_millis(dur_ms)).await;
        Ok(())
    }
}
