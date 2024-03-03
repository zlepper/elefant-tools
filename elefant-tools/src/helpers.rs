use std::future::Future;
use futures::stream::FuturesUnordered;
use futures::StreamExt;

pub(crate) trait StringExt {
    fn push_join(&mut self, separator: &str, items: impl IntoIterator<Item=impl AsRef<str>>);
}

impl StringExt for String {
    fn push_join(&mut self, separator: &str, items: impl IntoIterator<Item=impl AsRef<str>>) {
        for (idx, v) in items.into_iter().enumerate() {
            if idx > 0 {
                self.push_str(separator);
            }
            self.push_str(v.as_ref());
        }
    }
}

pub(crate) struct JoinHandles<T, E>
    where T: Future,
          T: Future<Output=Result<(), E>>
{
    futures: FuturesUnordered<T>,
}

impl<T, E> JoinHandles<T, E>
    where T: Future,
          T: Future<Output=Result<(), E>>
{
    
    pub fn new() -> Self {
        let futures = FuturesUnordered::new();
        
        Self {
            futures,
        }
    }
    
    pub fn push(&mut self, future: T) {
        self.futures.push(future);
    }

    pub async fn join_all(mut self) -> T::Output {
        while let Some(r) = self.futures.next().await {
            r?;
        }
        
        Ok(())
    }
    
    pub async fn wait_one(&mut self) -> Result<(), E> {
        if let Some(r) = self.futures.next().await {
            r
        } else {
            Ok(())
        }
    }
}