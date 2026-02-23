/// Dispatches through a `SequentialOrParallel` enum, running the same expression
/// for both variants.
macro_rules! with_both {
    ($val:expr, |$binding:ident| $body:expr) => {
        match $val {
            SequentialOrParallel::Sequential($binding) => $body,
            SequentialOrParallel::Parallel($binding) => $body,
        }
    };
}

#[cfg(any(test, feature = "test_utilities"))]
pub mod test_helpers;

mod chunk_reader;
mod copy_data;
mod error;
mod helpers;
mod models;
mod object_id;
mod parallel_runner;
mod pg_interval;
mod postgres_client_wrapper;
mod quoting;
mod schema_reader;
mod storage;
mod whitespace_ignorant_string;

pub use copy_data::*;
pub use error::*;
pub use models::*;
pub use object_id::ObjectId;
pub use elefant_client::PostgresConnectionSettings;
pub use postgres_client_wrapper::PostgresClientWrapper;
pub use quoting::IdentifierQuoter;
pub use storage::*;

pub(crate) fn default<T: Default>() -> T {
    T::default()
}
