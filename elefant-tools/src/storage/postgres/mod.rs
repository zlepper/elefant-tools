#[cfg(test)]
mod tests;
mod connection_pool;
mod postgres_instance_storage;
mod parallel_copy_source;
mod parallel_copy_destination;
mod sequential_copy_source;
mod sequential_copy_destination;

pub use postgres_instance_storage::PostgresInstanceStorage;