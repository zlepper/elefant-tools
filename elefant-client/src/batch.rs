use std::future::Future;

use crate::pool::ConnectionFactory;
use crate::{ElefantClientError, FromSqlRowOwned, SimpleQueryResult};

/// Recursive trait for collecting results from a batched simple query.
///
/// A batched simple query sends multiple SQL statements in one string (separated
/// by `;`). Each statement produces a separate result set. This trait collects
/// those result sets into a nested tuple structure `((((), Vec<A>), Vec<B>), Vec<C>)`.
///
/// Use [`FlattenTuple`] to convert the nested result into a flat tuple.
///
/// The base case is `()`, and each layer wraps the previous batch in a
/// `(Prev, Vec<T>)` tuple.
pub trait CollectBatch: Sized {
    fn collect<F: ConnectionFactory>(
        result: &mut SimpleQueryResult<'_, F>,
    ) -> impl Future<Output = Result<Self, ElefantClientError>>;
}

impl CollectBatch for () {
    fn collect<F: ConnectionFactory>(
        _result: &mut SimpleQueryResult<'_, F>,
    ) -> impl Future<Output = Result<Self, ElefantClientError>> {
        std::future::ready(Ok(()))
    }
}

impl<Prev: CollectBatch, T: FromSqlRowOwned> CollectBatch for (Prev, Vec<T>) {
    async fn collect<F: ConnectionFactory>(
        result: &mut SimpleQueryResult<'_, F>,
    ) -> Result<Self, ElefantClientError> {
        let prev = Prev::collect(result).await?;
        let current = result.collect_next_to_vec::<T>().await?;
        Ok((prev, current))
    }
}

/// Appends an element to a flat tuple, producing a tuple one element larger.
pub trait TupleAppend<T> {
    type Output;
    fn append(self, item: T) -> Self::Output;
}

macro_rules! impl_tuple_append {
    (@emit $($idx:tt: $T:ident),* $(,)?) => {
        impl<$($T,)* New> TupleAppend<New> for ($($T,)*) {
            type Output = ($($T,)* New,);
            #[inline]
            fn append(self, item: New) -> Self::Output {
                ($(self.$idx,)* item,)
            }
        }
    };
    (@step [$($done:tt)*]) => {
        impl_tuple_append!(@emit $($done)*);
    };
    (@step [$($done:tt)*] $idx:tt: $T:ident $(, $($rest:tt)*)?) => {
        impl_tuple_append!(@emit $($done)*);
        impl_tuple_append!(@step [$($done)* $idx: $T,] $($($rest)*)?);
    };
    ($($all:tt)*) => {
        impl_tuple_append!(@step [] $($all)*);
    };
}

impl_tuple_append!(0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5, 6: T6, 7: T7,
    8: T8, 9: T9, 10: T10, 11: T11, 12: T12, 13: T13, 14: T14, 15: T15);

/// Flattens a nested left-associated tuple like `((((), A), B), C)` into `(A, B, C)`.
pub trait FlattenTuple {
    type Output;
    fn flatten(self) -> Self::Output;
}

impl FlattenTuple for () {
    type Output = ();
    fn flatten(self) {}
}

impl<Prev: FlattenTuple, T> FlattenTuple for (Prev, T)
where
    Prev::Output: TupleAppend<T>,
{
    type Output = <Prev::Output as TupleAppend<T>>::Output;
    fn flatten(self) -> Self::Output {
        self.0.flatten().append(self.1)
    }
}
