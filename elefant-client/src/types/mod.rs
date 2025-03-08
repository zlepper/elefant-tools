mod core;
mod oid;
mod standard_types;
mod from_sql_row;

use std::error::Error;
use crate::ElefantClientError;
use crate::protocol::FieldDescription;
pub use oid::*;
pub use from_sql_row::*;

pub trait FromSql<'a>: Sized {
    fn from_sql_binary(
        raw: &'a [u8],
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>>;

    fn from_sql_text(
        raw: &'a str,
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>>;

    fn accepts(field: &FieldDescription) -> bool {
        Self::accepts_postgres_type(field.data_type_oid)
    }

    fn accepts_postgres_type(oid: i32) -> bool;

    fn from_null(field: &FieldDescription) -> Result<Self, ElefantClientError> {
        Err(ElefantClientError::UnexpectedNullValue {
            postgres_field: field.clone(),
        })
    }
}
/// A trait for types which can be created from a Postgres value without borrowing any data.
///
/// This is primarily useful for trait bounds on functions.
pub trait FromSqlOwned: for<'owned> FromSql<'owned> {}

impl<T> FromSqlOwned for T where T: for<'a> FromSql<'a> {}

pub trait ToSql {
    fn to_sql_binary(&self, target_buffer: &mut Vec<u8>) -> Result<(), Box<dyn Error + Sync + Send>>;
    fn is_null(&self) -> bool {
        false
    }
}

pub trait PostgresNamedType {
    const PG_NAME: &'static str;
}

pub trait DomainType {
    type Inner: for<'owned> FromSql<'owned>;

    fn from_inner(inner: Self::Inner) -> Self;

    fn accepts(field: &FieldDescription) -> bool {
        Self::accepts_postgres_type(field.data_type_oid)
    }
    
    fn accepts_postgres_type(oid: i32) -> bool;
}

#[macro_export] macro_rules! impl_from_sql_for_domain_type {
    ($typ: ty) => {
        impl<'a> FromSql<'a> for $typ {
            fn from_sql_binary(raw: &'a [u8], field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
                let inner = <Self as DomainType>::Inner::from_sql_binary(raw, field)?;

                Ok(Self::from_inner(inner))
            }

            fn from_sql_text(raw: &'a str, field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
                let inner = <Self as DomainType>::Inner::from_sql_text(raw, field)?;

                Ok(Self::from_inner(inner))
            }

            fn accepts(field: &FieldDescription) -> bool {
                <Self as DomainType>::accepts(field)
            }

            fn accepts_postgres_type(oid: i32) -> bool {
                <Self as DomainType>::accepts_postgres_type(oid)
            }
        }
    };
}

pub struct PostgresType {
    oid: i32,
    name: &'static str,
    /// The underlying type
    element: Option<&'static PostgresType>,
    /// True if this is an array type
    is_array: bool,

    array_delimiter: &'static str,
}

impl PostgresType {
    fn inner_most(&self) -> &PostgresType {
        match self.element {
            Some(element) => element.inner_most(),
            None => self,
        }
    }
}
