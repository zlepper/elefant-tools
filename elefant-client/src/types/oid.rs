use crate::protocol::FieldDescription;
use crate::{impl_from_sql_for_domain_type, DomainType, FromSql, PostgresType};
use std::error::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Oid(pub i32);

impl DomainType for Oid {
    type Inner = i32;

    fn from_inner(inner: Self::Inner) -> Self {
        Oid(inner)
    }

    fn accepts_postgres_type(oid: i32) -> bool {
        oid == PostgresType::OID.oid
    }
}

impl_from_sql_for_domain_type!(Oid);

#[cfg(test)]
mod tests {
    #[cfg(feature = "tokio")]
    mod tokio_connection {
        use crate::test_helpers::get_tokio_test_client;
        use crate::Oid;

        #[tokio::test]
        async fn handles_oid() {
            let mut client = get_tokio_test_client().await;

            let oid: Oid = client
                .read_single_column_and_row_exactly("select '26'::oid", &[])
                .await;

            assert_eq!(oid, Oid(26));
        }
    }
}
