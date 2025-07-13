use crate::{ElefantClientError, FromSql, PostgresDataRow};

pub trait FromSqlRow<'a>: Sized {
    fn from_sql_row(row: &'a PostgresDataRow) -> Result<Self, ElefantClientError>;
}

pub trait FromSqlRowOwned: for<'owned> FromSqlRow<'owned> {}

impl<T> FromSqlRowOwned for T where T: for<'a> FromSqlRow<'a> {}

impl<'a, T1> FromSqlRow<'a> for (T1,)
where
    T1: FromSql<'a>,
{
    fn from_sql_row(row: &'a PostgresDataRow) -> Result<Self, ElefantClientError> {
        row.require_columns(1)?;

        let t1 = row.get(0)?;
        Ok((t1,))
    }
}

impl<'a, T1, T2> FromSqlRow<'a> for (T1, T2)
where
    T1: FromSql<'a>,
    T2: FromSql<'a>,
{
    fn from_sql_row(row: &'a PostgresDataRow) -> Result<Self, ElefantClientError> {
        row.require_columns(2)?;

        let t1 = row.get(0)?;
        let t2 = row.get(1)?;
        Ok((t1, t2))
    }
}

impl<'a, T1, T2, T3> FromSqlRow<'a> for (T1, T2, T3)
where
    T1: FromSql<'a>,
    T2: FromSql<'a>,
    T3: FromSql<'a>,
{
    fn from_sql_row(row: &'a PostgresDataRow) -> Result<Self, ElefantClientError> {
        row.require_columns(3)?;

        let t1 = row.get(0)?;
        let t2 = row.get(1)?;
        let t3 = row.get(2)?;
        Ok((t1, t2, t3))
    }
}

impl<'a, T1, T2, T3, T4> FromSqlRow<'a> for (T1, T2, T3, T4)
where
    T1: FromSql<'a>,
    T2: FromSql<'a>,
    T3: FromSql<'a>,
    T4: FromSql<'a>,
{
    fn from_sql_row(row: &'a PostgresDataRow) -> Result<Self, ElefantClientError> {
        row.require_columns(4)?;

        let t1 = row.get(0)?;
        let t2 = row.get(1)?;
        let t3 = row.get(2)?;
        let t4 = row.get(3)?;
        Ok((t1, t2, t3, t4))
    }
}

impl<'a, T1, T2, T3, T4, T5> FromSqlRow<'a> for (T1, T2, T3, T4, T5)
where
    T1: FromSql<'a>,
    T2: FromSql<'a>,
    T3: FromSql<'a>,
    T4: FromSql<'a>,
    T5: FromSql<'a>,
{
    fn from_sql_row(row: &'a PostgresDataRow) -> Result<Self, ElefantClientError> {
        row.require_columns(5)?;

        let t1 = row.get(0)?;
        let t2 = row.get(1)?;
        let t3 = row.get(2)?;
        let t4 = row.get(3)?;
        let t5 = row.get(4)?;
        Ok((t1, t2, t3, t4, t5))
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use crate::test_helpers::get_tokio_test_client;

    #[tokio::test]
    async fn reads_multiple_columns_at_once() {
        let mut client = get_tokio_test_client().await;

        let query_result = client.query("select 1::int4, 2::int4", &[]).await.unwrap();
        let results = query_result.collect_to_vec::<(i32, i32)>().await.unwrap();

        assert_eq!(results, vec![(1, 2)]);
    }
}
