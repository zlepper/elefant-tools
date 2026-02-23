use crate::pg_interval::interval::Interval;
use crate::{FieldDescription, FromSqlBase, FromSqlBinary, FromSqlText, ToSql};
use std::error::Error;

const INTERVAL_OID: i32 = 1186;

impl<'a> FromSqlBase<'a> for Interval {
    fn accepts_postgres_type(oid: i32) -> bool {
        oid == INTERVAL_OID
    }
}

impl<'a> FromSqlBinary<'a> for Interval {
    fn from_sql_binary(
        raw: &'a [u8],
        _field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let microseconds = i64::from_be_bytes(raw[0..8].try_into().unwrap());
        let days = i32::from_be_bytes(raw[8..12].try_into().unwrap());
        let months = i32::from_be_bytes(raw[12..16].try_into().unwrap());
        Ok(Interval {
            months,
            days,
            microseconds,
        })
    }
}

impl<'a> FromSqlText<'a> for Interval {
    fn from_sql_text(
        raw: &'a str,
        _field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Interval::from_postgres(raw).map_err(|e| Box::new(e) as Box<dyn Error + Sync + Send>)
    }
}

impl ToSql for Interval {
    fn to_sql_binary(
        &self,
        target_buffer: &mut Vec<u8>,
    ) -> Result<(), Box<dyn Error + Sync + Send>> {
        target_buffer.extend_from_slice(&self.microseconds.to_be_bytes());
        target_buffer.extend_from_slice(&self.days.to_be_bytes());
        target_buffer.extend_from_slice(&self.months.to_be_bytes());
        Ok(())
    }
}
