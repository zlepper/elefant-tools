use crate::pg_interval::interval::Interval;
use bytes::{Buf, BufMut};
use elefant_client::{FieldDescription, FromSqlBase, FromSqlBinary, FromSqlText, ToSql};
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
        let mut raw = raw;
        let microseconds = raw.get_i64();
        let days = raw.get_i32();
        let months = raw.get_i32();
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
        target_buffer.put_i64(self.microseconds);
        target_buffer.put_i32(self.days);
        target_buffer.put_i32(self.months);
        Ok(())
    }
}
