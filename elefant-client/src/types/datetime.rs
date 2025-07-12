use std::error::Error;
use std::sync::LazyLock;
use crate::protocol::FieldDescription;
use crate::types::{FromSql, ToSql};
use time::{Date, Time, PrimitiveDateTime, OffsetDateTime, Month, format_description};

// PostgreSQL epoch: 2000-01-01
const PG_EPOCH_DAYS: i32 = 0; // Days since 2000-01-01
const PG_EPOCH_MICROSECONDS: i64 = 0; // Microseconds since 2000-01-01 00:00:00
const MICROSECONDS_PER_SECOND: i64 = 1_000_000;
const MICROSECONDS_PER_DAY: i64 = 24 * 60 * 60 * MICROSECONDS_PER_SECOND;

// Static format descriptions - parsed once and reused
static DATE_FORMAT: LazyLock<Vec<format_description::FormatItem<'static>>> = LazyLock::new(|| {
    format_description::parse("[year]-[month]-[day]")
        .expect("DATE format description should be valid")
});

static TIME_FORMAT: LazyLock<Vec<format_description::FormatItem<'static>>> = LazyLock::new(|| {
    format_description::parse("[hour]:[minute]:[second]")
        .expect("TIME format description should be valid")
});

static TIME_WITH_SUBSECONDS_FORMAT: LazyLock<Vec<format_description::FormatItem<'static>>> = LazyLock::new(|| {
    format_description::parse("[hour]:[minute]:[second].[subsecond]")
        .expect("TIME with subseconds format description should be valid")
});

static TIMESTAMP_FORMAT: LazyLock<Vec<format_description::FormatItem<'static>>> = LazyLock::new(|| {
    format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]")
        .expect("TIMESTAMP format description should be valid")
});

static TIMESTAMP_WITH_SUBSECONDS_FORMAT: LazyLock<Vec<format_description::FormatItem<'static>>> = LazyLock::new(|| {
    format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]")
        .expect("TIMESTAMP with subseconds format description should be valid")
});

static TIMESTAMPTZ_FORMAT: LazyLock<Vec<format_description::FormatItem<'static>>> = LazyLock::new(|| {
    format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second][offset_hour sign:mandatory]")
        .expect("TIMESTAMPTZ format description should be valid")
});

static TIMESTAMPTZ_WITH_SUBSECONDS_FORMAT: LazyLock<Vec<format_description::FormatItem<'static>>> = LazyLock::new(|| {
    format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond][offset_hour sign:mandatory]")
        .expect("TIMESTAMPTZ with subseconds format description should be valid")
});

// PostgreSQL DATE type - i32 days since 2000-01-01
impl<'a> FromSql<'a> for Date {
    fn from_sql_binary(raw: &'a [u8], field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() != 4 {
            return Err(format!("Invalid length for DATE. Expected 4 bytes, got {} bytes instead. Error occurred when parsing field {:?}", raw.len(), field).into());
        }
        
        let days_since_pg_epoch = i32::from_be_bytes(raw.try_into().unwrap());
        
        // PostgreSQL epoch is 2000-01-01
        let pg_epoch = Date::from_calendar_date(2000, Month::January, 1)
            .map_err(|e| format!("Failed to create PostgreSQL epoch date: {}", e))?;
        
        let result_date = pg_epoch + time::Duration::days(days_since_pg_epoch as i64);
        
        Ok(result_date)
    }

    fn from_sql_text(raw: &'a str, field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // PostgreSQL DATE format: YYYY-MM-DD
        Date::parse(raw, &DATE_FORMAT)
            .map_err(|e| format!("Failed to parse DATE from text '{}': {}. Error occurred when parsing field {:?}", raw, e, field).into())
    }

    fn accepts_postgres_type(oid: i32) -> bool {
        oid == 1082 // DATE OID
    }
}

impl ToSql for Date {
    fn to_sql_binary(&self, target_buffer: &mut Vec<u8>) -> Result<(), Box<dyn Error + Sync + Send>> {
        let pg_epoch = Date::from_calendar_date(2000, Month::January, 1)
            .map_err(|e| format!("Failed to create PostgreSQL epoch date: {}", e))?;
        
        let duration_since_epoch = *self - pg_epoch;
        let days = duration_since_epoch.whole_days() as i32;
        
        target_buffer.extend_from_slice(&days.to_be_bytes());
        Ok(())
    }
}

// PostgreSQL TIME type - i64 microseconds since midnight
impl<'a> FromSql<'a> for Time {
    fn from_sql_binary(raw: &'a [u8], field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() != 8 {
            return Err(format!("Invalid length for TIME. Expected 8 bytes, got {} bytes instead. Error occurred when parsing field {:?}", raw.len(), field).into());
        }
        
        let microseconds_since_midnight = i64::from_be_bytes(raw.try_into().unwrap());
        
        let total_seconds = microseconds_since_midnight / MICROSECONDS_PER_SECOND;
        let remaining_microseconds = (microseconds_since_midnight % MICROSECONDS_PER_SECOND) as u32;
        let remaining_nanoseconds = remaining_microseconds * 1000;
        
        let hours = (total_seconds / 3600) as u8;
        let minutes = ((total_seconds % 3600) / 60) as u8;
        let seconds = (total_seconds % 60) as u8;
        
        Time::from_hms_nano(hours, minutes, seconds, remaining_nanoseconds)
            .map_err(|e| format!("Failed to create TIME from components: {}. Error occurred when parsing field {:?}", e, field).into())
    }

    fn from_sql_text(raw: &'a str, field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // PostgreSQL TIME format: HH:MM:SS or HH:MM:SS.ffffff
        let format = if raw.contains('.') {
            &TIME_WITH_SUBSECONDS_FORMAT
        } else {
            &TIME_FORMAT
        };
        Time::parse(raw, format)
            .map_err(|e| format!("Failed to parse TIME from text '{}': {}. Error occurred when parsing field {:?}", raw, e, field).into())
    }

    fn accepts_postgres_type(oid: i32) -> bool {
        oid == 1083 // TIME OID
    }
}

impl ToSql for Time {
    fn to_sql_binary(&self, target_buffer: &mut Vec<u8>) -> Result<(), Box<dyn Error + Sync + Send>> {
        let total_microseconds = 
            (self.hour() as i64) * 3600 * MICROSECONDS_PER_SECOND +
            (self.minute() as i64) * 60 * MICROSECONDS_PER_SECOND +
            (self.second() as i64) * MICROSECONDS_PER_SECOND +
            (self.nanosecond() as i64) / 1000;
        
        target_buffer.extend_from_slice(&total_microseconds.to_be_bytes());
        Ok(())
    }
}

// PostgreSQL TIMESTAMP type - i64 microseconds since 2000-01-01 00:00:00
impl<'a> FromSql<'a> for PrimitiveDateTime {
    fn from_sql_binary(raw: &'a [u8], field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() != 8 {
            return Err(format!("Invalid length for TIMESTAMP. Expected 8 bytes, got {} bytes instead. Error occurred when parsing field {:?}", raw.len(), field).into());
        }
        
        let microseconds_since_pg_epoch = i64::from_be_bytes(raw.try_into().unwrap());
        
        // PostgreSQL epoch is 2000-01-01 00:00:00
        let pg_epoch = PrimitiveDateTime::new(
            Date::from_calendar_date(2000, Month::January, 1)
                .map_err(|e| format!("Failed to create PostgreSQL epoch date: {}", e))?,
            Time::from_hms(0, 0, 0)
                .map_err(|e| format!("Failed to create midnight time: {}", e))?
        );
        
        let result_datetime = pg_epoch + time::Duration::microseconds(microseconds_since_pg_epoch);
        
        Ok(result_datetime)
    }

    fn from_sql_text(raw: &'a str, field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // PostgreSQL TIMESTAMP format: YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM:SS.ffffff
        let format = if raw.contains('.') {
            &TIMESTAMP_WITH_SUBSECONDS_FORMAT
        } else {
            &TIMESTAMP_FORMAT
        };
        PrimitiveDateTime::parse(raw, format)
            .map_err(|e| format!("Failed to parse TIMESTAMP from text '{}': {}. Error occurred when parsing field {:?}", raw, e, field).into())
    }

    fn accepts_postgres_type(oid: i32) -> bool {
        oid == 1114 // TIMESTAMP OID
    }
}

impl ToSql for PrimitiveDateTime {
    fn to_sql_binary(&self, target_buffer: &mut Vec<u8>) -> Result<(), Box<dyn Error + Sync + Send>> {
        let pg_epoch = PrimitiveDateTime::new(
            Date::from_calendar_date(2000, Month::January, 1)
                .map_err(|e| format!("Failed to create PostgreSQL epoch date: {}", e))?,
            Time::from_hms(0, 0, 0)
                .map_err(|e| format!("Failed to create midnight time: {}", e))?
        );
        
        let duration_since_epoch = *self - pg_epoch;
        let microseconds = duration_since_epoch.whole_microseconds() as i64;
        
        target_buffer.extend_from_slice(&microseconds.to_be_bytes());
        Ok(())
    }
}

// PostgreSQL TIMESTAMPTZ type - i64 microseconds since 2000-01-01 00:00:00 UTC
impl<'a> FromSql<'a> for OffsetDateTime {
    fn from_sql_binary(raw: &'a [u8], field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() != 8 {
            return Err(format!("Invalid length for TIMESTAMPTZ. Expected 8 bytes, got {} bytes instead. Error occurred when parsing field {:?}", raw.len(), field).into());
        }
        
        let microseconds_since_pg_epoch = i64::from_be_bytes(raw.try_into().unwrap());
        
        // PostgreSQL TIMESTAMPTZ is stored as UTC microseconds since 2000-01-01 00:00:00 UTC
        let pg_epoch = OffsetDateTime::new_utc(
            Date::from_calendar_date(2000, Month::January, 1)
                .map_err(|e| format!("Failed to create PostgreSQL epoch date: {}", e))?,
            Time::from_hms(0, 0, 0)
                .map_err(|e| format!("Failed to create midnight time: {}", e))?
        );
        
        let result_datetime = pg_epoch + time::Duration::microseconds(microseconds_since_pg_epoch);
        
        Ok(result_datetime)
    }

    fn from_sql_text(raw: &'a str, field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // PostgreSQL TIMESTAMPTZ format: YYYY-MM-DD HH:MM:SS+TZ or YYYY-MM-DD HH:MM:SS.ffffff+TZ
        // The timezone offset can be +HH, +HH:MM, or just numbers like +00
        let format = if raw.contains('.') {
            &TIMESTAMPTZ_WITH_SUBSECONDS_FORMAT
        } else {
            &TIMESTAMPTZ_FORMAT
        };
        OffsetDateTime::parse(raw, format)
            .map_err(|e| format!("Failed to parse TIMESTAMPTZ from text '{}': {}. Error occurred when parsing field {:?}", raw, e, field).into())
    }

    fn accepts_postgres_type(oid: i32) -> bool {
        oid == 1184 // TIMESTAMPTZ OID
    }
}

impl ToSql for OffsetDateTime {
    fn to_sql_binary(&self, target_buffer: &mut Vec<u8>) -> Result<(), Box<dyn Error + Sync + Send>> {
        let pg_epoch = OffsetDateTime::new_utc(
            Date::from_calendar_date(2000, Month::January, 1)
                .map_err(|e| format!("Failed to create PostgreSQL epoch date: {}", e))?,
            Time::from_hms(0, 0, 0)
                .map_err(|e| format!("Failed to create midnight time: {}", e))?
        );
        
        // Convert to UTC for storage (PostgreSQL stores TIMESTAMPTZ as UTC)
        let utc_datetime = self.to_offset(time::UtcOffset::UTC);
        let duration_since_epoch = utc_datetime - pg_epoch;
        let microseconds = duration_since_epoch.whole_microseconds() as i64;
        
        target_buffer.extend_from_slice(&microseconds.to_be_bytes());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::{date, time, datetime, offset};

    #[cfg(feature = "tokio")]
    mod tokio_connection {
        use super::*;
        use crate::test_helpers::get_settings;
        use crate::tokio_connection::new_client;
        use tokio::test;

        #[test]
        async fn test_date_type() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test PostgreSQL epoch date (2000-01-01)
            let pg_epoch = date!(2000-01-01);
            let value: Date = client.read_single_value("select '2000-01-01'::date;", &[]).await.unwrap();
            assert_eq!(value, pg_epoch);

            // Test current date
            let current_date = date!(2024-01-15);
            let value: Date = client.read_single_value("select '2024-01-15'::date;", &[]).await.unwrap();
            assert_eq!(value, current_date);

            // Test round-trip with parameter binding
            client.execute_non_query("drop table if exists test_date_table; create table test_date_table(value date);", &[]).await.unwrap();
            client.execute_non_query("insert into test_date_table values ($1);", &[&current_date]).await.unwrap();
            let retrieved: Date = client.read_single_value("select value from test_date_table;", &[]).await.unwrap();
            assert_eq!(retrieved, current_date);

            // Test NULL handling
            let null_value: Option<Date> = client.read_single_value("select null::date;", &[]).await.unwrap();
            assert_eq!(null_value, None);
        }

        #[test]
        async fn test_time_type() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test midnight
            let midnight = time!(00:00:00);
            let value: Time = client.read_single_value("select '00:00:00'::time;", &[]).await.unwrap();
            assert_eq!(value, midnight);

            // Test time with microseconds
            let precise_time = time!(12:34:56.123456);
            let value: Time = client.read_single_value("select '12:34:56.123456'::time;", &[]).await.unwrap();
            assert_eq!(value, precise_time);

            // Test round-trip with parameter binding
            client.execute_non_query("drop table if exists test_time_table; create table test_time_table(value time);", &[]).await.unwrap();
            client.execute_non_query("insert into test_time_table values ($1);", &[&precise_time]).await.unwrap();
            let retrieved: Time = client.read_single_value("select value from test_time_table;", &[]).await.unwrap();
            assert_eq!(retrieved, precise_time);

            // Test NULL handling
            let null_value: Option<Time> = client.read_single_value("select null::time;", &[]).await.unwrap();
            assert_eq!(null_value, None);
        }

        #[test]
        async fn test_timestamp_type() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test PostgreSQL epoch timestamp (2000-01-01 00:00:00)
            let pg_epoch = datetime!(2000-01-01 00:00:00);
            let value: PrimitiveDateTime = client.read_single_value("select '2000-01-01 00:00:00'::timestamp;", &[]).await.unwrap();
            assert_eq!(value, pg_epoch);

            // Test timestamp with microseconds
            let precise_timestamp = datetime!(2024-01-15 12:34:56.123456);
            let value: PrimitiveDateTime = client.read_single_value("select '2024-01-15 12:34:56.123456'::timestamp;", &[]).await.unwrap();
            assert_eq!(value, precise_timestamp);

            // Test round-trip with parameter binding
            client.execute_non_query("drop table if exists test_timestamp_table; create table test_timestamp_table(value timestamp);", &[]).await.unwrap();
            client.execute_non_query("insert into test_timestamp_table values ($1);", &[&precise_timestamp]).await.unwrap();
            let retrieved: PrimitiveDateTime = client.read_single_value("select value from test_timestamp_table;", &[]).await.unwrap();
            assert_eq!(retrieved, precise_timestamp);

            // Test NULL handling
            let null_value: Option<PrimitiveDateTime> = client.read_single_value("select null::timestamp;", &[]).await.unwrap();
            assert_eq!(null_value, None);
        }

        #[test]
        async fn test_timestamptz_type() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test PostgreSQL epoch with UTC timezone
            let pg_epoch_utc = datetime!(2000-01-01 00:00:00).assume_utc();
            let value: OffsetDateTime = client.read_single_value("select '2000-01-01 00:00:00+00'::timestamptz;", &[]).await.unwrap();
            assert_eq!(value, pg_epoch_utc);

            // Test timestamptz with different timezone (stored as UTC internally)
            let utc_timestamp = datetime!(2024-01-15 12:34:56.123456).assume_utc();
            let value: OffsetDateTime = client.read_single_value("select '2024-01-15 12:34:56.123456+00'::timestamptz;", &[]).await.unwrap();
            assert_eq!(value, utc_timestamp);

            // Test round-trip with parameter binding
            client.execute_non_query("drop table if exists test_timestamptz_table; create table test_timestamptz_table(value timestamptz);", &[]).await.unwrap();
            client.execute_non_query("insert into test_timestamptz_table values ($1);", &[&utc_timestamp]).await.unwrap();
            let retrieved: OffsetDateTime = client.read_single_value("select value from test_timestamptz_table;", &[]).await.unwrap();
            assert_eq!(retrieved, utc_timestamp);

            // Test timezone conversion - EST to UTC
            let est_offset = offset!(-05:00);
            let est_timestamp = datetime!(2024-01-15 07:34:56.123456).assume_offset(est_offset);
            client.execute_non_query("insert into test_timestamptz_table values ($1);", &[&est_timestamp]).await.unwrap();
            let retrieved_utc: OffsetDateTime = client.read_single_value("select value from test_timestamptz_table order by value desc limit 1;", &[]).await.unwrap();
            // Should be converted to UTC (EST -5 hours = UTC +5 hours)
            let expected_utc = datetime!(2024-01-15 12:34:56.123456).assume_utc();
            assert_eq!(retrieved_utc, expected_utc);

            // Test NULL handling
            let null_value: Option<OffsetDateTime> = client.read_single_value("select null::timestamptz;", &[]).await.unwrap();
            assert_eq!(null_value, None);
        }

        #[test]
        async fn test_datetime_arrays() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test DATE array
            let dates = vec![date!(2000-01-01), date!(2024-01-15), date!(2024-12-31)];
            let value: Vec<Date> = client.read_single_value("select '{2000-01-01,2024-01-15,2024-12-31}'::date[];", &[]).await.unwrap();
            assert_eq!(value, dates);

            // Test TIME array
            let times = vec![time!(00:00:00), time!(12:34:56), time!(23:59:59.999999)];
            let value: Vec<Time> = client.read_single_value("select '{00:00:00,12:34:56,23:59:59.999999}'::time[];", &[]).await.unwrap();
            assert_eq!(value, times);

            // Test TIMESTAMP array with NULLs
            let timestamps = vec![Some(datetime!(2000-01-01 00:00:00)), None, Some(datetime!(2024-01-15 12:34:56))];
            let value: Vec<Option<PrimitiveDateTime>> = client.read_single_value("select '{\"2000-01-01 00:00:00\",null,\"2024-01-15 12:34:56\"}'::timestamp[];", &[]).await.unwrap();
            assert_eq!(value, timestamps);
        }
    }
}