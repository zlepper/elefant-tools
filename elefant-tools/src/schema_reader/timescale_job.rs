use crate::pg_interval::Interval;
use tokio_postgres::Row;
use crate::postgres_client_wrapper::FromRow;
use crate::schema_reader::define_working_query;

pub struct TimescaleJobResult {
    pub function_name: String,
    pub function_schema: String,
    pub schedule_interval: Interval,
    pub config: Option<String>,
    pub scheduled: bool,
    pub check_config_schema: Option<String>,
    pub check_config_name: Option<String>,
    pub fixed_schedule: bool,
}

impl FromRow for TimescaleJobResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(TimescaleJobResult {
            function_name: row.try_get(0)?,
            function_schema: row.try_get(1)?,
            schedule_interval: row.try_get(2)?,
            config: row.try_get(3)?,
            scheduled: row.try_get(4)?,
            check_config_schema: row.try_get(5)?,
            check_config_name: row.try_get(6)?,
            fixed_schedule: row.try_get(7)?,
        })
    }
}


//language=postgresql
define_working_query!(get_timescale_jobs, TimescaleJobResult, r#"
select job.proc_name,
       job.proc_schema,
       job.schedule_interval,
       job.config::text,
       job.scheduled,
       job.check_schema,
       job.check_name,
       job.fixed_schedule
from _timescaledb_config.bgw_job job
where job.proc_schema <> '_timescaledb_functions'
"#);