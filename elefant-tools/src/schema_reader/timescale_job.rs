use elefant_client::Interval;
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
    fn from_row(row: &elefant_client::PostgresDataRow<'_, '_>) -> crate::Result<Self> {
        Ok(TimescaleJobResult {
            function_name: row.get(0)?,
            function_schema: row.get(1)?,
            schedule_interval: row.get(2)?,
            config: row.get(3)?,
            scheduled: row.get(4)?,
            check_config_schema: row.get(5)?,
            check_config_name: row.get(6)?,
            fixed_schedule: row.get(7)?,
        })
    }
}

//language=postgresql
define_working_query!(
    get_timescale_jobs,
    TimescaleJobResult,
    r#"
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
"#
);
