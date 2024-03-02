use pg_interval::Interval;
use tokio_postgres::Row;
use crate::postgres_client_wrapper::FromRow;
use crate::schema_reader::define_working_query;

pub struct HypertableResult {
    pub table_schema: String,
    pub table_name: String,
    pub compression_enabled: bool,
    pub compression_chunk_interval: Option<Interval>,
    pub compression_schedule_interval: Option<Interval>,
    pub compress_after: Option<Interval>,
    pub compress_order_by: Option<Vec<String>>,
    pub compress_order_by_desc: Option<Vec<bool>>,
    pub compress_order_by_nulls_first: Option<Vec<bool>>,
    pub compress_segment_by: Option<Vec<String>>,
    pub retention_schedule_interval: Option<Interval>,
    pub retention_drop_after: Option<Interval>,
}

impl FromRow for HypertableResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(HypertableResult {
            table_schema: row.try_get(0)?,
            table_name: row.try_get(1)?,
            compression_enabled: row.try_get(2)?,
            compression_chunk_interval: row.try_get(3)?,
            compression_schedule_interval: row.try_get(4)?,
            compress_after: row.try_get(5)?,
            compress_order_by: row.try_get(6)?,
            compress_order_by_desc: row.try_get(7)?,
            compress_order_by_nulls_first: row.try_get(8)?,
            compress_segment_by: row.try_get(9)?,
            retention_schedule_interval: row.try_get(10)?,
            retention_drop_after: row.try_get(11)?,
        })
    }
}

//language=postgresql
define_working_query!(get_hypertables, HypertableResult, r#"
select ht.schema_name,
         ht.table_name,
         ht.compression_state = 1 as compression_enabled,
        _timescaledb_functions.to_interval(dim.compress_interval_length) as compress_chunk_time_interval,
        compression_job.schedule_interval,
        (compression_job.config->>'compress_after')::interval as compress_after,
        cs.orderby,
        cs.orderby_desc,
        cs.orderby_nullsfirst,
        cs.segmentby,
        retention_job.schedule_interval as retention_schedule_interval,
        (retention_job.config->>'drop_after')::interval as retention_drop_after
from _timescaledb_catalog.hypertable ht
left join _timescaledb_catalog.dimension dim on ht.id = dim.hypertable_id and dim.compress_interval_length is not null
left join _timescaledb_config.bgw_job compression_job on compression_job.hypertable_id = ht.id and compression_job.proc_name = 'policy_compression' and compression_job.proc_schema = '_timescaledb_functions'
left join _timescaledb_catalog.compression_settings cs on cs.relid = (ht.schema_name || '.' || ht.table_name)::regclass
left join _timescaledb_config.bgw_job retention_job on retention_job.hypertable_id = ht.id and retention_job.proc_name = 'policy_retention' and retention_job.proc_schema = '_timescaledb_functions'
join pg_catalog.pg_namespace n on ht.schema_name = n.nspname
         left join pg_depend dep on dep.objid = n.oid
WHERE (n.oid > 16384 or n.nspname = 'public')
    and (dep.objid is null or dep.deptype <> 'e' )
ORDER BY ht.schema_name, ht.table_name;
"#);

/*
SELECT j.id           AS job_id,
       j.application_name,
       j.schedule_interval,
       j.max_runtime,
       j.max_retries,
       j.retry_period,
       j.proc_schema,
       j.proc_name,
       j.owner,
       j.scheduled,
       j.fixed_schedule,
       j.config,
       js.next_start,
       j.initial_start,
       ht.schema_name AS hypertable_schema,
       ht.table_name  AS hypertable_name,
       j.check_schema,
       j.check_name
FROM _timescaledb_config.bgw_job j
         LEFT JOIN _timescaledb_catalog.hypertable ht ON ht.id = j.hypertable_id
         LEFT JOIN _timescaledb_internal.bgw_job_stat js ON js.job_id = j.id
 */