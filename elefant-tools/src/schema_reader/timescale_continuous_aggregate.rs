use pg_interval::Interval;
use tokio_postgres::Row;
use crate::postgres_client_wrapper::FromRow;
use crate::schema_reader::define_working_query;

pub struct ContinuousAggregateResult {
    pub hypertable_schema: String,
    pub hypertable_name: String,
    pub view_schema: String,
    pub view_name: String,
    pub materialized_only: bool,
    pub view_definition: String,
    pub refresh_interval: Option<Interval>,
    pub refresh_start_offset: Option<Interval>,
    pub refresh_end_offset: Option<Interval>,
    pub compression_enabled: bool,
    pub compress_job_interval: Option<Interval>,
    pub compress_after: Option<Interval>,
    pub compress_order_by: Option<Vec<String>>,
    pub compress_order_by_desc: Option<Vec<bool>>,
    pub compress_order_by_nulls_first: Option<Vec<bool>>,
    pub compress_segment_by: Option<Vec<String>>,
    pub compress_chunk_time_interval: Option<Interval>,
    pub retention_schedule_interval: Option<Interval>,
    pub retention_drop_after: Option<Interval>,
}

impl FromRow for ContinuousAggregateResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(ContinuousAggregateResult {
            hypertable_schema: row.try_get(0)?,
            hypertable_name: row.try_get(1)?,
            view_schema: row.try_get(2)?,
            view_name: row.try_get(3)?,
            materialized_only: row.try_get(4)?,
            view_definition: row.try_get(5)?,
            refresh_interval: row.try_get(6)?,
            refresh_start_offset: row.try_get(7)?,
            refresh_end_offset: row.try_get(8)?,
            compression_enabled: row.try_get(9)?,
            compress_job_interval: row.try_get(10)?,
            compress_after: row.try_get(11)?,
            compress_order_by: row.try_get(12)?,
            compress_order_by_desc: row.try_get(13)?,
            compress_order_by_nulls_first: row.try_get(14)?,
            compress_segment_by: row.try_get(15)?,
            compress_chunk_time_interval: row.try_get(16)?,
            retention_schedule_interval: row.try_get(17)?,
            retention_drop_after: row.try_get(18)?,
        })
    }
}

//language=postgresql
define_working_query!(get_continuous_aggregates, ContinuousAggregateResult, r#"

SELECT ht.schema_name                                       AS hypertable_schema,
       ht.table_name                                        AS hypertable_name,
       cagg.user_view_schema                                AS view_schema,
       cagg.user_view_name                                  AS view_name,
       cagg.materialized_only,
       pg_get_viewdef(c.oid)                                AS view_definition,
       refresh_job.schedule_interval                        as refresh_interval,
       (refresh_job.config ->> 'start_offset')::interval    as refresh_start_offset,
       (refresh_job.config ->> 'end_offset')::interval      as refresh_end_offset,
       CASE
           WHEN mat_ht.compressed_hypertable_id IS NOT NULL THEN true
           ELSE false
           END                                              AS compression_enabled,
       compress_job.schedule_interval                       as compress_job_interval,
       (compress_job.config ->> 'compress_after')::interval as compress_after,
       cs.orderby                                           as compress_orderby,
       cs.orderby_desc                                      as compress_orderby_desc,
       cs.orderby_nullsfirst                                as compress_orderby_nullsfirst,
       cs.segmentby                                         as compress_segmentby,
        _timescaledb_functions.to_interval(dim.compress_interval_length) as compress_chunk_time_interval,
        retention_job.schedule_interval as retention_schedule_interval,
        (retention_job.config->>'drop_after')::interval as retention_drop_after
FROM _timescaledb_catalog.continuous_agg cagg
         join _timescaledb_catalog.hypertable ht on cagg.raw_hypertable_id = ht.id
         join _timescaledb_catalog.hypertable mat_ht on cagg.mat_hypertable_id = mat_ht.id
         join pg_class c on c.relname = cagg.direct_view_name and c.relkind = 'v'
         join pg_namespace n on n.oid = c.relnamespace and n.nspname = cagg.direct_view_schema
        left join _timescaledb_catalog.dimension dim on mat_ht.id = dim.hypertable_id and dim.compress_interval_length is not null
         left join _timescaledb_config.bgw_job refresh_job on refresh_job.hypertable_id = mat_ht.id and
                                                              refresh_job.proc_name =
                                                              'policy_refresh_continuous_aggregate' and
                                                              refresh_job.proc_schema = '_timescaledb_functions'
         left join _timescaledb_config.bgw_job compress_job
                   on compress_job.hypertable_id = mat_ht.id and compress_job.proc_name = 'policy_compression' and
                      compress_job.proc_schema = '_timescaledb_functions'
         left join _timescaledb_catalog.compression_settings cs
                   on cs.relid = (mat_ht.schema_name || '.' || mat_ht.table_name)::regclass
left join _timescaledb_config.bgw_job retention_job on retention_job.hypertable_id = mat_ht.id and retention_job.proc_name = 'policy_retention' and retention_job.proc_schema = '_timescaledb_functions'
"#);