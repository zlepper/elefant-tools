use tokio_postgres::Row;
use crate::postgres_client_wrapper::FromRow;
use crate::schema_reader::define_working_query;

pub struct SequenceResult {
    pub schema_name: String,
    pub sequence_name: String,
    pub data_type: String,
    pub start_value: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub increment_by: i64,
    pub cycle: bool,
    pub cache_size: i64,
    pub last_value: Option<i64>,
    pub comment: Option<String>,
}

impl FromRow for SequenceResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(Self {
            schema_name: row.try_get(0)?,
            sequence_name: row.try_get(1)?,
            data_type: row.try_get(2)?,
            start_value: row.try_get(3)?,
            min_value: row.try_get(4)?,
            max_value: row.try_get(5)?,
            increment_by: row.try_get(6)?,
            cycle: row.try_get(7)?,
            cache_size: row.try_get(8)?,
            last_value: row.try_get(9)?,
            comment: row.try_get(10)?,
        })
    }
}

//language=postgresql
define_working_query!(get_sequences, SequenceResult, r#"
SELECT n.nspname                   AS schemaname,
       c.relname                   AS sequencename,
       t.typname                   AS data_type,
       s.seqstart                  AS start_value,
       s.seqmin                    AS min_value,
       s.seqmax                    AS max_value,
       s.seqincrement              AS increment_by,
       s.seqcycle                  AS cycle,
       s.seqcache                  AS cache_size,
       CASE
           WHEN has_sequence_privilege(c.oid, 'SELECT,USAGE'::text) THEN pg_sequence_last_value(c.oid::regclass)
           ELSE NULL::bigint
           END                     AS last_value,
         d.description               AS comment
FROM pg_sequence s
         JOIN pg_class c ON c.oid = s.seqrelid
         join pg_type t on t.oid = s.seqtypid
         LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
         left join pg_description d on d.objoid = c.oid
         left join pg_depend dep on dep.objid = n.oid
WHERE NOT pg_is_other_temp_schema(n.oid)
  AND c.relkind = 'S'::"char"
  and c.oid > 16384
  and (dep.objid is null or dep.deptype <> 'e' )
    and has_sequence_privilege(s.seqrelid, 'SELECT,USAGE,UPDATE')
order by schemaname, sequencename
"#);