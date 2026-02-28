use crate::postgres_client_wrapper::{FromRow, QueryResult};
use crate::schema_reader::SchemaReader;

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
    pub is_internally_created: bool,
    pub author_table: Option<String>,
    pub author_table_column_position: Option<i32>,
}

impl FromRow for SequenceResult {
    fn from_row(row: &elefant_client::PostgresDataRow<'_, '_>) -> crate::Result<Self> {
        Ok(Self {
            schema_name: row.get(0)?,
            sequence_name: row.get(1)?,
            data_type: row.get(2)?,
            start_value: row.get(3)?,
            min_value: row.get(4)?,
            max_value: row.get(5)?,
            increment_by: row.get(6)?,
            cycle: row.get(7)?,
            cache_size: row.get(8)?,
            last_value: row.get(9)?,
            comment: row.get(10)?,
            is_internally_created: row.get::<Option<char>>(11)? == Some('i'),
            author_table: row.get(12)?,
            author_table_column_position: row.get(13)?,
        })
    }
}

//language=postgresql
pub(in crate::schema_reader) const QUERY: &str = r#"
SELECT n.nspname      AS schemaname,
       c.relname      AS sequencename,
       t.typname      AS data_type,
       s.seqstart     AS start_value,
       s.seqmin       AS min_value,
       s.seqmax       AS max_value,
       s.seqincrement AS increment_by,
       s.seqcycle     AS cycle,
       s.seqcache     AS cache_size,
       CASE
           WHEN has_sequence_privilege(c.oid, 'SELECT,USAGE'::text) THEN pg_sequence_last_value(c.oid::regclass)
           ELSE NULL::bigint
           END        AS last_value,
       d.description  AS comment,
       col_dep.deptype,
       col_table_dep.relname as author_table,
       col_dep.refobjsubid as author_table_column_position
FROM pg_sequence s
         JOIN pg_class c ON c.oid = s.seqrelid
         join pg_type t on t.oid = s.seqtypid
         LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
         left join pg_description d on d.objoid = c.oid
         left join pg_depend dep on dep.objid = n.oid
         left join pg_depend col_dep on col_dep.objid = s.seqrelid and col_dep.deptype = 'i'
         left join pg_class col_table_dep on col_dep.refobjid = col_table_dep.oid
WHERE NOT pg_is_other_temp_schema(n.oid)
  AND c.relkind = 'S'::"char"
  and c.oid > 16384
  and (dep.objid is null or dep.deptype <> 'e')
  and has_sequence_privilege(s.seqrelid, 'SELECT,USAGE,UPDATE')
order by schemaname, sequencename;
"#;

impl QueryResult for SequenceResult {
    fn query(_version: i32) -> &'static str {
        QUERY
    }
}

impl SchemaReader<'_> {
    #[tracing::instrument(skip_all)]
    pub(in crate::schema_reader) async fn get_sequences(&self) -> crate::Result<Vec<SequenceResult>> {
        self.connection.get_results(QUERY).await
    }
}
