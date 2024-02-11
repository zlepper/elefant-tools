use crate::{PostgresTriggerEvent, PostgresTriggerLevel, PostgresTriggerTiming};
use crate::postgres_client_wrapper::{FromRow, RowEnumExt};
use crate::schema_reader::define_working_query;

pub struct TriggerResult {
    pub schema_name: String,
    pub name: String,
    pub table_name: String,
    pub event: PostgresTriggerEvent,
    pub timing: PostgresTriggerTiming,
    pub level: PostgresTriggerLevel,
    pub function_name: String,
    pub condition: Option<String>,
    pub old_table_name: Option<String>,
    pub new_table_name: Option<String>,
    pub comment: Option<String>,
}

impl FromRow for TriggerResult {
    fn from_row(row: tokio_postgres::Row) -> crate::Result<Self> {
        Ok(Self {
            schema_name: row.try_get(0)?,
            name: row.try_get(1)?,
            table_name: row.try_get(2)?,
            event: row.try_get_enum_value(3)?,
            timing: row.try_get_enum_value(4)?,
            level: row.try_get_enum_value(5)?,
            function_name: row.try_get(6)?,
            condition: row.try_get(7)?,
            old_table_name: row.try_get(8)?,
            new_table_name: row.try_get(9)?,
            comment: row.try_get(10)?,
        })
    }
}

//language=postgresql
define_working_query!(get_triggers, TriggerResult, r#"
SELECT n.nspname     AS trigger_schema,
       t.tgname      AS trigger_name,
       c.relname     AS table_name,
       em.char::"char"       AS event,
       CASE t.tgtype::integer & 66
           WHEN 2 THEN 'b'
           WHEN 64 THEN 'i'
           ELSE 'a'
           END::"char"       AS trigger_timing,
       CASE t.tgtype::integer & 1
           WHEN 1 THEN 'r'
           ELSE 's'
           END::"char"       AS trigger_level,
       proc.proname  AS function_name,
       (regexp_match(pg_get_triggerdef(t.oid),
                    '.{35,} WHEN \((.+)\) EXECUTE FUNCTION'::text))[1] AS condition,
       t.tgoldtable  AS action_reference_old_table,
       t.tgnewtable  AS action_reference_new_table,
         d.description AS comment
FROM
     pg_trigger t
     join pg_class c on t.tgrelid = c.oid
     join pg_namespace n on n.oid = c.relnamespace
     join (VALUES (4, 'i'), (8, 'd'), (16, 'u'), (32, 't')) em(num, char) on (t.tgtype & em.num) <> 0
     join pg_proc proc on t.tgfoid = proc.oid
     left join pg_description d on d.objoid = t.oid
WHERE
  NOT t.tgisinternal
  and c.oid > 16384;
"#);