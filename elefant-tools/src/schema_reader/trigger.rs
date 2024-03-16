use crate::{PostgresTriggerEvent, PostgresTriggerLevel, PostgresTriggerTiming};
use crate::postgres_client_wrapper::{FromRow};
use crate::schema_reader::define_working_query;

pub struct TriggerResult {
    pub schema_name: String,
    pub name: String,
    pub table_name: String,
    pub events: Vec<PostgresTriggerEvent>,
    pub timing: PostgresTriggerTiming,
    pub level: PostgresTriggerLevel,
    pub function_name: String,
    pub condition: Option<String>,
    pub old_table_name: Option<String>,
    pub new_table_name: Option<String>,
    pub comment: Option<String>,
    pub arguments: Option<String>,
}

impl FromRow for TriggerResult {
    fn from_row(row: tokio_postgres::Row) -> crate::Result<Self> {
        let trigger_type: i32 = row.try_get(3)?;
        
        let trigger_level = match trigger_type & 1 {
            1 => PostgresTriggerLevel::Row,
            _ => PostgresTriggerLevel::Statement,
        };
        
        let trigger_timing = match trigger_type & 66 {
            2 => PostgresTriggerTiming::Before,
            64 => PostgresTriggerTiming::InsteadOf,
            _ => PostgresTriggerTiming::After,
        };
        
        let mut trigger_events = Vec::with_capacity(1);
        
        if trigger_type & 4 != 0 {
            trigger_events.push(PostgresTriggerEvent::Insert);
        }
        
        if trigger_type & 8 != 0 {
            trigger_events.push(PostgresTriggerEvent::Delete);
        }
        
        if trigger_type & 16 != 0 {
            trigger_events.push(PostgresTriggerEvent::Update);
        }
        
        if trigger_type & 32 != 0 {
            trigger_events.push(PostgresTriggerEvent::Truncate);
        }
        
        
        Ok(Self {
            schema_name: row.try_get(0)?,
            name: row.try_get(1)?,
            table_name: row.try_get(2)?,
            events: trigger_events,
            timing: trigger_timing,
            level: trigger_level,
            function_name: row.try_get(4)?,
            condition: row.try_get(5)?,
            old_table_name: row.try_get(6)?,
            new_table_name: row.try_get(7)?,
            comment: row.try_get(8)?,
            arguments: row.try_get(9)?,
        })
    }
}

//language=postgresql
define_working_query!(get_triggers, TriggerResult, r#"
SELECT n.nspname     AS trigger_schema,
       t.tgname      AS trigger_name,
       c.relname     AS table_name,
       t.tgtype::integer as trigger_type,
       proc.proname  AS function_name,
       (regexp_match(pg_get_triggerdef(t.oid),
                     '.{35,} WHEN \((.+)\) EXECUTE FUNCTION'::text))[1] AS condition,
       t.tgoldtable  AS action_reference_old_table,
       t.tgnewtable  AS action_reference_new_table,
       d.description AS comment,
       (regexp_match(pg_get_triggerdef(t.oid),
                     'EXECUTE FUNCTION .+?\((.+)\)'::text))[1] AS arguments
FROM
    pg_trigger t
        join pg_class c on t.tgrelid = c.oid
        join pg_namespace n on n.oid = c.relnamespace
        join pg_proc proc on t.tgfoid = proc.oid
        left join pg_description d on d.objoid = t.oid
        left join pg_depend dep on dep.objid = n.oid
WHERE
    NOT t.tgisinternal
  and c.oid > 16384
  and (dep.objid is null or dep.deptype <> 'e' )
order by trigger_schema, trigger_name;
"#);