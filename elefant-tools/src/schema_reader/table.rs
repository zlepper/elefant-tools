use tokio_postgres::Row;
use crate::postgres_client_wrapper::{FromPgChar, FromRow, RowEnumExt};
use crate::{ElefantToolsError, TablePartitionStrategy};
use super::{define_working_query};

#[derive(Debug, Eq, PartialEq)]
pub struct TablesResult {
    pub schema_name: String,
    pub table_name: String,
    pub comment: Option<String>,
    pub table_type: TableType,
    pub partition_expression: Option<String>,
    pub partition_strategy: Option<TablePartitionStrategy>,
    pub default_partition_name: Option<String>,
    pub partition_column_indices: Option<Vec<i16>>,
    pub partition_expression_columns: Option<String>,
    pub parent_tables: Option<Vec<String>>,
    pub is_partition: bool,
    pub storage_parameters: Option<Vec<String>>,
    pub oid: i64,
    pub depends_on: Option<Vec<i64>>,
    pub type_oid: i64,
}


#[derive(Debug, Eq, PartialEq, Copy, Clone, Default)]
pub enum TableType {
    #[default]
    Table,
    PartitionedTable,
}

impl FromPgChar for TableType {
    fn from_pg_char(c: char) -> Result<Self, ElefantToolsError> {
        match c {
            'r' => Ok(TableType::Table),
            'p' => Ok(TableType::PartitionedTable),
            _ => Err(ElefantToolsError::InvalidTableType(c.to_string())),
        }
    }
}

impl FromRow for TablesResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(TablesResult {
            schema_name: row.try_get(0)?,
            table_name: row.try_get(1)?,
            comment: row.try_get(2)?,
            table_type: row.try_get_enum_value(3)?,
            partition_expression: row.try_get(4)?,
            partition_strategy: row.try_get_opt_enum_value(5)?,
            default_partition_name: row.try_get(6)?,
            partition_column_indices: row.try_get(7)?,
            partition_expression_columns: row.try_get(8)?,
            parent_tables: row.try_get(9)?,
            is_partition: row.try_get(10)?,
            storage_parameters: row.try_get(11)?,
            oid: row.try_get(12)?,
            depends_on: row.try_get(13)?,
            type_oid: row.try_get(14)?,
        })
    }
}


//language=postgresql
define_working_query!(get_tables, TablesResult, r#"
select
    ns.nspname,
    cl.relname,
    des.description,
    cl.relkind,
    pg_get_expr(cl.relpartbound, cl.oid) as partition_expression,
    pt.partstrat,
    default_partition.relname as default_partition,
    pt.partattrs,
    pg_get_expr(pt.partexprs, pt.partrelid) as partexprs,
    (select array_agg(parent.relname) from (select parent.relname from pg_inherits i
        join pg_class parent on i.inhparent = parent.oid
          where i.inhrelid = cl.oid
          order by i.inhseqno) parent) as parent_table,
    cl.relispartition,
    cl.reloptions,
   cl.oid::int8,
   (select array_agg(refobjid::int8) from pg_depend dep where cl.oid = dep.objid and dep.deptype <> 'e' and dep.refobjid > 16384 and dep.objid <> dep.refobjid) as depends_on,
   cl.reltype::int8
from pg_class cl
         join pg_catalog.pg_namespace ns on ns.oid = cl.relnamespace
         left join pg_description des on des.objoid = cl.oid and des.objsubid = 0
         left join pg_partitioned_table pt on pt.partrelid = cl.oid
         left join pg_class default_partition on default_partition.oid = pt.partdefid
         left join pg_depend dep on dep.objid = ns.oid
where cl.relkind in ('r', 'p')
  and cl.oid > 16384
  and (dep.objid is null or dep.deptype <> 'e' )
order by ns.nspname, cl.relname;
"#);