use crate::models::PostgresSequence;
use crate::models::*;
use crate::postgres_client_wrapper::{FromRow, PostgresClientWrapper};
use crate::Result;
use itertools::Itertools;
use std::str::FromStr;
use tokio_postgres::Row;

pub struct SchemaReader<'a> {
    connection: &'a PostgresClientWrapper,
}

impl SchemaReader<'_> {
    pub fn new(connection: &PostgresClientWrapper) -> SchemaReader {
        SchemaReader { connection }
    }

    pub async fn introspect_database(&self) -> Result<PostgresDatabase> {
        let tables = self.get_tables().await?;
        let columns = self.get_columns().await?;
        let key_columns = self.get_key_columns().await?;
        let check_constraints = self.get_check_constraints().await?;
        let indices = self.get_indices().await?;
        let index_columns = self.get_index_columns().await?;
        let sequences = self.get_sequences().await?;
        let foreign_keys = self.get_foreign_keys().await?;
        let foreign_key_columns = self.get_foreign_key_columns().await?;

        let mut db = PostgresDatabase { schemas: vec![] };

        for row in tables {
            let current_schema = db.get_or_create_schema_mut(&row.schema_name);

            let table = PostgresTable {
                name: row.table_name.clone(),
                columns: Self::add_columns(&columns, &row),
                constraints: Self::add_constraints(
                    &key_columns,
                    &check_constraints,
                    &foreign_keys,
                    &foreign_key_columns,
                    &row,
                ),
                indices: Self::add_indices(&indices, &index_columns, &row),
            };

            current_schema.tables.push(table);
        }

        for sequence in sequences {
            let current_schema = db.get_or_create_schema_mut(&sequence.schema_name);

            let sequence = PostgresSequence {
                name: sequence.sequence_name.clone(),
                data_type: sequence.data_type.clone(),
                start_value: sequence.start_value,
                increment: sequence.increment_by,
                min_value: sequence.min_value,
                max_value: sequence.max_value,
                cache_size: sequence.cache_size,
                cycle: sequence.cycle,
                last_value: sequence.last_value,
            };

            current_schema.sequences.push(sequence);
        }

        Ok(db)
    }

    fn add_columns(columns: &[TableColumnsResult], row: &TablesResult) -> Vec<PostgresColumn> {
        columns
            .iter()
            .filter(|c| c.schema_name == row.schema_name && c.table_name == row.table_name)
            .map(|column| column.to_postgres_column())
            .collect()
    }

    fn add_constraints(
        key_columns: &[KeyColumnUsageResult],
        check_constraints: &[CheckConstraintResult],
        foreign_keys: &[ForeignKeyResult],
        foreign_key_columns: &[ForeignKeyColumnResult],
        row: &TablesResult,
    ) -> Vec<PostgresConstraint> {
        let key_columns = key_columns
            .iter()
            .filter(|c| c.table_schema == row.schema_name && c.table_name == row.table_name)
            .group_by(|c| (c.constraint_name.clone(), c.key_type));
        let mut constraints: Vec<PostgresConstraint> = key_columns
            .into_iter()
            .map(|g| (g.0 .0, g.0 .1, g.1.collect_vec()))
            .map(
                |(constraint_name, constraint_type, key_columns)| match constraint_type {
                    ConstraintType::PrimaryKey => PostgresPrimaryKey {
                        name: constraint_name.clone(),
                        columns: key_columns
                            .iter()
                            .map(|c| PostgresPrimaryKeyColumn {
                                column_name: c.column_name.clone(),
                                ordinal_position: c.ordinal_position,
                            })
                            .collect(),
                    }
                    .into(),
                    ConstraintType::ForeignKey => {
                        // These are handled separately, and thus this panic should never execute
                        unreachable!("Unexpected foreign key when handling key columns");
                    }
                    ConstraintType::Check => {
                        // These are handled separately, and thus this panic should never execute
                        unreachable!("Unexpected check constraint when handling key columns");
                    }
                    ConstraintType::Unique => PostgresUniqueConstraint {
                        name: constraint_name.clone(),
                        distinct_nulls: key_columns
                            .iter()
                            .any(|c| c.nulls_distinct.is_some_and(|v| v)),
                        columns: key_columns
                            .iter()
                            .map(|c| PostgresUniqueConstraintColumn {
                                column_name: c.column_name.clone(),
                                ordinal_position: c.ordinal_position,
                            })
                            .collect(),
                    }
                    .into(),
                },
            )
            .collect();

        let mut check_constraints = check_constraints
            .iter()
            .filter(|c| c.table_schema == row.schema_name && c.table_name == row.table_name)
            .map(|check_constraint| {
                PostgresCheckConstraint {
                    name: check_constraint.constraint_name.clone(),
                    check_clause: check_constraint.check_clause.clone(),
                }
                .into()
            })
            .collect();

        constraints.append(&mut check_constraints);

        let mut foreign_key_constraints = foreign_keys
            .iter()
            .filter(|fk| {
                fk.source_table_schema_name == row.schema_name
                    && fk.source_table_name == row.table_name
            })
            .map(|fk| {
                PostgresForeignKey {
                    name: fk.constraint_name.clone(),
                    referenced_table: fk.target_table_name.clone(),
                    referenced_schema: if fk.source_table_schema_name == fk.target_table_schema_name
                    {
                        None
                    } else {
                        Some(fk.target_table_schema_name.clone())
                    },
                    delete_action: fk.delete_action,
                    update_action: fk.update_action,
                    columns: foreign_key_columns
                        .iter()
                        .filter(|c| {
                            c.source_table_name == row.table_name
                                && c.source_schema_name == row.schema_name
                                && c.constraint_name == fk.constraint_name
                        })
                        .enumerate()
                        .map(|(index, c)| PostgresForeignKeyColumn {
                            name: c.source_table_column_name.clone(),
                            ordinal_position: index as i32 + 1,
                            affected_by_delete_action: c.affected_by_delete_action,
                        })
                        .collect(),
                    referenced_columns: foreign_key_columns
                        .iter()
                        .filter(|c| {
                            c.source_table_name == row.table_name
                                && c.source_schema_name == row.schema_name
                                && c.constraint_name == fk.constraint_name
                        })
                        .enumerate()
                        .map(|(index, c)| PostgresForeignKeyReferencedColumn {
                            name: c.target_table_column_name.clone(),
                            ordinal_position: index as i32 + 1,
                        })
                        .collect(),
                }
                .into()
            })
            .collect();

        constraints.append(&mut foreign_key_constraints);

        constraints.sort();

        constraints
    }

    fn add_indices(
        indices: &[IndexResult],
        index_columns: &[IndexColumnResult],
        row: &TablesResult,
    ) -> Vec<PostgresIndex> {
        let mut result = vec![];

        let indices = indices
            .iter()
            .filter(|c| c.table_schema == row.schema_name && c.table_name == row.table_name);
        for index in indices {
            let index_columns = index_columns
                .iter()
                .filter(|c| {
                    c.table_schema == row.schema_name
                        && c.table_name == row.table_name
                        && c.index_name == index.index_name
                })
                .collect_vec();
            let mut key_columns = index_columns
                .iter()
                .filter(|c| c.is_key)
                .map(|c| PostgresIndexKeyColumn {
                    name: c.column_expression.clone(),
                    ordinal_position: c.ordinal_position,
                    direction: if index.can_sort {
                        Some(match c.is_desc {
                            Some(true) => PostgresIndexColumnDirection::Descending,
                            _ => PostgresIndexColumnDirection::Ascending,
                        })
                    } else {
                        None
                    },
                    nulls_order: if index.can_sort {
                        Some(match c.nulls_first {
                            Some(true) => PostgresIndexNullsOrder::First,
                            _ => PostgresIndexNullsOrder::Last,
                        })
                    } else {
                        None
                    },
                })
                .collect_vec();

            key_columns.sort();

            let mut included_columns = index_columns
                .iter()
                .filter(|c| !c.is_key)
                .map(|c| PostgresIndexIncludedColumn {
                    name: c.column_expression.clone(),
                    ordinal_position: c.ordinal_position,
                })
                .collect_vec();

            included_columns.sort();

            result.push(PostgresIndex {
                name: index.index_name.clone(),
                key_columns,
                index_type: index.index_type.clone(),
                predicate: index.index_predicate.clone(),
                included_columns,
            });
        }

        result.sort();

        result
    }

    async fn get_index_columns(&self) -> Result<Vec<IndexColumnResult>> {
        //language=postgresql
        self.connection
            .get_results(
                r#"
                select n.nspname                                              as table_schema,
                      table_class.relname                                    as table_name,
                      index_class.relname                                    as index_name,
                      a.attnum <= i.indnkeyatts                              as is_key,
                      pg_catalog.pg_get_indexdef(a.attrelid, a.attnum, true) as indexdef,
                      i.indoption[a.attnum - 1] & 1 <> 0                     as is_desc,
                      i.indoption[a.attnum - 1] & 2 <> 0                     as nulls_first,
                      a.attnum::int                                               as ordinal_position
               from pg_index i
                        join pg_class table_class on table_class.oid = i.indrelid
                        join pg_class index_class on index_class.oid = i.indexrelid
                        left join pg_namespace n on n.oid = table_class.relnamespace
                        left join pg_tablespace ts on ts.oid = index_class.reltablespace
                        join pg_catalog.pg_attribute a on a.attrelid = index_class.oid
               where a.attnum > 0
                 and not a.attisdropped
                 and n.nspname not in ('pg_catalog', 'pg_toast', 'information_schema')
                 and not i.indisprimary and not i.indisunique
               order by table_schema, table_name, index_name, ordinal_position
            "#,
            )
            .await
    }

    async fn get_indices(&self) -> Result<Vec<IndexResult>> {
        //language=postgresql
        self.connection
            .get_results(
                r#"
            select n.nspname           as table_schema,
                   table_class.relname as table_name,
                   index_class.relname as index_name,
                   pa.amname           as index_type,
                   pg_indexam_has_property(pa.oid, 'can_order') as can_sort,
                   pg_catalog.pg_get_expr(i.indpred, i.indrelid, true) as index_predicate
            from pg_index i
                     join pg_class table_class on table_class.oid = i.indrelid
                     join pg_class index_class on index_class.oid = i.indexrelid
                     left join pg_namespace n on n.oid = table_class.relnamespace
                     left join pg_tablespace ts on ts.oid = index_class.reltablespace
                     join pg_catalog.pg_am pa on index_class.relam = pa.oid
            where n.nspname not in ('pg_catalog', 'pg_toast', 'information_schema')
              and not i.indisprimary
              and not i.indisunique
        "#,
            )
            .await
    }

    async fn get_check_constraints(&self) -> Result<Vec<CheckConstraintResult>> {
        //language=postgresql
        self.connection.get_results(
            r#"
            select distinct t.table_schema, t.table_name, cc.constraint_name, cc.check_clause from information_schema.check_constraints cc
            join information_schema.table_constraints tc on cc.constraint_schema = tc.constraint_schema and cc.constraint_name = tc.constraint_name
            join information_schema.tables t on tc.table_schema = t.table_schema and tc.table_name = t.table_name
            join information_schema.constraint_column_usage ccu on cc.constraint_schema = ccu.constraint_schema and cc.constraint_name = ccu.constraint_name
            where t.table_schema not in ('pg_catalog', 'pg_toast', 'information_schema')
            order by t.table_schema, t.table_name, cc.constraint_name;
            "#
        ).await
    }

    async fn get_key_columns(&self) -> Result<Vec<KeyColumnUsageResult>> {
        //language=postgresql
        self.connection.get_results(
            r#"
            select kcu.table_schema, kcu.table_name, kcu.constraint_name, kcu.column_name, kcu.ordinal_position, kcu.position_in_unique_constraint, tc.constraint_type, tc.nulls_distinct from information_schema.key_column_usage kcu
            join information_schema.table_constraints tc on kcu.table_schema = tc.table_schema and kcu.table_name = tc.table_name and kcu.constraint_name = tc.constraint_name
            where tc.constraint_type = 'PRIMARY KEY' or tc.constraint_type = 'UNIQUE'
            order by kcu.table_schema, kcu.table_name, kcu.constraint_name, kcu.ordinal_position;
            "#
        ).await
    }

    async fn get_columns(&self) -> Result<Vec<TableColumnsResult>> {
        //language=postgresql
        self.connection.get_results(
            r#"
            select c.table_schema, c.table_name, c.column_name, c.ordinal_position, c.is_nullable, c.data_type, c.column_default, c.generation_expression from information_schema.tables t
            join information_schema.columns c on t.table_schema = c.table_schema and t.table_name = c.table_name
            where t.table_schema not in ('pg_catalog', 'pg_toast', 'information_schema') and t.table_type = 'BASE TABLE'
            order by c.table_schema, c.table_name, c.ordinal_position;
            "#
        ).await
    }

    async fn get_tables(&self) -> Result<Vec<TablesResult>> {
        //language=postgresql
        self.connection.get_results(
            r#"
            select table_schema, table_name from information_schema.tables
            where table_schema not in ('pg_catalog', 'pg_toast', 'information_schema') and table_type = 'BASE TABLE'
            order by table_schema, table_name;
            "#
        ).await
    }

    async fn get_sequences(&self) -> Result<Vec<SequenceResult>> {
        //language=postgresql
        self.connection
            .get_results(
                r#"
            select s.schemaname,
                   s.sequencename,
                   s.data_type::text,
                   s.start_value,
                   s.min_value,
                   s.max_value,
                   s.increment_by,
                   s.cycle,
                   s.cache_size,
                   s.last_value
            from pg_sequences s
            where s.schemaname not in ('pg_catalog', 'pg_toast', 'information_schema')
            order by s.schemaname, s.sequencename;
            "#,
            )
            .await
    }

    async fn get_foreign_keys(&self) -> Result<Vec<ForeignKeyResult>> {
        //language=postgresql
        self.connection
            .get_results(
                r#"
            select con.conname              as constraint_name,
                   con_ns.nspname           as constraint_schema_name,
                   tab.relname              as source_table_name,
                   tab_ns.nspname           as source_schema_name,
                   target.relname           as target_table_name,
                   target_ns.nspname        as target_schema_name,
                   con.confupdtype::text    as update_action,
                   con.confdeltype::text    as delete_action
            from pg_catalog.pg_constraint con
                     left join pg_catalog.pg_namespace con_ns on con_ns.oid = con.connamespace
                     join pg_catalog.pg_class tab on con.conrelid = tab.oid
                     left join pg_namespace tab_ns on tab_ns.oid = tab.relnamespace
                     join pg_catalog.pg_class target on con.confrelid = target.oid
                     left join pg_namespace target_ns on target_ns.oid = target.relnamespace
            where con.contype = 'f';
            "#,
            )
            .await
    }

    async fn get_foreign_key_columns(&self) -> Result<Vec<ForeignKeyColumnResult>> {
        //language=postgresql
        self.connection
            .get_results(
                r#"
            select con.conname       as constraint_name,
                   con_ns.nspname    as constraint_schema_name,
                   tab.relname       as source_table_name,
                   tab_ns.nspname    as source_schema_name,
                   source_table_attr.attname as source_table_column_name,
                   target_table_attr.attname as target_table_column_name,
                   (con.confdelsetcols is null or source_table_attr.attnum=any(con.confdelsetcols)) as affected_by_delete_action
            from pg_constraint con
                     left join pg_catalog.pg_namespace con_ns on con_ns.oid = con.connamespace
                     join pg_catalog.pg_class tab on con.conrelid = tab.oid
                     left join pg_namespace tab_ns on tab_ns.oid = tab.relnamespace
                     join unnest(con.conkey, con.confkey) as cols (conkey, confkey) on true
                     left join pg_attribute source_table_attr
                               on source_table_attr.attrelid = con.conrelid and source_table_attr.attnum = cols.conkey
                     left join pg_attribute target_table_attr
                               on target_table_attr.attrelid = con.confrelid and target_table_attr.attnum = cols.confkey
            where con.contype = 'f'
            "#,
            )
            .await
    }
}

#[derive(Debug, Eq, PartialEq)]
struct TablesResult {
    schema_name: String,
    table_name: String,
}

impl FromRow for TablesResult {
    fn from_row(row: Row) -> Result<Self> {
        Ok(TablesResult {
            schema_name: row.try_get(0)?,
            table_name: row.try_get(1)?,
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
struct TableColumnsResult {
    schema_name: String,
    table_name: String,
    column_name: String,
    ordinal_position: i32,
    is_nullable: bool,
    data_type: String,
    column_default: Option<String>,
    generated: Option<String>,
}

impl FromRow for TableColumnsResult {
    fn from_row(row: Row) -> Result<Self> {
        Ok(TableColumnsResult {
            schema_name: row.try_get(0)?,
            table_name: row.try_get(1)?,
            column_name: row.try_get(2)?,
            ordinal_position: row.try_get(3)?,
            is_nullable: row.try_get::<usize, String>(4)? != "NO",
            data_type: row.try_get(5)?,
            column_default: row.try_get(6)?,
            generated: row.try_get(7)?,
        })
    }
}

impl TableColumnsResult {
    fn to_postgres_column(&self) -> PostgresColumn {
        PostgresColumn {
            name: self.column_name.clone(),
            is_nullable: self.is_nullable,
            ordinal_position: self.ordinal_position,
            data_type: self.data_type.clone(),
            default_value: self.column_default.clone(),
            generated: self.generated.clone(),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct KeyColumnUsageResult {
    pub table_schema: String,
    pub table_name: String,
    pub constraint_name: String,
    pub column_name: String,
    pub ordinal_position: i32,
    pub position_in_unique_constraint: Option<i32>,
    pub key_type: ConstraintType,
    pub nulls_distinct: Option<bool>,
}

impl FromRow for KeyColumnUsageResult {
    fn from_row(row: Row) -> Result<Self> {
        Ok(KeyColumnUsageResult {
            table_schema: row.try_get(0)?,
            table_name: row.try_get(1)?,
            constraint_name: row.try_get(2)?,
            column_name: row.try_get(3)?,
            ordinal_position: row.try_get(4)?,
            position_in_unique_constraint: row.try_get(5)?,
            key_type: ConstraintType::from_str(row.try_get(6)?)?,
            nulls_distinct: match row.try_get::<usize, Option<&str>>(7)? {
                Some("YES") => Some(true),
                Some("NO") => Some(false),
                _ => None,
            },
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
struct CheckConstraintResult {
    pub table_schema: String,
    pub table_name: String,
    pub constraint_name: String,
    pub check_clause: String,
}

impl FromRow for CheckConstraintResult {
    fn from_row(row: Row) -> Result<Self> {
        Ok(CheckConstraintResult {
            table_schema: row.try_get(0)?,
            table_name: row.try_get(1)?,
            constraint_name: row.try_get(2)?,
            check_clause: row.try_get(3)?,
        })
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
enum ConstraintType {
    PrimaryKey,
    ForeignKey,
    Check,
    Unique,
}

impl FromStr for ConstraintType {
    type Err = crate::ElefantToolsError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "PRIMARY KEY" => Ok(ConstraintType::PrimaryKey),
            "FOREIGN KEY" => Ok(ConstraintType::ForeignKey),
            "CHECK" => Ok(ConstraintType::Check),
            "UNIQUE" => Ok(ConstraintType::Unique),
            _ => Err(crate::ElefantToolsError::UnknownConstraintType(
                s.to_string(),
            )),
        }
    }
}

struct IndexColumnResult {
    table_schema: String,
    table_name: String,
    index_name: String,
    is_key: bool,
    column_expression: String,
    is_desc: Option<bool>,
    nulls_first: Option<bool>,
    ordinal_position: i32,
}

impl FromRow for IndexColumnResult {
    fn from_row(row: Row) -> Result<Self> {
        Ok(IndexColumnResult {
            table_schema: row.try_get(0)?,
            table_name: row.try_get(1)?,
            index_name: row.try_get(2)?,
            is_key: row.try_get(3)?,
            column_expression: row.try_get(4)?,
            is_desc: row.try_get(5)?,
            nulls_first: row.try_get(6)?,
            ordinal_position: row.try_get(7)?,
        })
    }
}

struct IndexResult {
    table_schema: String,
    table_name: String,
    index_name: String,
    index_type: String,
    can_sort: bool,
    index_predicate: Option<String>,
}

impl FromRow for IndexResult {
    fn from_row(row: Row) -> Result<Self> {
        Ok(IndexResult {
            table_schema: row.try_get(0)?,
            table_name: row.try_get(1)?,
            index_name: row.try_get(2)?,
            index_type: row.try_get(3)?,
            can_sort: row.try_get(4)?,
            index_predicate: row.try_get(5)?,
        })
    }
}

struct SequenceResult {
    schema_name: String,
    sequence_name: String,
    data_type: String,
    start_value: i64,
    min_value: i64,
    max_value: i64,
    increment_by: i64,
    cycle: bool,
    cache_size: i64,
    last_value: Option<i64>,
}

impl FromRow for SequenceResult {
    fn from_row(row: Row) -> Result<Self> {
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
        })
    }
}

struct ForeignKeyResult {
    constraint_name: String,
    constraint_schema_name: String,
    source_table_name: String,
    source_table_schema_name: String,
    target_table_name: String,
    target_table_schema_name: String,
    update_action: ReferenceAction,
    delete_action: ReferenceAction,
}

impl FromRow for ForeignKeyResult {
    fn from_row(row: Row) -> Result<Self> {
        Ok(Self {
            constraint_name: row.try_get(0)?,
            constraint_schema_name: row.try_get(1)?,
            source_table_name: row.try_get(2)?,
            source_table_schema_name: row.try_get(3)?,
            target_table_name: row.try_get(4)?,
            target_table_schema_name: row.try_get(5)?,
            update_action: ReferenceAction::from_str(row.try_get(6)?)?,
            delete_action: ReferenceAction::from_str(row.try_get(7)?)?,
        })
    }
}

struct ForeignKeyColumnResult {
    constraint_name: String,
    constraint_schema_name: String,
    source_table_name: String,
    source_schema_name: String,
    source_table_column_name: String,
    target_table_column_name: String,
    affected_by_delete_action: bool,
}

impl FromRow for ForeignKeyColumnResult {
    fn from_row(row: Row) -> Result<Self> {
        Ok(Self {
            constraint_name: row.try_get(0)?,
            constraint_schema_name: row.try_get(1)?,
            source_table_name: row.try_get(2)?,
            source_schema_name: row.try_get(3)?,
            source_table_column_name: row.try_get(4)?,
            target_table_column_name: row.try_get(5)?,
            affected_by_delete_action: row.try_get(6)?,
        })
    }
}

#[cfg(test)]
pub mod tests;
