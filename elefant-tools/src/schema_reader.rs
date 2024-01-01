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

        let mut db = PostgresDatabase { schemas: vec![] };

        for row in tables {
            let current_schema = match db.schemas.last_mut() {
                Some(last) if last.name == row.schema_name => last,
                _ => {
                    db.schemas.push(PostgresSchema {
                        name: row.schema_name.clone(),
                        tables: vec![],
                    });

                    db.schemas.last_mut().unwrap()
                }
            };

            let table = PostgresTable {
                name: row.table_name.clone(),
                columns: Self::add_columns(&columns, &row),
                constraints: Self::add_constraints(&key_columns, &check_constraints, &row),
                indices: Self::add_indices(&indices, &index_columns, &row),
            };

            current_schema.tables.push(table);
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
        row: &TablesResult,
    ) -> Vec<PostgresConstraint> {
        let key_columns = key_columns
            .iter()
            .filter(|c| c.table_schema == row.schema_name && c.table_name == row.table_name)
            .group_by(|c| (c.constraint_name.clone(), c.key_type));
        let mut constraints: Vec<PostgresConstraint> = key_columns
            .into_iter()
            .map(|g| (g.0.0, g.0.1, g.1.collect_vec()))
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
                        todo!()
                    }
                    ConstraintType::Check => {
                        // These are handled separately, and thus this panic should never execute
                        unreachable!("Unexpected check constraint when handling key columns");
                    }
                    ConstraintType::Unique => PostgresUniqueConstraint {
                        name: constraint_name.clone(),
                        distinct_nulls: key_columns.iter().any(|c| c.nulls_distinct.is_some_and(|v| v)),
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

        constraints.sort();

        constraints
    }

    fn add_indices(indices: &[IndexResult], index_columns: &[IndexColumnResult], row: &TablesResult) -> Vec<PostgresIndex> {
        let mut result = vec![];

        let indices = indices.iter().filter(|c| c.table_schema == row.schema_name && c.table_name == row.table_name);
        for index in indices {
            let index_columns = index_columns
                .iter()
                .filter(|c| c.table_schema == row.schema_name && c.table_name == row.table_name && c.index_name == index.index_name)
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
        self.connection.get_results(r#"
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
        "#).await
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
            where tc.constraint_type = 'PRIMARY KEY' or tc.constraint_type = 'FOREIGN KEY' or tc.constraint_type = 'UNIQUE'
            order by kcu.table_schema, kcu.table_name, kcu.constraint_name, kcu.ordinal_position;
            "#
        ).await
    }

    async fn get_columns(&self) -> Result<Vec<TableColumnsResult>> {
        //language=postgresql
        self.connection.get_results(
            r#"
            select c.table_schema, c.table_name, c.column_name, c.ordinal_position, c.is_nullable, c.data_type from information_schema.tables t
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


#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::test_helpers::{get_test_helper, TestHelper};
    use tokio::test;

    pub async fn introspect_schema(test_helper: &TestHelper) -> PostgresDatabase {
        let conn = test_helper.get_conn();
        let reader = SchemaReader::new(conn);
        reader.introspect_database().await.unwrap()
    }

    #[test]
    async fn reads_simple_schema() {
        let helper = get_test_helper().await;
        helper
            .execute_not_query(
                r#"
        create table my_table(
            id serial primary key,
            name text not null unique,
            age int not null check (age > 21),
            constraint my_multi_check check (age > 21 and age < 65 and name is not null)
        );

        create index lower_case_name_idx on my_table (lower(name));
        "#,
            )
            .await;

        let db = introspect_schema(&helper).await;

        assert_eq!(
            db,
            PostgresDatabase {
                schemas: vec![PostgresSchema {
                    name: "public".to_string(),
                    tables: vec![PostgresTable {
                        name: "my_table".to_string(),
                        columns: vec![
                            PostgresColumn {
                                name: "id".to_string(),
                                ordinal_position: 1,
                                is_nullable: false,
                                data_type: "integer".to_string(),
                            },
                            PostgresColumn {
                                name: "name".to_string(),
                                ordinal_position: 2,
                                is_nullable: false,
                                data_type: "text".to_string(),
                            },
                            PostgresColumn {
                                name: "age".to_string(),
                                ordinal_position: 3,
                                is_nullable: false,
                                data_type: "integer".to_string(),
                            },
                        ],
                        constraints: vec![
                            PostgresConstraint::PrimaryKey(PostgresPrimaryKey {
                                name: "my_table_pkey".to_string(),
                                columns: vec![PostgresPrimaryKeyColumn {
                                    column_name: "id".to_string(),
                                    ordinal_position: 1,
                                }],
                            }),
                            PostgresConstraint::Unique(PostgresUniqueConstraint {
                                name: "my_table_name_key".to_string(),
                                columns: vec![PostgresUniqueConstraintColumn {
                                    column_name: "name".to_string(),
                                    ordinal_position: 1,
                                }],
                                distinct_nulls: true,
                            }),
                            PostgresConstraint::Check(PostgresCheckConstraint {
                                name: "my_multi_check".to_string(),
                                check_clause:
                                "(((age > 21) AND (age < 65) AND (name IS NOT NULL)))"
                                    .to_string(),
                            }),
                            PostgresConstraint::Check(PostgresCheckConstraint {
                                name: "my_table_age_check".to_string(),
                                check_clause: "((age > 21))".to_string(),
                            }),
                        ],
                        indices: vec![
                            PostgresIndex {
                                name: "lower_case_name_idx".to_string(),
                                key_columns: vec![PostgresIndexKeyColumn {
                                    name: "lower(name)".to_string(),
                                    ordinal_position: 1,
                                    direction: Some(PostgresIndexColumnDirection::Ascending),
                                    nulls_order: Some(PostgresIndexNullsOrder::Last),
                                }],
                                index_type: "btree".to_string(),
                                predicate: None,
                                included_columns: vec![],
                            }
                        ],
                    }],
                }]
            }
        )
    }

    #[test]
    async fn table_without_columns() {
        let helper = get_test_helper().await;
        helper
            .execute_not_query(
                r#"
        create table my_table();
        "#,
            )
            .await;

        let db = introspect_schema(&helper).await;

        assert_eq!(
            db,
            PostgresDatabase {
                schemas: vec![PostgresSchema {
                    tables: vec![PostgresTable {
                        name: "my_table".to_string(),
                        columns: vec![],
                        constraints: vec![],
                        indices: vec![],
                    }],
                    name: "public".to_string(),
                }]
            }
        )
    }

    #[test]
    async fn table_without_primary_key() {
        let helper = get_test_helper().await;
        helper
            .execute_not_query(
                r#"
        create table my_table(
            name text not null,
            age int not null
        );
        "#,
            )
            .await;

        let db = introspect_schema(&helper).await;

        assert_eq!(
            db,
            PostgresDatabase {
                schemas: vec![PostgresSchema {
                    name: "public".to_string(),
                    tables: vec![PostgresTable {
                        name: "my_table".to_string(),
                        columns: vec![
                            PostgresColumn {
                                name: "name".to_string(),
                                ordinal_position: 1,
                                is_nullable: false,
                                data_type: "text".to_string(),
                            },
                            PostgresColumn {
                                name: "age".to_string(),
                                ordinal_position: 2,
                                is_nullable: false,
                                data_type: "integer".to_string(),
                            },
                        ],
                        constraints: vec![],
                        indices: vec![],
                    }],
                }]
            }
        )
    }

    #[test]
    async fn composite_primary_keys() {
        let helper = get_test_helper().await;
        helper
            .execute_not_query(
                r#"
        create table my_table(
            id_part_1 int not null,
            id_part_2 int not null,
            name text,
            age int,
            constraint my_table_pk primary key (id_part_1, id_part_2)
        );
        "#,
            )
            .await;

        let db = introspect_schema(&helper).await;

        assert_eq!(
            db,
            PostgresDatabase {
                schemas: vec![PostgresSchema {
                    name: "public".to_string(),
                    tables: vec![PostgresTable {
                        name: "my_table".to_string(),
                        columns: vec![
                            PostgresColumn {
                                name: "id_part_1".to_string(),
                                ordinal_position: 1,
                                is_nullable: false,
                                data_type: "integer".to_string(),
                            },
                            PostgresColumn {
                                name: "id_part_2".to_string(),
                                ordinal_position: 2,
                                is_nullable: false,
                                data_type: "integer".to_string(),
                            },
                            PostgresColumn {
                                name: "name".to_string(),
                                ordinal_position: 3,
                                is_nullable: true,
                                data_type: "text".to_string(),
                            },
                            PostgresColumn {
                                name: "age".to_string(),
                                ordinal_position: 4,
                                is_nullable: true,
                                data_type: "integer".to_string(),
                            },
                        ],
                        constraints: vec![PostgresConstraint::PrimaryKey(PostgresPrimaryKey {
                            name: "my_table_pk".to_string(),
                            columns: vec![
                                PostgresPrimaryKeyColumn {
                                    column_name: "id_part_1".to_string(),
                                    ordinal_position: 1,
                                },
                                PostgresPrimaryKeyColumn {
                                    column_name: "id_part_2".to_string(),
                                    ordinal_position: 2,
                                },
                            ],
                        }), ],
                        indices: vec![],
                    }],
                }]
            }
        )
    }

    #[test]
    async fn indices() {
        let helper = get_test_helper().await;
        helper
            .execute_not_query(
                r#"
        create table my_table(
            value int
        );

        create index my_table_value_asc_nulls_first on my_table(value asc nulls first);
        create index my_table_value_asc_nulls_last on my_table(value asc nulls last);
        create index my_table_value_desc_nulls_first on my_table(value desc nulls first);
        create index my_table_value_desc_nulls_last on my_table(value desc nulls last);

        "#,
            )
            .await;

        let db = introspect_schema(&helper).await;

        assert_eq!(
            db,
            PostgresDatabase {
                schemas: vec![PostgresSchema {
                    name: "public".to_string(),
                    tables: vec![PostgresTable {
                        name: "my_table".to_string(),
                        columns: vec![PostgresColumn {
                            name: "value".to_string(),
                            ordinal_position: 1,
                            is_nullable: true,
                            data_type: "integer".to_string(),
                        }, ],
                        constraints: vec![],
                        indices: vec![
                            PostgresIndex {
                                name: "my_table_value_asc_nulls_first".to_string(),
                                key_columns: vec![PostgresIndexKeyColumn {
                                    name: "value".to_string(),
                                    ordinal_position: 1,
                                    direction: Some(PostgresIndexColumnDirection::Ascending),
                                    nulls_order: Some(PostgresIndexNullsOrder::First),
                                }],
                                index_type: "btree".to_string(),
                                predicate: None,
                                included_columns: vec![],
                            },
                            PostgresIndex {
                                name: "my_table_value_asc_nulls_last".to_string(),
                                key_columns: vec![PostgresIndexKeyColumn {
                                    name: "value".to_string(),
                                    ordinal_position: 1,
                                    direction: Some(PostgresIndexColumnDirection::Ascending),
                                    nulls_order: Some(PostgresIndexNullsOrder::Last),
                                }],
                                index_type: "btree".to_string(),
                                predicate: None,
                                included_columns: vec![],
                            },
                            PostgresIndex {
                                name: "my_table_value_desc_nulls_first".to_string(),
                                key_columns: vec![PostgresIndexKeyColumn {
                                    name: "value".to_string(),
                                    ordinal_position: 1,
                                    direction: Some(PostgresIndexColumnDirection::Descending),
                                    nulls_order: Some(PostgresIndexNullsOrder::First),
                                }],
                                index_type: "btree".to_string(),
                                predicate: None,
                                included_columns: vec![],
                            },
                            PostgresIndex {
                                name: "my_table_value_desc_nulls_last".to_string(),
                                key_columns: vec![PostgresIndexKeyColumn {
                                    name: "value".to_string(),
                                    ordinal_position: 1,
                                    direction: Some(PostgresIndexColumnDirection::Descending),
                                    nulls_order: Some(PostgresIndexNullsOrder::Last),
                                }],
                                index_type: "btree".to_string(),
                                predicate: None,
                                included_columns: vec![],
                            },
                        ],
                    }],
                }]
            }
        )
    }

    #[test]
    async fn index_types() {
        let helper = get_test_helper().await;
        helper
            .execute_not_query(
                r#"
        create table my_table(
            free_text tsvector
        );

        create index my_table_gist on my_table using gist (free_text);
        create index my_table_gin on my_table using gin (free_text);
        "#,
            )
            .await;

        let db = introspect_schema(&helper).await;

        assert_eq!(
            db,
            PostgresDatabase {
                schemas: vec![PostgresSchema {
                    name: "public".to_string(),
                    tables: vec![PostgresTable {
                        name: "my_table".to_string(),
                        columns: vec![PostgresColumn {
                            name: "free_text".to_string(),
                            ordinal_position: 1,
                            is_nullable: true,
                            data_type: "tsvector".to_string(),
                        }, ],
                        constraints: vec![],
                        indices: vec![
                            PostgresIndex {
                                name: "my_table_gin".to_string(),
                                key_columns: vec![PostgresIndexKeyColumn {
                                    name: "free_text".to_string(),
                                    ordinal_position: 1,
                                    direction: None,
                                    nulls_order: None,
                                }],
                                index_type: "gin".to_string(),
                                predicate: None,
                                included_columns: vec![],
                            },
                            PostgresIndex {
                                name: "my_table_gist".to_string(),
                                key_columns: vec![PostgresIndexKeyColumn {
                                    name: "free_text".to_string(),
                                    ordinal_position: 1,
                                    direction: None,
                                    nulls_order: None,
                                }],
                                index_type: "gist".to_string(),
                                predicate: None,
                                included_columns: vec![],
                            },
                        ],
                    }],
                }]
            }
        )
    }

    #[test]
    async fn filtered_index() {
        let helper = get_test_helper().await;
        helper
            .execute_not_query(
                r#"
        create table my_table(
            value int
        );

        create index my_table_idx on my_table (value) where (value % 2 = 0);
        "#,
            )
            .await;

        let db = introspect_schema(&helper).await;

        assert_eq!(
            db,
            PostgresDatabase {
                schemas: vec![PostgresSchema {
                    name: "public".to_string(),
                    tables: vec![PostgresTable {
                        name: "my_table".to_string(),
                        columns: vec![PostgresColumn {
                            name: "value".to_string(),
                            ordinal_position: 1,
                            is_nullable: true,
                            data_type: "integer".to_string(),
                        }, ],
                        constraints: vec![],
                        indices: vec![
                            PostgresIndex {
                                name: "my_table_idx".to_string(),
                                key_columns: vec![PostgresIndexKeyColumn {
                                    name: "value".to_string(),
                                    ordinal_position: 1,
                                    direction: Some(PostgresIndexColumnDirection::Ascending),
                                    nulls_order: Some(PostgresIndexNullsOrder::Last),
                                }],
                                index_type: "btree".to_string(),
                                predicate: Some("(value % 2) = 0".to_string()),
                                included_columns: vec![],
                            },
                        ],
                    }],
                }]
            }
        )
    }

    #[test]
    async fn index_with_include() {
        let helper = get_test_helper().await;
        //language=postgresql
        helper
            .execute_not_query(
                r#"
        create table my_table(
            value int,
            another_value int
        );

        create index my_table_idx on my_table (value) include (another_value);
        "#,
            )
            .await;

        let db = introspect_schema(&helper).await;

        assert_eq!(
            db,
            PostgresDatabase {
                schemas: vec![PostgresSchema {
                    name: "public".to_string(),
                    tables: vec![PostgresTable {
                        name: "my_table".to_string(),
                        columns: vec![
                            PostgresColumn {
                                name: "value".to_string(),
                                ordinal_position: 1,
                                is_nullable: true,
                                data_type: "integer".to_string(),
                            },
                            PostgresColumn {
                                name: "another_value".to_string(),
                                ordinal_position: 2,
                                is_nullable: true,
                                data_type: "integer".to_string(),
                            },
                        ],
                        constraints: vec![],
                        indices: vec![
                            PostgresIndex {
                                name: "my_table_idx".to_string(),
                                key_columns: vec![PostgresIndexKeyColumn {
                                    name: "value".to_string(),
                                    ordinal_position: 1,
                                    direction: Some(PostgresIndexColumnDirection::Ascending),
                                    nulls_order: Some(PostgresIndexNullsOrder::Last),
                                }],
                                index_type: "btree".to_string(),
                                predicate: None,
                                included_columns: vec![
                                    PostgresIndexIncludedColumn {
                                        name: "another_value".to_string(),
                                        ordinal_position: 2,
                                    }
                                ],
                            },
                        ],
                    }],
                }]
            }
        )
    }

    #[test]
    async fn table_with_non_distinct_nulls() {
        let helper = get_test_helper().await;
        //language=postgresql
        helper
            .execute_not_query(
                r#"
        create table my_table(
            value int unique nulls not distinct
        );
        "#,
            )
            .await;

        let db = introspect_schema(&helper).await;

        assert_eq!(
            db,
            PostgresDatabase {
                schemas: vec![PostgresSchema {
                    name: "public".to_string(),
                    tables: vec![PostgresTable {
                        name: "my_table".to_string(),
                        columns: vec![
                            PostgresColumn {
                                name: "value".to_string(),
                                ordinal_position: 1,
                                is_nullable: true,
                                data_type: "integer".to_string(),
                            },
                        ],
                        constraints: vec![
                            PostgresConstraint::Unique(PostgresUniqueConstraint {
                                name: "my_table_value_key".to_string(),
                                columns: vec![PostgresUniqueConstraintColumn {
                                    column_name: "value".to_string(),
                                    ordinal_position: 1,
                                }],
                                distinct_nulls: false,
                            })
                        ],
                        indices: vec![],
                    }],
                }]
            }
        )
    }
}
