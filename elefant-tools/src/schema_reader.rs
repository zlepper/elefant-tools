use std::str::FromStr;
use tokio_postgres::Row;
use crate::postgres_client_wrapper::{FromRow, PostgresClientWrapper};
use crate::{PostgresCheckConstraint, Result};
use crate::models::{PostgresColumn, PostgresDatabase, PostgresPrimaryKey, PostgresPrimaryKeyColumn, PostgresSchema, PostgresTable};

pub struct SchemaReader<'a> {
    connection: &'a PostgresClientWrapper,
}

impl SchemaReader<'_> {
    pub fn new(connection: &PostgresClientWrapper) -> SchemaReader {
        SchemaReader {
            connection
        }
    }

    pub async fn introspect_database(&self) -> Result<PostgresDatabase> {
        //language=postgresql
        let tables = self.connection.get_results::<TablesResult>(
            r#"
            select table_schema, table_name from information_schema.tables
            where table_schema not in ('pg_catalog', 'information_schema') and table_type = 'BASE TABLE'
            order by table_schema, table_name;
            "#
        ).await?;

        //language=postgresql
        let columns = self.connection.get_results::<TableColumnsResult>(
            r#"
            select c.table_schema, c.table_name, c.column_name, c.ordinal_position, c.is_nullable, c.data_type from information_schema.tables t
            join information_schema.columns c on t.table_schema = c.table_schema and t.table_name = c.table_name
            where t.table_schema not in ('pg_catalog', 'information_schema') and t.table_type = 'BASE TABLE'
            order by c.table_schema, c.table_name, c.ordinal_position;
            "#
        ).await?;

        //language=postgresql
        let key_columns = self.connection.get_results::<KeyColumnUsageResult>(
            r#"
            select kcu.table_schema, kcu.table_name, kcu.constraint_name, kcu.column_name, kcu.ordinal_position, kcu.position_in_unique_constraint, tc.constraint_type from information_schema.key_column_usage kcu
            join information_schema.table_constraints tc on kcu.table_schema = tc.table_schema and kcu.table_name = tc.table_name and kcu.constraint_name = tc.constraint_name
            where tc.constraint_type = 'PRIMARY KEY' or tc.constraint_type = 'FOREIGN KEY'
            order by kcu.table_schema, kcu.table_name, kcu.constraint_name, kcu.ordinal_position;
            "#
        ).await?;

        //language=postgresql
        let check_constraints = self.connection.get_results::<CheckConstraintResult>(
            r#"
            select distinct t.table_schema, t.table_name, cc.constraint_name, cc.check_clause from information_schema.check_constraints cc
            join information_schema.table_constraints tc on cc.constraint_schema = tc.constraint_schema and cc.constraint_name = tc.constraint_name
            join information_schema.tables t on tc.table_schema = t.table_schema and tc.table_name = t.table_name
            join information_schema.constraint_column_usage ccu on cc.constraint_schema = ccu.constraint_schema and cc.constraint_name = ccu.constraint_name
            where t.table_schema not in ('pg_catalog', 'information_schema')
            order by t.table_schema, t.table_name, cc.constraint_name;
            "#
        ).await?;

        let mut db = PostgresDatabase {
            schemas: vec![]
        };

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

            let columns = columns.iter().filter(|c| c.schema_name == row.schema_name && c.table_name == row.table_name);

            let key_columns = key_columns.iter().filter(|c| c.table_schema == row.schema_name && c.table_name == row.table_name);

            let check_constraints = check_constraints.iter().filter(|c| c.table_schema == row.schema_name && c.table_name == row.table_name);

            let mut table = PostgresTable::new(&row.table_name);

            for column in columns {
                table.columns.push(PostgresColumn {
                    name: column.column_name.clone(),
                    is_nullable: column.is_nullable,
                    ordinal_position: column.ordinal_position,
                    data_type: column.data_type.clone(),
                });
            }

            for key_column in key_columns {
                match key_column.key_type {
                    ConstraintType::PrimaryKey => {
                        if table.primary_key.is_none() {
                            table.primary_key = Some(PostgresPrimaryKey {
                                name: key_column.constraint_name.clone(),
                                columns: vec![],
                            });
                        }

                        table.primary_key.as_mut().unwrap().columns.push(PostgresPrimaryKeyColumn {
                            column_name: key_column.column_name.clone(),
                            ordinal_position: key_column.ordinal_position,
                        });
                    }
                    _ => todo!()
                }
            }

            for check_constraint in check_constraints {
                table.check_constraints.push(PostgresCheckConstraint {
                    name: check_constraint.constraint_name.clone(),
                    check_clause: check_constraint.check_clause.clone(),
                });
            }

            current_schema.tables.push(table);
        }


        Ok(db)
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

#[derive(Debug, Eq, PartialEq)]
struct KeyColumnUsageResult {
    pub table_schema: String,
    pub table_name: String,
    pub constraint_name: String,
    pub column_name: String,
    pub ordinal_position: i32,
    pub position_in_unique_constraint: Option<i32>,
    pub key_type: ConstraintType,
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

#[derive(Debug, Eq, PartialEq)]
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
            _ => Err(crate::ElefantToolsError::UnknownConstraintType(s.to_string())),
        }
    }
}


#[cfg(test)]
pub mod tests {
    use super::*;
    use tokio::test;
    use crate::models::{PostgresColumn, PostgresDatabase, PostgresPrimaryKey, PostgresPrimaryKeyColumn, PostgresSchema, PostgresTable};
    use crate::test_helpers::{get_test_helper, TestHelper};

    pub async fn introspect_schema(test_helper: &TestHelper) -> PostgresDatabase {
        let conn = test_helper.get_conn();
        let reader = SchemaReader::new(conn);
        reader.introspect_database().await.unwrap()
    }

    #[test]
    async fn reads_simple_schema() {
        let helper = get_test_helper().await;
        helper.execute_not_query(r#"
        create table my_table(
            id serial primary key,
            name text not null,
            age int not null check (age > 21),
            constraint my_multi_check check (age > 21 and age < 65 and name is not null)
        );
        "#).await;

        let db = introspect_schema(&helper).await;

        assert_eq!(db, PostgresDatabase {
            schemas: vec![
                PostgresSchema {
                    name: "public".to_string(),
                    tables: vec![
                        PostgresTable {
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
                            primary_key: Some(PostgresPrimaryKey {
                                name: "my_table_pkey".to_string(),
                                columns: vec![
                                    PostgresPrimaryKeyColumn {
                                        column_name: "id".to_string(),
                                        ordinal_position: 1,
                                    }
                                ],
                            }),
                            check_constraints: vec![
                                PostgresCheckConstraint {
                                    name: "my_multi_check".to_string(),
                                    check_clause: "(((age > 21) AND (age < 65) AND (name IS NOT NULL)))".to_string(),
                                },
                                PostgresCheckConstraint {
                                    name: "my_table_age_check".to_string(),
                                    check_clause: "((age > 21))".to_string(),
                                },
                            ],
                        }
                    ],
                }
            ]
        })
    }

    #[test]
    async fn table_without_columns() {
        let helper = get_test_helper().await;
        helper.execute_not_query(r#"
        create table my_table();
        "#).await;

        let db = introspect_schema(&helper).await;

        assert_eq!(db, PostgresDatabase {
            schemas: vec![
                PostgresSchema {
                    tables: vec![
                        PostgresTable {
                            name: "my_table".to_string(),
                            primary_key: None,
                            columns: vec![],
                            check_constraints: vec![],
                        }
                    ],
                    name: "public".to_string(),
                }
            ]
        })
    }

    #[test]
    async fn table_without_primary_key() {
        let helper = get_test_helper().await;
        helper.execute_not_query(r#"
        create table my_table(
            name text not null,
            age int not null
        );
        "#).await;

        let db = introspect_schema(&helper).await;

        assert_eq!(db, PostgresDatabase {
            schemas: vec![
                PostgresSchema {
                    name: "public".to_string(),
                    tables: vec![
                        PostgresTable {
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
                            primary_key: None,
                            check_constraints: vec![],
                        }
                    ],
                }
            ]
        })
    }

    #[test]
    async fn composite_primary_keys() {
        let helper = get_test_helper().await;
        helper.execute_not_query(r#"
        create table my_table(
            id_part_1 int not null,
            id_part_2 int not null,
            name text,
            age int,
            constraint my_table_pk primary key (id_part_1, id_part_2)
        );
        "#).await;

        let db = introspect_schema(&helper).await;

        assert_eq!(db, PostgresDatabase {
            schemas: vec![
                PostgresSchema {
                    name: "public".to_string(),
                    tables: vec![
                        PostgresTable {
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
                            primary_key: Some(PostgresPrimaryKey {
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
                            }),
                            check_constraints: vec![],
                        }
                    ],
                }
            ]
        })
    }
}