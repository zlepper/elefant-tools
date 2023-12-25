use itertools::Itertools;
use crate::ddl_query_builder::DdlQueryBuilder;
use crate::models::{PostgresDatabase, PostgresSchema, PostgresTable};
use crate::postgres_client_wrapper::PostgresClientWrapper;
use crate::Result;

pub struct SchemaImporter<'a> {
    connection: &'a PostgresClientWrapper,
}

impl SchemaImporter<'_> {
    pub fn new(connection: &PostgresClientWrapper) -> SchemaImporter {
        SchemaImporter {
            connection
        }
    }

    pub async fn import_database(&self, db: &PostgresDatabase) -> Result<()> {
        self.import_table_structures(db).await?;
        self.import_table_validations(db).await?;

        Ok(())
    }

    pub async fn import_table_structures(&self, db: &PostgresDatabase) -> Result<()> {
        for schema in &db.schemas {
            self.create_schema(schema).await?;


            for table in &schema.tables {
                self.create_table(schema, table).await?;
            }

        }

        Ok(())
    }

    pub async fn import_table_validations(&self, db: &PostgresDatabase) -> Result<()> {
        // for table in db.tables.iter() {
        //     self.import_table_validations(table).await?;
        // }

        Ok(())
    }

    async fn create_schema(&self, schema: &PostgresSchema) -> Result<()> {
        let sql = format!("create schema if not exists {}", schema.name);
        self.connection.execute_non_query(&sql).await?;

        Ok(())
    }

    async fn create_table(&self, schema: &PostgresSchema, table: &PostgresTable) -> Result<()> {
        let mut query_builder = DdlQueryBuilder::new();
        let mut table_builder = query_builder.create_table(&schema.name, &table.name);



        for column in &table.columns {
            let mut column_builder = table_builder.column(&column.name, &column.data_type);

            if !column.is_nullable {
                column_builder.not_null();
            }
        }

        if let Some(pk) = &table.primary_key {

            let columns = pk.columns.iter().sorted_by_key(|c| c.ordinal_position).map(|c| c.column_name.as_str());

            table_builder.primary_key(&pk.name, columns);
        }


        let sql = query_builder.build();

        self.connection.execute_non_query(&sql).await?;


        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::*;
    use crate::schema_reader::tests::introspect_schema;
    use tokio::test;

    async fn import_database(test_helper: &TestHelper, db: &PostgresDatabase) {
        let conn = test_helper.get_conn();
        let importer = SchemaImporter::new(conn);
        importer.import_database(db).await.expect("Failed to import database");
    }

    async fn test_schema_import(schema_sql: &str) {
        let source_db = get_test_helper().await;
        source_db.execute_not_query(schema_sql).await;

        let source_schema = introspect_schema(&source_db).await;

        drop(source_db);

        let target_db = get_test_helper().await;

        import_database(&target_db, &source_schema).await;

        let destination_schema = introspect_schema(&target_db).await;

        assert_eq!(source_schema, destination_schema);
    }

    #[test]
    async fn test_import_database() {
        test_schema_import(r#"
        create table my_table(
            id serial primary key,
            name text not null,
            age int not null
        );
        "#).await;
    }

    #[test]
    async fn composite_primary_keys() {
        test_schema_import(r#"
        create table my_table(
            id int not null,
            name text not null,
            age int not null,
            constraint my_pk primary key (id, name)
        );
        "#).await;
    }

}