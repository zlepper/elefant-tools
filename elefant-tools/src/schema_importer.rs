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
        let sql = schema.get_create_statement();
        self.connection.execute_non_query(&sql).await?;

        Ok(())
    }

    async fn create_table(&self, schema: &PostgresSchema, table: &PostgresTable) -> Result<()> {
        let sql = table.get_create_statement(schema);

        self.connection.execute_non_query(&sql).await?;


        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::test_helpers::*;
    use crate::schema_reader::tests::introspect_schema;
    use tokio::test;

    pub async fn import_database_schema(test_helper: &TestHelper, db: &PostgresDatabase) {
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

        import_database_schema(&target_db, &source_schema).await;

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