use itertools::Itertools;
use crate::ddl_query_builder::DdlQueryBuilder;
use crate::storage::DataFormat;

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresDatabase {
    pub schemas: Vec<PostgresSchema>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresSchema {
    pub tables: Vec<PostgresTable>,
    pub name: String,
}

impl PostgresSchema {
    pub fn get_create_statement(&self) -> String {
        format!("create schema if not exists {};", self.name)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresTable {
    pub name: String,
    pub columns: Vec<PostgresColumn>,
    pub primary_key: Option<PostgresPrimaryKey>,
}

impl PostgresTable {
    pub fn new(name: &str) -> Self {
        PostgresTable {
            name: name.to_string(),
            columns: vec![],
            primary_key: None,
        }
    }

    pub fn get_create_statement(&self, schema: &PostgresSchema) -> String {
        let mut query_builder = DdlQueryBuilder::new();
        let mut table_builder = query_builder.create_table(&schema.name, &self.name);


        for column in &self.columns {
            let mut column_builder = table_builder.column(&column.name, &column.data_type);

            if !column.is_nullable {
                column_builder.not_null();
            }
        }

        if let Some(pk) = &self.primary_key {
            let columns = pk.columns.iter().sorted_by_key(|c| c.ordinal_position).map(|c| c.column_name.as_str());

            table_builder.primary_key(&pk.name, columns);
        }


        query_builder.build()
    }

    pub fn get_copy_in_command(&self, schema: &PostgresSchema, data_format: &DataFormat) -> String {
        let mut s = "copy ".to_string();
        s.push_str(&schema.name);
        s.push('.');
        s.push_str(&self.name);

        s.push_str(" (");

        let cols = self.columns.iter()
            .sorted_by_key(|c| c.ordinal_position)
            .map(|c| c.name.as_str())
            .join(", ");

        s.push_str(&cols);

        s.push_str(") from stdin with (format ");
        match data_format {
            DataFormat::Text => {
                s.push_str("text");
            }
            DataFormat::PostgresBinary { .. } => {
                s.push_str("binary");
            }
        }
        s.push_str(", header false);");

        s
    }

    pub fn get_copy_out_command(&self, schema: &PostgresSchema, data_format: &DataFormat) -> String {
        let mut s = "copy ".to_string();
        s.push_str(&schema.name);
        s.push('.');
        s.push_str(&self.name);

        s.push_str(" (");

        let cols = self.columns.iter()
            .sorted_by_key(|c| c.ordinal_position)
            .map(|c| c.name.as_str())
            .join(", ");

        s.push_str(&cols);
        s.push_str(") ");

        s.push_str(" to stdout with (format ");
        match data_format {
            DataFormat::Text => {
                s.push_str("text");
            }
            DataFormat::PostgresBinary { .. } => {
                s.push_str("binary");
            }
        }
        s.push_str(", header false, encoding 'utf-8');");

        s
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresColumn {
    pub name: String,
    pub ordinal_position: i32,
    pub is_nullable: bool,
    pub data_type: String,
}

impl PostgresColumn {
    pub fn get_simplified_data_type(&self) -> SimplifiedDataType {
        match self.data_type.as_str() {
            "bigint"|"integer"|"smallint"|"real"|"double precision" => SimplifiedDataType::Number,
            "boolean" => SimplifiedDataType::Bool,
            _ => SimplifiedDataType::Text,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum SimplifiedDataType {
    Number,
    Text,
    Bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresPrimaryKey {
    pub name: String,
    pub columns: Vec<PostgresPrimaryKeyColumn>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresPrimaryKeyColumn {
    pub column_name: String,
    pub ordinal_position: i32,
}
