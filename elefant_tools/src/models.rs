
#[derive(Debug, Eq, PartialEq)]
pub struct PostgresDatabase {
    pub schemas: Vec<PostgresSchema>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresSchema {
    pub tables: Vec<PostgresTable>,
    pub name: String,
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
}

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresColumn {
    pub name: String,
    pub ordinal_position: i32,
    pub is_nullable: bool,
    pub data_type: String,
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
