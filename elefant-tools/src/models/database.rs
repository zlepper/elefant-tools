use crate::models::schema::PostgresSchema;

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresDatabase {
    pub schemas: Vec<PostgresSchema>,
}
