use crate::models::schema::PostgresSchema;

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresDatabase {
    pub schemas: Vec<PostgresSchema>,
}

impl PostgresDatabase {
    pub fn get_or_create_schema_mut(&mut self, schema_name: &str) -> &mut PostgresSchema {
        if let Some(position) = self.schemas.iter().position(|s| s.name == schema_name) {
            self.schemas.get_mut(position).unwrap()
        } else {
            let new_schema = PostgresSchema {
                name: schema_name.to_string(),
                tables: Vec::new(),
                sequences: Vec::new(),
                views: Vec::new(),
            };

            self.schemas.push(new_schema);
            self.schemas.last_mut().unwrap()
        }
    }
}
