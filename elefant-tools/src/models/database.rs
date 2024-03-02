use crate::{default, TimescaleDbUserDefinedJob};
use crate::models::extension::PostgresExtension;
use crate::models::schema::PostgresSchema;

#[derive(Debug, Eq, PartialEq, Default)]
pub struct PostgresDatabase {
    pub schemas: Vec<PostgresSchema>,
    pub enabled_extensions: Vec<PostgresExtension>,
    pub timescale_support: TimescaleSupport,
}

#[derive(Debug, Eq, PartialEq, Default)]
pub struct TimescaleSupport {
    pub is_enabled: bool,
    pub timescale_toolkit_is_enabled: bool,
    pub user_defined_jobs: Vec<TimescaleDbUserDefinedJob>,
}

impl PostgresDatabase {
    pub fn get_or_create_schema_mut(&mut self, schema_name: &str) -> &mut PostgresSchema {
        if let Some(position) = self.schemas.iter().position(|s| s.name == schema_name) {
            self.schemas.get_mut(position).unwrap()
        } else {
            let new_schema = PostgresSchema {
                name: schema_name.to_string(),
                ..default()
            };

            self.schemas.push(new_schema);
            self.schemas.last_mut().unwrap()
        }
    }
}