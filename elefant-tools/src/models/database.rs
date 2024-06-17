use crate::models::extension::PostgresExtension;
use crate::models::schema::PostgresSchema;
use crate::object_id::ObjectId;
use crate::{default, TimescaleDbUserDefinedJob};
use serde::{Deserialize, Serialize};

#[derive(Debug, Eq, PartialEq, Default, Clone, Serialize, Deserialize)]
pub struct PostgresDatabase {
    pub schemas: Vec<PostgresSchema>,
    pub enabled_extensions: Vec<PostgresExtension>,
    pub timescale_support: TimescaleSupport,
    pub object_id: ObjectId,
}

#[derive(Debug, Eq, PartialEq, Default, Clone, Serialize, Deserialize)]
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

    pub fn filtered_to_schema(&self, schema: &str) -> Self {
        PostgresDatabase {
            timescale_support: TimescaleSupport {
                user_defined_jobs: self
                    .timescale_support
                    .user_defined_jobs
                    .iter()
                    .filter(|j| j.function_schema == schema)
                    .cloned()
                    .collect(),
                ..self.timescale_support.clone()
            },
            schemas: self
                .schemas
                .iter()
                .filter(|s| s.name == schema)
                .cloned()
                .collect(),
            ..self.clone()
        }
    }

    pub fn with_renamed_schema(&self, old_schema_name: &str, new_schema_name: &str) -> Self {
        PostgresDatabase {
            timescale_support: TimescaleSupport {
                user_defined_jobs: self
                    .timescale_support
                    .user_defined_jobs
                    .iter()
                    .map(|j| {
                        if j.function_schema == old_schema_name {
                            TimescaleDbUserDefinedJob {
                                function_schema: new_schema_name.to_string(),
                                ..j.clone()
                            }
                        } else {
                            j.clone()
                        }
                    })
                    .collect(),
                ..self.timescale_support.clone()
            },
            schemas: self
                .schemas
                .iter()
                .map(|s| {
                    if s.name == old_schema_name {
                        PostgresSchema {
                            name: new_schema_name.to_string(),
                            ..s.clone()
                        }
                    } else {
                        s.clone()
                    }
                })
                .collect(),
            ..self.clone()
        }
    }

    pub(crate) fn try_get_schema(&self, schema_name: &str) -> Option<&PostgresSchema> {
        self.schemas.iter().find(|s| s.name == schema_name)
    }
}
