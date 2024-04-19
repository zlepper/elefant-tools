use serde::{Deserialize, Serialize};
use crate::pg_interval::Interval;

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct HypertableRetention {
    pub drop_after: Interval,
    pub schedule_interval: Interval,
}

impl HypertableRetention {
    pub fn add_retention(&self, sql: &mut String, escaped_relation_name: &str) {
        sql.push_str("select add_retention_policy('");
        sql.push_str(escaped_relation_name);
        sql.push_str("', drop_after => INTERVAL '");
        sql.push_str(&self.drop_after.to_postgres());
        sql.push_str("', schedule_interval => INTERVAL '");
        sql.push_str(&self.schedule_interval.to_postgres());
        sql.push_str("');");
    }
}