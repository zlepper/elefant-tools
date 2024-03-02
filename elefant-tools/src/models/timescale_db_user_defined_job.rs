use pg_interval::Interval;
use crate::quoting::{IdentifierQuoter, Quotable, quote_value_string};
use crate::quoting::AttemptedKeywordUsage::{TypeOrFunctionName};
use crate::whitespace_ignorant_string::WhitespaceIgnorantString;

#[derive(Debug, Eq, PartialEq)]
pub struct TimescaleDbUserDefinedJob {
    pub function_name: String,
    pub function_schema: String,
    pub schedule_interval: Interval,
    pub config: Option<WhitespaceIgnorantString>,
    pub scheduled: bool,
    pub check_config_name: Option<String>,
    pub check_config_schema: Option<String>,
    pub fixed_schedule: bool,
}

impl Default for TimescaleDbUserDefinedJob {
    fn default() -> Self {
        TimescaleDbUserDefinedJob {
            function_name: String::new(),
            function_schema: String::new(),
            schedule_interval: Interval::new(0, 1, 0),
            config: None,
            scheduled: false,
            check_config_name: None,
            check_config_schema: None,
            fixed_schedule: false,
        }
    }
}

impl TimescaleDbUserDefinedJob {
    pub fn get_create_sql(&self, identifier_quoter: &IdentifierQuoter) -> String {
        let mut sql = "select add_job('".to_string();
        sql.push_str(&self.function_schema.quote(identifier_quoter, TypeOrFunctionName));
        sql.push('.');
        sql.push_str(&self.function_name.quote(identifier_quoter, TypeOrFunctionName));
        sql.push_str("', interval '");
        sql.push_str(&self.schedule_interval.to_postgres());
        sql.push('\'');

        if let Some(config) = &self.config {
            sql.push_str(", config => ");
            sql.push_str(&quote_value_string(config));
        }

        if !self.scheduled {
            sql.push_str(", scheduled => false");
        }

        if let (Some(check_config_name), Some(check_config_schema)) = (&self.check_config_name, &self.check_config_schema) {
            sql.push_str(", check_config => '");
            sql.push_str(&check_config_schema.quote(identifier_quoter, TypeOrFunctionName));
            sql.push('.');
            sql.push_str(&check_config_name.quote(identifier_quoter, TypeOrFunctionName));
            sql.push('\'');
        }

        if !self.fixed_schedule {
            sql.push_str(", fixed_schedule => false");
        }

        sql.push_str(");");

        sql
    }

}
