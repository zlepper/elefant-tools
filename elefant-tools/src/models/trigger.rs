use crate::{ElefantToolsError, PostgresSchema};
use crate::postgres_client_wrapper::FromPgChar;
use crate::quoting::{IdentifierQuoter, Quotable, quote_value_string};

#[derive(Debug, Eq, PartialEq, Default)]
pub struct PostgresTrigger {
    pub name: String,
    pub table_name: String,
    pub event: PostgresTriggerEvent,
    pub timing: PostgresTriggerTiming,
    pub level: PostgresTriggerLevel,
    pub function_name: String,
    pub condition: Option<String>,
    pub old_table_name: Option<String>,
    pub new_table_name: Option<String>,
    pub comment: Option<String>,
}

impl PostgresTrigger {
    pub fn get_create_statement(&self, schema: &PostgresSchema, identifier_quoter: &IdentifierQuoter) -> String {
        let mut sql = "create trigger ".to_string();

        sql.push_str(&self.name.quote(identifier_quoter));
        sql.push(' ');
        sql.push_str(match self.timing {
            PostgresTriggerTiming::Before => "before",
            PostgresTriggerTiming::After => "after",
            PostgresTriggerTiming::InsteadOf => "instead of",
        });
        sql.push(' ');
        sql.push_str(match self.event {
            PostgresTriggerEvent::Insert => "insert",
            PostgresTriggerEvent::Update => "update",
            PostgresTriggerEvent::Delete => "delete",
            PostgresTriggerEvent::Truncate => "truncate",
        });
        sql.push_str(" on ");
        sql.push_str(&schema.name.quote(identifier_quoter));
        sql.push('.');
        sql.push_str(&self.table_name.quote(identifier_quoter));
        sql.push_str(" for each ");
        sql.push_str(match self.level {
            PostgresTriggerLevel::Row => "row",
            PostgresTriggerLevel::Statement => "statement",
        });

        if let Some(cond) = &self.condition {
            sql.push_str(" when (");
            sql.push_str(cond);
            sql.push(')');
        }

        sql.push_str(" execute function ");
        sql.push_str(&self.function_name.quote(identifier_quoter));
        sql.push_str("();");

        if let Some(comment) = &self.comment {
            sql.push_str("\ncomment on trigger ");
            sql.push_str(&self.name.quote(identifier_quoter));
            sql.push_str(" on ");
            sql.push_str(&schema.name.quote(identifier_quoter));
            sql.push('.');
            sql.push_str(&self.table_name.quote(identifier_quoter));
            sql.push_str(" is ");
            sql.push_str(&quote_value_string(comment));
            sql.push(';');
        }


        sql
    }
}

#[derive(Debug, Eq, PartialEq, Default)]
pub enum PostgresTriggerEvent {
    #[default]
    Insert,
    Update,
    Delete,
    Truncate,
}

impl FromPgChar for PostgresTriggerEvent {
    fn from_pg_char(c: char) -> Result<Self, ElefantToolsError> {
        match c {
            'i' => Ok(PostgresTriggerEvent::Insert),
            'u' => Ok(PostgresTriggerEvent::Update),
            'd' => Ok(PostgresTriggerEvent::Delete),
            't' => Ok(PostgresTriggerEvent::Truncate),
            _ => Err(ElefantToolsError::UnknownTriggerEvent(c.to_string()))
        }
    }
}

#[derive(Debug, Eq, PartialEq, Default)]
pub enum PostgresTriggerTiming {
    #[default]
    Before,
    After,
    InsteadOf,
}

impl FromPgChar for PostgresTriggerTiming {
    fn from_pg_char(c: char) -> Result<Self, ElefantToolsError> {
        match c {
            'b' => Ok(PostgresTriggerTiming::Before),
            'a' => Ok(PostgresTriggerTiming::After),
            'i' => Ok(PostgresTriggerTiming::InsteadOf),
            _ => Err(ElefantToolsError::UnknownTriggerTiming(c.to_string()))
        }
    }
}

#[derive(Debug, Eq, PartialEq, Default)]
pub enum PostgresTriggerLevel {
    #[default]
    Row,
    Statement,
}

impl FromPgChar for PostgresTriggerLevel {
    fn from_pg_char(c: char) -> Result<Self, ElefantToolsError> {

        match c {
            'r' => Ok(PostgresTriggerLevel::Row),
            's' => Ok(PostgresTriggerLevel::Statement),
            _ => Err(ElefantToolsError::UnknownTriggerLevel(c.to_string()))
        }
    }
}