use crate::{ElefantToolsError, PostgresSchema};
use crate::helpers::StringExt;
use crate::object_id::ObjectId;
use crate::postgres_client_wrapper::FromPgChar;
use crate::quoting::{IdentifierQuoter, Quotable, quote_value_string};
use crate::quoting::AttemptedKeywordUsage::{ColumnName, TypeOrFunctionName};

#[derive(Debug, Eq, PartialEq, Default, Clone)]
pub struct PostgresTrigger {
    pub name: String,
    pub table_name: String,
    pub events: Vec<PostgresTriggerEvent>,
    pub timing: PostgresTriggerTiming,
    pub level: PostgresTriggerLevel,
    pub function_name: String,
    pub condition: Option<String>,
    pub old_table_name: Option<String>,
    pub new_table_name: Option<String>,
    pub comment: Option<String>,
    pub object_id: ObjectId,
    pub arguments: Option<String>,
}

impl PostgresTrigger {
    pub fn get_create_statement(&self, schema: &PostgresSchema, identifier_quoter: &IdentifierQuoter) -> String {
        let mut sql = "create trigger ".to_string();

        sql.push_str(&self.name.quote(identifier_quoter, ColumnName));
        sql.push(' ');
        sql.push_str(match self.timing {
            PostgresTriggerTiming::Before => "before",
            PostgresTriggerTiming::After => "after",
            PostgresTriggerTiming::InsteadOf => "instead of",
        });
        sql.push(' ');
        
        sql.push_join(" or ", self.events.iter().map(|e| e.get_event_name()));
        
        sql.push_str(" on ");
        sql.push_str(&schema.name.quote(identifier_quoter, ColumnName));
        sql.push('.');
        sql.push_str(&self.table_name.quote(identifier_quoter, ColumnName));
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
        sql.push_str(&self.function_name.quote(identifier_quoter, TypeOrFunctionName));
        sql.push_str("(");
        
        if let Some(args) = &self.arguments {
            sql.push_str(args);
        }
        
        sql.push_str(");");

        if let Some(comment) = &self.comment {
            sql.push_str("\ncomment on trigger ");
            sql.push_str(&self.name.quote(identifier_quoter, ColumnName));
            sql.push_str(" on ");
            sql.push_str(&schema.name.quote(identifier_quoter, ColumnName));
            sql.push('.');
            sql.push_str(&self.table_name.quote(identifier_quoter, ColumnName));
            sql.push_str(" is ");
            sql.push_str(&quote_value_string(comment));
            sql.push(';');
        }


        sql
    }
}

#[derive(Debug, Eq, PartialEq, Default, Clone)]
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

impl PostgresTriggerEvent {
    fn get_event_name(&self) -> &str {
        match self {
            PostgresTriggerEvent::Insert => "insert",
            PostgresTriggerEvent::Update => "update",
            PostgresTriggerEvent::Delete => "delete",
            PostgresTriggerEvent::Truncate => "truncate",
        }
    }
}

#[derive(Debug, Eq, PartialEq, Default, Clone)]
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

#[derive(Debug, Eq, PartialEq, Default, Clone)]
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