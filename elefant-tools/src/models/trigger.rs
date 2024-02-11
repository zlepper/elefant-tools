use crate::ElefantToolsError;
use crate::postgres_client_wrapper::FromPgChar;

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