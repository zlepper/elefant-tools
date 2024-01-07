use std::cmp::Ordering;
use std::str::FromStr;
use itertools::Itertools;
use crate::{ElefantToolsError, PostgresSchema, PostgresTable};
use crate::postgres_client_wrapper::FromPgChar;
use crate::quoting::{IdentifierQuoter, Quotable, QuotableIter};

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresForeignKey {
    pub name: String,
    pub columns: Vec<PostgresForeignKeyColumn>,
    pub referenced_schema: Option<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<PostgresForeignKeyReferencedColumn>,
    pub update_action: ReferenceAction,
    pub delete_action: ReferenceAction,
}

impl Default for PostgresForeignKey {
    fn default() -> Self {
        Self {
            name: String::new(),
            columns: Vec::new(),
            referenced_schema: None,
            referenced_table: String::new(),
            referenced_columns: Vec::new(),
            update_action: ReferenceAction::NoAction,
            delete_action: ReferenceAction::NoAction,
        }
    }
}

impl PostgresForeignKey {
    pub fn get_create_statement(&self, table: &PostgresTable, schema: &PostgresSchema, identifier_quoter: &IdentifierQuoter) -> String {
        let mut sql = format!("alter table {}.{} add constraint {} foreign key (",
                              schema.name.quote(identifier_quoter), table.name.quote(identifier_quoter), self.name.quote(identifier_quoter));

        let columns = self.columns.iter()
            .sorted_by_key(|c| c.ordinal_position)
            .map(|c| c.name.as_str())
            .quote(identifier_quoter)
            .join(", ");

        sql.push_str(&columns);
        sql.push_str(") references ");
        let referenced_schema = self.referenced_schema.as_ref().unwrap_or(&schema.name);
        sql.push_str(&referenced_schema.quote(identifier_quoter));
        sql.push('.');
        sql.push_str(&self.referenced_table.quote(identifier_quoter));
        sql.push_str(" (");

        let referenced_columns = self.referenced_columns.iter()
            .sorted_by_key(|c| c.ordinal_position)
            .map(|c| c.name.as_str())
            .quote(identifier_quoter)
            .join(", ");

        sql.push_str(&referenced_columns);
        sql.push(')');


        if self.update_action != ReferenceAction::NoAction {
            sql.push_str(" on update ");
            sql.push_str(match self.update_action {
                ReferenceAction::NoAction => unreachable!(),
                ReferenceAction::Restrict => "restrict",
                ReferenceAction::Cascade => "cascade",
                ReferenceAction::SetNull => "set null",
                ReferenceAction::SetDefault => "set default",
            });
        }

        if self.delete_action != ReferenceAction::NoAction {
            sql.push_str(" on delete ");
            sql.push_str(match self.delete_action {
                ReferenceAction::NoAction => unreachable!(),
                ReferenceAction::Restrict => "restrict",
                ReferenceAction::Cascade => "cascade",
                ReferenceAction::SetNull => "set null",
                ReferenceAction::SetDefault => "set default",
            });
        }

        if self.columns.iter().any(|c| !c.affected_by_delete_action)  {
            let affected_columns = self.columns.iter().filter(|c| c.affected_by_delete_action)
                .map(|c| c.name.as_str())
                .quote(identifier_quoter)
                .join(", ");

            sql.push('(');
            sql.push_str(&affected_columns);
            sql.push(')');
        }

        sql.push(';');


        sql
    }
}

impl Ord for PostgresForeignKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}

impl PartialOrd for PostgresForeignKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresForeignKeyColumn {
    pub name: String,
    pub ordinal_position: i32,
    pub affected_by_delete_action: bool,
}

impl Ord for PostgresForeignKeyColumn {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ordinal_position.cmp(&other.ordinal_position)
    }
}

impl PartialOrd for PostgresForeignKeyColumn {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresForeignKeyReferencedColumn {
    pub name: String,
    pub ordinal_position: i32,
}

impl Ord for PostgresForeignKeyReferencedColumn {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ordinal_position.cmp(&other.ordinal_position)
    }
}

impl PartialOrd for PostgresForeignKeyReferencedColumn {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum ReferenceAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

impl FromStr for ReferenceAction {
    type Err = crate::ElefantToolsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "a"|"NO ACTION" => Ok(ReferenceAction::NoAction),
            "r"|"RESTRICT" => Ok(ReferenceAction::Restrict),
            "c"|"CASCADE" => Ok(ReferenceAction::Cascade),
            "n"|"SET NULL" => Ok(ReferenceAction::SetNull),
            "d"|"SET DEFAULT" => Ok(ReferenceAction::SetDefault),
            _ => Err(crate::ElefantToolsError::UnknownForeignKeyAction(s.to_string())),
        }
    }
}

impl FromPgChar for ReferenceAction {
    fn from_pg_char(c: char) -> Result<Self, ElefantToolsError> {
        match c {
            'a' => Ok(ReferenceAction::NoAction),
            'r' => Ok(ReferenceAction::Restrict),
            'c' => Ok(ReferenceAction::Cascade),
            'n' => Ok(ReferenceAction::SetNull),
            'd' => Ok(ReferenceAction::SetDefault),
            _ => Err(ElefantToolsError::UnknownForeignKeyAction(c.to_string())),
        }
    }
}