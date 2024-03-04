use std::cmp::Ordering;
use crate::{PostgresSchema, PostgresTable};
use crate::helpers::StringExt;
use crate::object_id::ObjectId;
use crate::quoting::{IdentifierQuoter, Quotable, quote_value_string};
use crate::quoting::AttemptedKeywordUsage::ColumnName;

#[derive(Debug, Eq, PartialEq, Default, Clone)]
pub struct PostgresIndex {
    pub name: String,
    pub key_columns: Vec<PostgresIndexKeyColumn>,
    pub index_type: String,
    pub predicate: Option<String>,
    pub included_columns: Vec<PostgresIndexIncludedColumn>,
    pub index_constraint_type: PostgresIndexType,
    pub storage_parameters: Vec<String>,
    pub comment: Option<String>,
    pub object_id: ObjectId,
}

#[derive(Debug, Eq, PartialEq, Default, Clone)]
pub enum PostgresIndexType {
    PrimaryKey,
    Unique {
        nulls_distinct: bool,
    },
    #[default]
    Index,
}

impl Ord for PostgresIndex {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}

impl PartialOrd for PostgresIndex {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PostgresIndex {
    pub fn get_create_index_command(&self, schema: &PostgresSchema, table: &PostgresTable, identifier_quoter: &IdentifierQuoter) -> String {
        if PostgresIndexType::PrimaryKey == self.index_constraint_type {
            return format!("alter table {}.{} add constraint {} primary key ({});", schema.name.quote(identifier_quoter, ColumnName), table.name.quote(identifier_quoter, ColumnName), self.name.quote(identifier_quoter, ColumnName), self.key_columns.iter().map(|c| c.name.quote(identifier_quoter, ColumnName)).collect::<Vec<String>>().join(", "));
        }

        let index_type = match self.index_constraint_type {
            PostgresIndexType::Unique{..} => "unique ",
            _ => "",
        };

        let mut command = format!("create {}index {} on {}.{} using {} (", index_type, self.name.quote(identifier_quoter, ColumnName), schema.name.quote(identifier_quoter, ColumnName), table.name.quote(identifier_quoter, ColumnName), self.index_type);

        for (i, column) in self.key_columns.iter().enumerate() {
            if i > 0 {
                command.push_str(", ");
            }

            command.push_str(&column.name);

            match column.direction {
                Some(PostgresIndexColumnDirection::Ascending) => {
                    command.push_str(" asc");
                },
                Some(PostgresIndexColumnDirection::Descending) => {
                    command.push_str(" desc");
                },
                _ => {},
            }

            match column.nulls_order {
                Some(PostgresIndexNullsOrder::First) => {
                    command.push_str(" nulls first");
                },
                Some(PostgresIndexNullsOrder::Last) => {
                    command.push_str(" nulls last");
                },
                _ => {}
            }
        }

        command.push(')');

        if !self.included_columns.is_empty() {
            command.push_str(" include (");

            command.push_join(", ", self.included_columns.iter().map(|c| c.name.quote(identifier_quoter, ColumnName)));

            command.push(')');
        }

        if let PostgresIndexType::Unique { nulls_distinct: false } = self.index_constraint_type {
            command.push_str(" nulls not distinct")
        }

        if !self.storage_parameters.is_empty() {
            command.push_str(" with (");
            command.push_join(", ", self.storage_parameters.iter());
            command.push(')');
        }

        if let Some(ref predicate) = self.predicate {
            command.push_str(" where ");
            command.push_str(predicate);
        }

        command.push(';');

        if let Some(comment) = &self.comment {
            command.push_str("\ncomment on index ");
            command.push_str(&self.name.quote(identifier_quoter, ColumnName));
            command.push_str(" is ");
            command.push_str(&quote_value_string(comment));
            command.push(';');
        }

        command
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct PostgresIndexKeyColumn {
    pub name: String,
    pub ordinal_position: i32,
    pub direction: Option<PostgresIndexColumnDirection>,
    pub nulls_order: Option<PostgresIndexNullsOrder>,
}

impl Ord for PostgresIndexKeyColumn {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ordinal_position.cmp(&other.ordinal_position)
    }
}

impl PartialOrd for PostgresIndexKeyColumn {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum PostgresIndexColumnDirection {
    Ascending,
    Descending,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum PostgresIndexNullsOrder {
    First,
    Last,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct PostgresIndexIncludedColumn {
    pub name: String,
    pub ordinal_position: i32,
}

impl Ord for PostgresIndexIncludedColumn {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ordinal_position.cmp(&other.ordinal_position)
    }
}

impl PartialOrd for PostgresIndexIncludedColumn {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}