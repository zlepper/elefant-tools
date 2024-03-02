use pg_interval::Interval;
use crate::{HypertableCompression, PostgresSchema};
use crate::models::hypertable_retention::HypertableRetention;
use crate::quoting::{quote_value_string, IdentifierQuoter, Quotable};
use crate::quoting::AttemptedKeywordUsage::ColumnName;
use crate::whitespace_ignorant_string::WhitespaceIgnorantString;

#[derive(Debug, Eq, PartialEq, Default)]
pub struct PostgresView {
    pub name: String,
    pub definition: WhitespaceIgnorantString,
    pub columns: Vec<PostgresViewColumn>,
    pub comment: Option<String>,
    pub is_materialized: bool,
    pub view_options: ViewOptions
}

impl PostgresView {
    pub fn get_create_view_sql(&self, schema: &PostgresSchema, identifier_quoter: &IdentifierQuoter) -> String {
        let escaped_relation_name = format!("{}.{}", schema.name.quote(identifier_quoter, ColumnName), self.name.quote(identifier_quoter, ColumnName));

        let mut sql = "create".to_string();

        if self.is_materialized {
            sql.push_str(" materialized");
        }

        sql.push_str(" view ");
        sql.push_str(&escaped_relation_name);

        sql.push_str(" (");

        for (i, column) in self.columns.iter().enumerate() {
            if i != 0 {
                sql.push_str(", ");
            }

            sql.push_str(&column.name.quote(identifier_quoter, ColumnName));
        }

        sql.push_str(") ");

        if let ViewOptions::TimescaleContinuousAggregate {..} = &self.view_options {
            sql.push_str("with (timescaledb.continuous) ");
        }


        sql.push_str("as ");

        sql.push_str(&self.definition);

        if let ViewOptions::TimescaleContinuousAggregate {..} = &self.view_options {
            while sql.ends_with(';') {
                sql.pop();
            }
            sql.push_str(" with no data;");
        }

        if let Some(comment) = &self.comment {
            sql.push_str("\ncomment on ");
            if self.is_materialized {
                sql.push_str("materialized ");
            }
            sql.push_str("view ");
            sql.push_str(&escaped_relation_name);
            sql.push_str(" is ");
            sql.push_str(&quote_value_string(comment));
            sql.push(';');
        }


        if let ViewOptions::TimescaleContinuousAggregate { refresh, compression, retention} = &self.view_options {
            if let Some(refresh) = refresh {
                sql.push_str("\nselect add_continuous_aggregate_policy('");
                sql.push_str(&escaped_relation_name);
                sql.push_str("', start_offset => INTERVAL '");
                sql.push_str(&refresh.start_offset.to_postgres());
                sql.push_str("', end_offset => INTERVAL '");
                sql.push_str(&refresh.end_offset.to_postgres());
                sql.push_str("', schedule_interval => INTERVAL '");
                sql.push_str(&refresh.interval.to_postgres());
                sql.push_str("');");
            }

            if let Some(compression) = compression {
                sql.push_str("alter materialized view ");
                compression.add_compression_settings(&mut sql, &escaped_relation_name, identifier_quoter);
            }


            if let Some(retention) = retention {
                sql.push('\n');
                retention.add_retention(&mut sql, &escaped_relation_name);
            }
        }

        sql
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresViewColumn {
    pub name: String,
    pub ordinal_position: i32,
}

#[derive(Debug, Eq, PartialEq, Default)]
pub enum ViewOptions {
    #[default]
    None,
    TimescaleContinuousAggregate {
        refresh: Option<TimescaleContinuousAggregateRefreshOptions>,
        compression: Option<HypertableCompression>,
        retention: Option<HypertableRetention>,
    },
}

#[derive(Debug, Eq, PartialEq)]
pub struct TimescaleContinuousAggregateRefreshOptions {
    pub interval: Interval,
    pub start_offset: Interval,
    pub end_offset: Interval,
}