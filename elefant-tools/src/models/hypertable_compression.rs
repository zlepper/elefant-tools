use pg_interval::Interval;
use crate::helpers::StringExt;
use crate::quoting::AttemptedKeywordUsage::ColumnName;
use crate::quoting::{IdentifierQuoter, Quotable};

#[derive(Debug, Eq, PartialEq, Clone, Default)]
pub struct HypertableCompression {
    pub enabled: bool,
    pub segment_by_columns: Option<Vec<String>>,
    pub order_by_columns: Option<Vec<HypertableCompressionOrderedColumn>>,
    pub chunk_time_interval: Option<Interval>,
    pub compression_schedule_interval: Option<Interval>,
    pub compress_after: Option<Interval>,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct HypertableCompressionOrderedColumn {
    pub column_name: String,
    pub descending: bool,
    pub nulls_first: bool,
}

impl Default for HypertableCompressionOrderedColumn {
    fn default() -> Self {
        Self {
            column_name: "".to_string(),
            descending: true,
            nulls_first: true,
        }
    }
}

impl HypertableCompression {
    pub fn add_compression_settings(&self, sql: &mut String, escaped_relation_name: &str, identifier_quoter: &IdentifierQuoter) {

        sql.push_str(escaped_relation_name);
        sql.push_str(" set (\n\ttimescaledb.compress = ");
        sql.push_str(&self.enabled.to_string());

        if let Some(segment_by) = &self.segment_by_columns {
            sql.push_str(",\n\ttimescaledb.compress_segmentby = '");
            sql.push_join(", ", segment_by.iter().map(|c| c.quote(identifier_quoter, ColumnName)));
            sql.push('\'');
        }

        if let Some(order_by) = &self.order_by_columns {
            sql.push_str(",\n\ttimescaledb.compress_orderby = '");
            for (idx, order_by) in order_by.iter().enumerate() {
                if idx > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(&order_by.column_name.quote(identifier_quoter, ColumnName));
                if !order_by.descending {
                    sql.push_str(" ASC");
                } else {
                    sql.push_str(" DESC");
                }
                if order_by.nulls_first {
                    sql.push_str(" NULLS FIRST");
                } else {
                    sql.push_str(" NULLS LAST");
                }
            }
            sql.push('\'');
        }

        if let Some(chunk_time_interval) = self.chunk_time_interval {
            sql.push_str(",\n\ttimescaledb.compress_chunk_time_interval = '");
            sql.push_str(&chunk_time_interval.to_postgres());
            sql.push('\'');
        }

        sql.push_str("\n);");

        if let Some(compress_after) = self.compress_after {
            sql.push_str("\nselect public.add_compression_policy('");
            sql.push_str(escaped_relation_name);
            sql.push_str("', compress_after => INTERVAL '");
            sql.push_str(&compress_after.to_postgres());
            sql.push('\'');

            if let Some(schedule_interval) = self.compression_schedule_interval {
                sql.push_str(", schedule_interval => INTERVAL '");
                sql.push_str(&schedule_interval.to_postgres());
                sql.push('\'');
            }

            sql.push_str(");");
        }
    }
}