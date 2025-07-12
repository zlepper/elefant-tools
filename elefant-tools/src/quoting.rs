use std::collections::HashMap;

/// Provides utilities for quoting identifiers in PostgreSQL as needed.
#[derive(Debug)]
pub struct IdentifierQuoter {
    /// Keywords that might need to be escaped, and whether they are allowed to be used as column names or type/function names.
    keywords: HashMap<String, AllowedKeywordUsage>,
}

/// How a keyword is allowed to be used.
#[derive(Debug, Copy, Clone)]
pub struct AllowedKeywordUsage {
    pub column_name: bool,
    pub type_or_function_name: bool,
}

/// How an identifier is attempted to be used.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum AttemptedKeywordUsage {
    ColumnName,
    TypeOrFunctionName,
    Other,
}

impl IdentifierQuoter {
    /// Creates a new IdentifierQuoter with the specified keywords and their allowed usages.
    pub fn new(keywords: HashMap<String, AllowedKeywordUsage>) -> Self {
        Self { keywords }
    }

    /// Creates a new IdentifierQuoter with no keywords.
    ///
    /// This is mainly useful for testing as it doesn't require connecting to Postgres.
    pub fn empty() -> Self {
        Self {
            keywords: HashMap::new(),
        }
    }

    /// Quotes an identifier as needed.
    ///
    /// Ported from <https://github.com/postgres/postgres/blob/97957fdbaa429c7c582d4753b108cb1e23e1b28a/src/backend/utils/adt/ruleutils.c#L11975>
    pub fn quote(&self, identifier: impl AsRef<str>, usage: AttemptedKeywordUsage) -> String {
        let identifier = identifier.as_ref();

        if identifier.is_empty() {
            return "\"\"".to_string();
        }

        let mut chars = identifier.chars();

        let safe = if let Some(allowed) = self.keywords.get(identifier) {
            match usage {
                AttemptedKeywordUsage::ColumnName => allowed.column_name,
                AttemptedKeywordUsage::TypeOrFunctionName => allowed.type_or_function_name,
                AttemptedKeywordUsage::Other => false,
            }
        } else {
            matches!(chars.next(), Some('a'..='z' | '_'))
                && chars.all(|c| matches!(c, 'a'..='z' | '0'..='9' | '_'))
        };

        if safe {
            identifier.to_string()
        } else {
            let escaped = identifier.replace('"', r#""""#);

            format!("\"{escaped}\"")
        }
    }

    /// Quotes multiple identifiers as needed.
    pub fn quote_iter<'a, 's, S: AsRef<str>, I: IntoIterator<Item = S>>(
        &'a self,
        identifiers: I,
        usage: AttemptedKeywordUsage,
    ) -> impl Iterator<Item = String> + 'a
    where
        <I as IntoIterator>::IntoIter: 'a,
    {
        identifiers.into_iter().map(move |i| self.quote(i, usage))
    }
}

/// A trait for types that can be quoted.
pub(crate) trait Quotable {
    /// Quotes the value as needed.
    fn quote(&self, quoter: &IdentifierQuoter, usage: AttemptedKeywordUsage) -> String;
}

impl<S> Quotable for S
where
    S: AsRef<str>,
{
    fn quote(&self, quoter: &IdentifierQuoter, usage: AttemptedKeywordUsage) -> String {
        quoter.quote(self, usage)
    }
}

/// A trait for types that can be quoted as an iterator.
pub(crate) trait QuotableIter: Sized {
    fn quote(self, quoter: &IdentifierQuoter, usage: AttemptedKeywordUsage)
        -> IteratorQuoter<Self>;
}

impl<I> QuotableIter for I
where
    I: Iterator,
    I::Item: AsRef<str>,
{
    fn quote(
        self,
        quoter: &IdentifierQuoter,
        usage: AttemptedKeywordUsage,
    ) -> IteratorQuoter<Self> {
        IteratorQuoter {
            quoter,
            usage,
            iter: self,
        }
    }
}

/// The iterator implementation used then quoting an iterator of values
pub(crate) struct IteratorQuoter<'q, I> {
    quoter: &'q IdentifierQuoter,
    usage: AttemptedKeywordUsage,
    iter: I,
}

impl<I> Iterator for IteratorQuoter<'_, I>
where
    I: Iterator,
    I::Item: AsRef<str>,
{
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|i| self.quoter.quote(i, self.usage))
    }
}

/// Quotes a a string value for usage in Postgres.
pub(crate) fn quote_value_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use crate::quoting::{AllowedKeywordUsage, AttemptedKeywordUsage};
    use std::collections::HashMap;

    #[test]
    fn quoting() {
        let quoter = super::IdentifierQuoter::new(HashMap::from([(
            "table".to_string(),
            AllowedKeywordUsage {
                type_or_function_name: false,
                column_name: false,
            },
        )]));

        macro_rules! test_quote {
            ($identifier:literal, $expected:literal) => {
                let quoted = quoter.quote($identifier, AttemptedKeywordUsage::Other);
                assert_eq!(quoted, $expected);
            };
        }

        test_quote!("table", "\"table\"");
        test_quote!("table1", "table1");
        test_quote!("table_1", "table_1");
        test_quote!("table-1", "\"table-1\"");
        test_quote!("table 1", "\"table 1\"");
        test_quote!("1table", "\"1table\"");
        test_quote!("my_table", "my_table");
        test_quote!("MyTable", "\"MyTable\"");
        test_quote!("my\"table", "\"my\"\"table\"");
        test_quote!("", "\"\"");
    }
}
