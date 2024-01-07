use std::collections::HashSet;

#[derive(Debug)]
pub struct IdentifierQuoter {
    keywords: HashSet<String>,
}

impl IdentifierQuoter {
    pub fn new(keywords: HashSet<String>) -> Self {
        Self {
            keywords,
        }
    }

    pub fn empty() -> Self {
        Self {
            keywords: HashSet::new(),
        }
    }

    pub fn quote(&self, identifier: impl AsRef<str>) -> String {
        // Ported from: https://github.com/postgres/postgres/blob/97957fdbaa429c7c582d4753b108cb1e23e1b28a/src/backend/utils/adt/ruleutils.c#L11975

        let identifier = identifier.as_ref();

        if identifier.is_empty() {
            return "\"\"".to_string();
        }

        let mut chars = identifier.chars();

        let safe = matches!(chars.next(), Some('a'..='z' | '_')) && chars.all(|c| matches!(c, 'a'..='z' | '0'..='9' | '_')) && !self.keywords.contains(identifier);

        if safe {
            identifier.to_string()
        } else {
            let escaped = identifier.replace('"', r#""""#);

            format!("\"{}\"", escaped)
        }
    }

    pub fn quote_iter<'a, 's, S: AsRef<str>, I: IntoIterator<Item=S>>(&'a self, identifiers: I) -> impl Iterator<Item=String> + 'a where <I as IntoIterator>::IntoIter: 'a {
        identifiers.into_iter().map(move |i| self.quote(i))
    }
}

pub(crate) trait Quotable {
    fn quote(&self, quoter: &IdentifierQuoter) -> String;
}

impl<S> Quotable for S
    where S: AsRef<str>,

{

    fn quote(&self, quoter: &IdentifierQuoter) -> String{
        quoter.quote(self)
    }
}

pub(crate) trait QuotableIter: Sized {
    fn quote(self, quoter: &IdentifierQuoter) -> IteratorQuoter<Self>;
}


impl<I> QuotableIter for I
    where I: Iterator,
          I::Item: AsRef<str>,
{
    fn quote(self, quoter: &IdentifierQuoter) -> IteratorQuoter<Self> {
        IteratorQuoter {
            quoter,
            iter: self,
        }
    }
}

pub(crate) struct IteratorQuoter<'q, I> {
    quoter: &'q IdentifierQuoter,
    iter: I
}

impl<I> Iterator for IteratorQuoter<'_, I>
    where I: Iterator,
          I::Item: AsRef<str>,
{
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|i| self.quoter.quote(i))
    }

}


#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    #[test]
    fn quoting() {
        let quoter = super::IdentifierQuoter::new(HashSet::from(["table".to_string()]));

        macro_rules! test_quote {
            ($identifier:literal, $expected:literal) => {
                let quoted = quoter.quote($identifier);
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