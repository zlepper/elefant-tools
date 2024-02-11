use crate::quoting::{IdentifierQuoter, Quotable, QuotableIter};
use itertools::Itertools;

pub struct DdlQueryBuilder<'q> {
    sql: String,
    identifier_quoter: &'q IdentifierQuoter,
}

impl<'a> DdlQueryBuilder<'a> {
    pub fn new(identifier_quoter: &'a IdentifierQuoter) -> Self {
        Self {
            sql: String::new(),
            identifier_quoter,
        }
    }

    pub fn create_table(&mut self, schema: &str, table: &str) -> DdlTableBuilder<'a, '_> {
        self.sql.push_str(&format!(
            "create table {}.{} (",
            schema.quote(self.identifier_quoter),
            table.quote(self.identifier_quoter)
        ));

        DdlTableBuilder {
            query_builder: self,
            has_first_line: false,
        }
    }

    pub fn build(mut self) -> String {
        self.sql.push_str("\n);");

        self.sql
    }
}

pub struct DdlTableBuilder<'q, 'b> {
    query_builder: &'b mut DdlQueryBuilder<'q>,
    has_first_line: bool,
}

impl<'a, 'q> DdlTableBuilder<'a, 'q> {
    pub fn column<'b>(&'b mut self, name: &str, data_type: &str) -> DdlTableColumnBuilder<'b> {
        let name = self.query_builder.identifier_quoter.quote(name);
        self.start_new_line();
        self.query_builder
            .sql
            .push_str(&format!("    {} {}", name, data_type));

        DdlTableColumnBuilder {
            sql: &mut self.query_builder.sql,
        }
    }

    pub fn primary_key<'i, S: AsRef<str>>(
        &mut self,
        name: &str,
        columns: impl IntoIterator<Item = S>,
    ) -> &mut Self {
        self.start_new_line();
        let cols = columns
            .into_iter()
            .quote(self.query_builder.identifier_quoter)
            .join(", ");
        self.query_builder.sql.push_str(&format!(
            "    constraint {} primary key ({})",
            name.quote(self.query_builder.identifier_quoter),
            cols
        ));

        self
    }

    pub fn check_constraint(&mut self, name: &str, expression: &str) -> &mut Self {
        self.start_new_line();
        self.query_builder.sql.push_str(&format!(
            "    constraint {} check {}",
            name.quote(self.query_builder.identifier_quoter),
            expression
        ));

        self
    }

    fn start_new_line(&mut self) {
        if self.has_first_line {
            self.query_builder.sql.push_str(",\n")
        } else {
            self.query_builder.sql.push('\n');
            self.has_first_line = true;
        }
    }
}

pub struct DdlTableColumnBuilder<'a> {
    sql: &'a mut String,
}

impl<'a> DdlTableColumnBuilder<'a> {
    pub fn not_null(&mut self) -> &mut Self {
        self.sql.push_str(" not null");

        self
    }

    pub fn generated(&mut self, expression: &str) -> &mut Self {
        self.sql.push_str(" generated always as (");
        self.sql.push_str(expression);
        self.sql.push_str(") stored");

        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;

    #[test]
    fn builds_create_table_expression() {
        let quoter = IdentifierQuoter::empty();
        let mut builder = DdlQueryBuilder::new(&quoter);
        let mut table_builder = builder.create_table("public", "my_table");
        table_builder.column("id", "int");
        table_builder.column("name", "varchar(255)");
        table_builder.primary_key("pk_table", vec!["id"]);
        let result = builder.build();

        assert_eq!(
            result,
            indoc! {r#"
        create table public.my_table (
            id int,
            name varchar(255),
            constraint pk_table primary key (id)
        );"#}
        );
    }

    #[test]
    fn multiple_primary_keys() {
        let quoter = IdentifierQuoter::empty();
        let mut builder = DdlQueryBuilder::new(&quoter);
        let mut table_builder = builder.create_table("public", "my_table");
        table_builder.column("id", "int");
        table_builder.column("name", "varchar(255)");
        table_builder.primary_key("pk_table", vec!["id", "name"]);
        let result = builder.build();

        assert_eq!(
            result,
            indoc! {r#"
        create table public.my_table (
            id int,
            name varchar(255),
            constraint pk_table primary key (id, name)
        );"#}
        );
    }

    #[test]
    fn columns_only() {
        let quoter = IdentifierQuoter::empty();
        let mut builder = DdlQueryBuilder::new(&quoter);
        let mut table_builder = builder.create_table("public", "my_table");
        table_builder.column("id", "int");
        table_builder.column("name", "varchar(255)");
        let result = builder.build();

        assert_eq!(
            result,
            indoc! {r#"
        create table public.my_table (
            id int,
            name varchar(255)
        );"#}
        );
    }

    #[test]
    fn not_null_columns() {
        let quoter = IdentifierQuoter::empty();
        let mut builder = DdlQueryBuilder::new(&quoter);
        let mut table_builder = builder.create_table("public", "my_table");
        table_builder.column("id", "int").not_null();
        table_builder.column("name", "varchar(255)").not_null();
        let result = builder.build();

        assert_eq!(
            result,
            indoc! {r#"
        create table public.my_table (
            id int not null,
            name varchar(255) not null
        );"#}
        );
    }

    #[test]
    fn check_constraint_single_column() {
        let quoter = IdentifierQuoter::empty();
        let mut builder = DdlQueryBuilder::new(&quoter);
        let mut table_builder = builder.create_table("public", "my_table");
        table_builder.column("id", "int");
        table_builder.column("name", "varchar(255)");
        table_builder.check_constraint("check_name", "(name != 'foo')");
        let result = builder.build();

        assert_eq!(
            result,
            indoc! {r#"
        create table public.my_table (
            id int,
            name varchar(255),
            constraint check_name check (name != 'foo')
        );"#}
        );
    }

    #[test]
    fn check_constraint_multiple_column() {
        let quoter = IdentifierQuoter::empty();
        let mut builder = DdlQueryBuilder::new(&quoter);
        let mut table_builder = builder.create_table("public", "my_table");
        table_builder.column("id", "int");
        table_builder.column("name", "varchar(255)");
        table_builder.check_constraint("check_name", "(name != 'foo' and id > 0)");
        let result = builder.build();

        assert_eq!(
            result,
            indoc! {r#"
        create table public.my_table (
            id int,
            name varchar(255),
            constraint check_name check (name != 'foo' and id > 0)
        );"#}
        );
    }
    #[test]
    fn generated_column() {
        let quoter = IdentifierQuoter::empty();
        let mut builder = DdlQueryBuilder::new(&quoter);
        let mut table_builder = builder.create_table("public", "my_table");
        table_builder.column("name", "text");
        table_builder
            .column("search", "tsvector")
            .generated("to_tsvector('english', name)");
        let result = builder.build();

        assert_eq!(
            result,
            indoc! {r#"
        create table public.my_table (
            name text,
            search tsvector generated always as (to_tsvector('english', name)) stored
        );"#}
        );
    }
}
