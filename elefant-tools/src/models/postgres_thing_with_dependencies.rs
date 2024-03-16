use crate::{ObjectId, PostgresAggregateFunction, PostgresFunction, PostgresSchema, PostgresTable, PostgresView};
use crate::object_id::HaveDependencies;

pub(crate) enum PostgresThingWithDependencies<'a> {
    Table(&'a PostgresTable, &'a PostgresSchema),
    View(&'a PostgresView, &'a PostgresSchema),
    Function(&'a PostgresFunction, &'a PostgresSchema),
    AggregateFunction(&'a PostgresAggregateFunction, &'a PostgresSchema)
}


impl<'a> HaveDependencies for &PostgresThingWithDependencies<'a> {
    fn depends_on(&self) -> &Vec<ObjectId> {
        match self {
            PostgresThingWithDependencies::Table(table, _) => &table.depends_on,
            PostgresThingWithDependencies::View(view, _) => &view.depends_on,
            PostgresThingWithDependencies::Function(function, _) => &function.depends_on,
            PostgresThingWithDependencies::AggregateFunction(aggregate_function, _) => &aggregate_function.depends_on,
        }
    }

    fn object_id(&self) -> ObjectId {
        match self {
            PostgresThingWithDependencies::Table(table, _) => table.object_id,
            PostgresThingWithDependencies::View(view, _) => view.object_id,
            PostgresThingWithDependencies::Function(function, _) => function.object_id,
            PostgresThingWithDependencies::AggregateFunction(aggregate_function, _) => aggregate_function.object_id,
        }
    }
}

impl<'a> PostgresThingWithDependencies<'a> {
    pub fn get_create_sql(&self, identifier_quoter: &crate::IdentifierQuoter) -> String {
        match self {
            PostgresThingWithDependencies::Table(table, schema) => table.get_create_statement(schema, identifier_quoter),
            PostgresThingWithDependencies::View(view, schema) => view.get_create_view_sql(schema, identifier_quoter),
            PostgresThingWithDependencies::Function(function, schema) => function.get_create_statement(schema, identifier_quoter),
            PostgresThingWithDependencies::AggregateFunction(aggregate_function, schema) => aggregate_function.get_create_statement(schema, identifier_quoter),
        }
    }
}

