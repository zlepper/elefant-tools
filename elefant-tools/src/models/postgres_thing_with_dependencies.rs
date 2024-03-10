use crate::{ObjectId, PostgresAggregateFunction, PostgresFunction, PostgresSchema, PostgresTable, PostgresView};
use crate::object_id::HaveDependencies;

pub(crate) enum PostgresThingWithDependencies {
    Table(PostgresTable),
    View(PostgresView),
    Function(PostgresFunction),
    AggregateFunction(PostgresAggregateFunction)
}


impl HaveDependencies for &PostgresThingWithDependencies {
    fn depends_on(&self) -> &Vec<ObjectId> {
        match self {
            PostgresThingWithDependencies::Table(table) => &table.depends_on,
            PostgresThingWithDependencies::View(view) => &view.depends_on,
            PostgresThingWithDependencies::Function(function) => &function.depends_on,
            PostgresThingWithDependencies::AggregateFunction(aggregate_function) => &aggregate_function.depends_on,
        }
    }

    fn object_id(&self) -> ObjectId {
        match self {
            PostgresThingWithDependencies::Table(table) => table.object_id,
            PostgresThingWithDependencies::View(view) => view.object_id,
            PostgresThingWithDependencies::Function(function) => function.object_id,
            PostgresThingWithDependencies::AggregateFunction(aggregate_function) => aggregate_function.object_id,
        }
    }
}

impl PostgresThingWithDependencies {
    pub fn get_create_sql(&self, schema: &PostgresSchema, identifier_quoter: &crate::IdentifierQuoter) -> String {
        match self {
            PostgresThingWithDependencies::Table(table) => table.get_create_statement(schema, identifier_quoter),
            PostgresThingWithDependencies::View(view) => view.get_create_view_sql(schema, identifier_quoter),
            PostgresThingWithDependencies::Function(function) => function.get_create_statement(schema, identifier_quoter),
            PostgresThingWithDependencies::AggregateFunction(aggregate_function) => aggregate_function.get_create_statement(schema, identifier_quoter),
        }
    }
}

impl From<PostgresTable> for PostgresThingWithDependencies {
    fn from(value: PostgresTable) -> Self {
        PostgresThingWithDependencies::Table(value)
    }
}

impl From<PostgresView> for PostgresThingWithDependencies {
    fn from(value: PostgresView) -> Self {
        PostgresThingWithDependencies::View(value)
    }
}

impl From<PostgresFunction> for PostgresThingWithDependencies {
    fn from(value: PostgresFunction) -> Self {
        PostgresThingWithDependencies::Function(value)
    }
}

impl From<PostgresAggregateFunction> for PostgresThingWithDependencies {
    fn from(value: PostgresAggregateFunction) -> Self {
        PostgresThingWithDependencies::AggregateFunction(value)
    }
}
