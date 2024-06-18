use crate::schema_reader::tests::test_introspection;
use crate::test_helpers;
use crate::test_helpers::TestHelper;
use crate::{
    default, PartitionedTableColumns, PostgresColumn, PostgresDatabase, PostgresSchema,
    PostgresTable, TablePartitionStrategy, TableTypeDetails, TimescaleSupport,
};
use elefant_test_macros::pg_test;

#[pg_test(arg(postgres = 12))]
#[pg_test(arg(postgres = 13))]
#[pg_test(arg(postgres = 14))]
#[pg_test(arg(postgres = 15))]
#[pg_test(arg(postgres = 16))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
#[pg_test(arg(pg_bouncer = 15))]
async fn range_partitions(helper: &TestHelper) {
    test_introspection(
        helper,
        r#"
CREATE TABLE sales (
                       sale_id INT,
                       sale_date DATE,
                       product_id INT,
                       quantity INT,
                       amount NUMERIC
) partition by range (sale_date);

CREATE TABLE sales_january PARTITION OF sales
    FOR VALUES FROM ('2023-01-01') TO ('2023-02-01');

CREATE TABLE sales_february PARTITION OF sales
    FOR VALUES FROM ('2023-02-01') TO ('2023-03-01');

CREATE TABLE sales_march PARTITION OF sales
    FOR VALUES FROM ('2023-03-01') TO ('2023-04-01');
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![
                    PostgresTable {
                        name: "sales".to_string(),
                        object_id: 2.into(),
                        columns: vec![
                            PostgresColumn {
                                name: "sale_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "sale_date".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "date".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "product_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "quantity".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "amount".to_string(),
                                is_nullable: true,
                                ordinal_position: 5,
                                data_type: "numeric".to_string(),
                                ..default()
                            },
                        ],
                        table_type: TableTypeDetails::PartitionedParentTable {
                            partition_columns: PartitionedTableColumns::Columns(vec![
                                "sale_date".to_string()
                            ]),
                            default_partition_name: None,
                            partition_strategy: TablePartitionStrategy::Range,
                        },
                        ..default()
                    },
                    PostgresTable {
                        name: "sales_february".to_string(),
                        table_type: TableTypeDetails::PartitionedChildTable {
                            partition_expression:
                                "FOR VALUES FROM ('2023-02-01') TO ('2023-03-01')".to_string(),
                            parent_table: "sales".to_string(),
                        },
                        depends_on: vec![2.into()],
                        columns: vec![
                            PostgresColumn {
                                name: "sale_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "sale_date".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "date".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "product_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "quantity".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "amount".to_string(),
                                is_nullable: true,
                                ordinal_position: 5,
                                data_type: "numeric".to_string(),
                                ..default()
                            },
                        ],
                        ..default()
                    },
                    PostgresTable {
                        name: "sales_january".to_string(),
                        table_type: TableTypeDetails::PartitionedChildTable {
                            partition_expression:
                                "FOR VALUES FROM ('2023-01-01') TO ('2023-02-01')".to_string(),
                            parent_table: "sales".to_string(),
                        },
                        depends_on: vec![2.into()],
                        columns: vec![
                            PostgresColumn {
                                name: "sale_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "sale_date".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "date".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "product_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "quantity".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "amount".to_string(),
                                is_nullable: true,
                                ordinal_position: 5,
                                data_type: "numeric".to_string(),
                                ..default()
                            },
                        ],
                        ..default()
                    },
                    PostgresTable {
                        name: "sales_march".to_string(),
                        table_type: TableTypeDetails::PartitionedChildTable {
                            partition_expression:
                                "FOR VALUES FROM ('2023-03-01') TO ('2023-04-01')".to_string(),
                            parent_table: "sales".to_string(),
                        },
                        depends_on: vec![2.into()],
                        columns: vec![
                            PostgresColumn {
                                name: "sale_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "sale_date".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "date".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "product_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "quantity".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "amount".to_string(),
                                is_nullable: true,
                                ordinal_position: 5,
                                data_type: "numeric".to_string(),
                                ..default()
                            },
                        ],
                        ..default()
                    },
                ],
                ..default()
            }],
            timescale_support: TimescaleSupport::from_test_helper(helper),
            ..default()
        },
    )
    .await;
}

#[pg_test(arg(postgres = 12))]
#[pg_test(arg(postgres = 13))]
#[pg_test(arg(postgres = 14))]
#[pg_test(arg(postgres = 15))]
#[pg_test(arg(postgres = 16))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
#[pg_test(arg(pg_bouncer = 15))]
async fn list_partitions(helper: &TestHelper) {
    test_introspection(
        helper,
        r#"
CREATE TABLE products (
    product_id int,
    category TEXT,
    product_name TEXT,
    price NUMERIC
) partition by list(category);

CREATE TABLE electronics PARTITION OF products
    FOR VALUES IN ('Electronics');

CREATE TABLE clothing PARTITION OF products
    FOR VALUES IN ('Clothing');

CREATE TABLE furniture PARTITION OF products
    FOR VALUES IN ('Furniture');
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![
                    PostgresTable {
                        name: "clothing".to_string(),
                        table_type: TableTypeDetails::PartitionedChildTable {
                            partition_expression: "FOR VALUES IN ('Clothing')".to_string(),
                            parent_table: "products".to_string(),
                        },
                        columns: vec![
                            PostgresColumn {
                                name: "product_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "category".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "text".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "product_name".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "text".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "price".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "numeric".to_string(),
                                ..default()
                            },
                        ],
                        depends_on: vec![5.into()],
                        ..default()
                    },
                    PostgresTable {
                        name: "electronics".to_string(),
                        table_type: TableTypeDetails::PartitionedChildTable {
                            partition_expression: "FOR VALUES IN ('Electronics')".to_string(),
                            parent_table: "products".to_string(),
                        },
                        columns: vec![
                            PostgresColumn {
                                name: "product_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "category".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "text".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "product_name".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "text".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "price".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "numeric".to_string(),
                                ..default()
                            },
                        ],
                        depends_on: vec![5.into()],
                        ..default()
                    },
                    PostgresTable {
                        name: "furniture".to_string(),
                        table_type: TableTypeDetails::PartitionedChildTable {
                            partition_expression: "FOR VALUES IN ('Furniture')".to_string(),
                            parent_table: "products".to_string(),
                        },
                        columns: vec![
                            PostgresColumn {
                                name: "product_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "category".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "text".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "product_name".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "text".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "price".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "numeric".to_string(),
                                ..default()
                            },
                        ],
                        depends_on: vec![5.into()],
                        ..default()
                    },
                    PostgresTable {
                        name: "products".to_string(),
                        object_id: 5.into(),
                        columns: vec![
                            PostgresColumn {
                                name: "product_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "category".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "text".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "product_name".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "text".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "price".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "numeric".to_string(),
                                ..default()
                            },
                        ],
                        table_type: TableTypeDetails::PartitionedParentTable {
                            partition_strategy: TablePartitionStrategy::List,
                            default_partition_name: None,
                            partition_columns: PartitionedTableColumns::Columns(vec![
                                "category".to_string()
                            ]),
                        },
                        ..default()
                    },
                ],
                ..default()
            }],
            timescale_support: TimescaleSupport::from_test_helper(helper),
            ..default()
        },
    )
    .await;
}

#[pg_test(arg(postgres = 12))]
#[pg_test(arg(postgres = 13))]
#[pg_test(arg(postgres = 14))]
#[pg_test(arg(postgres = 15))]
#[pg_test(arg(postgres = 16))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
#[pg_test(arg(pg_bouncer = 15))]
async fn hash_partitions(helper: &TestHelper) {
    test_introspection(
        helper,
        r#"
CREATE TABLE orders (
    order_id int,
    order_date DATE,
    customer_id INT,
    total_amount NUMERIC
) partition by hash(customer_id);

CREATE TABLE orders_1 PARTITION OF orders
    FOR VALUES WITH (MODULUS 3, REMAINDER 0);

CREATE TABLE orders_2 PARTITION OF orders
    FOR VALUES WITH (MODULUS 3, REMAINDER 1);

CREATE TABLE orders_3 PARTITION OF orders
    FOR VALUES WITH (MODULUS 3, REMAINDER 2);
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![
                    PostgresTable {
                        name: "orders".to_string(),
                        columns: vec![
                            PostgresColumn {
                                name: "order_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "order_date".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "date".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "customer_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "total_amount".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "numeric".to_string(),
                                ..default()
                            },
                        ],
                        object_id: 2.into(),
                        table_type: TableTypeDetails::PartitionedParentTable {
                            partition_strategy: TablePartitionStrategy::Hash,
                            default_partition_name: None,
                            partition_columns: PartitionedTableColumns::Columns(vec![
                                "customer_id".to_string(),
                            ]),
                        },
                        ..default()
                    },
                    PostgresTable {
                        name: "orders_1".to_string(),
                        table_type: TableTypeDetails::PartitionedChildTable {
                            partition_expression: "FOR VALUES WITH (modulus 3, remainder 0)"
                                .to_string(),
                            parent_table: "orders".to_string(),
                        },
                        depends_on: vec![2.into()],
                        columns: vec![
                            PostgresColumn {
                                name: "order_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "order_date".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "date".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "customer_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "total_amount".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "numeric".to_string(),
                                ..default()
                            },
                        ],
                        ..default()
                    },
                    PostgresTable {
                        name: "orders_2".to_string(),
                        table_type: TableTypeDetails::PartitionedChildTable {
                            partition_expression: "FOR VALUES WITH (modulus 3, remainder 1)"
                                .to_string(),
                            parent_table: "orders".to_string(),
                        },
                        depends_on: vec![2.into()],
                        columns: vec![
                            PostgresColumn {
                                name: "order_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "order_date".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "date".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "customer_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "total_amount".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "numeric".to_string(),
                                ..default()
                            },
                        ],
                        ..default()
                    },
                    PostgresTable {
                        name: "orders_3".to_string(),
                        table_type: TableTypeDetails::PartitionedChildTable {
                            partition_expression: "FOR VALUES WITH (modulus 3, remainder 2)"
                                .to_string(),
                            parent_table: "orders".to_string(),
                        },
                        depends_on: vec![2.into()],
                        columns: vec![
                            PostgresColumn {
                                name: "order_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "order_date".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "date".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "customer_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "total_amount".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "numeric".to_string(),
                                ..default()
                            },
                        ],
                        ..default()
                    },
                ],
                ..default()
            }],
            timescale_support: TimescaleSupport::from_test_helper(helper),
            ..default()
        },
    )
    .await;
}
