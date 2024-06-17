use crate::pg_interval::Interval;
use crate::schema_reader::tests::test_introspection;
use crate::test_helpers;
use crate::test_helpers::TestHelper;
use crate::TableTypeDetails::TimescaleHypertable;
use crate::{
    default, FunctionKind, HypertableCompression, HypertableCompressionOrderedColumn,
    HypertableDimension, HypertableRetention, PostgresColumn, PostgresDatabase, PostgresFunction,
    PostgresIndex, PostgresIndexColumnDirection, PostgresIndexKeyColumn, PostgresIndexNullsOrder,
    PostgresIndexType, PostgresSchema, PostgresTable, PostgresView, PostgresViewColumn,
    TableTypeDetails, TimescaleContinuousAggregateRefreshOptions, TimescaleDbUserDefinedJob,
    TimescaleSupport, ViewOptions,
};
use elefant_test_macros::pg_test;
use ordered_float::NotNan;

#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
async fn inspect_hypertable(helper: &TestHelper) {
    test_introspection(
        helper,
        r#"
CREATE TABLE stocks_real_time (
  time TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  price DOUBLE PRECISION NULL,
  day_volume INT NOT NULL
);

SELECT create_hypertable('stocks_real_time', by_range('time', '7 days'::interval));
SELECT add_dimension('stocks_real_time', by_hash('symbol', 4));
SELECT add_dimension('stocks_real_time', by_range('day_volume', 100));

insert into stocks_real_time (time, symbol, price, day_volume) values ('2023-01-01 00:00:00', 'AAPL', 100.0, 1000);

CREATE INDEX ix_symbol_time ON stocks_real_time (symbol, time DESC);
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                tables: vec![PostgresTable {
                    name: "stocks_real_time".to_string(),
                    columns: vec![
                        PostgresColumn {
                            name: "time".to_string(),
                            ordinal_position: 1,
                            is_nullable: false,
                            data_type: "timestamptz".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "symbol".to_string(),
                            ordinal_position: 2,
                            is_nullable: false,
                            data_type: "text".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "price".to_string(),
                            ordinal_position: 3,
                            is_nullable: true,
                            data_type: "float8".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "day_volume".to_string(),
                            ordinal_position: 4,
                            is_nullable: false,
                            data_type: "int4".to_string(),
                            ..default()
                        },
                    ],
                    indices: vec![PostgresIndex {
                        name: "ix_symbol_time".to_string(),
                        key_columns: vec![
                            PostgresIndexKeyColumn {
                                name: "symbol".to_string(),
                                ordinal_position: 1,
                                direction: Some(PostgresIndexColumnDirection::Ascending),
                                nulls_order: Some(PostgresIndexNullsOrder::Last),
                            },
                            PostgresIndexKeyColumn {
                                name: "\"time\"".to_string(),
                                ordinal_position: 2,
                                direction: Some(PostgresIndexColumnDirection::Descending),
                                nulls_order: Some(PostgresIndexNullsOrder::First),
                            },
                        ],
                        index_type: "btree".to_string(),
                        index_constraint_type: PostgresIndexType::Index,
                        ..default()
                    }, PostgresIndex {
                        name: "stocks_real_time_time_idx".to_string(),
                        key_columns: vec![
                            PostgresIndexKeyColumn {
                                name: "\"time\"".to_string(),
                                ordinal_position: 1,
                                direction: Some(PostgresIndexColumnDirection::Descending),
                                nulls_order: Some(PostgresIndexNullsOrder::First),
                            }
                        ],
                        index_type: "btree".to_string(),
                        index_constraint_type: PostgresIndexType::Index,
                        ..default()
                    },
                    ],
                    table_type: TableTypeDetails::TimescaleHypertable {
                        dimensions: vec![
                            HypertableDimension::Time {
                                column_name: "time".to_string(),
                                time_interval: Interval::new(0, 7, 0),
                            },
                            HypertableDimension::SpacePartitions {
                                column_name: "symbol".to_string(),
                                num_partitions: 4,
                            },
                            HypertableDimension::SpaceInterval {
                                column_name: "day_volume".to_string(),
                                integer_interval: 100,
                            },
                        ],
                        compression: None,
                        retention: None,
                    },
                    ..default()
                }],
                name: "public".to_string(),
                ..default()
            }],
            timescale_support: TimescaleSupport {
                is_enabled: true,
                timescale_toolkit_is_enabled: true,
                ..default()
            },
            ..default()
        },
    )
        .await;
}

#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
async fn inspect_compressed(helper: &TestHelper) {
    test_introspection(
        helper,
        r#"
CREATE TABLE stocks_real_time (
  time TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  price DOUBLE PRECISION NULL,
  day_volume INT NOT NULL
);

SELECT create_hypertable('stocks_real_time', by_range('time', '7 days'::interval));

alter table stocks_real_time set(
    timescaledb.compress,
        timescaledb.compress_segmentby = 'symbol',
        timescaledb.compress_orderby = 'time,day_volume',
        timescaledb.compress_chunk_time_interval='14 days'
        );

select add_compression_policy('stocks_real_time', interval '7 days');
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                tables: vec![PostgresTable {
                    name: "stocks_real_time".to_string(),
                    columns: vec![
                        PostgresColumn {
                            name: "time".to_string(),
                            ordinal_position: 1,
                            is_nullable: false,
                            data_type: "timestamptz".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "symbol".to_string(),
                            ordinal_position: 2,
                            is_nullable: false,
                            data_type: "text".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "price".to_string(),
                            ordinal_position: 3,
                            is_nullable: true,
                            data_type: "float8".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "day_volume".to_string(),
                            ordinal_position: 4,
                            is_nullable: false,
                            data_type: "int4".to_string(),
                            ..default()
                        },
                    ],
                    indices: vec![PostgresIndex {
                        name: "stocks_real_time_time_idx".to_string(),
                        key_columns: vec![PostgresIndexKeyColumn {
                            name: "\"time\"".to_string(),
                            ordinal_position: 1,
                            direction: Some(PostgresIndexColumnDirection::Descending),
                            nulls_order: Some(PostgresIndexNullsOrder::First),
                        }],
                        index_type: "btree".to_string(),
                        index_constraint_type: PostgresIndexType::Index,
                        ..default()
                    }],
                    table_type: TableTypeDetails::TimescaleHypertable {
                        dimensions: vec![HypertableDimension::Time {
                            column_name: "time".to_string(),
                            time_interval: Interval::new(0, 7, 0),
                        }],
                        compression: Some(HypertableCompression {
                            enabled: true,
                            segment_by_columns: Some(vec!["symbol".to_string()]),
                            order_by_columns: Some(vec![
                                HypertableCompressionOrderedColumn {
                                    column_name: "time".to_string(),
                                    nulls_first: false,
                                    descending: false,
                                },
                                HypertableCompressionOrderedColumn {
                                    column_name: "day_volume".to_string(),
                                    nulls_first: false,
                                    descending: false,
                                },
                            ]),
                            chunk_time_interval: Some(Interval::new(0, 14, 0)),
                            compression_schedule_interval: Some(Interval::new(0, 0, 43200000000)),
                            compress_after: Some(Interval::new(0, 7, 0)),
                        }),
                        retention: None,
                    },
                    ..default()
                }],
                name: "public".to_string(),
                ..default()
            }],
            timescale_support: TimescaleSupport {
                is_enabled: true,
                timescale_toolkit_is_enabled: true,
                ..default()
            },
            ..default()
        },
    )
    .await;
}

#[pg_test(arg(timescale_db = 15))]
async fn inspect_continuous_aggregates_15(helper: &TestHelper) {
    test_introspection(
        helper,
        r#"
CREATE TABLE stocks_real_time (
  time TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  price DOUBLE PRECISION NULL,
  day_volume INT NOT NULL
);

SELECT create_hypertable('stocks_real_time', by_range('time', '7 days'::interval));


CREATE MATERIALIZED VIEW stock_candlestick_daily
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 day', "time") AS day,
  symbol,
  max(price) AS high,
  first(price, time) AS open,
  last(price, time) AS close,
  min(price) AS low
FROM stocks_real_time srt
GROUP BY day, symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy('stock_candlestick_daily',
                                       start_offset => INTERVAL '6 month',
                                       end_offset => INTERVAL '1 day',
                                       schedule_interval => INTERVAL '1 hour');

alter materialized view stock_candlestick_daily set (timescaledb.compress = true);

SELECT add_compression_policy('stock_candlestick_daily', compress_after=>'360 days'::interval);
SELECT add_retention_policy('stock_candlestick_daily', INTERVAL '2 years');
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![PostgresTable {
                    name: "stocks_real_time".to_string(),
                    columns: vec![
                        PostgresColumn {
                            name: "time".to_string(),
                            ordinal_position: 1,
                            is_nullable: false,
                            data_type: "timestamptz".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "symbol".to_string(),
                            ordinal_position: 2,
                            is_nullable: false,
                            data_type: "text".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "price".to_string(),
                            ordinal_position: 3,
                            is_nullable: true,
                            data_type: "float8".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "day_volume".to_string(),
                            ordinal_position: 4,
                            is_nullable: false,
                            data_type: "int4".to_string(),
                            ..default()
                        },
                    ],
                    indices: vec![PostgresIndex {
                        name: "stocks_real_time_time_idx".to_string(),
                        key_columns: vec![PostgresIndexKeyColumn {
                            name: "\"time\"".to_string(),
                            ordinal_position: 1,
                            direction: Some(PostgresIndexColumnDirection::Descending),
                            nulls_order: Some(PostgresIndexNullsOrder::First),
                        }],
                        index_type: "btree".to_string(),
                        index_constraint_type: PostgresIndexType::Index,
                        ..default()
                    }],
                    table_type: TimescaleHypertable {
                        dimensions: vec![HypertableDimension::Time {
                            column_name: "time".to_string(),
                            time_interval: Interval::new(0, 7, 0),
                        }],
                        compression: None,
                        retention: None,
                    },
                    ..default()
                }],
                views: vec![PostgresView {
                    name: "stock_candlestick_daily".to_string(),
                    columns: vec![
                        PostgresViewColumn {
                            name: "day".to_string(),
                            ordinal_position: 1,
                        },
                        PostgresViewColumn {
                            name: "symbol".to_string(),
                            ordinal_position: 2,
                        },
                        PostgresViewColumn {
                            name: "high".to_string(),
                            ordinal_position: 3,
                        },
                        PostgresViewColumn {
                            name: "open".to_string(),
                            ordinal_position: 4,
                        },
                        PostgresViewColumn {
                            name: "close".to_string(),
                            ordinal_position: 5,
                        },
                        PostgresViewColumn {
                            name: "low".to_string(),
                            ordinal_position: 6,
                        },
                    ],
                    is_materialized: true,
                    definition: r#"SELECT time_bucket('1 day'::interval, srt."time") AS day,
    srt.symbol,
    max(srt.price) AS high,
    first(srt.price, srt."time") AS open,
    last(srt.price, srt."time") AS close,
    min(srt.price) AS low
   FROM stocks_real_time srt
  GROUP BY (time_bucket('1 day'::interval, srt."time")), srt.symbol;"#
                        .into(),
                    view_options: ViewOptions::TimescaleContinuousAggregate {
                        refresh: Some(TimescaleContinuousAggregateRefreshOptions {
                            start_offset: Interval::new(6, 0, 0),
                            end_offset: Interval::new(0, 1, 0),
                            interval: Interval::new(0, 0, 3600000000),
                        }),
                        compression: Some(HypertableCompression {
                            enabled: true,
                            segment_by_columns: Some(vec!["symbol".to_string()]),
                            order_by_columns: Some(vec![HypertableCompressionOrderedColumn {
                                column_name: "day".to_string(),
                                nulls_first: false,
                                descending: false,
                            }]),
                            chunk_time_interval: None,
                            compression_schedule_interval: Some(Interval::new(0, 0, 43200000000)),
                            compress_after: Some(Interval::new(0, 360, 0)),
                        }),
                        retention: Some(HypertableRetention {
                            schedule_interval: Interval::new(0, 1, 0),
                            drop_after: Interval::new(24, 0, 0),
                        }),
                    },
                    ..default()
                }],
                ..default()
            }],
            timescale_support: TimescaleSupport {
                is_enabled: true,
                timescale_toolkit_is_enabled: true,
                ..default()
            },
            ..default()
        },
    )
    .await;
}

#[pg_test(arg(timescale_db = 16))]
async fn inspect_continuous_aggregates_16(helper: &TestHelper) {
    test_introspection(
        helper,
        r#"
CREATE TABLE stocks_real_time (
  time TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  price DOUBLE PRECISION NULL,
  day_volume INT NOT NULL
);

SELECT create_hypertable('stocks_real_time', by_range('time', '7 days'::interval));


CREATE MATERIALIZED VIEW stock_candlestick_daily
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 day', "time") AS day,
  symbol,
  max(price) AS high,
  first(price, time) AS open,
  last(price, time) AS close,
  min(price) AS low
FROM stocks_real_time srt
GROUP BY day, symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy('stock_candlestick_daily',
                                       start_offset => INTERVAL '6 month',
                                       end_offset => INTERVAL '1 day',
                                       schedule_interval => INTERVAL '1 hour');

alter materialized view stock_candlestick_daily set (timescaledb.compress = true);

SELECT add_compression_policy('stock_candlestick_daily', compress_after=>'360 days'::interval);
SELECT add_retention_policy('stock_candlestick_daily', INTERVAL '2 years');
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![PostgresTable {
                    name: "stocks_real_time".to_string(),
                    columns: vec![
                        PostgresColumn {
                            name: "time".to_string(),
                            ordinal_position: 1,
                            is_nullable: false,
                            data_type: "timestamptz".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "symbol".to_string(),
                            ordinal_position: 2,
                            is_nullable: false,
                            data_type: "text".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "price".to_string(),
                            ordinal_position: 3,
                            is_nullable: true,
                            data_type: "float8".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "day_volume".to_string(),
                            ordinal_position: 4,
                            is_nullable: false,
                            data_type: "int4".to_string(),
                            ..default()
                        },
                    ],
                    indices: vec![PostgresIndex {
                        name: "stocks_real_time_time_idx".to_string(),
                        key_columns: vec![PostgresIndexKeyColumn {
                            name: "\"time\"".to_string(),
                            ordinal_position: 1,
                            direction: Some(PostgresIndexColumnDirection::Descending),
                            nulls_order: Some(PostgresIndexNullsOrder::First),
                        }],
                        index_type: "btree".to_string(),
                        index_constraint_type: PostgresIndexType::Index,
                        ..default()
                    }],
                    table_type: TimescaleHypertable {
                        dimensions: vec![HypertableDimension::Time {
                            column_name: "time".to_string(),
                            time_interval: Interval::new(0, 7, 0),
                        }],
                        compression: None,
                        retention: None,
                    },
                    ..default()
                }],
                views: vec![PostgresView {
                    name: "stock_candlestick_daily".to_string(),
                    columns: vec![
                        PostgresViewColumn {
                            name: "day".to_string(),
                            ordinal_position: 1,
                        },
                        PostgresViewColumn {
                            name: "symbol".to_string(),
                            ordinal_position: 2,
                        },
                        PostgresViewColumn {
                            name: "high".to_string(),
                            ordinal_position: 3,
                        },
                        PostgresViewColumn {
                            name: "open".to_string(),
                            ordinal_position: 4,
                        },
                        PostgresViewColumn {
                            name: "close".to_string(),
                            ordinal_position: 5,
                        },
                        PostgresViewColumn {
                            name: "low".to_string(),
                            ordinal_position: 6,
                        },
                    ],
                    is_materialized: true,
                    definition: r#"SELECT time_bucket('1 day'::interval, "time") AS day,
    symbol,
    max(price) AS high,
    first(price, "time") AS open,
    last(price, "time") AS close,
    min(price) AS low
   FROM stocks_real_time srt
  GROUP BY (time_bucket('1 day'::interval, "time")), symbol;"#
                        .into(),
                    view_options: ViewOptions::TimescaleContinuousAggregate {
                        refresh: Some(TimescaleContinuousAggregateRefreshOptions {
                            start_offset: Interval::new(6, 0, 0),
                            end_offset: Interval::new(0, 1, 0),
                            interval: Interval::new(0, 0, 3600000000),
                        }),
                        compression: Some(HypertableCompression {
                            enabled: true,
                            segment_by_columns: Some(vec!["symbol".to_string()]),
                            order_by_columns: Some(vec![HypertableCompressionOrderedColumn {
                                column_name: "day".to_string(),
                                nulls_first: false,
                                descending: false,
                            }]),
                            chunk_time_interval: None,
                            compression_schedule_interval: Some(Interval::new(0, 0, 43200000000)),
                            compress_after: Some(Interval::new(0, 360, 0)),
                        }),
                        retention: Some(HypertableRetention {
                            schedule_interval: Interval::new(0, 1, 0),
                            drop_after: Interval::new(24, 0, 0),
                        }),
                    },
                    ..default()
                }],
                ..default()
            }],
            timescale_support: TimescaleSupport {
                is_enabled: true,
                timescale_toolkit_is_enabled: true,
                ..default()
            },
            ..default()
        },
    )
    .await;
}

#[pg_test(arg(timescale_db = 15))]
async fn inspect_retention_policies(helper: &TestHelper) {
    test_introspection(
        helper,
        r#"
CREATE TABLE conditions (
  time TIMESTAMPTZ NOT NULL
);

SELECT create_hypertable('conditions', by_range('time', '1 hour'::interval));
SELECT add_retention_policy('conditions', INTERVAL '24 hours');
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![PostgresTable {
                    name: "conditions".to_string(),
                    columns: vec![PostgresColumn {
                        name: "time".to_string(),
                        ordinal_position: 1,
                        is_nullable: false,
                        data_type: "timestamptz".to_string(),
                        ..default()
                    }],
                    indices: vec![PostgresIndex {
                        name: "conditions_time_idx".to_string(),
                        key_columns: vec![PostgresIndexKeyColumn {
                            name: "\"time\"".to_string(),
                            ordinal_position: 1,
                            direction: Some(PostgresIndexColumnDirection::Descending),
                            nulls_order: Some(PostgresIndexNullsOrder::First),
                        }],
                        index_type: "btree".to_string(),
                        index_constraint_type: PostgresIndexType::Index,
                        ..default()
                    }],
                    table_type: TableTypeDetails::TimescaleHypertable {
                        dimensions: vec![HypertableDimension::Time {
                            column_name: "time".to_string(),
                            time_interval: Interval::new(0, 0, 3600000000),
                        }],
                        compression: None,
                        retention: Some(HypertableRetention {
                            drop_after: Interval::new(0, 0, 86400000000),
                            schedule_interval: Interval::new(0, 1, 0),
                        }),
                    },
                    ..default()
                }],
                ..default()
            }],
            timescale_support: TimescaleSupport {
                is_enabled: true,
                timescale_toolkit_is_enabled: true,
                ..default()
            },
            ..default()
        },
    )
    .await;
}

#[pg_test(arg(timescale_db = 15))]
async fn inspect_user_defined_jobs(helper: &TestHelper) {
    test_introspection(
        helper,
        r#"
CREATE PROCEDURE user_defined_action(job_id INT, config JSONB)
    LANGUAGE PLPGSQL AS
    $$
    BEGIN
        RAISE NOTICE 'Executing job % with config %', job_id, config;
    END
    $$;
    
SELECT add_job('user_defined_action', '1h', config => '{"hypertable":"metrics"}');
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                functions: vec![PostgresFunction {
                    function_name: "user_defined_action".to_string(),
                    language: "plpgsql".to_string(),
                    sql_body: r#"BEGIN
                        RAISE NOTICE 'Executing job % with config %', job_id, config;
                    END"#
                        .into(),
                    arguments: "IN job_id integer, IN config jsonb".to_string(),
                    estimated_cost: NotNan::new(100.0).unwrap(),
                    kind: FunctionKind::Procedure,
                    ..default()
                }],
                ..default()
            }],
            timescale_support: TimescaleSupport {
                is_enabled: true,
                timescale_toolkit_is_enabled: true,
                user_defined_jobs: vec![TimescaleDbUserDefinedJob {
                    function_name: "user_defined_action".to_string(),
                    function_schema: "public".to_string(),
                    schedule_interval: Interval::new(0, 0, 3600000000),
                    config: Some(r#"{"hypertable":"metrics"}"#.into()),
                    scheduled: true,
                    check_config_name: None,
                    check_config_schema: None,
                    fixed_schedule: true,
                    ..default()
                }],
            },
            ..default()
        },
    )
    .await;
}
