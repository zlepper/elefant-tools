use super::*;
use crate::pool::ConnectionFactory;
use crate::postgres_client::PostgresClient;
use crate::tokio_connection::new_client;
use crate::PostgresConnectionSettings;

fn unique_suffix() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..12].to_string()
}

fn regular_settings() -> PostgresConnectionSettings {
    PostgresConnectionSettings::new("localhost")
        .port(5415)
        .password("passw0rd")
}

fn replication_settings() -> PostgresConnectionSettings {
    PostgresConnectionSettings::new("localhost")
        .port(5415)
        .password("passw0rd")
        .replication("database")
}

async fn create_slot_on_regular<F: ConnectionFactory>(
    client: &mut PostgresClient<F>,
    slot_name: &str,
) -> Lsn {
    let lsn_str: String = client
        .try_read_single_value_simple(&format!(
            "SELECT lsn::text FROM pg_create_logical_replication_slot('{slot_name}', 'pgoutput')"
        ))
        .await
        .unwrap();
    Lsn::from_pg_string(&lsn_str).unwrap()
}

async fn drop_slot_on_regular<F: ConnectionFactory>(
    client: &mut PostgresClient<F>,
    slot_name: &str,
) {
    let _ = client
        .execute_non_query_simple(&format!(
            "SELECT pg_drop_replication_slot('{slot_name}')"
        ))
        .await;
}

#[tokio::test]
async fn basic_logical_replication() {
    let sfx = unique_suffix();
    let table = format!("repl_test_basic_{sfx}");
    let slot = format!("test_slot_basic_{sfx}");
    let pub_name = format!("test_pub_basic_{sfx}");

    let mut regular = new_client(regular_settings()).await.unwrap();
    regular
        .execute_non_query_simple(&format!(
            "DROP PUBLICATION IF EXISTS {pub_name}; \
             DROP TABLE IF EXISTS {table}; \
             CREATE TABLE {table}(id int PRIMARY KEY, value text); \
             CREATE PUBLICATION {pub_name} FOR TABLE {table};"
        ))
        .await
        .unwrap();

    let consistent_lsn = create_slot_on_regular(&mut regular, &slot).await;

    regular
        .execute_non_query_simple(&format!(
            "INSERT INTO {table} VALUES (1, 'hello'), (2, 'world');"
        ))
        .await
        .unwrap();

    let mut repl = new_client(replication_settings()).await.unwrap();
    let mut stream = repl
        .start_replication(
            &slot,
            consistent_lsn,
            &format!("proto_version '1', publication_names '{pub_name}'"),
        )
        .await
        .unwrap();

    let mut saw_begin = false;
    let mut saw_commit = false;
    let mut relation_name = String::new();
    let mut relation_col_count = 0usize;
    let mut insert_values: Vec<(String, String)> = Vec::new();
    let mut last_lsn = consistent_lsn;

    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let msg = stream
                .next_message()
                .await
                .unwrap()
                .expect("unexpected end of stream");
            match msg {
                ReplicationMessage::XLogData(xlog) => {
                    last_lsn = xlog.end_lsn;
                    let pgmsg = parse_pgoutput_message(xlog.data).unwrap();
                    match pgmsg {
                        PgOutputMessage::Begin(_) => saw_begin = true,
                        PgOutputMessage::Relation(rel) => {
                            relation_name = rel.name.into_owned();
                            relation_col_count = rel.columns.len();
                        }
                        PgOutputMessage::Insert(ins) => {
                            let id = match &ins.tuple.columns[0] {
                                TupleColumn::Text(t) => t.to_string(),
                                _ => panic!("Expected text column for id"),
                            };
                            let val = match &ins.tuple.columns[1] {
                                TupleColumn::Text(t) => t.to_string(),
                                _ => panic!("Expected text column for value"),
                            };
                            insert_values.push((id, val));
                        }
                        PgOutputMessage::Commit(_) => {
                            saw_commit = true;
                            break;
                        }
                        _ => {}
                    }
                }
                ReplicationMessage::PrimaryKeepalive(_) => {}
            }
        }
    })
    .await;
    assert!(result.is_ok(), "Timed out waiting for replication messages");

    stream
        .send_status_update(last_lsn, last_lsn, last_lsn)
        .await
        .unwrap();

    assert!(saw_begin, "Should have seen BEGIN");
    assert!(saw_commit, "Should have seen COMMIT");
    assert_eq!(relation_name, table);
    assert_eq!(relation_col_count, 2);
    assert_eq!(
        insert_values,
        vec![
            ("1".to_string(), "hello".to_string()),
            ("2".to_string(), "world".to_string()),
        ]
    );

    drop(stream);
    drop(repl);
    drop_slot_on_regular(&mut regular, &slot).await;
    regular
        .execute_non_query_simple(&format!(
            "DROP PUBLICATION IF EXISTS {pub_name}; \
             DROP TABLE IF EXISTS {table};"
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn replication_handles_column_addition() {
    let sfx = unique_suffix();
    let table = format!("repl_test_addcol_{sfx}");
    let slot = format!("test_slot_addcol_{sfx}");
    let pub_name = format!("test_pub_addcol_{sfx}");

    let mut regular = new_client(regular_settings()).await.unwrap();
    regular
        .execute_non_query_simple(&format!(
            "DROP PUBLICATION IF EXISTS {pub_name}; \
             DROP TABLE IF EXISTS {table}; \
             CREATE TABLE {table}(id int PRIMARY KEY, value text); \
             CREATE PUBLICATION {pub_name} FOR TABLE {table};"
        ))
        .await
        .unwrap();

    let consistent_lsn = create_slot_on_regular(&mut regular, &slot).await;

    regular
        .execute_non_query_simple(&format!(
            "INSERT INTO {table} VALUES (1, 'before');"
        ))
        .await
        .unwrap();

    regular
        .execute_non_query_simple(&format!(
            "ALTER TABLE {table} ADD COLUMN extra text DEFAULT 'def';"
        ))
        .await
        .unwrap();

    regular
        .execute_non_query_simple(&format!(
            "INSERT INTO {table} VALUES (2, 'after', 'extra_val');"
        ))
        .await
        .unwrap();

    let mut repl = new_client(replication_settings()).await.unwrap();
    let mut stream = repl
        .start_replication(
            &slot,
            consistent_lsn,
            &format!("proto_version '1', publication_names '{pub_name}'"),
        )
        .await
        .unwrap();

    let mut relation_versions: Vec<(String, usize)> = Vec::new();
    let mut insert_col_counts: Vec<usize> = Vec::new();
    let mut commits_seen = 0u32;
    let mut last_lsn = consistent_lsn;

    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let msg = stream
                .next_message()
                .await
                .unwrap()
                .expect("unexpected end of stream");
            match msg {
                ReplicationMessage::XLogData(xlog) => {
                    last_lsn = xlog.end_lsn;
                    let pgmsg = parse_pgoutput_message(xlog.data).unwrap();
                    match pgmsg {
                        PgOutputMessage::Relation(rel) => {
                            relation_versions
                                .push((rel.name.into_owned(), rel.columns.len()));
                        }
                        PgOutputMessage::Insert(ins) => {
                            insert_col_counts.push(ins.tuple.columns.len());
                        }
                        PgOutputMessage::Commit(_) => {
                            commits_seen += 1;
                            if commits_seen >= 2 {
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                ReplicationMessage::PrimaryKeepalive(_) => {}
            }
        }
    })
    .await;
    assert!(result.is_ok(), "Timed out waiting for replication messages");

    stream
        .send_status_update(last_lsn, last_lsn, last_lsn)
        .await
        .unwrap();

    assert!(
        relation_versions.len() >= 2,
        "Expected at least 2 Relation messages, got {relation_versions:?}"
    );
    assert_eq!(relation_versions[0].1, 2, "First relation should have 2 columns");
    assert_eq!(
        relation_versions.last().unwrap().1,
        3,
        "Last relation should have 3 columns after ALTER TABLE ADD COLUMN"
    );

    assert_eq!(insert_col_counts.len(), 2);
    assert_eq!(insert_col_counts[0], 2);
    assert_eq!(insert_col_counts[1], 3);

    drop(stream);
    drop(repl);
    drop_slot_on_regular(&mut regular, &slot).await;
    regular
        .execute_non_query_simple(&format!(
            "DROP PUBLICATION IF EXISTS {pub_name}; \
             DROP TABLE IF EXISTS {table};"
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn replication_handles_column_removal() {
    let sfx = unique_suffix();
    let table = format!("repl_test_dropcol_{sfx}");
    let slot = format!("test_slot_dropcol_{sfx}");
    let pub_name = format!("test_pub_dropcol_{sfx}");

    let mut regular = new_client(regular_settings()).await.unwrap();
    regular
        .execute_non_query_simple(&format!(
            "DROP PUBLICATION IF EXISTS {pub_name}; \
             DROP TABLE IF EXISTS {table}; \
             CREATE TABLE {table}(id int PRIMARY KEY, old_col text, value text); \
             CREATE PUBLICATION {pub_name} FOR TABLE {table};"
        ))
        .await
        .unwrap();

    let consistent_lsn = create_slot_on_regular(&mut regular, &slot).await;

    regular
        .execute_non_query_simple(&format!(
            "INSERT INTO {table} VALUES (1, 'old_data', 'value1');"
        ))
        .await
        .unwrap();

    regular
        .execute_non_query_simple(&format!(
            "ALTER TABLE {table} DROP COLUMN old_col;"
        ))
        .await
        .unwrap();

    regular
        .execute_non_query_simple(&format!(
            "INSERT INTO {table} VALUES (2, 'value2');"
        ))
        .await
        .unwrap();

    let mut repl = new_client(replication_settings()).await.unwrap();
    let mut stream = repl
        .start_replication(
            &slot,
            consistent_lsn,
            &format!("proto_version '1', publication_names '{pub_name}'"),
        )
        .await
        .unwrap();

    let mut relation_col_counts: Vec<usize> = Vec::new();
    let mut relation_col_names: Vec<Vec<String>> = Vec::new();
    let mut insert_col_counts: Vec<usize> = Vec::new();
    let mut commits_seen = 0u32;
    let mut last_lsn = consistent_lsn;

    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let msg = stream
                .next_message()
                .await
                .unwrap()
                .expect("unexpected end of stream");
            match msg {
                ReplicationMessage::XLogData(xlog) => {
                    last_lsn = xlog.end_lsn;
                    let pgmsg = parse_pgoutput_message(xlog.data).unwrap();
                    match pgmsg {
                        PgOutputMessage::Relation(rel) => {
                            relation_col_counts.push(rel.columns.len());
                            relation_col_names.push(
                                rel.columns
                                    .iter()
                                    .map(|c| c.name.to_string())
                                    .collect(),
                            );
                        }
                        PgOutputMessage::Insert(ins) => {
                            insert_col_counts.push(ins.tuple.columns.len());
                        }
                        PgOutputMessage::Commit(_) => {
                            commits_seen += 1;
                            if commits_seen >= 2 {
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                ReplicationMessage::PrimaryKeepalive(_) => {}
            }
        }
    })
    .await;
    assert!(result.is_ok(), "Timed out waiting for replication messages");

    stream
        .send_status_update(last_lsn, last_lsn, last_lsn)
        .await
        .unwrap();

    assert!(
        relation_col_counts.len() >= 2,
        "Expected at least 2 Relation messages, got {relation_col_counts:?}"
    );
    assert_eq!(relation_col_counts[0], 3, "First relation should have 3 columns");
    assert_eq!(
        relation_col_counts.last().copied().unwrap(),
        2,
        "Last relation should have 2 columns after DROP COLUMN"
    );

    assert_eq!(
        relation_col_names[0],
        vec!["id", "old_col", "value"],
        "First relation should have id, old_col, value"
    );
    assert_eq!(
        relation_col_names.last().unwrap().as_slice(),
        &["id", "value"],
        "Last relation should have only id, value"
    );

    assert_eq!(insert_col_counts.len(), 2);
    assert_eq!(insert_col_counts[0], 3);
    assert_eq!(insert_col_counts[1], 2);

    drop(stream);
    drop(repl);
    drop_slot_on_regular(&mut regular, &slot).await;
    regular
        .execute_non_query_simple(&format!(
            "DROP PUBLICATION IF EXISTS {pub_name}; \
             DROP TABLE IF EXISTS {table};"
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn basic_logical_replication_binary() {
    let sfx = unique_suffix();
    let table = format!("repl_test_bin_{sfx}");
    let slot = format!("test_slot_bin_{sfx}");
    let pub_name = format!("test_pub_bin_{sfx}");

    let mut regular = new_client(regular_settings()).await.unwrap();
    regular
        .execute_non_query_simple(&format!(
            "DROP PUBLICATION IF EXISTS {pub_name}; \
             DROP TABLE IF EXISTS {table}; \
             CREATE TABLE {table}(id int PRIMARY KEY, value text, flag bool); \
             CREATE PUBLICATION {pub_name} FOR TABLE {table};"
        ))
        .await
        .unwrap();

    let consistent_lsn = create_slot_on_regular(&mut regular, &slot).await;

    regular
        .execute_non_query_simple(&format!(
            "INSERT INTO {table} VALUES (1, 'hello', true), (2, 'world', false);"
        ))
        .await
        .unwrap();

    let mut repl = new_client(replication_settings()).await.unwrap();
    let mut stream = repl
        .start_replication(
            &slot,
            consistent_lsn,
            &format!("proto_version '2', binary 'true', publication_names '{pub_name}'"),
        )
        .await
        .unwrap();

    let mut saw_begin = false;
    let mut saw_commit = false;
    let mut relation_name = String::new();
    let mut insert_values: Vec<(i32, String, bool)> = Vec::new();
    let mut last_lsn = consistent_lsn;

    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let msg = stream
                .next_message()
                .await
                .unwrap()
                .expect("unexpected end of stream");
            match msg {
                ReplicationMessage::XLogData(xlog) => {
                    last_lsn = xlog.end_lsn;
                    let pgmsg = parse_pgoutput_message(xlog.data).unwrap();
                    match pgmsg {
                        PgOutputMessage::Begin(_) => saw_begin = true,
                        PgOutputMessage::Relation(rel) => {
                            relation_name = rel.name.into_owned();
                        }
                        PgOutputMessage::Insert(ins) => {
                            let id = match &ins.tuple.columns[0] {
                                TupleColumn::Binary(b) => {
                                    i32::from_be_bytes((*b).try_into().unwrap())
                                }
                                other => {
                                    panic!("Expected Binary column for id, got {other:?}")
                                }
                            };
                            let value = match &ins.tuple.columns[1] {
                                TupleColumn::Binary(b) => {
                                    std::str::from_utf8(b).unwrap().to_string()
                                }
                                other => {
                                    panic!("Expected Binary column for value, got {other:?}")
                                }
                            };
                            let flag = match &ins.tuple.columns[2] {
                                TupleColumn::Binary(b) => b[0] != 0,
                                other => {
                                    panic!("Expected Binary column for flag, got {other:?}")
                                }
                            };
                            insert_values.push((id, value, flag));
                        }
                        PgOutputMessage::Commit(_) => {
                            saw_commit = true;
                            break;
                        }
                        _ => {}
                    }
                }
                ReplicationMessage::PrimaryKeepalive(_) => {}
            }
        }
    })
    .await;
    assert!(result.is_ok(), "Timed out waiting for replication messages");

    stream
        .send_status_update(last_lsn, last_lsn, last_lsn)
        .await
        .unwrap();

    assert!(saw_begin);
    assert!(saw_commit);
    assert_eq!(relation_name, table);
    assert_eq!(
        insert_values,
        vec![
            (1, "hello".to_string(), true),
            (2, "world".to_string(), false),
        ]
    );

    drop(stream);
    drop(repl);
    drop_slot_on_regular(&mut regular, &slot).await;
    regular
        .execute_non_query_simple(&format!(
            "DROP PUBLICATION IF EXISTS {pub_name}; \
             DROP TABLE IF EXISTS {table};"
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn replication_binary_handles_column_addition() {
    let sfx = unique_suffix();
    let table = format!("repl_test_binadd_{sfx}");
    let slot = format!("test_slot_binadd_{sfx}");
    let pub_name = format!("test_pub_binadd_{sfx}");

    let mut regular = new_client(regular_settings()).await.unwrap();
    regular
        .execute_non_query_simple(&format!(
            "DROP PUBLICATION IF EXISTS {pub_name}; \
             DROP TABLE IF EXISTS {table}; \
             CREATE TABLE {table}(id int PRIMARY KEY, value text); \
             CREATE PUBLICATION {pub_name} FOR TABLE {table};"
        ))
        .await
        .unwrap();

    let consistent_lsn = create_slot_on_regular(&mut regular, &slot).await;

    regular
        .execute_non_query_simple(&format!(
            "INSERT INTO {table} VALUES (1, 'before');"
        ))
        .await
        .unwrap();

    regular
        .execute_non_query_simple(&format!(
            "ALTER TABLE {table} ADD COLUMN extra int DEFAULT 42;"
        ))
        .await
        .unwrap();

    regular
        .execute_non_query_simple(&format!(
            "INSERT INTO {table} VALUES (2, 'after', 99);"
        ))
        .await
        .unwrap();

    let mut repl = new_client(replication_settings()).await.unwrap();
    let mut stream = repl
        .start_replication(
            &slot,
            consistent_lsn,
            &format!("proto_version '2', binary 'true', publication_names '{pub_name}'"),
        )
        .await
        .unwrap();

    let mut relation_col_counts: Vec<usize> = Vec::new();
    let mut insert_col_counts: Vec<usize> = Vec::new();
    let mut commits_seen = 0u32;
    let mut last_lsn = consistent_lsn;

    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let msg = stream
                .next_message()
                .await
                .unwrap()
                .expect("unexpected end of stream");
            match msg {
                ReplicationMessage::XLogData(xlog) => {
                    last_lsn = xlog.end_lsn;
                    let pgmsg = parse_pgoutput_message(xlog.data).unwrap();
                    match pgmsg {
                        PgOutputMessage::Relation(rel) => {
                            relation_col_counts.push(rel.columns.len());
                        }
                        PgOutputMessage::Insert(ins) => {
                            for col in &ins.tuple.columns {
                                assert!(
                                    matches!(col, TupleColumn::Binary(_)),
                                    "Expected Binary column in binary mode, got {col:?}"
                                );
                            }
                            insert_col_counts.push(ins.tuple.columns.len());
                        }
                        PgOutputMessage::Commit(_) => {
                            commits_seen += 1;
                            if commits_seen >= 2 {
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                ReplicationMessage::PrimaryKeepalive(_) => {}
            }
        }
    })
    .await;
    assert!(result.is_ok(), "Timed out waiting for replication messages");

    stream
        .send_status_update(last_lsn, last_lsn, last_lsn)
        .await
        .unwrap();

    assert!(relation_col_counts.len() >= 2);
    assert_eq!(relation_col_counts[0], 2);
    assert_eq!(relation_col_counts.last().copied().unwrap(), 3);

    assert_eq!(insert_col_counts.len(), 2);
    assert_eq!(insert_col_counts[0], 2);
    assert_eq!(insert_col_counts[1], 3);

    drop(stream);
    drop(repl);
    drop_slot_on_regular(&mut regular, &slot).await;
    regular
        .execute_non_query_simple(&format!(
            "DROP PUBLICATION IF EXISTS {pub_name}; \
             DROP TABLE IF EXISTS {table};"
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn replication_captures_updates() {
    let sfx = unique_suffix();
    let table = format!("repl_test_upd_{sfx}");
    let slot = format!("test_slot_upd_{sfx}");
    let pub_name = format!("test_pub_upd_{sfx}");

    let mut regular = new_client(regular_settings()).await.unwrap();
    regular
        .execute_non_query_simple(&format!(
            "DROP PUBLICATION IF EXISTS {pub_name}; \
             DROP TABLE IF EXISTS {table}; \
             CREATE TABLE {table}(id int PRIMARY KEY, value text); \
             ALTER TABLE {table} REPLICA IDENTITY FULL; \
             CREATE PUBLICATION {pub_name} FOR TABLE {table};"
        ))
        .await
        .unwrap();

    let consistent_lsn = create_slot_on_regular(&mut regular, &slot).await;

    regular
        .execute_non_query_simple(&format!(
            "INSERT INTO {table} VALUES (1, 'original'); \
             UPDATE {table} SET value = 'modified' WHERE id = 1;"
        ))
        .await
        .unwrap();

    let mut repl = new_client(replication_settings()).await.unwrap();
    let mut stream = repl
        .start_replication(
            &slot,
            consistent_lsn,
            &format!("proto_version '1', publication_names '{pub_name}'"),
        )
        .await
        .unwrap();

    let mut saw_update = false;
    let mut update_old_value: Option<String> = None;
    let mut update_new_value: Option<String> = None;
    let mut saw_commit = false;
    let mut last_lsn = consistent_lsn;

    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let msg = stream
                .next_message()
                .await
                .unwrap()
                .expect("unexpected end of stream");
            match msg {
                ReplicationMessage::XLogData(xlog) => {
                    last_lsn = xlog.end_lsn;
                    let pgmsg = parse_pgoutput_message(xlog.data).unwrap();
                    match pgmsg {
                        PgOutputMessage::Update(upd) => {
                            saw_update = true;
                            if let Some(ref old) = upd.old_tuple {
                                if let TupleColumn::Text(t) = &old.columns[1] {
                                    update_old_value = Some(t.to_string());
                                }
                            }
                            if let TupleColumn::Text(t) = &upd.new_tuple.columns[1] {
                                update_new_value = Some(t.to_string());
                            }
                        }
                        PgOutputMessage::Commit(_) => {
                            saw_commit = true;
                            break;
                        }
                        _ => {}
                    }
                }
                ReplicationMessage::PrimaryKeepalive(_) => {}
            }
        }
    })
    .await;
    assert!(result.is_ok(), "Timed out waiting for replication messages");

    stream
        .send_status_update(last_lsn, last_lsn, last_lsn)
        .await
        .unwrap();

    assert!(saw_update, "Should have seen UPDATE");
    assert!(saw_commit, "Should have seen COMMIT");
    assert_eq!(update_old_value.as_deref(), Some("original"));
    assert_eq!(update_new_value.as_deref(), Some("modified"));

    drop(stream);
    drop(repl);
    drop_slot_on_regular(&mut regular, &slot).await;
    regular
        .execute_non_query_simple(&format!(
            "DROP PUBLICATION IF EXISTS {pub_name}; \
             DROP TABLE IF EXISTS {table};"
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn replication_captures_deletes() {
    let sfx = unique_suffix();
    let table = format!("repl_test_del_{sfx}");
    let slot = format!("test_slot_del_{sfx}");
    let pub_name = format!("test_pub_del_{sfx}");

    let mut regular = new_client(regular_settings()).await.unwrap();
    regular
        .execute_non_query_simple(&format!(
            "DROP PUBLICATION IF EXISTS {pub_name}; \
             DROP TABLE IF EXISTS {table}; \
             CREATE TABLE {table}(id int PRIMARY KEY, value text); \
             ALTER TABLE {table} REPLICA IDENTITY FULL; \
             CREATE PUBLICATION {pub_name} FOR TABLE {table};"
        ))
        .await
        .unwrap();

    let consistent_lsn = create_slot_on_regular(&mut regular, &slot).await;

    regular
        .execute_non_query_simple(&format!(
            "INSERT INTO {table} VALUES (1, 'to_delete'); \
             DELETE FROM {table} WHERE id = 1;"
        ))
        .await
        .unwrap();

    let mut repl = new_client(replication_settings()).await.unwrap();
    let mut stream = repl
        .start_replication(
            &slot,
            consistent_lsn,
            &format!("proto_version '1', publication_names '{pub_name}'"),
        )
        .await
        .unwrap();

    let mut saw_delete = false;
    let mut deleted_id: Option<String> = None;
    let mut deleted_value: Option<String> = None;
    let mut saw_commit = false;
    let mut last_lsn = consistent_lsn;

    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let msg = stream
                .next_message()
                .await
                .unwrap()
                .expect("unexpected end of stream");
            match msg {
                ReplicationMessage::XLogData(xlog) => {
                    last_lsn = xlog.end_lsn;
                    let pgmsg = parse_pgoutput_message(xlog.data).unwrap();
                    match pgmsg {
                        PgOutputMessage::Delete(del) => {
                            saw_delete = true;
                            if let TupleColumn::Text(t) = &del.old_tuple.columns[0] {
                                deleted_id = Some(t.to_string());
                            }
                            if let TupleColumn::Text(t) = &del.old_tuple.columns[1] {
                                deleted_value = Some(t.to_string());
                            }
                        }
                        PgOutputMessage::Commit(_) => {
                            saw_commit = true;
                            break;
                        }
                        _ => {}
                    }
                }
                ReplicationMessage::PrimaryKeepalive(_) => {}
            }
        }
    })
    .await;
    assert!(result.is_ok(), "Timed out waiting for replication messages");

    stream
        .send_status_update(last_lsn, last_lsn, last_lsn)
        .await
        .unwrap();

    assert!(saw_delete, "Should have seen DELETE");
    assert!(saw_commit, "Should have seen COMMIT");
    assert_eq!(deleted_id.as_deref(), Some("1"));
    assert_eq!(deleted_value.as_deref(), Some("to_delete"));

    drop(stream);
    drop(repl);
    drop_slot_on_regular(&mut regular, &slot).await;
    regular
        .execute_non_query_simple(&format!(
            "DROP PUBLICATION IF EXISTS {pub_name}; \
             DROP TABLE IF EXISTS {table};"
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn replication_captures_truncate() {
    let sfx = unique_suffix();
    let table = format!("repl_test_trunc_{sfx}");
    let slot = format!("test_slot_trunc_{sfx}");
    let pub_name = format!("test_pub_trunc_{sfx}");

    let mut regular = new_client(regular_settings()).await.unwrap();
    regular
        .execute_non_query_simple(&format!(
            "DROP PUBLICATION IF EXISTS {pub_name}; \
             DROP TABLE IF EXISTS {table}; \
             CREATE TABLE {table}(id int PRIMARY KEY, value text); \
             CREATE PUBLICATION {pub_name} FOR TABLE {table};"
        ))
        .await
        .unwrap();

    let consistent_lsn = create_slot_on_regular(&mut regular, &slot).await;

    regular
        .execute_non_query_simple(&format!(
            "INSERT INTO {table} VALUES (1, 'a'), (2, 'b');"
        ))
        .await
        .unwrap();

    regular
        .execute_non_query_simple(&format!("TRUNCATE {table};"))
        .await
        .unwrap();

    let mut repl = new_client(replication_settings()).await.unwrap();
    let mut stream = repl
        .start_replication(
            &slot,
            consistent_lsn,
            &format!("proto_version '1', publication_names '{pub_name}'"),
        )
        .await
        .unwrap();

    let mut saw_truncate = false;
    let mut truncate_relation_count = 0usize;
    let mut commits_seen = 0u32;
    let mut last_lsn = consistent_lsn;

    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let msg = stream
                .next_message()
                .await
                .unwrap()
                .expect("unexpected end of stream");
            match msg {
                ReplicationMessage::XLogData(xlog) => {
                    last_lsn = xlog.end_lsn;
                    let pgmsg = parse_pgoutput_message(xlog.data).unwrap();
                    match pgmsg {
                        PgOutputMessage::Truncate(trunc) => {
                            saw_truncate = true;
                            truncate_relation_count = trunc.relation_ids.len();
                        }
                        PgOutputMessage::Commit(_) => {
                            commits_seen += 1;
                            if commits_seen >= 2 {
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                ReplicationMessage::PrimaryKeepalive(_) => {}
            }
        }
    })
    .await;
    assert!(result.is_ok(), "Timed out waiting for replication messages");

    stream
        .send_status_update(last_lsn, last_lsn, last_lsn)
        .await
        .unwrap();

    assert!(saw_truncate, "Should have seen TRUNCATE");
    assert_eq!(
        truncate_relation_count, 1,
        "TRUNCATE should reference 1 relation"
    );

    drop(stream);
    drop(repl);
    drop_slot_on_regular(&mut regular, &slot).await;
    regular
        .execute_non_query_simple(&format!(
            "DROP PUBLICATION IF EXISTS {pub_name}; \
             DROP TABLE IF EXISTS {table};"
        ))
        .await
        .unwrap();
}
