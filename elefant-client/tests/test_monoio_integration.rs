#![cfg(feature = "monoio")]

use elefant_client::monoio_connection::new_client;
use elefant_client::{PostgresConnectionSettings, QueryResultSet};

fn get_settings() -> PostgresConnectionSettings {
    PostgresConnectionSettings {
        password: "passw0rd".to_string(),
        port: 5415,
        ..Default::default()
    }
}

#[monoio::test]
async fn test_monoio_basic_functionality() {
    let mut client = new_client(get_settings()).await.unwrap();

    // Test simple query
    let mut query_result = client
        .query("SELECT 42::int4 as answer", &[])
        .await
        .unwrap();
    let result_set = query_result.next_result_set().await.unwrap();

    match result_set {
        QueryResultSet::RowDescriptionReceived(mut row_reader) => {
            let row = row_reader.next_row().await.unwrap().unwrap();
            let value_bytes = row.get_some_bytes()[0].unwrap();
            let value_str = String::from_utf8(value_bytes.to_vec()).unwrap();
            assert_eq!(value_str, "42");
        }
        _ => panic!("Expected row data"),
    }
}

#[monoio::test]
async fn test_monoio_table_operations() {
    let mut client = new_client(get_settings()).await.unwrap();

    // Test table operations
    client
        .query("DROP TABLE IF EXISTS monoio_integration_test", &[])
        .await
        .unwrap();
    client
        .query(
            "CREATE TABLE monoio_integration_test (id INTEGER, data TEXT)",
            &[],
        )
        .await
        .unwrap();
    client
        .query(
            "INSERT INTO monoio_integration_test VALUES (1, 'hello'), (2, 'world')",
            &[],
        )
        .await
        .unwrap();

    let mut query_result = client
        .query(
            "SELECT id, data FROM monoio_integration_test ORDER BY id",
            &[],
        )
        .await
        .unwrap();
    let result_set = query_result.next_result_set().await.unwrap();

    match result_set {
        QueryResultSet::RowDescriptionReceived(mut row_reader) => {
            // Read first row
            let row1 = row_reader.next_row().await.unwrap().unwrap();
            let row1_data = row1.get_some_bytes();
            assert_eq!(
                String::from_utf8(row1_data[0].unwrap().to_vec()).unwrap(),
                "1"
            );
            assert_eq!(
                String::from_utf8(row1_data[1].unwrap().to_vec()).unwrap(),
                "hello"
            );

            // Read second row
            let row2 = row_reader.next_row().await.unwrap().unwrap();
            let row2_data = row2.get_some_bytes();
            assert_eq!(
                String::from_utf8(row2_data[0].unwrap().to_vec()).unwrap(),
                "2"
            );
            assert_eq!(
                String::from_utf8(row2_data[1].unwrap().to_vec()).unwrap(),
                "world"
            );

            // Verify no more rows
            assert!(row_reader.next_row().await.unwrap().is_none());
        }
        _ => panic!("Expected row data"),
    }

    client
        .query("DROP TABLE monoio_integration_test", &[])
        .await
        .unwrap();
}
