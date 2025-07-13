use crate::protocol::FieldDescription;
use crate::types::{FromSql, ToSql, PostgresType};
use std::error::Error;

/// PostgreSQL POINT geometric type representing x,y coordinates
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

impl Point {
    pub fn new(x: f64, y: f64) -> Self {
        Point { x, y }
    }
}

impl<'a> FromSql<'a> for Point {
    fn from_sql_binary(
        raw: &'a [u8],
        _field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // PostgreSQL POINT binary format: two consecutive float8 (f64) values
        if raw.len() != 16 {
            return Err(format!("Expected 16 bytes for POINT, got {}", raw.len()).into());
        }
        
        // Extract two big-endian f64 values (x, y coordinates)
        let x_bytes: [u8; 8] = raw[0..8].try_into()
            .map_err(|e| format!("Failed to extract x coordinate bytes: {}", e))?;
        let y_bytes: [u8; 8] = raw[8..16].try_into()
            .map_err(|e| format!("Failed to extract y coordinate bytes: {}", e))?;
        
        let x = f64::from_be_bytes(x_bytes);
        let y = f64::from_be_bytes(y_bytes);
        
        Ok(Point { x, y })
    }

    fn from_sql_text(
        raw: &'a str,
        _field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // PostgreSQL POINT text format: (x,y) 
        // In arrays, points may be quoted: "(x,y)"
        let trimmed = raw.trim();
        
        // Handle quoted points in arrays by stripping outer quotes
        let unquoted = if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
            &trimmed[1..trimmed.len()-1]
        } else {
            trimmed
        };
        
        if !unquoted.starts_with('(') || !unquoted.ends_with(')') {
            return Err(format!("Invalid POINT text format: '{}' - expected format: (x,y)", raw).into());
        }
        
        let inner = &unquoted[1..unquoted.len()-1];
        let parts: Vec<&str> = inner.split(',').collect();
        if parts.len() != 2 {
            return Err(format!("POINT must have exactly 2 coordinates, got {} in '{}'", parts.len(), raw).into());
        }
        
        let x: f64 = parts[0].trim().parse()
            .map_err(|e| format!("Failed to parse x coordinate '{}': {}", parts[0].trim(), e))?;
        let y: f64 = parts[1].trim().parse()
            .map_err(|e| format!("Failed to parse y coordinate '{}': {}", parts[1].trim(), e))?;
        
        Ok(Point { x, y })
    }

    fn accepts_postgres_type(oid: i32) -> bool {
        oid == PostgresType::POINT.oid
    }
}

impl ToSql for Point {
    fn to_sql_binary(
        &self,
        target_buffer: &mut Vec<u8>,
    ) -> Result<(), Box<dyn Error + Sync + Send>> {
        // Based on reverse engineering, assume two big-endian f64 values
        target_buffer.extend_from_slice(&self.x.to_be_bytes());
        target_buffer.extend_from_slice(&self.y.to_be_bytes());
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "tokio")]
    mod tokio_connection {
        use super::*;
        use crate::test_helpers::get_settings;
        use crate::tokio_connection::new_client;
        use tokio::test;

        #[test]
        async fn test_point_edge_cases() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test very large coordinates
            let large: Point = client
                .read_single_value("select '(1e10, -1e10)'::point;", &[])
                .await
                .unwrap();
            assert_eq!(large.x, 1e10);
            assert_eq!(large.y, -1e10);

            // Test very small coordinates
            let small: Point = client
                .read_single_value("select '(1e-10, -1e-10)'::point;", &[])
                .await
                .unwrap();
            assert_eq!(small.x, 1e-10);
            assert_eq!(small.y, -1e-10);

            // Test special float values
            let infinity: Point = client
                .read_single_value("select '(Infinity, -Infinity)'::point;", &[])
                .await
                .unwrap();
            assert!(infinity.x.is_infinite() && infinity.x.is_sign_positive());
            assert!(infinity.y.is_infinite() && infinity.y.is_sign_negative());
        }

        #[test]
        async fn test_point_basic_values() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test origin point
            let origin: Point = client
                .read_single_value("select '(0, 0)'::point;", &[])
                .await
                .unwrap();
            assert_eq!(origin.x, 0.0);
            assert_eq!(origin.y, 0.0);

            // Test positive coordinates
            let positive: Point = client
                .read_single_value("select '(1.5, 2.5)'::point;", &[])
                .await
                .unwrap();
            assert_eq!(positive.x, 1.5);
            assert_eq!(positive.y, 2.5);

            // Test negative coordinates
            let negative: Point = client
                .read_single_value("select '(-3.17, -2.71)'::point;", &[])
                .await
                .unwrap();
            assert_eq!(negative.x, -3.17);
            assert_eq!(negative.y, -2.71);
        }

        #[test]
        async fn test_point_round_trip() {
            let mut client = new_client(get_settings()).await.unwrap();

            client.execute_non_query("drop table if exists test_point_table; create table test_point_table(location point);", &[]).await.unwrap();

            let test_points = vec![
                Point::new(0.0, 0.0),
                Point::new(1.0, 1.0),
                Point::new(-1.0, -1.0),
                Point::new(123.456, 789.012),
                Point::new(-999.999, 123.123),
            ];

            for test_point in &test_points {
                client
                    .execute_non_query("insert into test_point_table values ($1);", &[test_point])
                    .await
                    .unwrap();

                let retrieved: Point = client
                    .read_single_value("select location from test_point_table order by location <-> point(0,0) limit 1;", &[])
                    .await
                    .unwrap();
                
                // Use approximate equality for floating point comparison
                assert!((retrieved.x - test_point.x).abs() < f64::EPSILON, 
                       "X coordinate mismatch: {} != {}", retrieved.x, test_point.x);
                assert!((retrieved.y - test_point.y).abs() < f64::EPSILON,
                       "Y coordinate mismatch: {} != {}", retrieved.y, test_point.y);
                
                // Clean up for next iteration
                client.execute_non_query("delete from test_point_table;", &[]).await.unwrap();
            }
        }

        #[test]
        async fn test_point_null_handling() {
            let mut client = new_client(get_settings()).await.unwrap();

            let null_point: Option<Point> = client
                .read_single_value("select null::point;", &[])
                .await
                .unwrap();
            assert_eq!(null_point, None);
        }

        #[test]
        async fn test_point_array_support() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test with the generic Vec<Point> that now handles comma conflicts correctly
            let point_array: Vec<Point> = client
                .read_single_value("select ARRAY[point(0,0), point(1,1), point(-1,-1)];", &[])
                .await
                .unwrap();
            
            let expected = vec![
                Point::new(0.0, 0.0),
                Point::new(1.0, 1.0),
                Point::new(-1.0, -1.0),
            ];
            
            assert_eq!(point_array.len(), expected.len());
            for (actual, expected) in point_array.iter().zip(expected.iter()) {
                assert!((actual.x - expected.x).abs() < f64::EPSILON);
                assert!((actual.y - expected.y).abs() < f64::EPSILON);
            }
        }
    }
}