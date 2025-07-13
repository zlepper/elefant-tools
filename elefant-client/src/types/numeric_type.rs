use crate::protocol::FieldDescription;
use crate::types::{FromSql, ToSql};
use crate::types::PostgresType;
use rust_decimal::Decimal;
use std::error::Error;

// PostgreSQL NUMERIC type - arbitrary precision decimal values
// Binary format: ndigits (i16) + weight (i16) + sign (i16) + dscale (i16) + digits (array of i16)
impl<'a> FromSql<'a> for Decimal {
    fn from_sql_binary(
        raw: &'a [u8],
        _field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // PostgreSQL NUMERIC binary format:
        // - ndigits (i16): number of digits in the digits array
        // - weight (i16): weight of the first digit (base 10000)
        // - sign (i16): 0x0000 = positive, 0x4000 = negative, 0xC000 = NaN
        // - dscale (i16): display scale (decimal places)
        // - digits (array of i16): base-10000 digits
        
        if raw.len() < 8 {
            return Err("NUMERIC data too short".into());
        }
        
        // Parse header fields
        let ndigits = i16::from_be_bytes([raw[0], raw[1]]);
        let weight = i16::from_be_bytes([raw[2], raw[3]]);
        let sign = i16::from_be_bytes([raw[4], raw[5]]);
        let _dscale = i16::from_be_bytes([raw[6], raw[7]]);
        
        // Check for NaN
        if sign == 0xC000u16 as i16 {
            return Err("NUMERIC NaN values are not supported by rust_decimal".into());
        }
        
        // Validate sign
        let is_negative = match sign {
            0x0000 => false,
            0x4000 => true,
            _ => return Err(format!("Invalid NUMERIC sign: {sign:#x}").into()),
        };
        
        // Check we have enough data for all digits
        let expected_len = 8 + (ndigits as usize * 2);
        if raw.len() < expected_len {
            return Err(format!("NUMERIC data too short: expected {} bytes, got {}", expected_len, raw.len()).into());
        }
        
        // Parse digits (base-10000)
        let mut digits = Vec::with_capacity(ndigits as usize);
        for i in 0..ndigits {
            let offset = 8 + (i as usize * 2);
            let digit = i16::from_be_bytes([raw[offset], raw[offset + 1]]);
            if !(0..10000).contains(&digit) {
                return Err(format!("Invalid NUMERIC digit: {digit}").into());
            }
            digits.push(digit);
        }
        
        // Convert PostgreSQL base-10000 digits directly to rust_decimal
        // Build up the mantissa value directly without string conversion
        let mut mantissa: i128 = 0;
        let mantissa_scale;
        
        // Calculate the total decimal places this represents
        let digits_before_decimal = (weight + 1) as i32;
        let total_digit_positions = ndigits as i32;
        
        if digits_before_decimal <= 0 {
            // All digits are fractional
            // Scale = leading zeros + all digit positions
            mantissa_scale = (-digits_before_decimal * 4 + total_digit_positions * 4) as u32;
            
            // Build mantissa from the digits
            for &digit in &digits {
                mantissa = mantissa * 10000 + digit as i128;
            }
        } else if digits_before_decimal >= total_digit_positions {
            // All digits are before decimal point  
            mantissa_scale = 0;
            
            // Build mantissa from digits and add trailing zeros
            for &digit in &digits {
                mantissa = mantissa * 10000 + digit as i128;
            }
            
            // Add trailing zeros for extra weight positions
            let extra_zero_positions = (digits_before_decimal - total_digit_positions) * 4;
            for _ in 0..extra_zero_positions {
                mantissa *= 10;
            }
        } else {
            // Mixed: some before, some after decimal point
            mantissa_scale = ((total_digit_positions - digits_before_decimal) * 4) as u32;
            
            // Build mantissa from all digits
            for &digit in &digits {
                mantissa = mantissa * 10000 + digit as i128;
            }
        }
        
        // Apply sign
        if is_negative {
            mantissa = -mantissa;
        }
        
        // Create Decimal directly from mantissa and scale
        match Decimal::try_from_i128_with_scale(mantissa, mantissa_scale) {
            Ok(decimal) => Ok(decimal),
            Err(e) => Err(format!("Failed to create Decimal from mantissa {mantissa} with scale {mantissa_scale}: {e}").into()),
        }
    }

    fn from_sql_text(
        raw: &'a str,
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // NUMERIC text format is standard decimal notation
        match raw.parse::<Decimal>() {
            Ok(decimal) => Ok(decimal),
            Err(e) => Err(format!(
                "Failed to parse NUMERIC from text '{raw}': {e}. Error occurred when parsing field {field:?}"
            ).into()),
        }
    }

    fn accepts_postgres_type(oid: i32) -> bool {
        oid == PostgresType::NUMERIC.oid
    }
}

impl ToSql for Decimal {
    fn to_sql_binary(
        &self,
        target_buffer: &mut Vec<u8>,
    ) -> Result<(), Box<dyn Error + Sync + Send>> {
        // Handle zero case efficiently
        if self.is_zero() {
            target_buffer.extend_from_slice(&0i16.to_be_bytes()); // ndigits
            target_buffer.extend_from_slice(&0i16.to_be_bytes()); // weight 
            target_buffer.extend_from_slice(&0i16.to_be_bytes()); // sign (positive)
            target_buffer.extend_from_slice(&0i16.to_be_bytes()); // dscale
            return Ok(());
        }
        
        // Work directly with the internal representation
        let mantissa = self.mantissa().abs(); // Get absolute mantissa
        let scale = self.scale() as i16; // dscale value
        let is_negative = self.is_sign_negative();
        
        // The key insight: rust_decimal mantissa represents the number scaled by 10^scale
        // For example: 123.456 has mantissa=123456, scale=3
        // We need to convert this to PostgreSQL's base-10000 representation
        
        // Step 1: Extract decimal digits from mantissa using arithmetic (no strings!)
        let mut decimal_digits = Vec::new();
        let mut temp_mantissa = mantissa;
        
        if temp_mantissa == 0 {
            decimal_digits.push(0);
        } else {
            while temp_mantissa > 0 {
                decimal_digits.insert(0, (temp_mantissa % 10) as u8);
                temp_mantissa /= 10;
            }
        }
        
        // Step 2: Determine how many digits are before decimal point
        let total_decimal_digits = decimal_digits.len() as i16;
        let digits_before_decimal = total_decimal_digits - scale;
        
        // Step 3: Group decimal digits into base-10000 (4 decimal digits per group)
        let mut digits_10000 = Vec::new();
        
        // Process integer part (group from right to left to align with base-10000 boundaries)
        if digits_before_decimal > 0 {
            let mut i = digits_before_decimal as usize;
            while i > 0 {
                let start = i.saturating_sub(4);
                let mut group_value: i16 = 0;
                for digit_idx in start..i {
                    group_value = group_value * 10 + decimal_digits[digit_idx] as i16;
                }
                digits_10000.insert(0, group_value);
                i = start;
            }
        }
        
        // Process fractional part (group from left to right)
        if scale > 0 {
            let fractional_start = if digits_before_decimal > 0 { 
                digits_before_decimal as usize 
            } else { 
                0 
            };
            
            let mut i = fractional_start;
            while i < decimal_digits.len() {
                let end = std::cmp::min(i + 4, decimal_digits.len());
                let mut group_value: i16 = 0;
                
                // Build the group value
                for digit_idx in i..end {
                    group_value = group_value * 10 + decimal_digits[digit_idx] as i16;
                }
                
                // Pad with zeros for fractional part (base-10000 groups must represent 4 decimal places)
                let digits_in_group = end - i;
                for _ in digits_in_group..4 {
                    group_value *= 10;
                }
                
                digits_10000.push(group_value);
                i += 4;
            }
        }
        
        // Calculate PostgreSQL weight (position of most significant base-10000 digit)
        let weight = if digits_before_decimal <= 0 {
            // Pure fractional number - weight is negative
            let leading_zero_groups = (-digits_before_decimal + 3) / 4;
            -(leading_zero_groups + 1)
        } else {
            // Has integer part
            ((digits_before_decimal + 3) / 4) - 1
        };
        
        // Remove trailing zero digits (but preserve at least one digit for fractional numbers)
        while digits_10000.len() > 1 && digits_10000.last() == Some(&0) {
            digits_10000.pop();
        }
        
        let ndigits = digits_10000.len() as i16;
        let sign = if is_negative { 0x4000u16 as i16 } else { 0x0000i16 };
        
        // Write PostgreSQL NUMERIC binary format
        target_buffer.extend_from_slice(&ndigits.to_be_bytes());
        target_buffer.extend_from_slice(&weight.to_be_bytes());
        target_buffer.extend_from_slice(&sign.to_be_bytes());
        target_buffer.extend_from_slice(&scale.to_be_bytes()); // dscale
        
        for digit in digits_10000 {
            target_buffer.extend_from_slice(&digit.to_be_bytes());
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    #[cfg(feature = "tokio")]
    mod tokio_connection {
        use super::*;
        use crate::test_helpers::get_settings;
        use crate::tokio_connection::new_client;
        use tokio::test;

        #[test]
        async fn test_numeric_basic_values() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test zero
            let zero: Decimal = client
                .read_single_value("select 0::numeric;", &[])
                .await
                .unwrap();
            assert_eq!(zero, Decimal::from(0));

            // Test positive integer
            let positive: Decimal = client
                .read_single_value("select 12345::numeric;", &[])
                .await
                .unwrap();
            assert_eq!(positive, Decimal::from(12345));

            // Test negative integer
            let negative: Decimal = client
                .read_single_value("select -67890::numeric;", &[])
                .await
                .unwrap();
            assert_eq!(negative, Decimal::from(-67890));

            // Test decimal
            let decimal: Decimal = client
                .read_single_value("select 123.456::numeric;", &[])
                .await
                .unwrap();
            assert_eq!(decimal, "123.456".parse::<Decimal>().unwrap());
        }

        #[test]
        async fn test_numeric_precision_scale() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test high precision
            let high_precision: Decimal = client
                .read_single_value("select 123456789.123456789::numeric(18,9);", &[])
                .await
                .unwrap();
            assert_eq!(high_precision, "123456789.123456789".parse::<Decimal>().unwrap());

            // Test many decimal places
            let many_decimals: Decimal = client
                .read_single_value("select 1.000000001::numeric(10,9);", &[])
                .await
                .unwrap();
            assert_eq!(many_decimals, "1.000000001".parse::<Decimal>().unwrap());

            // Test large integer
            let large_int: Decimal = client
                .read_single_value("select 999999999999999999::numeric;", &[])
                .await
                .unwrap();
            assert_eq!(large_int, "999999999999999999".parse::<Decimal>().unwrap());
        }

        #[test]
        async fn test_numeric_postgresql_direct() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test what PostgreSQL actually sends for small decimals
            let small_decimal: Decimal = client
                .read_single_value("select 0.000000001::numeric;", &[])
                .await
                .unwrap();
            
            let expected = "0.000000001".parse::<Decimal>().unwrap();
            assert_eq!(small_decimal, expected, "Direct PostgreSQL read failed");
        }

        #[test]
        async fn test_numeric_small_decimal_round_trip() {
            let mut client = new_client(get_settings()).await.unwrap();

            client.execute_non_query("drop table if exists test_numeric_debug; create table test_numeric_debug(value numeric);", &[]).await.unwrap();

            // Test the failing case
            let test_value = "0.000000001".parse::<Decimal>().unwrap();
            
            client
                .execute_non_query("insert into test_numeric_debug values ($1);", &[&test_value])
                .await
                .unwrap();

            let retrieved: Decimal = client
                .read_single_value("select value from test_numeric_debug;", &[])
                .await
                .unwrap();
            
            assert_eq!(retrieved, test_value, "Small decimal round-trip failed for {test_value}");
        }

        #[test]
        async fn test_numeric_round_trip() {
            let mut client = new_client(get_settings()).await.unwrap();

            client.execute_non_query("drop table if exists test_numeric_table; create table test_numeric_table(value numeric);", &[]).await.unwrap();

            let test_values = vec![
                "0".parse::<Decimal>().unwrap(),
                "123.456".parse::<Decimal>().unwrap(),
                "-789.012".parse::<Decimal>().unwrap(),
                "999999999.999999999".parse::<Decimal>().unwrap(),
                "0.000000001".parse::<Decimal>().unwrap(),
                "1000000000".parse::<Decimal>().unwrap(),
            ];

            for test_value in &test_values {
                client
                    .execute_non_query("insert into test_numeric_table values ($1);", &[test_value])
                    .await
                    .unwrap();

                let retrieved: Decimal = client
                    .read_single_value("select value from test_numeric_table order by value desc limit 1;", &[])
                    .await
                    .unwrap();
                
                assert_eq!(&retrieved, test_value, "Round-trip failed for {test_value}");
                
                // Clean up for next iteration
                client.execute_non_query("delete from test_numeric_table;", &[]).await.unwrap();
            }
        }

        #[test]
        async fn test_numeric_null_handling() {
            let mut client = new_client(get_settings()).await.unwrap();

            let null_value: Option<Decimal> = client
                .read_single_value("select null::numeric;", &[])
                .await
                .unwrap();
            assert_eq!(null_value, None);
        }

        #[test]
        async fn test_numeric_edge_cases() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test very small number
            let small: Decimal = client
                .read_single_value("select 0.0001::numeric;", &[])
                .await
                .unwrap();
            assert_eq!(small, "0.0001".parse::<Decimal>().unwrap());

            // Test number with trailing zeros
            let trailing_zeros: Decimal = client
                .read_single_value("select 123.4500::numeric;", &[])
                .await
                .unwrap();
            assert_eq!(trailing_zeros, "123.45".parse::<Decimal>().unwrap()); // PostgreSQL should normalize
        }

        #[test]
        async fn test_numeric_error_handling() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test NaN should return an error
            let nan_result = client
                .read_single_value::<Decimal>("select 'NaN'::numeric;", &[])
                .await;
            assert!(nan_result.is_err(), "Expected error for NaN NUMERIC");
            assert!(nan_result.unwrap_err().to_string().contains("NaN"));

            // Test very large numbers that might exceed rust_decimal precision
            // rust_decimal supports up to 28 digits of precision
            let large_result = client
                .read_single_value::<Decimal>("select 99999999999999999999999999999999999999.999999999999999999999999999999999999::numeric;", &[])
                .await;
            // This might succeed or fail depending on rust_decimal's limits - we just want to ensure it doesn't panic
            match large_result {
                Ok(_) => {}, // Fine if it works
                Err(e) => {
                    // Should be a clean error, not a panic
                    println!("Large number error (expected): {e}");
                }
            }
        }

        #[test]
        async fn test_numeric_array_support() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test that reading numeric arrays works (PostgreSQL arrays automatically supported)
            let numeric_array: Vec<Decimal> = client
                .read_single_value("select ARRAY[0::numeric, 123.456::numeric, -789.012::numeric, 0.000000001::numeric];", &[])
                .await
                .unwrap();
            
            let expected = vec![
                "0".parse::<Decimal>().unwrap(),
                "123.456".parse::<Decimal>().unwrap(),
                "-789.012".parse::<Decimal>().unwrap(),
                "0.000000001".parse::<Decimal>().unwrap(),
            ];
            
            assert_eq!(numeric_array, expected);
        }
    }
}