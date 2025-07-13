use crate::protocol::FieldDescription;
use crate::types::{FromSql, ToSql, PostgresType};
use std::error::Error;
use std::net::IpAddr;

/// PostgreSQL INET type representing IP addresses with optional subnet mask
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inet {
    pub ip: IpAddr,
    pub prefix_len: Option<u8>,
}

impl Inet {
    pub fn new(ip: IpAddr) -> Self {
        Inet { ip, prefix_len: None }
    }
    
    pub fn with_prefix(ip: IpAddr, prefix_len: u8) -> Self {
        Inet { ip, prefix_len: Some(prefix_len) }
    }
}

impl std::fmt::Display for Inet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.prefix_len {
            Some(prefix) => write!(f, "{}/{}", self.ip, prefix),
            None => write!(f, "{}", self.ip),
        }
    }
}

impl std::str::FromStr for Inet {
    type Err = Box<dyn Error + Sync + Send>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((ip_str, prefix_str)) = s.split_once('/') {
            let ip: IpAddr = ip_str.parse()?;
            let prefix_len: u8 = prefix_str.parse()?;
            Ok(Inet::with_prefix(ip, prefix_len))
        } else {
            let ip: IpAddr = s.parse()?;
            Ok(Inet::new(ip))
        }
    }
}

impl<'a> FromSql<'a> for Inet {
    fn from_sql_binary(
        raw: &'a [u8],
        _field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() < 4 {
            return Err(format!("INET binary data too short: {} bytes", raw.len()).into());
        }

        let family = raw[0];
        let bits = raw[1];
        let _is_cidr = raw[2]; // Seems to always be 0
        let addr_size = raw[3];

        if raw.len() < 4 + addr_size as usize {
            return Err(format!("INET binary data incomplete: expected {addr_size} address bytes").into());
        }

        let addr_bytes = &raw[4..4 + addr_size as usize];
        
        // Determine prefix length - if bits equals full address size, no CIDR prefix
        let prefix_len = match family {
            2 => if bits == 32 { None } else { Some(bits) }, // IPv4
            3 => if bits == 128 { None } else { Some(bits) }, // IPv6
            _ => return Err(format!("Unknown address family: {family}").into()),
        };

        let ip = match family {
            2 => {
                // IPv4
                if addr_size != 4 {
                    return Err(format!("Invalid IPv4 address size: {addr_size}").into());
                }
                let ipv4_bytes: [u8; 4] = addr_bytes.try_into()
                    .map_err(|_| "Failed to convert IPv4 bytes")?;
                IpAddr::V4(std::net::Ipv4Addr::from(ipv4_bytes))
            }
            3 => {
                // IPv6
                if addr_size != 16 {
                    return Err(format!("Invalid IPv6 address size: {addr_size}").into());
                }
                let ipv6_bytes: [u8; 16] = addr_bytes.try_into()
                    .map_err(|_| "Failed to convert IPv6 bytes")?;
                IpAddr::V6(std::net::Ipv6Addr::from(ipv6_bytes))
            }
            _ => return Err(format!("Unsupported address family: {family}").into()),
        };

        Ok(Inet { ip, prefix_len })
    }

    fn from_sql_text(
        raw: &'a str,
        _field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // PostgreSQL INET text format: "IP" or "IP/prefix"
        raw.trim().parse()
    }

    fn accepts_postgres_type(oid: i32) -> bool {
        oid == PostgresType::INET.oid || oid == PostgresType::CIDR.oid
    }
}

impl ToSql for Inet {
    fn to_sql_binary(
        &self,
        target_buffer: &mut Vec<u8>,
    ) -> Result<(), Box<dyn Error + Sync + Send>> {
        // Implement PostgreSQL INET binary format
        // Format: [family][bits][is_cidr][addr_size][address_bytes...]
        
        match self.ip {
            IpAddr::V4(ipv4) => {
                target_buffer.push(2); // Family: IPv4 = 2
                
                // Bits: use prefix_len if present, otherwise 32 for full address
                let bits = self.prefix_len.unwrap_or(32);
                target_buffer.push(bits);
                
                target_buffer.push(0); // is_cidr flag (always 0 based on observations)
                target_buffer.push(4); // Address size: IPv4 = 4 bytes
                
                // IPv4 address bytes
                target_buffer.extend_from_slice(&ipv4.octets());
            }
            IpAddr::V6(ipv6) => {
                target_buffer.push(3); // Family: IPv6 = 3
                
                // Bits: use prefix_len if present, otherwise 128 for full address
                let bits = self.prefix_len.unwrap_or(128);
                target_buffer.push(bits);
                
                target_buffer.push(0); // is_cidr flag (always 0 based on observations)
                target_buffer.push(16); // Address size: IPv6 = 16 bytes
                
                // IPv6 address bytes
                target_buffer.extend_from_slice(&ipv6.octets());
            }
        }
        
        Ok(())
    }
}

// Convenience type alias for CIDR (same as INET in PostgreSQL)
pub type Cidr = Inet;

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "tokio")]
    mod tokio_connection {
        use super::*;
        use crate::test_helpers::get_settings;
        use crate::tokio_connection::new_client;
        use std::net::{Ipv4Addr, Ipv6Addr};
        use tokio::test;

        #[test]
        async fn test_inet_edge_cases() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test localhost addresses
            let localhost_v4: Inet = client
                .read_single_value("select '127.0.0.1'::inet;", &[])
                .await
                .unwrap();
            assert_eq!(localhost_v4.to_string(), "127.0.0.1");

            let localhost_v6: Inet = client
                .read_single_value("select '::1'::inet;", &[])
                .await
                .unwrap();
            assert_eq!(localhost_v6.to_string(), "::1");

            // Test various CIDR notations
            let class_a: Inet = client
                .read_single_value("select '10.0.0.0/8'::inet;", &[])
                .await
                .unwrap();
            assert_eq!(class_a.prefix_len, Some(8));

            let class_b: Inet = client
                .read_single_value("select '172.16.0.0/12'::inet;", &[])
                .await
                .unwrap();
            assert_eq!(class_b.prefix_len, Some(12));

            let class_c: Inet = client
                .read_single_value("select '192.168.0.0/16'::inet;", &[])
                .await
                .unwrap();
            assert_eq!(class_c.prefix_len, Some(16));

            // Test IPv6 with different prefix lengths
            let ipv6_64: Inet = client
                .read_single_value("select 'fe80::/64'::inet;", &[])
                .await
                .unwrap();
            assert_eq!(ipv6_64.prefix_len, Some(64));
        }

        #[test]
        async fn test_inet_basic_values() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test IPv4 address
            let ipv4: Inet = client
                .read_single_value("select '192.168.1.1'::inet;", &[])
                .await
                .unwrap();
            assert_eq!(ipv4.ip, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)));
            assert_eq!(ipv4.prefix_len, None);

            // Test IPv4 with CIDR
            let ipv4_cidr: Inet = client
                .read_single_value("select '10.0.0.0/8'::inet;", &[])
                .await
                .unwrap();
            assert_eq!(ipv4_cidr.ip, IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0)));
            assert_eq!(ipv4_cidr.prefix_len, Some(8));

            // Test IPv6 address
            let ipv6: Inet = client
                .read_single_value("select '::1'::inet;", &[])
                .await
                .unwrap();
            assert_eq!(ipv6.ip, IpAddr::V6(Ipv6Addr::LOCALHOST));
            assert_eq!(ipv6.prefix_len, None);

            // Test IPv6 with CIDR
            let ipv6_cidr: Inet = client
                .read_single_value("select '2001:db8::/32'::inet;", &[])
                .await
                .unwrap();
            if let IpAddr::V6(v6) = ipv6_cidr.ip {
                assert_eq!(v6.segments(), [0x2001, 0xdb8, 0, 0, 0, 0, 0, 0]);
            } else {
                panic!("Expected IPv6 address");
            }
            assert_eq!(ipv6_cidr.prefix_len, Some(32));
        }

        #[test]
        async fn test_cidr_type() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test CIDR type (should work the same as INET)
            let cidr: Cidr = client
                .read_single_value("select '192.168.0.0/16'::cidr;", &[])
                .await
                .unwrap();
            assert_eq!(cidr.ip, IpAddr::V4(Ipv4Addr::new(192, 168, 0, 0)));
            assert_eq!(cidr.prefix_len, Some(16));
        }

        #[test]
        async fn test_inet_null_handling() {
            let mut client = new_client(get_settings()).await.unwrap();

            let null_inet: Option<Inet> = client
                .read_single_value("select null::inet;", &[])
                .await
                .unwrap();
            assert_eq!(null_inet, None);
        }

        #[test]
        async fn test_inet_binary_format() {
            let mut client = new_client(get_settings()).await.unwrap();
            
            // Create a table for testing binary format
            client.execute_non_query("drop table if exists test_inet_binary; create table test_inet_binary(id int, addr inet);", &[]).await.unwrap();
            client.execute_non_query("
                insert into test_inet_binary values 
                (1, '127.0.0.1'),
                (2, '192.168.1.1/24'), 
                (3, '::1'),
                (4, '2001:db8::/32'),
                (5, '10.0.0.0/8'),
                (6, 'fe80::/64');
            ", &[]).await.unwrap();
            
            // Test reading with binary format (forced by parameter binding on ID)
            let test_cases = vec![
                (1, "127.0.0.1", None),
                (2, "192.168.1.1", Some(24u8)),
                (3, "::1", None),
                (4, "2001:db8::", Some(32u8)),
                (5, "10.0.0.0", Some(8u8)),
                (6, "fe80::", Some(64u8)),
            ];
            
            for (id, expected_ip_str, expected_prefix) in test_cases {
                let result: Inet = client.read_single_value("select addr from test_inet_binary where id = $1;", &[&id]).await.unwrap();
                
                // Verify IP address
                let expected_ip: IpAddr = expected_ip_str.parse().unwrap();
                assert_eq!(result.ip, expected_ip, "IP address mismatch for ID {id}");
                
                // Verify prefix length
                assert_eq!(result.prefix_len, expected_prefix, "Prefix length mismatch for ID {id}");
            }
        }

        #[test]
        async fn test_inet_round_trip_binary() {
            let mut client = new_client(get_settings()).await.unwrap();
            
            client.execute_non_query("drop table if exists test_inet_roundtrip; create table test_inet_roundtrip(addr inet);", &[]).await.unwrap();
            
            let test_values: Vec<Inet> = vec![
                "127.0.0.1".parse().unwrap(),
                "192.168.1.1/24".parse().unwrap(),
                "::1".parse().unwrap(),
                "2001:db8::/32".parse().unwrap(),
                "10.0.0.0/8".parse().unwrap(),
            ];
            
            for test_value in &test_values {
                // Insert using parameter binding (binary format)
                client.execute_non_query("insert into test_inet_roundtrip values ($1);", &[test_value]).await.unwrap();
                
                // Read back using parameter binding (binary format)
                let retrieved: Inet = client.read_single_value("select addr from test_inet_roundtrip where addr = $1;", &[test_value]).await.unwrap();
                
                assert_eq!(retrieved.ip, test_value.ip, "Round-trip IP mismatch");
                assert_eq!(retrieved.prefix_len, test_value.prefix_len, "Round-trip prefix mismatch");
                
                // Clean up for next iteration
                client.execute_non_query("delete from test_inet_roundtrip;", &[]).await.unwrap();
            }
        }
    }
}