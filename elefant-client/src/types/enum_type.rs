use std::collections::HashMap;
use std::error::Error;

/// Trait implemented by Rust enums that map to PostgreSQL enum types.
///
/// Can be implemented manually or via the `#[derive(PostgresEnum)]` macro.
pub trait PostgresEnum: Sized {
    /// The PostgreSQL type name (e.g., "mood").
    /// Schema-qualify if needed (e.g., "public.mood").
    const PG_TYPE_NAME: &'static str;

    /// Convert this variant to its PostgreSQL label string.
    fn to_label(&self) -> &'static str;

    /// Parse a PostgreSQL label string into a variant.
    fn from_label(label: &str) -> Result<Self, Box<dyn Error + Sync + Send>>;
}

/// Registration data for an enum type, used when inserting into the registry.
#[derive(Debug, Clone)]
pub struct EnumOidEntry {
    pub oid: i32,
    pub array_oid: i32,
    pub schema: String,
}

/// Runtime registry mapping PostgreSQL enum OIDs to their type names.
///
/// Keyed by OID for O(1) lookups on the hot path (`accepts_with_registry`).
/// Supports multiple schemas having enums with the same name (different OIDs).
#[derive(Debug, Clone, Default)]
pub struct EnumTypeRegistry {
    /// element OID -> list of type names this OID was registered under
    by_oid: HashMap<i32, Vec<String>>,
    /// array OID -> element OID
    array_oids: HashMap<i32, i32>,
}

impl EnumTypeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert an OID entry for the given type name.
    pub fn insert(&mut self, name: String, entry: EnumOidEntry) {
        self.by_oid.entry(entry.oid).or_default().push(name);
        if entry.array_oid != 0 {
            self.array_oids.insert(entry.array_oid, entry.oid);
        }
    }

    /// Check if the given OID matches any entry for the named type.
    pub fn has_oid_for_type(&self, name: &str, oid: i32) -> bool {
        self.by_oid
            .get(&oid)
            .is_some_and(|names| names.iter().any(|n| n == name))
    }

    /// Check if the given OID matches any registered enum array OID.
    pub fn is_enum_array_oid(&self, oid: i32) -> bool {
        self.array_oids.contains_key(&oid)
    }

    /// Look up the element OID for a registered enum array OID.
    pub fn element_oid_for_array_oid(&self, array_oid: i32) -> Option<i32> {
        self.array_oids.get(&array_oid).copied()
    }

    pub fn is_empty(&self) -> bool {
        self.by_oid.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_has_oid_for_type() {
        let mut registry = EnumTypeRegistry::new();
        registry.insert(
            "mood".to_string(),
            EnumOidEntry {
                oid: 16384,
                array_oid: 16385,
                schema: "public".to_string(),
            },
        );

        assert!(registry.has_oid_for_type("mood", 16384));
        assert!(!registry.has_oid_for_type("mood", 99999));
        assert!(!registry.has_oid_for_type("other", 16384));
    }

    #[test]
    fn test_registry_multiple_schemas() {
        let mut registry = EnumTypeRegistry::new();
        registry.insert(
            "mood".to_string(),
            EnumOidEntry {
                oid: 16384,
                array_oid: 16385,
                schema: "public".to_string(),
            },
        );
        registry.insert(
            "mood".to_string(),
            EnumOidEntry {
                oid: 16400,
                array_oid: 16401,
                schema: "other_schema".to_string(),
            },
        );

        assert!(registry.has_oid_for_type("mood", 16384));
        assert!(registry.has_oid_for_type("mood", 16400));
        assert!(!registry.has_oid_for_type("mood", 99999));
    }

    #[test]
    fn test_registry_array_oid() {
        let mut registry = EnumTypeRegistry::new();
        registry.insert(
            "mood".to_string(),
            EnumOidEntry {
                oid: 16384,
                array_oid: 16385,
                schema: "public".to_string(),
            },
        );

        assert!(registry.is_enum_array_oid(16385));
        assert!(!registry.is_enum_array_oid(16384));
        assert!(!registry.is_enum_array_oid(99999));
    }

    #[cfg(all(feature = "tokio", feature = "derive"))]
    mod tokio_connection {
        use crate::test_helpers::get_settings;
        use crate::tokio_connection::TokioConnectionFactory;
        use crate::{PostgresEnum, PostgresPool};
        use tokio::test;

        #[derive(Debug, Clone, PartialEq, PostgresEnum)]
        #[postgres(name = "mood")]
        enum Mood {
            Happy,
            Sad,
            Neutral,
        }

        #[derive(Debug, Clone, PartialEq, PostgresEnum)]
        enum UserRole {
            Admin,
            #[postgres(label = "regular_user")]
            Regular,
            Guest,
        }

        async fn setup_enum_client() -> crate::pool::PoolableClient<TokioConnectionFactory> {
            // First create the enum type using a plain connection
            let mut plain_client = crate::tokio_connection::new_client(get_settings())
                .await
                .unwrap();
            plain_client
                .execute_non_query_simple(
                    "DROP TYPE IF EXISTS mood CASCADE;
                     CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral');
                     DROP TYPE IF EXISTS user_role CASCADE;
                     CREATE TYPE user_role AS ENUM ('admin', 'regular_user', 'guest');
                     DROP TABLE IF EXISTS enum_test;
                     CREATE TABLE enum_test (id serial PRIMARY KEY, m mood, r user_role);",
                )
                .await
                .unwrap();
            drop(plain_client);

            // Now create a pool with registered enums
            let settings = get_settings()
                .register_enum::<Mood>()
                .register_enum::<UserRole>();

            let pool = PostgresPool::new(TokioConnectionFactory, settings)
                .await
                .unwrap();

            // Verify registry was populated
            assert!(
                !pool.enum_registry().is_empty(),
                "Enum registry should not be empty after registering enums"
            );

            pool.get_client().await.unwrap()
        }

        #[test]
        async fn test_enum_text_mode() {
            let mut client = setup_enum_client().await;

            // Read enum via simple query (text mode)
            let value: Mood = client
                .read_single_value_simple("SELECT 'happy'::mood")
                .await;
            assert_eq!(value, Mood::Happy);

            let value: Mood = client.read_single_value_simple("SELECT 'sad'::mood").await;
            assert_eq!(value, Mood::Sad);
        }

        #[test]
        async fn test_enum_binary_mode() {
            let mut client = setup_enum_client().await;

            // Read enum via prepared statement (binary mode)
            let value: Mood = client
                .read_single_value("SELECT 'neutral'::mood", &[])
                .await;
            assert_eq!(value, Mood::Neutral);
        }

        #[test]
        async fn test_enum_as_parameter() {
            let mut client = setup_enum_client().await;

            client
                .execute_non_query_simple(
                    "DELETE FROM enum_test;
                     INSERT INTO enum_test (m, r) VALUES ('happy', 'admin');",
                )
                .await
                .unwrap();

            // Send enum as parameter and read it back
            client
                .execute_non_query(
                    "UPDATE enum_test SET m = $1 WHERE r = $2",
                    &[&Mood::Sad, &UserRole::Admin],
                )
                .await
                .unwrap();

            let value: Mood = client
                .read_single_value("SELECT m FROM enum_test WHERE r = 'admin'::user_role", &[])
                .await;
            assert_eq!(value, Mood::Sad);
        }

        #[test]
        async fn test_enum_nullable() {
            let mut client = setup_enum_client().await;

            client
                .execute_non_query_simple(
                    "DELETE FROM enum_test;
                     INSERT INTO enum_test (m, r) VALUES (NULL, 'guest');",
                )
                .await
                .unwrap();

            let value: Option<Mood> = client
                .read_single_value("SELECT m FROM enum_test WHERE r = 'guest'::user_role", &[])
                .await;
            assert_eq!(value, None);

            // Now with a non-null value
            client
                .execute_non_query_simple(
                    "UPDATE enum_test SET m = 'happy' WHERE r = 'guest'::user_role",
                )
                .await
                .unwrap();

            let value: Option<Mood> = client
                .read_single_value("SELECT m FROM enum_test WHERE r = 'guest'::user_role", &[])
                .await;
            assert_eq!(value, Some(Mood::Happy));
        }

        #[test]
        async fn test_enum_array_text_mode() {
            let mut client = setup_enum_client().await;

            let value: Vec<Mood> = client
                .read_single_value_simple("SELECT ARRAY['happy', 'sad', 'neutral']::mood[]")
                .await;
            assert_eq!(value, vec![Mood::Happy, Mood::Sad, Mood::Neutral]);
        }

        #[test]
        async fn test_enum_array_binary_mode() {
            let mut client = setup_enum_client().await;

            let value: Vec<Mood> = client
                .read_single_value("SELECT ARRAY['happy', 'sad']::mood[]", &[])
                .await;
            assert_eq!(value, vec![Mood::Happy, Mood::Sad]);
        }

        #[test]
        async fn test_enum_empty_array() {
            let mut client = setup_enum_client().await;

            let value: Vec<Mood> = client
                .read_single_value_simple("SELECT ARRAY[]::mood[]")
                .await;
            assert_eq!(value, Vec::<Mood>::new());

            let value: Vec<Mood> = client
                .read_single_value("SELECT ARRAY[]::mood[]", &[])
                .await;
            assert_eq!(value, Vec::<Mood>::new());
        }

        #[test]
        async fn test_enum_custom_labels() {
            let mut client = setup_enum_client().await;

            let value: UserRole = client
                .read_single_value_simple("SELECT 'regular_user'::user_role")
                .await;
            assert_eq!(value, UserRole::Regular);

            let value: UserRole = client
                .read_single_value("SELECT 'admin'::user_role", &[])
                .await;
            assert_eq!(value, UserRole::Admin);
        }

        #[test]
        async fn test_pool_no_enums_no_query() {
            // A pool with no registered enums should not query for OIDs
            let settings = get_settings();
            let pool = PostgresPool::new(TokioConnectionFactory, settings)
                .await
                .unwrap();
            assert!(pool.enum_registry().is_empty());
        }

        #[derive(Debug, Clone, PartialEq, PostgresEnum)]
        #[postgres(name = "nonexistent_enum_type_12345")]
        enum NonexistentEnum {
            A,
        }

        #[test]
        async fn test_unregistered_enum_silently_skipped() {
            // Register a name that doesn't exist in the database
            let settings = get_settings().register_enum::<NonexistentEnum>();

            // Should not error
            let pool = PostgresPool::new(TokioConnectionFactory, settings)
                .await
                .unwrap();
            // Registry will be empty since the type doesn't exist
            assert!(pool.enum_registry().is_empty());
        }
    }
}
