/// Describes how data can be copied when using the `COPY` command in postgres. 
#[derive(Debug, Clone)]
pub enum DataFormat {
    /// Slightly slower, but works across postgres versions, is human-readable and can be
    /// outputted in text files.
    Text,

    /// Faster, but has strict requirements to the postgres version and is not human-readable.
    PostgresBinary {
        postgres_version: Option<String>,
    },
}

impl PartialEq for DataFormat {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DataFormat::Text, DataFormat::Text) => true,
            (DataFormat::PostgresBinary { postgres_version: left_pg_version }, DataFormat::PostgresBinary { postgres_version: right_pg_version }) => match (left_pg_version, right_pg_version) {
                (None, _) => true,
                (_, None) => true,
                (Some(left), Some(right)) => left == right,
            },
            _ => false,
        }
    }
}
