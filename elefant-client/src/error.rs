use crate::protocol;
use crate::protocol::{ErrorResponse, FieldDescription};
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum ElefantClientError {
    IoError(std::io::Error),
    PostgresMessageParseError(protocol::PostgresMessageParseError),
    UnexpectedBackendMessage(String),
    PostgresError(String),
    ServerError(Box<PostgresServerError>),
    UnsupportedFieldType {
        desired_rust_type: &'static str,
        postgres_field: FieldDescription,
    },
    DataTypeParseError {
        original_error: Box<dyn Error + Sync + Send>,
        column_index: usize,
    },
    NoResultsReturned,
    UnexpectedNullValue {
        postgres_field: FieldDescription,
    },
    NotEnoughColumns {
        desired: usize,
        actual: usize,
    },
    UnsupportedAuthenticationMethod(String),
    TlsError(String),
    BatchQueryUnexpectedEnd,
}

impl From<std::io::Error> for ElefantClientError {
    fn from(value: std::io::Error) -> Self {
        ElefantClientError::IoError(value)
    }
}

impl From<protocol::PostgresMessageParseError> for ElefantClientError {
    fn from(value: protocol::PostgresMessageParseError) -> Self {
        ElefantClientError::PostgresMessageParseError(value)
    }
}

impl From<ErrorResponse<'_>> for ElefantClientError {
    fn from(value: ErrorResponse<'_>) -> Self {
        ElefantClientError::ServerError(Box::new(value.into()))
    }
}

/// An error reported by the Postgres server.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PostgresServerError {
    pub severity: String,
    /// The SQLSTATE code, e.g. `42P01` for an undefined table.
    pub code: String,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub context: Option<String>,
}

impl From<ErrorResponse<'_>> for PostgresServerError {
    fn from(value: ErrorResponse<'_>) -> Self {
        let mut error = PostgresServerError::default();
        let mut localized_severity = None;

        // Field types are documented at https://www.postgresql.org/docs/current/protocol-error-fields.html
        for field in value.fields {
            let value = field.value.into_owned();
            match field.field_type {
                b'S' => localized_severity = Some(value),
                b'V' => error.severity = value,
                b'C' => error.code = value,
                b'M' => error.message = value,
                b'D' => error.detail = Some(value),
                b'H' => error.hint = Some(value),
                b'W' => error.context = Some(value),
                _ => {}
            }
        }

        // 'V' is only sent by Postgres 9.6+, 'S' might be localized.
        if error.severity.is_empty() {
            error.severity = localized_severity.unwrap_or_default();
        }

        error
    }
}

impl Display for PostgresServerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}: {}", self.severity, self.code, self.message)?;
        if let Some(detail) = &self.detail {
            write!(f, "\nDETAIL: {detail}")?;
        }
        if let Some(hint) = &self.hint {
            write!(f, "\nHINT: {hint}")?;
        }
        if let Some(context) = &self.context {
            write!(f, "\nCONTEXT: {context}")?;
        }
        Ok(())
    }
}

impl Display for ElefantClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ElefantClientError::IoError(e) => {
                write!(f, "IO error: {e}")
            }
            ElefantClientError::PostgresMessageParseError(e) => {
                write!(f, "Postgres message parse error: {e}")
            }
            ElefantClientError::UnexpectedBackendMessage(e) => {
                write!(f, "Unexpected backend message: {e}")
            }
            ElefantClientError::PostgresError(e) => {
                write!(f, "Postgres error: {e}")
            }
            ElefantClientError::ServerError(e) => {
                write!(f, "{e}")
            }
            ElefantClientError::UnsupportedFieldType {
                postgres_field,
                desired_rust_type,
            } => {
                write!(f, "Unsupported field type: {postgres_field:?} for desired rust type: {desired_rust_type}")
            }
            ElefantClientError::DataTypeParseError {
                column_index,
                original_error,
            } => {
                write!(
                    f,
                    "Error while parsing response data: {original_error:?} as index {column_index}"
                )
            }
            ElefantClientError::NoResultsReturned => {
                write!(f, "No results returned from query.")
            }
            ElefantClientError::UnexpectedNullValue { postgres_field } => {
                write!(
                    f,
                    "Unexpected null value when processing field: {postgres_field:?}."
                )
            }
            ElefantClientError::NotEnoughColumns { desired, actual } => {
                write!(
                    f,
                    "Not enough columns returned. Desired: {desired}, Actual: {actual}"
                )
            }
            ElefantClientError::UnsupportedAuthenticationMethod(method) => {
                write!(f, "Unsupported authentication method: {method}")
            }
            ElefantClientError::TlsError(msg) => {
                write!(f, "TLS error: {msg}")
            }
            ElefantClientError::BatchQueryUnexpectedEnd => {
                write!(
                    f,
                    "Batch query ended unexpectedly: expected more result sets"
                )
            }
        }
    }
}

impl Error for ElefantClientError {}

#[cfg(test)]
mod tests {
    use crate::protocol::{ErrorField, ErrorResponse};
    use crate::ElefantClientError;

    fn error_response(fields: &[(u8, &'static str)]) -> ErrorResponse<'static> {
        ErrorResponse {
            fields: fields
                .iter()
                .map(|(field_type, value)| ErrorField {
                    field_type: *field_type,
                    value: (*value).into(),
                })
                .collect(),
        }
    }

    #[test]
    fn server_error_displays_severity_code_and_message() {
        let response = error_response(&[
            (b'S', "ERROR"),
            (b'V', "ERROR"),
            (b'C', "42P01"),
            (b'M', "relation \"public.missing_seq\" does not exist"),
            (b'F', "namespace.c"),
            (b'L', "631"),
            (b'R', "RangeVarGetRelidExtended"),
        ]);

        let error = ElefantClientError::from(response);

        assert_eq!(
            error.to_string(),
            "ERROR 42P01: relation \"public.missing_seq\" does not exist"
        );
    }

    #[test]
    fn server_error_displays_detail_hint_and_context() {
        let response = error_response(&[
            (b'S', "FEJL"),
            (b'V', "ERROR"),
            (b'C', "23505"),
            (
                b'M',
                "duplicate key value violates unique constraint \"people_name_key\"",
            ),
            (b'D', "Key (name)=(foo) already exists."),
            (b'H', "Use another name."),
            (
                b'W',
                "SQL statement \"insert into people (name) values ('foo')\"",
            ),
            (b'n', "people_name_key"),
        ]);

        let error = ElefantClientError::from(response);

        assert_eq!(
            error.to_string(),
            "ERROR 23505: duplicate key value violates unique constraint \"people_name_key\"\n\
             DETAIL: Key (name)=(foo) already exists.\n\
             HINT: Use another name.\n\
             CONTEXT: SQL statement \"insert into people (name) values ('foo')\""
        );
    }
}
