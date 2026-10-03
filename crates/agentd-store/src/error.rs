use std::{error::Error, fmt};

/// Domain failures retain their meaning when wrapped in `anyhow::Error`.
/// Database/decoding failures are internal errors, never input rejection.
#[derive(Debug)]
pub enum StoreError {
    Validation(String),
    NotFound(String),
    Conflict(String),
    Database(anyhow::Error),
}

impl StoreError {
    pub(crate) fn database(error: impl Into<anyhow::Error>) -> Self {
        Self::Database(error.into())
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Validation(message) | Self::NotFound(message) | Self::Conflict(message) => {
                formatter.write_str(message)
            }
            Self::Database(error) => write!(formatter, "database error: {error}"),
        }
    }
}

impl Error for StoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

impl From<libsql::Error> for StoreError {
    fn from(error: libsql::Error) -> Self {
        Self::database(error)
    }
}

pub(crate) fn validation(message: impl fmt::Display) -> anyhow::Error {
    StoreError::Validation(message.to_string()).into()
}

pub(crate) fn not_found(message: impl fmt::Display) -> anyhow::Error {
    StoreError::NotFound(message.to_string()).into()
}

pub(crate) fn conflict(message: impl fmt::Display) -> anyhow::Error {
    StoreError::Conflict(message.to_string()).into()
}
