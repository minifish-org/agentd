use agentd_store::StoreError;
use anyhow::Result;
use axum::{http::StatusCode, response::IntoResponse, Json};
use uuid::Uuid;

pub(crate) fn json_result<T: serde::Serialize>(result: Result<T>) -> axum::response::Response {
    match result {
        Ok(value) => (StatusCode::OK, Json(value)).into_response(),
        Err(error) => error_response(error),
    }
}

pub(crate) fn error_status(error: &anyhow::Error) -> StatusCode {
    match error.downcast_ref::<StoreError>() {
        Some(StoreError::Validation(_)) => StatusCode::BAD_REQUEST,
        Some(StoreError::NotFound(_)) => StatusCode::NOT_FOUND,
        Some(StoreError::Conflict(_)) => StatusCode::CONFLICT,
        Some(StoreError::Database(_)) => StatusCode::INTERNAL_SERVER_ERROR,
        None if error.downcast_ref::<agentd_api::ApiError>().is_some()
            || error.downcast_ref::<serde_json::Error>().is_some()
            || error.downcast_ref::<toml::de::Error>().is_some()
            || error.downcast_ref::<uuid::Error>().is_some()
            || error.downcast_ref::<std::str::Utf8Error>().is_some() =>
        {
            StatusCode::BAD_REQUEST
        }
        None => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

pub(crate) fn error_response(error: impl Into<anyhow::Error>) -> axum::response::Response {
    let error = error.into();
    let status = error_status(&error);
    if status.is_server_error() {
        tracing::error!(%error, "request failed");
    }
    (
        status,
        Json(serde_json::json!({ "error": error.to_string() })),
    )
        .into_response()
}

pub(crate) fn parse_uuid(raw: &str) -> Result<Uuid> {
    Ok(Uuid::parse_str(raw)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrapped_domain_errors_keep_their_http_meaning() {
        for (error, expected) in [
            (
                StoreError::Validation("input".into()),
                StatusCode::BAD_REQUEST,
            ),
            (
                StoreError::NotFound("missing".into()),
                StatusCode::NOT_FOUND,
            ),
            (StoreError::Conflict("changed".into()), StatusCode::CONFLICT),
            (
                StoreError::Database(anyhow::anyhow!("storage unavailable")),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
        ] {
            let error = anyhow::Error::new(error).context("operation failed");
            assert_eq!(error_status(&error), expected);
        }
        assert_eq!(
            error_status(&anyhow::anyhow!("unexpected failure")),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            error_status(&Uuid::parse_str("invalid").unwrap_err().into()),
            StatusCode::BAD_REQUEST
        );
    }
}
