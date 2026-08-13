use anyhow::Result;
use axum::{http::StatusCode, response::IntoResponse, Json};
use uuid::Uuid;

pub(crate) fn json_result<T: serde::Serialize>(result: Result<T>) -> axum::response::Response {
    match result {
        Ok(value) => (StatusCode::OK, Json(serde_json::to_value(value).unwrap())).into_response(),
        Err(error) => error_response(error),
    }
}

pub(crate) fn error_response(error: impl std::fmt::Display) -> axum::response::Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({ "error": error.to_string() })),
    )
        .into_response()
}

pub(crate) fn parse_uuid(raw: &str) -> Result<Uuid> {
    Ok(Uuid::parse_str(raw)?)
}
