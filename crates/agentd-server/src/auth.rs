use axum::{
    extract::{Request, State},
    http::{HeaderMap, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};

/// Shared with request auditing so a supplied header alone never establishes
/// identity. An API without a configured token has no authenticated principal.
pub(crate) fn authenticated_api_token(token: Option<&str>, headers: &HeaderMap) -> bool {
    let Some(expected) = token else {
        return false;
    };
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        == Some(expected)
}

/// Bearer-token gate for the `/v1` API. No-op when `api_token` is unset.
pub(crate) async fn require_api_token(
    State(token): State<Option<String>>,
    req: Request,
    next: Next,
) -> Response {
    if let Some(expected) = token.as_deref() {
        let path = req.uri().path();
        // Guard the API surface; leave the static console page (`/`, `/console`)
        // and CORS preflight open so the page can load and browsers can probe.
        if (path == "/v1" || path.starts_with("/v1/"))
            && req.method() != Method::OPTIONS
            && !authenticated_api_token(Some(expected), req.headers())
        {
            return (StatusCode::UNAUTHORIZED, "missing or invalid API token").into_response();
        }
    }
    next.run(req).await
}
