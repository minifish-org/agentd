use crate::{error_response, json_result, AppState};
use agentd_api::{ArtifactPath, ArtifactRef};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{
        header::{CONTENT_LENGTH, CONTENT_TYPE, ETAG},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::{IntoResponse, Response},
    Json,
};
use chrono::Utc;
use serde::Deserialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Deserialize)]
pub(crate) struct ArtifactListQuery {
    pub(crate) prefix: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) cursor: Option<String>,
}

pub(crate) async fn list_artifacts(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    Query(query): Query<ArtifactListQuery>,
) -> impl IntoResponse {
    json_result(
        state
            .store
            .list_artifacts_page(
                &tenant,
                query.prefix.as_deref(),
                query.cursor.as_deref(),
                query.limit.unwrap_or(100),
            )
            .await,
    )
}

pub(crate) async fn read_artifact(
    State(state): State<AppState>,
    Path((tenant, path)): Path<(String, String)>,
) -> Response {
    let path = match ArtifactPath::parse(&path) {
        Ok(path) => path,
        Err(error) => return error_response(error),
    };
    let (body, content_type, _) = match state.store.get_artifact(&tenant, path.as_str()).await {
        Ok(Some(artifact)) => artifact,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error":"artifact not found"})),
            )
                .into_response()
        }
        Err(error) => return error_response(error),
    };
    body_response(
        body,
        content_type
            .as_deref()
            .unwrap_or("application/octet-stream"),
    )
}

pub(crate) async fn write_artifact(
    State(state): State<AppState>,
    Path((tenant, path)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = match ArtifactPath::parse(&path) {
        Ok(path) => path,
        Err(error) => return error_response(error),
    };
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream");
    let sha256 = sha256_hex(&body);
    let metadata = serde_json::json!({
        "artifact_ref": ArtifactRef::new(&tenant, &path).to_string(),
        "content_type": content_type,
        "size_bytes": body.len(),
        "sha256": sha256,
        "updated_at": Utc::now().to_rfc3339(),
    });
    match state
        .store
        .put_artifact(
            &tenant,
            path.as_str(),
            &body,
            content_type,
            Some(&metadata.to_string()),
        )
        .await
    {
        Ok(()) => (StatusCode::OK, Json(metadata)).into_response(),
        Err(error) => error_response(error),
    }
}

pub(crate) async fn delete_artifact(
    State(state): State<AppState>,
    Path((tenant, path)): Path<(String, String)>,
) -> impl IntoResponse {
    let path = match ArtifactPath::parse(&path) {
        Ok(path) => path,
        Err(error) => return error_response(error),
    };
    json_result(
        state
            .store
            .delete_artifact(&tenant, path.as_str())
            .await
            .map(|()| serde_json::json!({"deleted":true,"path":path.as_str()})),
    )
}

fn body_response(body: Vec<u8>, content_type: &str) -> Response {
    let len = body.len();
    let sha256 = sha256_hex(&body);
    let mut response = body.into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_str(content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    response.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&len.to_string()).unwrap(),
    );
    response.headers_mut().insert(
        ETAG,
        HeaderValue::from_str(&format!("\"{sha256}\"")).unwrap(),
    );
    response
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
