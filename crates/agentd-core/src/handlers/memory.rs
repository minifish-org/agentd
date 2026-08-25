use crate::CapabilityEngine;
use anyhow::{anyhow, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const MEMORY_RRF_CANDIDATE_LIMIT: usize = 10;
const MEMORY_RESULT_LIMIT: usize = 5;

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MemoryListCursor {
    version: u8,
    tenant: String,
    namespace: String,
    after_id: String,
}

fn required<'a>(params: &'a Value, field: &str) -> Result<&'a str> {
    params
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("memory tool requires {field}"))
}

fn namespace(params: &Value) -> &str {
    params
        .get("namespace")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("default")
}

fn encode_list_cursor(tenant: &str, namespace: &str, after_id: &str) -> Result<String> {
    let cursor = MemoryListCursor {
        version: 1,
        tenant: tenant.to_string(),
        namespace: namespace.to_string(),
        after_id: after_id.to_string(),
    };
    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&cursor)?))
}

fn decode_list_cursor(cursor: &str, tenant: &str, namespace: &str) -> Result<String> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor.trim())
        .map_err(|_| anyhow!("invalid memory_list cursor"))?;
    let cursor: MemoryListCursor =
        serde_json::from_slice(&bytes).map_err(|_| anyhow!("invalid memory_list cursor"))?;
    if cursor.version != 1
        || cursor.tenant != tenant
        || cursor.namespace != namespace
        || cursor.after_id.trim().is_empty()
    {
        return Err(anyhow!(
            "memory_list cursor does not match the current tenant and namespace"
        ));
    }
    Ok(cursor.after_id)
}

impl CapabilityEngine {
    pub(crate) async fn execute_memory_get(&self, tenant: &str, params: &Value) -> Result<Value> {
        let item = self
            .store
            .get_memory(tenant, namespace(params), required(params, "id")?)
            .await?;
        Ok(json!({"item":item}))
    }

    pub(crate) async fn execute_memory_search(
        &self,
        tenant: &str,
        params: &Value,
    ) -> Result<Value> {
        let matches = self
            .search_memory(
                tenant,
                namespace(params),
                required(params, "query")?,
                params.get("limit").and_then(Value::as_u64).unwrap_or(5) as usize,
            )
            .await?;
        Ok(json!({"matches":matches}))
    }

    pub(crate) async fn execute_memory_list(&self, tenant: &str, params: &Value) -> Result<Value> {
        let namespace = namespace(params);
        let after_id = params
            .get("cursor")
            .and_then(Value::as_str)
            .map(|cursor| decode_list_cursor(cursor, tenant, namespace))
            .transpose()?;
        let page = self
            .store
            .list_memory_page(
                tenant,
                namespace,
                after_id.as_deref(),
                params.get("limit").and_then(Value::as_u64).unwrap_or(50) as usize,
            )
            .await?;
        let next_cursor = page
            .next_after_id
            .as_deref()
            .map(|after_id| encode_list_cursor(tenant, namespace, after_id))
            .transpose()?;
        Ok(json!({"items":page.items,"next_cursor":next_cursor}))
    }

    pub async fn search_memory(
        &self,
        tenant: &str,
        namespace: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<agentd_store::MemoryItem>> {
        let query = query.trim();
        if query.is_empty() {
            return Err(anyhow!("memory query is required"));
        }
        let embedding = self.embed_query(query).await?;
        let candidates = self
            .store
            .search_memory(
                tenant,
                namespace,
                query,
                &embedding,
                MEMORY_RRF_CANDIDATE_LIMIT,
            )
            .await?;
        let mut reranked = self.rerank_memory(query, candidates).await?;
        reranked.truncate(limit.clamp(1, MEMORY_RESULT_LIMIT));
        Ok(reranked)
    }

    pub(crate) async fn execute_memory_put(&self, tenant: &str, params: &Value) -> Result<Value> {
        let text = required(params, "text")?;
        if text.len() > agentd_store::MAX_MEMORY_TEXT_BYTES {
            return Err(anyhow!(
                "memory text exceeds {} UTF-8 bytes; store long content as an artifact",
                agentd_store::MAX_MEMORY_TEXT_BYTES
            ));
        }
        let embedding = self.embed_passage(text).await?;
        let item = self
            .store
            .put_memory(
                tenant,
                namespace(params),
                required(params, "id")?,
                text,
                &embedding,
            )
            .await?;
        Ok(json!({"item":item}))
    }

    pub(crate) async fn execute_memory_delete(
        &self,
        tenant: &str,
        params: &Value,
    ) -> Result<Value> {
        let deleted = self
            .store
            .delete_memory(tenant, namespace(params), required(params, "id")?)
            .await?;
        Ok(json!({"deleted":deleted}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CapabilityEngine, CapabilityEngineConfig};
    use agentd_store::{AgentdStore, MEMORY_EMBEDDING_DIM};
    use serde_json::json;

    fn embedding() -> Vec<f32> {
        let mut embedding = vec![0.0; MEMORY_EMBEDDING_DIM];
        embedding[0] = 1.0;
        embedding
    }

    async fn engine<F>(embed: F) -> (tempfile::TempDir, AgentdStore, CapabilityEngine)
    where
        F: Fn(&str) -> Result<Vec<f32>> + Send + Sync + 'static,
    {
        let directory = tempfile::tempdir().unwrap();
        let store = AgentdStore::new(directory.path().join("agentd.db").to_str().unwrap())
            .await
            .unwrap();
        store.create_tenant("demo", &json!({})).await.unwrap();
        let engine =
            CapabilityEngine::new_with_config(store.clone(), CapabilityEngineConfig::default())
                .with_test_embedding(embed)
                .with_test_reranker(|_, documents| {
                    Ok((0..documents.len()).map(|index| -(index as f32)).collect())
                });
        (directory, store, engine)
    }

    #[tokio::test]
    async fn failed_embedding_does_not_create_memory() {
        let (_directory, store, engine) =
            engine(|_| Err(anyhow!("built-in embedding failed"))).await;
        assert!(engine
            .execute_memory_put(
                "demo",
                &json!({"namespace":"profile","id":"favorite","text":"likes durian"}),
            )
            .await
            .is_err());
        assert!(store
            .get_memory("demo", "profile", "favorite")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn memory_tool_search_is_semantic_and_clamps_result_count() {
        let (_directory, store, engine) = engine(|_| Ok(embedding())).await;
        let vector = embedding();
        for index in 0..25 {
            store
                .put_memory(
                    "demo",
                    "profile",
                    &format!("fact-{index:02}"),
                    &format!("durable fact {index}"),
                    &vector,
                )
                .await
                .unwrap();
        }
        let default = engine
            .execute_memory_search(
                "demo",
                &json!({"namespace":"profile","query":"unrelated paraphrase"}),
            )
            .await
            .unwrap();
        assert_eq!(default["matches"].as_array().unwrap().len(), 5);
        let clamped = engine
            .search_memory("demo", "profile", "unrelated paraphrase", 100)
            .await
            .unwrap();
        assert_eq!(clamped.len(), 5);
        assert_eq!(clamped[0].id, "fact-00");
    }

    #[tokio::test]
    async fn memory_search_reranks_only_the_rrf_top_ten() {
        let directory = tempfile::tempdir().unwrap();
        let store = AgentdStore::new(directory.path().join("agentd.db").to_str().unwrap())
            .await
            .unwrap();
        store.create_tenant("demo", &json!({})).await.unwrap();
        let engine =
            CapabilityEngine::new_with_config(store.clone(), CapabilityEngineConfig::default())
                .with_test_embedding(|_| Ok(embedding()))
                .with_test_reranker(|query, documents| {
                    assert_eq!(query, "query");
                    assert_eq!(documents.len(), 10);
                    Ok(documents
                        .iter()
                        .map(|document| {
                            document
                                .split_whitespace()
                                .last()
                                .unwrap()
                                .parse::<f32>()
                                .unwrap()
                        })
                        .collect())
                });
        let vector = embedding();
        for index in 0..15 {
            store
                .put_memory(
                    "demo",
                    "profile",
                    &format!("fact-{index:02}"),
                    &format!("candidate {index}"),
                    &vector,
                )
                .await
                .unwrap();
        }

        let matches = engine
            .search_memory("demo", "profile", "query", 5)
            .await
            .unwrap();
        assert_eq!(matches.len(), 5);
        assert_eq!(matches[0].id, "fact-09");
        assert_eq!(matches[4].id, "fact-05");
        assert!(matches.iter().all(|item| {
            item.score
                .is_some_and(|score| score.is_finite() && score > 0.0 && score <= 1.0)
        }));
    }

    #[tokio::test]
    async fn memory_list_cursor_is_bound_to_tenant_and_namespace() {
        let (_directory, store, engine) = engine(|_| Ok(embedding())).await;
        store.create_tenant("other", &json!({})).await.unwrap();
        let vector = embedding();
        for id in ["alpha", "bravo", "charlie"] {
            store
                .put_memory("demo", "profile", id, &format!("fact {id}"), &vector)
                .await
                .unwrap();
        }

        let first = engine
            .execute_memory_list("demo", &json!({"namespace":"profile","limit":2}))
            .await
            .unwrap();
        assert_eq!(first["items"].as_array().unwrap().len(), 2);
        assert_eq!(first["items"][0]["id"], "alpha");
        assert!(first["items"][0].get("tenant").is_none());
        assert!(first["items"][0].get("namespace").is_none());
        assert!(first["items"][0].get("embedding").is_none());
        let cursor = first["next_cursor"].as_str().unwrap();

        let second = engine
            .execute_memory_list(
                "demo",
                &json!({"namespace":"profile","limit":2,"cursor":cursor}),
            )
            .await
            .unwrap();
        assert_eq!(second["items"][0]["id"], "charlie");
        assert!(second["next_cursor"].is_null());

        for invalid in [
            engine
                .execute_memory_list("demo", &json!({"namespace":"other","cursor":cursor}))
                .await,
            engine
                .execute_memory_list("other", &json!({"namespace":"profile","cursor":cursor}))
                .await,
            engine
                .execute_memory_list("demo", &json!({"namespace":"profile","cursor":"bad"}))
                .await,
        ] {
            assert!(invalid.is_err());
        }
    }
}
