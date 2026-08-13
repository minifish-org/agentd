use crate::CapabilityEngine;
use anyhow::{anyhow, Result};
use serde_json::{json, Value};

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
        self.store
            .search_memory(tenant, namespace, query, &embedding, limit)
            .await
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
                .with_test_embedding(embed);
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
        assert_eq!(clamped.len(), 20);
        assert_eq!(clamped[0].id, "fact-00");
    }
}
