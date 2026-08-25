use crate::CapabilityEngine;
use agentd_store::MemoryItem;
use anyhow::{anyhow, Context, Result};
use fastembed::{
    RerankInitOptionsUserDefined, TextRerank, TokenizerFiles, UserDefinedRerankingModel,
};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

pub const BUILTIN_RERANKER_MODEL_ID: &str = "BAAI/bge-reranker-v2-m3";
pub const BUILTIN_RERANKER_ARTIFACT_ID: &str =
    "onnx-community/bge-reranker-v2-m3-ONNX@6f5ff65298512715a1e669753bc754d2bc8f367b/onnx/model_int8.onnx";

const MODEL_DIR_ENV: &str = "AGENTD_RERANKER_MODEL_DIR";
const DEFAULT_MODEL_DIR: &str = "/opt/agentd/models/bge-reranker-v2-m3";
const MODEL_MAX_TOKENS: usize = 1_024;
const MODEL_THREADS: usize = 2;
const RERANK_BATCH_SIZE: usize = 10;
const MODEL_ASSETS: [(&str, &str); 5] = [
    (
        "config.json",
        "122e922dcfed6503c8721e6fe1daf090340c3d95ca7f3aa3a72730b321a51cfd",
    ),
    (
        "onnx/model_int8.onnx",
        "912fc1215c2dbff6499700534bd8d31253af01573861abbfc43afd1fab6cce5d",
    ),
    (
        "special_tokens_map.json",
        "8c785abebea9ae3257b61681b4e6fd8365ceafde980c21970d001e834cf10835",
    ),
    (
        "tokenizer.json",
        "8bf8afbfd11306bd872018c53bfdf2e160a56f8edbcf49933324404791c148d3",
    ),
    (
        "tokenizer_config.json",
        "b87c8703482b0300d3da30e201519aa641f6a450f5eb5bf1e624afbf70c74d80",
    ),
];

struct LoadedReranker {
    model: TextRerank,
}

impl LoadedReranker {
    fn load() -> Result<Self> {
        let directory = model_directory();
        let onnx_file = verified_asset_path(&directory, "onnx/model_int8.onnx")?;
        let files = TokenizerFiles {
            tokenizer_file: read_verified_asset(&directory, "tokenizer.json")?,
            config_file: read_verified_asset(&directory, "config.json")?,
            special_tokens_map_file: read_verified_asset(&directory, "special_tokens_map.json")?,
            tokenizer_config_file: read_verified_asset(&directory, "tokenizer_config.json")?,
        };
        let model = UserDefinedRerankingModel::new(onnx_file, files);
        let model = TextRerank::try_new_from_user_defined(
            model,
            RerankInitOptionsUserDefined::new()
                .with_max_length(MODEL_MAX_TOKENS)
                .with_intra_threads(MODEL_THREADS),
        )
        .with_context(|| {
            format!(
                "failed to load built-in reranker {BUILTIN_RERANKER_MODEL_ID} from {BUILTIN_RERANKER_ARTIFACT_ID}"
            )
        })?;
        Ok(Self { model })
    }

    fn scores(&mut self, query: &str, documents: &[String]) -> Result<Vec<f32>> {
        let results = self
            .model
            .rerank(query.to_string(), documents, false, Some(RERANK_BATCH_SIZE))
            .context("built-in reranker inference failed")?;
        if results.len() != documents.len() {
            return Err(anyhow!(
                "built-in reranker returned {} scores for {} documents",
                results.len(),
                documents.len()
            ));
        }
        let mut scores = vec![None; documents.len()];
        for result in results {
            if result.index >= scores.len()
                || !result.score.is_finite()
                || scores[result.index].is_some()
            {
                return Err(anyhow!("built-in reranker returned an invalid score"));
            }
            scores[result.index] = Some(result.score);
        }
        scores
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| anyhow!("built-in reranker omitted a document score"))
    }
}

#[derive(Clone)]
pub(crate) struct BuiltInReranker {
    loaded: Arc<Mutex<Option<LoadedReranker>>>,
    #[cfg(test)]
    test_reranker: Option<Arc<TestReranker>>,
}

#[cfg(test)]
type TestReranker = dyn Fn(&str, &[String]) -> Result<Vec<f32>> + Send + Sync;

impl Default for BuiltInReranker {
    fn default() -> Self {
        Self {
            loaded: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            test_reranker: None,
        }
    }
}

impl BuiltInReranker {
    async fn scores(&self, query: &str, documents: &[String]) -> Result<Vec<f32>> {
        #[cfg(test)]
        if let Some(rerank) = &self.test_reranker {
            return rerank(query, documents);
        }

        let loaded = self.loaded.clone();
        let query = query.to_string();
        let documents = documents.to_vec();
        tokio::task::spawn_blocking(move || {
            let mut loaded = loaded
                .lock()
                .map_err(|_| anyhow!("built-in reranker lock is poisoned"))?;
            if loaded.is_none() {
                *loaded = Some(LoadedReranker::load()?);
            }
            loaded
                .as_mut()
                .expect("reranker initialized above")
                .scores(&query, &documents)
        })
        .await
        .context("built-in reranker worker failed")?
    }

    async fn rerank(&self, query: &str, candidates: Vec<MemoryItem>) -> Result<Vec<MemoryItem>> {
        if candidates.is_empty() {
            return Ok(candidates);
        }
        let documents = candidates
            .iter()
            .map(|candidate| candidate.text.clone())
            .collect::<Vec<_>>();
        let scores = self.scores(query, &documents).await?;
        if scores.len() != candidates.len() {
            return Err(anyhow!(
                "built-in reranker returned {} scores for {} candidates",
                scores.len(),
                candidates.len()
            ));
        }
        if scores.iter().any(|score| !score.is_finite()) {
            return Err(anyhow!("built-in reranker returned a non-finite score"));
        }

        let mut scored = candidates
            .into_iter()
            .zip(scores)
            .enumerate()
            .map(|(rrf_rank, (item, raw_score))| (item, raw_score, rrf_rank))
            .collect::<Vec<_>>();
        scored.sort_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.2.cmp(&right.2))
                .then_with(|| left.0.id.cmp(&right.0.id))
        });
        Ok(scored
            .into_iter()
            .map(|(mut item, raw_score, _)| {
                item.score = Some(sigmoid(raw_score) as f64);
                item
            })
            .collect())
    }

    async fn warm_up(&self) -> Result<()> {
        self.scores(
            "agentd reranker startup check",
            &["agentd startup passage".to_string()],
        )
        .await
        .map(|_| ())
    }

    #[cfg(test)]
    fn with_test_reranker<F>(mut self, rerank: F) -> Self
    where
        F: Fn(&str, &[String]) -> Result<Vec<f32>> + Send + Sync + 'static,
    {
        self.test_reranker = Some(Arc::new(rerank));
        self
    }
}

impl CapabilityEngine {
    pub async fn warm_up_retrieval_models(&self) -> Result<()> {
        self.warm_up_embedding().await?;
        self.reranker.warm_up().await
    }

    pub(crate) async fn rerank_memory(
        &self,
        query: &str,
        candidates: Vec<MemoryItem>,
    ) -> Result<Vec<MemoryItem>> {
        self.reranker.rerank(query, candidates).await
    }

    #[cfg(test)]
    pub(crate) fn with_test_reranker<F>(mut self, rerank: F) -> Self
    where
        F: Fn(&str, &[String]) -> Result<Vec<f32>> + Send + Sync + 'static,
    {
        self.reranker = self.reranker.with_test_reranker(rerank);
        self
    }
}

fn sigmoid(value: f32) -> f32 {
    if value >= 0.0 {
        1.0 / (1.0 + (-value).exp())
    } else {
        let exp = value.exp();
        exp / (1.0 + exp)
    }
}

fn model_directory() -> PathBuf {
    std::env::var_os(MODEL_DIR_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_MODEL_DIR))
}

fn expected_hash(relative_path: &str) -> Result<&'static str> {
    MODEL_ASSETS
        .iter()
        .find_map(|(path, hash)| (*path == relative_path).then_some(*hash))
        .ok_or_else(|| anyhow!("unknown built-in reranker asset: {relative_path}"))
}

fn verified_asset_path(directory: &Path, relative_path: &str) -> Result<PathBuf> {
    let expected = expected_hash(relative_path)?;
    let path = directory.join(relative_path);
    let mut file = File::open(&path)
        .with_context(|| format!("missing built-in reranker asset {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .with_context(|| format!("failed to read reranker asset {}", path.display()))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    let actual = hex::encode(hasher.finalize());
    if actual != expected {
        return Err(anyhow!(
            "built-in reranker asset {} failed SHA-256 validation: expected {expected}, got {actual}",
            path.display()
        ));
    }
    Ok(path)
}

fn read_verified_asset(directory: &Path, relative_path: &str) -> Result<Vec<u8>> {
    let path = verified_asset_path(directory, relative_path)?;
    std::fs::read(&path)
        .with_context(|| format!("failed to read reranker asset {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sigmoid_is_stable_and_bounded() {
        assert_eq!(sigmoid(0.0), 0.5);
        assert!(sigmoid(100.0) <= 1.0);
        assert!(sigmoid(-100.0) >= 0.0);
        assert!(sigmoid(2.0) > sigmoid(-2.0));
    }

    #[tokio::test]
    #[ignore = "requires the pinned bge-reranker-v2-m3 model assets"]
    async fn pinned_model_reranks_multilingual_documents() {
        let reranker = BuiltInReranker::default();
        reranker.warm_up().await.unwrap();
        let scores = reranker
            .scores(
                "用户喜欢什么水果？",
                &[
                    "用户最喜欢的水果是榴莲。".to_string(),
                    "服务器每周日执行备份。".to_string(),
                ],
            )
            .await
            .unwrap();
        assert_eq!(scores.len(), 2);
        assert!(scores[0] > scores[1], "unexpected scores: {scores:?}");
    }
}
