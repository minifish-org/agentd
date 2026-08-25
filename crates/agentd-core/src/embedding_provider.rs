use crate::CapabilityEngine;
use agentd_store::MEMORY_EMBEDDING_DIM;
use anyhow::{anyhow, Context, Result};
use fastembed::{
    InitOptionsUserDefined, Pooling, QuantizationMode, TextEmbedding, TokenizerFiles,
    UserDefinedEmbeddingModel,
};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tokenizers::Tokenizer;

pub const BUILTIN_EMBEDDING_MODEL_ID: &str =
    "intfloat/multilingual-e5-small@614241f622f53c4eeff9890bdc4f31cfecc418b3";
pub const BUILTIN_EMBEDDING_ARTIFACT_ID: &str =
    "Xenova/multilingual-e5-small@761b726dd34fb83930e26aab4e9ac3899aa1fa78/onnx/model_int8.onnx";
pub const BUILTIN_EMBEDDING_DIMENSION: usize = MEMORY_EMBEDDING_DIM;

const MODEL_DIR_ENV: &str = "AGENTD_EMBEDDING_MODEL_DIR";
const DEFAULT_MODEL_DIR: &str = "/opt/agentd/models/multilingual-e5-small";
const MODEL_MAX_TOKENS: usize = 512;
const MODEL_THREADS: usize = 2;
const MODEL_ASSETS: [(&str, &str); 5] = [
    (
        "config.json",
        "cb99455288675345e1a4f411438d5d0adbba5fbd3a67ea4fb03c015433b996c1",
    ),
    (
        "onnx/model_int8.onnx",
        "4d24e2bc01a447951524466ef533e52944bf48509e6552810bcee1a2711cb02c",
    ),
    (
        "special_tokens_map.json",
        "d05497f1da52c5e09554c0cd874037a083e1dc1b9cfd48034d1c717f1afc07a7",
    ),
    (
        "tokenizer.json",
        "0b44a9d7b51c3c62626640cda0e2c2f70fdacdc25bbbd68038369d14ebdf4c39",
    ),
    (
        "tokenizer_config.json",
        "a1d6bc8734a6f635dc158508bef000f8e2e5a759c7d92f984b2c86e5ff53425b",
    ),
];

#[derive(Clone, Copy, Debug)]
enum EmbeddingInput {
    Query,
    Passage,
}

impl EmbeddingInput {
    fn prefix(self) -> &'static str {
        match self {
            Self::Query => "query: ",
            Self::Passage => "passage: ",
        }
    }
}

struct LoadedEmbedding {
    model: TextEmbedding,
    validation_tokenizer: Tokenizer,
}

impl LoadedEmbedding {
    fn load() -> Result<Self> {
        let directory = model_directory();
        let config_file = read_verified_asset(&directory, "config.json")?;
        let onnx_file = read_verified_asset(&directory, "onnx/model_int8.onnx")?;
        let special_tokens_map_file = read_verified_asset(&directory, "special_tokens_map.json")?;
        let tokenizer_file = read_verified_asset(&directory, "tokenizer.json")?;
        let tokenizer_config_file = read_verified_asset(&directory, "tokenizer_config.json")?;

        let files = TokenizerFiles {
            tokenizer_file,
            config_file,
            special_tokens_map_file,
            tokenizer_config_file,
        };
        let model = UserDefinedEmbeddingModel::new(onnx_file, files)
            .with_pooling(Pooling::Mean)
            .with_quantization(QuantizationMode::Dynamic);
        let model = TextEmbedding::try_new_from_user_defined(
            model,
            InitOptionsUserDefined::new()
                .with_max_length(MODEL_MAX_TOKENS)
                .with_intra_threads(MODEL_THREADS),
        )
        .with_context(|| {
            format!("failed to load built-in embedding model {BUILTIN_EMBEDDING_MODEL_ID}")
        })?;
        let mut validation_tokenizer = model.tokenizer.clone();
        validation_tokenizer
            .with_truncation(None)
            .map_err(|error| anyhow!("failed to configure embedding tokenizer: {error}"))?;
        Ok(Self {
            model,
            validation_tokenizer,
        })
    }

    fn embed(&mut self, input: &str) -> Result<Vec<f32>> {
        let token_count = self
            .validation_tokenizer
            .encode(input, true)
            .map_err(|error| anyhow!("failed to tokenize memory text: {error}"))?
            .len();
        if token_count > MODEL_MAX_TOKENS {
            return Err(anyhow!(
                "memory text requires {token_count} embedding tokens; maximum is {MODEL_MAX_TOKENS}; store long content as an artifact"
            ));
        }

        let mut embeddings = self
            .model
            .embed([input], Some(1))
            .context("built-in embedding inference failed")?;
        let embedding = embeddings
            .pop()
            .ok_or_else(|| anyhow!("built-in embedding model returned no vector"))?;
        if embedding.len() != BUILTIN_EMBEDDING_DIMENSION {
            return Err(anyhow!(
                "built-in embedding dimension {} does not match schema dimension {}",
                embedding.len(),
                BUILTIN_EMBEDDING_DIMENSION
            ));
        }
        if embedding.iter().any(|value| !value.is_finite()) {
            return Err(anyhow!("built-in embedding contains a non-finite value"));
        }
        Ok(embedding)
    }
}

#[derive(Clone)]
pub(crate) struct BuiltInEmbedding {
    loaded: Arc<Mutex<Option<LoadedEmbedding>>>,
    #[cfg(test)]
    test_embedding: Option<Arc<TestEmbedding>>,
}

#[cfg(test)]
type TestEmbedding = dyn Fn(EmbeddingInput, &str) -> Result<Vec<f32>> + Send + Sync;

impl Default for BuiltInEmbedding {
    fn default() -> Self {
        Self {
            loaded: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            test_embedding: None,
        }
    }
}

impl BuiltInEmbedding {
    async fn embed(&self, kind: EmbeddingInput, text: &str) -> Result<Vec<f32>> {
        let input = prepare_embedding_input(kind, text)?;
        #[cfg(test)]
        if let Some(embed) = &self.test_embedding {
            return embed(kind, &input);
        }

        let loaded = self.loaded.clone();
        tokio::task::spawn_blocking(move || {
            let mut loaded = loaded
                .lock()
                .map_err(|_| anyhow!("built-in embedding lock is poisoned"))?;
            if loaded.is_none() {
                *loaded = Some(LoadedEmbedding::load()?);
            }
            loaded
                .as_mut()
                .expect("embedding initialized above")
                .embed(&input)
        })
        .await
        .context("built-in embedding worker failed")?
    }

    async fn warm_up(&self) -> Result<()> {
        self.embed(EmbeddingInput::Passage, "agentd embedding startup check")
            .await
            .map(|_| ())
    }

    #[cfg(test)]
    fn with_test_embedding<F>(mut self, embed: F) -> Self
    where
        F: Fn(EmbeddingInput, &str) -> Result<Vec<f32>> + Send + Sync + 'static,
    {
        self.test_embedding = Some(Arc::new(embed));
        self
    }
}

impl CapabilityEngine {
    pub async fn warm_up_embedding(&self) -> Result<()> {
        self.embedding.warm_up().await
    }

    pub(crate) async fn embed_query(&self, text: &str) -> Result<Vec<f32>> {
        self.embedding.embed(EmbeddingInput::Query, text).await
    }

    pub(crate) async fn embed_passage(&self, text: &str) -> Result<Vec<f32>> {
        self.embedding.embed(EmbeddingInput::Passage, text).await
    }

    #[cfg(test)]
    pub(crate) fn with_test_embedding<F>(mut self, embed: F) -> Self
    where
        F: Fn(&str) -> Result<Vec<f32>> + Send + Sync + 'static,
    {
        self.embedding = self
            .embedding
            .with_test_embedding(move |_kind, input| embed(input));
        self
    }
}

fn prepare_embedding_input(kind: EmbeddingInput, text: &str) -> Result<String> {
    let text = text.trim();
    if text.is_empty() {
        return Err(anyhow!("embedding input is required"));
    }
    Ok(format!("{}{text}", kind.prefix()))
}

fn model_directory() -> PathBuf {
    std::env::var_os(MODEL_DIR_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_MODEL_DIR))
}

fn read_verified_asset(directory: &Path, relative_path: &str) -> Result<Vec<u8>> {
    let expected = MODEL_ASSETS
        .iter()
        .find_map(|(path, hash)| (*path == relative_path).then_some(*hash))
        .ok_or_else(|| anyhow!("unknown built-in embedding asset: {relative_path}"))?;
    let path = directory.join(relative_path);
    let bytes = std::fs::read(&path)
        .with_context(|| format!("missing built-in embedding asset {}", path.display()))?;
    let actual = hex::encode(Sha256::digest(&bytes));
    if actual != expected {
        return Err(anyhow!(
            "built-in embedding asset {} failed SHA-256 validation: expected {expected}, got {actual}",
            path.display()
        ));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn e5_inputs_use_fixed_query_and_passage_prefixes() {
        assert_eq!(
            prepare_embedding_input(EmbeddingInput::Query, "  榴莲偏好 ").unwrap(),
            "query: 榴莲偏好"
        );
        assert_eq!(
            prepare_embedding_input(EmbeddingInput::Passage, "likes durian").unwrap(),
            "passage: likes durian"
        );
        assert!(prepare_embedding_input(EmbeddingInput::Query, "  ").is_err());
    }

    #[tokio::test]
    #[ignore = "requires the pinned multilingual-e5-small model assets"]
    async fn pinned_model_generates_normalized_384_dimension_vectors() {
        let embedding = BuiltInEmbedding::default();
        embedding.warm_up().await.unwrap();
        let vector = embedding
            .embed(EmbeddingInput::Query, "我喜欢热带水果")
            .await
            .unwrap();
        assert_eq!(vector.len(), BUILTIN_EMBEDDING_DIMENSION);
        let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-4, "unexpected norm {norm}");
    }
}
