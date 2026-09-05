use anyhow::{bail, Context, Result};
use clap::Parser;
use serde::Deserialize;
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Parser)]
#[command(name = "agentd")]
pub(crate) struct Cli {
    /// Explicit config path. When unset, agentd searches for a config in
    /// this order:
    ///   1. `$AGENTD_CONFIG` environment variable (if set + non-empty)
    ///   2. `$HOME/.agentd.toml` (per-user config, kept out of version
    ///      control so it can hold real secrets like API keys)
    ///   3. `configs/agentd.toml` (the in-repo template; placeholder values
    ///      only)
    #[arg(long)]
    pub(crate) config: Option<String>,
    /// Delete the database before starting with the current schema.
    #[arg(long)]
    pub(crate) reset_data: bool,
}

pub(crate) fn resolve_config_path(cli_path: Option<&str>) -> String {
    if let Some(path) = cli_path.filter(|s| !s.is_empty()) {
        return path.to_string();
    }
    if let Ok(env_path) = std::env::var("AGENTD_CONFIG") {
        if !env_path.trim().is_empty() {
            return env_path;
        }
    }
    if let Ok(home) = std::env::var("HOME") {
        let user_config = format!("{home}/.agentd.toml");
        if std::path::Path::new(&user_config).exists() {
            return user_config;
        }
    }
    "configs/agentd.toml".to_string()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    pub(crate) rest_addr: String,
    pub(crate) database_path: String,
    pub(crate) scheduler_tick_ms: u64,
    pub(crate) run_concurrency: Option<usize>,
    pub(crate) dispatch_poll_interval_ms: Option<u64>,
    pub(crate) http_timeout_secs: Option<u64>,
    // OpenAI-compatible LLM provider (the only LLM transport).
    pub(crate) llm_api_base: Option<String>,
    pub(crate) llm_api_key: Option<String>,
    pub(crate) llm_model: Option<String>,
    /// Optional bearer token. When set, every `/v1/*` request must carry
    /// `Authorization: Bearer <token>`. Unset is allowed only for a loopback
    /// listener. CORS preflight (OPTIONS) is always allowed.
    #[serde(default)]
    pub(crate) api_token: Option<String>,
    /// Operator-supplied fallback `system_prompt` for an agent without a persona.
    #[serde(default)]
    pub(crate) default_chat_system_prompt: Option<String>,
    #[serde(default)]
    pub(crate) sandbox: Option<SandboxConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SandboxConfig {
    #[serde(default)]
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) image: Option<String>,
    #[serde(default = "default_sandbox_cpus")]
    pub(crate) cpus: u8,
    #[serde(default = "default_sandbox_memory_mib")]
    pub(crate) memory_mib: u32,
    #[serde(default = "default_sandbox_command_timeout_ms")]
    pub(crate) default_command_timeout_ms: u64,
    #[serde(default = "default_sandbox_max_command_timeout_ms")]
    pub(crate) max_command_timeout_ms: u64,
    #[serde(default = "default_sandbox_output_bytes")]
    pub(crate) max_output_bytes_per_stream: usize,
    #[serde(default)]
    pub(crate) state_dir: Option<String>,
}

fn default_sandbox_cpus() -> u8 {
    1
}

fn default_sandbox_memory_mib() -> u32 {
    512
}

fn default_sandbox_command_timeout_ms() -> u64 {
    30_000
}

fn default_sandbox_max_command_timeout_ms() -> u64 {
    60_000
}

fn default_sandbox_output_bytes() -> usize {
    512 * 1024
}

impl Config {
    pub(crate) fn resolve_runtime_paths(&mut self) -> Result<()> {
        let home = std::env::var_os("HOME").map(PathBuf::from);
        self.database_path = resolve_home_path(&self.database_path, home.as_deref())?;
        if let Some(state_dir) = self
            .sandbox
            .as_mut()
            .and_then(|sandbox| sandbox.state_dir.as_mut())
        {
            *state_dir = resolve_home_path(state_dir, home.as_deref())?;
        }
        Ok(())
    }

    pub(crate) fn sandbox_runtime_config(
        &self,
    ) -> Result<Option<agentd_core::SandboxManagerConfig>> {
        let Some(sandbox) = self.sandbox.as_ref().filter(|sandbox| sandbox.enabled) else {
            return Ok(None);
        };
        let image = sandbox
            .image
            .as_deref()
            .filter(|image| !image.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("sandbox.image is required when sandbox is enabled"))?;
        let state_dir = sandbox
            .state_dir
            .as_deref()
            .filter(|path| !path.trim().is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!("sandbox.state_dir is required when sandbox is enabled")
            })?;
        Ok(Some(agentd_core::SandboxManagerConfig {
            image: image.to_string(),
            cpus: sandbox.cpus,
            memory_mib: sandbox.memory_mib,
            default_command_timeout: Duration::from_millis(sandbox.default_command_timeout_ms),
            max_command_timeout: Duration::from_millis(sandbox.max_command_timeout_ms),
            max_output_bytes_per_stream: sandbox.max_output_bytes_per_stream,
            state_dir: PathBuf::from(state_dir),
        }))
    }

    pub(crate) fn rest_listener(&self) -> Result<SocketAddr> {
        let listener: SocketAddr = self
            .rest_addr
            .parse()
            .with_context(|| format!("invalid rest_addr: {}", self.rest_addr))?;
        let token_missing = self
            .api_token
            .as_deref()
            .is_none_or(|token| token.trim().is_empty());
        if !listener.ip().is_loopback() && token_missing {
            bail!("api_token is required when rest_addr binds to a non-loopback address");
        }
        Ok(listener)
    }
}

fn resolve_home_path(raw: &str, home: Option<&Path>) -> Result<String> {
    let suffix = raw
        .strip_prefix("${HOME}")
        .or_else(|| raw.strip_prefix('~'));
    let path = match suffix {
        Some("") => home
            .ok_or_else(|| anyhow::anyhow!("HOME is required to resolve runtime paths"))?
            .to_path_buf(),
        Some(suffix) if suffix.starts_with('/') => home
            .ok_or_else(|| anyhow::anyhow!("HOME is required to resolve runtime paths"))?
            .join(&suffix[1..]),
        Some(_) => bail!("unsupported home-relative runtime path: {raw}"),
        None => PathBuf::from(raw),
    };
    if !path.is_absolute() {
        bail!("runtime data path must be absolute: {raw}");
    }
    Ok(path.to_string_lossy().into_owned())
}

#[cfg(test)]
mod tests {
    use super::{resolve_home_path, Config};
    use std::{net::SocketAddr, path::Path, time::Duration};

    fn config_with(rest_addr: &str, api_token: Option<&str>) -> Config {
        let token = api_token
            .map(|token| format!("api_token = {token:?}\n"))
            .unwrap_or_default();
        toml::from_str(&format!(
            r#"
rest_addr = {rest_addr:?}
database_path = "/tmp/agentd.db"
scheduler_tick_ms = 1000
{token}"#
        ))
        .unwrap()
    }

    #[test]
    fn runtime_paths_expand_home_without_using_the_working_directory() {
        let home = Path::new("/Users/test");
        assert_eq!(
            resolve_home_path(
                "${HOME}/Library/Application Support/agentd/data/agentd.db",
                Some(home)
            )
            .unwrap(),
            "/Users/test/Library/Application Support/agentd/data/agentd.db"
        );
        assert_eq!(
            resolve_home_path("~/.local/state/agentd/agentd.db", Some(home)).unwrap(),
            "/Users/test/.local/state/agentd/agentd.db"
        );
    }

    #[test]
    fn runtime_paths_reject_relative_locations() {
        assert!(resolve_home_path(".agentd/agentd.db", Some(Path::new("/Users/test"))).is_err());
    }

    #[test]
    fn removed_embedding_provider_config_is_rejected() {
        let base = r#"
rest_addr = "127.0.0.1:8080"
database_path = "/tmp/agentd.db"
scheduler_tick_ms = 1000
"#;
        toml::from_str::<Config>(base).unwrap();
        let legacy = format!(
            "{base}\nembedding_api_base = \"http://127.0.0.1:8000/v1\"\nembedding_model = \"local-embedding\"\n"
        );
        assert!(toml::from_str::<Config>(&legacy).is_err());
    }

    #[test]
    fn loopback_listener_allows_missing_api_token() {
        assert_eq!(
            config_with("127.0.0.1:8080", None).rest_listener().unwrap(),
            "127.0.0.1:8080".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            config_with("[::1]:8080", None).rest_listener().unwrap(),
            "[::1]:8080".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn non_loopback_listener_requires_non_empty_api_token() {
        for address in ["0.0.0.0:8080", "[::]:8080", "192.0.2.10:8080"] {
            let error = config_with(address, None).rest_listener().unwrap_err();
            assert!(error.to_string().contains("api_token is required"));

            let error = config_with(address, Some("   "))
                .rest_listener()
                .unwrap_err();
            assert!(error.to_string().contains("api_token is required"));
        }
    }

    #[test]
    fn non_loopback_listener_accepts_api_token() {
        assert_eq!(
            config_with("0.0.0.0:8080", Some("secret"))
                .rest_listener()
                .unwrap(),
            "0.0.0.0:8080".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn sandbox_config_is_optional_and_disabled_by_default() {
        let config = config_with("127.0.0.1:8080", None);
        assert!(config.sandbox_runtime_config().unwrap().is_none());

        let config: Config = toml::from_str(
            r#"
rest_addr = "127.0.0.1:8080"
database_path = "/tmp/agentd.db"
scheduler_tick_ms = 1000

[sandbox]
enabled = false
"#,
        )
        .unwrap();
        assert!(config.sandbox_runtime_config().unwrap().is_none());
    }

    #[test]
    fn enabled_sandbox_uses_bounded_defaults_and_requires_paths() {
        let config: Config = toml::from_str(
            r#"
rest_addr = "127.0.0.1:8080"
database_path = "/tmp/agentd.db"
scheduler_tick_ms = 1000

[sandbox]
enabled = true
image = "ghcr.io/minifish-org/agentd-sandbox@sha256:test"
state_dir = "/var/lib/agentd/microsandbox"
"#,
        )
        .unwrap();
        let sandbox = config.sandbox_runtime_config().unwrap().unwrap();
        assert_eq!(sandbox.cpus, 1);
        assert_eq!(sandbox.memory_mib, 512);
        assert_eq!(sandbox.default_command_timeout, Duration::from_secs(30));
        assert_eq!(sandbox.max_command_timeout, Duration::from_secs(60));
        assert_eq!(sandbox.max_output_bytes_per_stream, 512 * 1024);

        let missing: Config = toml::from_str(
            r#"
rest_addr = "127.0.0.1:8080"
database_path = "/tmp/agentd.db"
scheduler_tick_ms = 1000

[sandbox]
enabled = true
"#,
        )
        .unwrap();
        assert!(missing.sandbox_runtime_config().is_err());
    }
}
