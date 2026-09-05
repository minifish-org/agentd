use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use microsandbox::{
    sandbox::SandboxHandle, setup::Setup, LocalBackend, MicrosandboxError, Sandbox,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap},
    fs::OpenOptions,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant as StdInstant},
};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use uuid::Uuid;

const MANAGED_LABEL: &str = "agentd.managed";
const MANAGED_LABEL_VALUE: &str = "true";
const RUN_LABEL: &str = "agentd.run_id";
const DEFAULT_CWD: &str = "/workspace";
const HARD_MAX_COMMAND_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub struct SandboxManagerConfig {
    pub image: String,
    pub cpus: u8,
    pub memory_mib: u32,
    pub default_command_timeout: Duration,
    pub max_command_timeout: Duration,
    pub max_output_bytes_per_stream: usize,
    pub state_dir: PathBuf,
}

impl SandboxManagerConfig {
    fn validate(&self) -> Result<()> {
        if self.image.trim().is_empty() {
            return Err(anyhow!("sandbox.image must not be empty"));
        }
        if self.cpus == 0 {
            return Err(anyhow!("sandbox.cpus must be greater than zero"));
        }
        if self.memory_mib == 0 {
            return Err(anyhow!("sandbox.memory_mib must be greater than zero"));
        }
        if self.default_command_timeout.is_zero() {
            return Err(anyhow!(
                "sandbox.default_command_timeout_ms must be greater than zero"
            ));
        }
        if self.max_command_timeout < self.default_command_timeout {
            return Err(anyhow!(
                "sandbox.max_command_timeout_ms must be at least default_command_timeout_ms"
            ));
        }
        if self.max_command_timeout > HARD_MAX_COMMAND_TIMEOUT {
            return Err(anyhow!(
                "sandbox.max_command_timeout_ms must not exceed 60000"
            ));
        }
        if self.max_output_bytes_per_stream == 0 {
            return Err(anyhow!(
                "sandbox.max_output_bytes_per_stream must be greater than zero"
            ));
        }
        if !self.state_dir.is_absolute() {
            return Err(anyhow!("sandbox.state_dir must be absolute"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RunExecutionContext {
    pub run_id: Uuid,
    pub tenant: String,
    pub agent_ref: String,
    pub scope: String,
    pub deadline: Instant,
}

#[derive(Clone)]
pub struct SandboxSessionManager {
    config: Arc<SandboxManagerConfig>,
    backend: Arc<dyn SandboxBackend>,
    sessions: Arc<Mutex<HashMap<Uuid, Arc<Mutex<SessionSlot>>>>>,
}

impl std::fmt::Debug for SandboxSessionManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SandboxSessionManager")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct SessionSlot {
    closing: bool,
    creating: Option<JoinHandle<Result<Arc<dyn SandboxInstance>>>>,
    instance: Option<Arc<dyn SandboxInstance>>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum SandboxSessionRequest {
    Exec {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        cwd: Option<String>,
        #[serde(default)]
        env: BTreeMap<String, String>,
        #[serde(default)]
        timeout_ms: Option<u64>,
    },
    Shell {
        script: String,
        #[serde(default)]
        cwd: Option<String>,
        #[serde(default)]
        env: BTreeMap<String, String>,
        #[serde(default)]
        timeout_ms: Option<u64>,
    },
}

#[derive(Debug, Clone)]
struct SandboxCreateSpec {
    name: String,
    run_id: Uuid,
    image: String,
    cpus: u8,
    memory_mib: u32,
}

#[derive(Debug, Clone)]
struct SandboxCommand {
    program: String,
    args: Vec<String>,
    cwd: String,
    env: BTreeMap<String, String>,
    timeout: Duration,
}

#[derive(Debug)]
enum SandboxCommandOutcome {
    Completed {
        exit_code: i32,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
    },
    TimedOut,
    FailedToStart(String),
}

#[async_trait]
trait SandboxBackend: Send + Sync {
    async fn create(&self, spec: SandboxCreateSpec) -> Result<Arc<dyn SandboxInstance>>;
    async fn reap_managed(&self) -> Result<usize>;
}

#[async_trait]
trait SandboxInstance: Send + Sync {
    async fn execute(&self, command: SandboxCommand) -> Result<SandboxCommandOutcome>;
    async fn destroy(&self) -> Result<()>;
}

#[derive(Debug)]
struct MicrosandboxBackend;

struct MicrosandboxInstance {
    sandbox: Sandbox,
}

impl SandboxSessionManager {
    pub async fn new_microsandbox(config: SandboxManagerConfig) -> Result<Self> {
        config.validate()?;
        tokio::fs::create_dir_all(&config.state_dir)
            .await
            .with_context(|| {
                format!(
                    "failed to create microsandbox state directory {}",
                    config.state_dir.display()
                )
            })?;
        preflight_host()?;

        Setup::builder()
            .base_dir(config.state_dir.clone())
            .build()
            .install()
            .await
            .context("failed to install or verify microsandbox runtime")?;
        let local = LocalBackend::builder()
            .home(config.state_dir.clone())
            .default_cpus(config.cpus)
            .default_memory_mib(config.memory_mib)
            .shell("/bin/bash")
            .workdir(DEFAULT_CWD)
            .build()
            .await
            .context("failed to initialize microsandbox local backend")?;
        microsandbox::set_default_backend(local);

        Ok(Self::from_backend(config, Arc::new(MicrosandboxBackend)))
    }

    fn from_backend(config: SandboxManagerConfig, backend: Arc<dyn SandboxBackend>) -> Self {
        Self {
            config: Arc::new(config),
            backend,
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn reap_orphans(&self) -> Result<usize> {
        self.backend.reap_managed().await
    }

    pub async fn destroy_run(&self, run_id: Uuid) -> Result<()> {
        let slot = self.sessions.lock().await.remove(&run_id);
        let Some(slot) = slot else {
            return Ok(());
        };
        let (instance, creating) = {
            let mut state = slot.lock().await;
            state.closing = true;
            (state.instance.take(), state.creating.take())
        };
        let mut instances = instance.into_iter().collect::<Vec<_>>();
        if let Some(creating) = creating {
            match creating.await {
                Ok(Ok(instance)) => instances.push(instance),
                Ok(Err(error)) => {
                    tracing::warn!(run_id = %run_id, error = %error, "sandbox creation failed while closing session");
                }
                Err(error) => {
                    tracing::warn!(run_id = %run_id, error = %error, "sandbox creation task failed while closing session");
                }
            }
        }
        if instances.is_empty() {
            return Ok(());
        }
        let started = StdInstant::now();
        let mut result = Ok(());
        for instance in instances {
            if let Err(error) = instance.destroy().await {
                if result.is_ok() {
                    result = Err(error);
                }
            }
        }
        match &result {
            Ok(()) => tracing::info!(
                run_id = %run_id,
                duration_ms = started.elapsed().as_millis() as u64,
                "sandbox session destroyed"
            ),
            Err(error) => tracing::warn!(
                run_id = %run_id,
                duration_ms = started.elapsed().as_millis() as u64,
                error = %error,
                "failed to destroy sandbox session"
            ),
        }
        result
    }

    pub async fn destroy_all(&self) -> Result<()> {
        let run_ids: Vec<Uuid> = self.sessions.lock().await.keys().copied().collect();
        let mut first_error = None;
        for run_id in run_ids {
            if let Err(error) = self.destroy_run(run_id).await {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub(crate) async fn execute(
        &self,
        context: &RunExecutionContext,
        params: &Value,
    ) -> Result<Value> {
        let request: SandboxSessionRequest = serde_json::from_value(params.clone())
            .map_err(|error| anyhow!("invalid sandbox_session input: {error}"))?;
        let (action, program, args, cwd, env, requested_timeout_ms) = match request {
            SandboxSessionRequest::Exec {
                command,
                args,
                cwd,
                env,
                timeout_ms,
            } => {
                if command.trim().is_empty() {
                    return Err(anyhow!("sandbox_session exec command must not be empty"));
                }
                ("exec", command, args, cwd, env, timeout_ms)
            }
            SandboxSessionRequest::Shell {
                script,
                cwd,
                env,
                timeout_ms,
            } => {
                if script.trim().is_empty() {
                    return Err(anyhow!("sandbox_session shell script must not be empty"));
                }
                (
                    "shell",
                    "/bin/bash".to_string(),
                    vec!["-lc".to_string(), script],
                    cwd,
                    env,
                    timeout_ms,
                )
            }
        };
        let cwd = cwd.unwrap_or_else(|| DEFAULT_CWD.to_string());
        if !cwd.starts_with('/') {
            return Err(anyhow!(
                "sandbox_session cwd must be an absolute guest path"
            ));
        }
        for name in env.keys() {
            if !valid_env_name(name) {
                return Err(anyhow!(
                    "sandbox_session env contains invalid variable name {name:?}"
                ));
            }
        }
        if requested_timeout_ms == Some(0) {
            return Err(anyhow!(
                "sandbox_session timeout_ms must be greater than zero"
            ));
        }

        let configured_timeout = requested_timeout_ms
            .map(Duration::from_millis)
            .unwrap_or(self.config.default_command_timeout);
        if configured_timeout > self.config.max_command_timeout {
            return Err(anyhow!(
                "sandbox_session timeout_ms must not exceed {}",
                self.config.max_command_timeout.as_millis()
            ));
        }
        let remaining = context.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(command_result_json(
                action,
                SandboxCommandOutcome::TimedOut,
                Duration::ZERO,
                self.config.max_output_bytes_per_stream,
            ));
        }
        let slot = {
            let mut sessions = self.sessions.lock().await;
            sessions
                .entry(context.run_id)
                .or_insert_with(|| Arc::new(Mutex::new(SessionSlot::default())))
                .clone()
        };
        let started = StdInstant::now();
        let outcome = {
            let mut state = slot.lock().await;
            if state.closing {
                return Err(anyhow!("sandbox session is closing"));
            }
            if state.instance.is_none() {
                if state.creating.is_none() {
                    let name = format!("agentd-run-{}", context.run_id.simple());
                    tracing::info!(
                        run_id = %context.run_id,
                        tenant = %context.tenant,
                        agent = %context.agent_ref,
                        scope = %context.scope,
                        sandbox = %name,
                        "creating sandbox session"
                    );
                    let backend = self.backend.clone();
                    let spec = SandboxCreateSpec {
                        name,
                        run_id: context.run_id,
                        image: self.config.image.clone(),
                        cpus: self.config.cpus,
                        memory_mib: self.config.memory_mib,
                    };
                    state.creating = Some(tokio::spawn(async move { backend.create(spec).await }));
                }
                let created = state
                    .creating
                    .as_mut()
                    .expect("sandbox creation task initialized")
                    .await;
                state.creating.take();
                let created = created.context("sandbox creation task failed")?;
                state.instance = Some(created?);
            }
            let remaining = context.deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                SandboxCommandOutcome::TimedOut
            } else {
                state
                    .instance
                    .as_ref()
                    .expect("sandbox instance initialized")
                    .execute(SandboxCommand {
                        program,
                        args,
                        cwd,
                        env,
                        timeout: configured_timeout.min(remaining),
                    })
                    .await?
            }
        };
        let duration = started.elapsed();
        let result = command_result_json(
            action,
            outcome,
            duration,
            self.config.max_output_bytes_per_stream,
        );
        tracing::info!(
            run_id = %context.run_id,
            action,
            duration_ms = duration.as_millis() as u64,
            success = result["success"].as_bool().unwrap_or(false),
            exit_code = ?result["exit_code"].as_i64(),
            timed_out = result["timed_out"].as_bool().unwrap_or(false),
            "sandbox command completed"
        );
        Ok(result)
    }
}

#[async_trait]
impl SandboxBackend for MicrosandboxBackend {
    async fn create(&self, spec: SandboxCreateSpec) -> Result<Arc<dyn SandboxInstance>> {
        let sandbox = Sandbox::builder(&spec.name)
            .image(spec.image)
            .cpus(spec.cpus)
            .memory(spec.memory_mib)
            .shell("/bin/bash")
            .patch(|patch| patch.mkdir(DEFAULT_CWD, Some(0o755)))
            .workdir(DEFAULT_CWD)
            .label(MANAGED_LABEL, MANAGED_LABEL_VALUE)
            .label(RUN_LABEL, spec.run_id.to_string())
            .replace()
            .create()
            .await
            .with_context(|| format!("failed to create sandbox {}", spec.name))?;
        Ok(Arc::new(MicrosandboxInstance { sandbox }))
    }

    async fn reap_managed(&self) -> Result<usize> {
        let mut cursor = None;
        let mut handles: Vec<SandboxHandle> = Vec::new();
        loop {
            let page_cursor = cursor.clone();
            let page = Sandbox::list_with(|builder| {
                let builder = builder.limit(100).label(MANAGED_LABEL, MANAGED_LABEL_VALUE);
                match page_cursor {
                    Some(cursor) => builder.cursor(cursor),
                    None => builder,
                }
            })
            .await
            .context("failed to list managed microsandboxes")?;
            handles.extend(page.sandboxes);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }

        let count = handles.len();
        let mut first_error = None;
        for handle in handles {
            if let Err(error) = handle.destroy().await {
                tracing::warn!(sandbox = %handle.name(), error = %error, "failed to reap sandbox");
                if first_error.is_none() {
                    first_error = Some(anyhow!(error));
                }
            }
        }
        first_error.map_or(Ok(count), Err)
    }
}

#[async_trait]
impl SandboxInstance for MicrosandboxInstance {
    async fn execute(&self, command: SandboxCommand) -> Result<SandboxCommandOutcome> {
        let SandboxCommand {
            program,
            args,
            cwd,
            env,
            timeout,
        } = command;
        let result = self
            .sandbox
            .exec_with(program, |options| {
                options.args(args).cwd(cwd).envs(env).timeout(timeout)
            })
            .await;
        match result {
            Ok(output) => Ok(SandboxCommandOutcome::Completed {
                exit_code: output.status().code,
                stdout: output.stdout_bytes().to_vec(),
                stderr: output.stderr_bytes().to_vec(),
            }),
            Err(MicrosandboxError::ExecTimeout(_)) => Ok(SandboxCommandOutcome::TimedOut),
            Err(error @ MicrosandboxError::ExecFailed(_)) => {
                Ok(SandboxCommandOutcome::FailedToStart(error.to_string()))
            }
            Err(error) => Err(anyhow!(error).context("microsandbox command failed")),
        }
    }

    async fn destroy(&self) -> Result<()> {
        self.sandbox
            .destroy()
            .await
            .context("failed to stop and remove microsandbox")
    }
}

fn command_result_json(
    action: &str,
    outcome: SandboxCommandOutcome,
    duration: Duration,
    output_limit: usize,
) -> Value {
    let (exit_code, stdout, stderr, timed_out) = match outcome {
        SandboxCommandOutcome::Completed {
            exit_code,
            stdout,
            stderr,
        } => (Some(exit_code), stdout, stderr, false),
        SandboxCommandOutcome::TimedOut => (None, Vec::new(), Vec::new(), true),
        SandboxCommandOutcome::FailedToStart(message) => {
            (None, Vec::new(), message.into_bytes(), false)
        }
    };
    let (stdout, stdout_truncated) = truncate_output(&stdout, output_limit);
    let (stderr, stderr_truncated) = truncate_output(&stderr, output_limit);
    json!({
        "action": action,
        "success": exit_code == Some(0) && !timed_out,
        "exit_code": exit_code,
        "stdout": stdout,
        "stderr": stderr,
        "timed_out": timed_out,
        "duration_ms": duration.as_millis().min(u64::MAX as u128) as u64,
        "truncated": {
            "stdout": stdout_truncated,
            "stderr": stderr_truncated,
        }
    })
}

fn truncate_output(bytes: &[u8], limit: usize) -> (String, bool) {
    let truncated = bytes.len() > limit;
    let retained = &bytes[..bytes.len().min(limit)];
    (String::from_utf8_lossy(retained).into_owned(), truncated)
}

fn valid_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some('_' | 'A'..='Z' | 'a'..='z'))
        && chars.all(|ch| matches!(ch, '_' | 'A'..='Z' | 'a'..='z' | '0'..='9'))
}

#[cfg(target_os = "linux")]
fn preflight_host() -> Result<()> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/kvm")
        .context("sandbox is enabled but /dev/kvm is not available for read/write access")?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn preflight_host() -> Result<()> {
    let _ = OpenOptions::new();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

    #[derive(Default)]
    struct FakeBackend {
        creates: AtomicUsize,
        destroys: Arc<AtomicUsize>,
    }

    struct FakeInstance {
        destroys: Arc<AtomicUsize>,
    }

    struct BlockingBackend {
        started: Arc<Notify>,
        destroys: Arc<AtomicUsize>,
    }

    struct BlockingInstance {
        started: Arc<Notify>,
        destroys: Arc<AtomicUsize>,
    }

    struct CreatingBackend {
        started: Arc<Notify>,
        release: Arc<Notify>,
        destroys: Arc<AtomicUsize>,
    }

    struct FailingBackend {
        fail_create: bool,
    }

    struct FailingInstance;

    #[async_trait]
    impl SandboxBackend for FakeBackend {
        async fn create(&self, _spec: SandboxCreateSpec) -> Result<Arc<dyn SandboxInstance>> {
            self.creates.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(FakeInstance {
                destroys: self.destroys.clone(),
            }))
        }

        async fn reap_managed(&self) -> Result<usize> {
            Ok(0)
        }
    }

    #[async_trait]
    impl SandboxInstance for FakeInstance {
        async fn execute(&self, command: SandboxCommand) -> Result<SandboxCommandOutcome> {
            Ok(SandboxCommandOutcome::Completed {
                exit_code: 0,
                stdout: format!("{} {:?}", command.program, command.args).into_bytes(),
                stderr: Vec::new(),
            })
        }

        async fn destroy(&self) -> Result<()> {
            self.destroys.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl SandboxBackend for BlockingBackend {
        async fn create(&self, _spec: SandboxCreateSpec) -> Result<Arc<dyn SandboxInstance>> {
            Ok(Arc::new(BlockingInstance {
                started: self.started.clone(),
                destroys: self.destroys.clone(),
            }))
        }

        async fn reap_managed(&self) -> Result<usize> {
            Ok(0)
        }
    }

    #[async_trait]
    impl SandboxInstance for BlockingInstance {
        async fn execute(&self, _command: SandboxCommand) -> Result<SandboxCommandOutcome> {
            self.started.notify_one();
            std::future::pending().await
        }

        async fn destroy(&self) -> Result<()> {
            self.destroys.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl SandboxBackend for CreatingBackend {
        async fn create(&self, _spec: SandboxCreateSpec) -> Result<Arc<dyn SandboxInstance>> {
            self.started.notify_one();
            self.release.notified().await;
            Ok(Arc::new(FakeInstance {
                destroys: self.destroys.clone(),
            }))
        }

        async fn reap_managed(&self) -> Result<usize> {
            Ok(0)
        }
    }

    #[async_trait]
    impl SandboxBackend for FailingBackend {
        async fn create(&self, _spec: SandboxCreateSpec) -> Result<Arc<dyn SandboxInstance>> {
            if self.fail_create {
                Err(anyhow!("create infrastructure failure"))
            } else {
                Ok(Arc::new(FailingInstance))
            }
        }

        async fn reap_managed(&self) -> Result<usize> {
            Ok(0)
        }
    }

    #[async_trait]
    impl SandboxInstance for FailingInstance {
        async fn execute(&self, _command: SandboxCommand) -> Result<SandboxCommandOutcome> {
            Err(anyhow!("communication infrastructure failure"))
        }

        async fn destroy(&self) -> Result<()> {
            Ok(())
        }
    }

    fn config(output_limit: usize) -> SandboxManagerConfig {
        SandboxManagerConfig {
            image: "test".into(),
            cpus: 1,
            memory_mib: 512,
            default_command_timeout: Duration::from_secs(30),
            max_command_timeout: Duration::from_secs(60),
            max_output_bytes_per_stream: output_limit,
            state_dir: PathBuf::from("/tmp/agentd-sandbox-test"),
        }
    }

    fn context(run_id: Uuid) -> RunExecutionContext {
        RunExecutionContext {
            run_id,
            tenant: "demo".into(),
            agent_ref: "bot".into(),
            scope: "chat/1".into(),
            deadline: Instant::now() + Duration::from_secs(60),
        }
    }

    #[tokio::test]
    async fn one_session_is_reused_per_run_and_destroy_is_idempotent() {
        let backend = Arc::new(FakeBackend::default());
        let manager = SandboxSessionManager::from_backend(config(1024), backend.clone());
        let first_run = Uuid::new_v4();
        let second_run = Uuid::new_v4();
        for run_id in [first_run, first_run, second_run] {
            manager
                .execute(
                    &context(run_id),
                    &json!({"action":"exec","command":"printf","args":["ok"]}),
                )
                .await
                .unwrap();
        }
        assert_eq!(backend.creates.load(Ordering::SeqCst), 2);
        manager.destroy_run(first_run).await.unwrap();
        manager.destroy_run(first_run).await.unwrap();
        manager.destroy_all().await.unwrap();
        assert_eq!(backend.destroys.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn concurrent_first_calls_create_only_one_session() {
        let backend = Arc::new(FakeBackend::default());
        let manager = SandboxSessionManager::from_backend(config(1024), backend.clone());
        let run_id = Uuid::new_v4();
        let tasks: Vec<_> = (0..8)
            .map(|_| {
                let manager = manager.clone();
                tokio::spawn(async move {
                    manager
                        .execute(
                            &context(run_id),
                            &json!({"action":"exec","command":"printf","args":["ok"]}),
                        )
                        .await
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        assert_eq!(backend.creates.load(Ordering::SeqCst), 1);
        manager.destroy_all().await.unwrap();
    }

    #[tokio::test]
    async fn request_is_strict_and_shell_uses_bash_login_command() {
        let backend = Arc::new(FakeBackend::default());
        let manager = SandboxSessionManager::from_backend(config(1024), backend);
        let ctx = context(Uuid::new_v4());
        let result = manager
            .execute(
                &ctx,
                &json!({"action":"shell","script":"echo ok","unknown":true}),
            )
            .await;
        assert!(result.unwrap_err().to_string().contains("unknown field"));

        let result = manager
            .execute(&ctx, &json!({"action":"shell","script":"echo ok"}))
            .await
            .unwrap();
        assert!(result["stdout"]
            .as_str()
            .is_some_and(|stdout| stdout.contains("/bin/bash") && stdout.contains("-lc")));

        let result = manager
            .execute(
                &ctx,
                &json!({"action":"exec","command":"sleep","timeout_ms":60001}),
            )
            .await;
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must not exceed 60000"));
    }

    #[tokio::test]
    async fn infrastructure_failures_are_tool_errors() {
        for fail_create in [true, false] {
            let manager = SandboxSessionManager::from_backend(
                config(1024),
                Arc::new(FailingBackend { fail_create }),
            );
            let result = manager
                .execute(
                    &context(Uuid::new_v4()),
                    &json!({"action":"exec","command":"true"}),
                )
                .await;
            assert!(result.is_err());
            manager.destroy_all().await.unwrap();
        }
    }

    #[test]
    fn manager_config_enforces_the_hard_timeout_limit() {
        let mut invalid = config(1024);
        invalid.max_command_timeout = Duration::from_millis(60_001);
        assert!(invalid
            .validate()
            .unwrap_err()
            .to_string()
            .contains("must not exceed 60000"));
    }

    #[tokio::test]
    async fn aborted_command_releases_session_for_cancellation_cleanup() {
        let started = Arc::new(Notify::new());
        let destroys = Arc::new(AtomicUsize::new(0));
        let backend = Arc::new(BlockingBackend {
            started: started.clone(),
            destroys: destroys.clone(),
        });
        let manager = SandboxSessionManager::from_backend(config(1024), backend);
        let run_id = Uuid::new_v4();
        let task_manager = manager.clone();
        let task = tokio::spawn(async move {
            task_manager
                .execute(
                    &context(run_id),
                    &json!({"action":"exec","command":"sleep","args":["forever"]}),
                )
                .await
        });
        started.notified().await;
        task.abort();
        let _ = task.await;
        manager.destroy_run(run_id).await.unwrap();
        assert_eq!(destroys.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancellation_during_creation_destroys_the_created_session() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let destroys = Arc::new(AtomicUsize::new(0));
        let manager = SandboxSessionManager::from_backend(
            config(1024),
            Arc::new(CreatingBackend {
                started: started.clone(),
                release: release.clone(),
                destroys: destroys.clone(),
            }),
        );
        let run_id = Uuid::new_v4();
        let task_manager = manager.clone();
        let task = tokio::spawn(async move {
            task_manager
                .execute(&context(run_id), &json!({"action":"exec","command":"true"}))
                .await
        });
        started.notified().await;
        task.abort();
        let _ = task.await;

        let cleanup_manager = manager.clone();
        let cleanup = tokio::spawn(async move { cleanup_manager.destroy_run(run_id).await });
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), cleanup)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(destroys.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn output_is_bounded_per_stream_and_timeouts_are_normal_results() {
        let result = command_result_json(
            "exec",
            SandboxCommandOutcome::Completed {
                exit_code: 7,
                stdout: b"abcdef".to_vec(),
                stderr: b"uvwxyz".to_vec(),
            },
            Duration::from_millis(4),
            3,
        );
        assert_eq!(result["success"], false);
        assert_eq!(result["exit_code"], 7);
        assert_eq!(result["stdout"], "abc");
        assert_eq!(result["stderr"], "uvw");
        assert_eq!(result["truncated"], json!({"stdout":true,"stderr":true}));

        let timeout = command_result_json(
            "shell",
            SandboxCommandOutcome::TimedOut,
            Duration::from_secs(1),
            3,
        );
        assert_eq!(timeout["timed_out"], true);
        assert_eq!(timeout["exit_code"], Value::Null);
    }

    #[tokio::test]
    #[ignore = "requires a local microsandbox hypervisor and AGENTD_SANDBOX_TEST_IMAGE"]
    async fn local_hypervisor_session_persists_within_run_and_isolates_other_runs() {
        let image = std::env::var("AGENTD_SANDBOX_TEST_IMAGE")
            .expect("set AGENTD_SANDBOX_TEST_IMAGE to a built Dockerfile.sandbox image");
        // macOS has a 104-byte Unix socket path limit; its default per-user
        // temporary directory is too deep for microsandbox's derived sockets.
        let state = tempfile::tempdir_in("/tmp").unwrap();
        let manager = SandboxSessionManager::new_microsandbox(SandboxManagerConfig {
            image,
            cpus: 1,
            memory_mib: 512,
            default_command_timeout: Duration::from_secs(30),
            max_command_timeout: Duration::from_secs(60),
            max_output_bytes_per_stream: 512 * 1024,
            state_dir: state.path().to_path_buf(),
        })
        .await
        .unwrap();
        manager.reap_orphans().await.unwrap();

        let first = Uuid::new_v4();
        let first_context = RunExecutionContext {
            deadline: Instant::now() + Duration::from_secs(600),
            ..context(first)
        };
        let bootstrap = manager
            .execute(
                &first_context,
                &json!({
                    "action":"shell",
                    "script":"command -v curl >/dev/null && command -v python3 >/dev/null && command -v node >/dev/null || (apt-get update -qq && apt-get install -y -qq --no-install-recommends ca-certificates curl python3 nodejs)",
                    "timeout_ms":60000
                }),
            )
            .await
            .unwrap();
        assert_eq!(bootstrap["success"], true, "{bootstrap}");
        let python = manager
            .execute(
                &first_context,
                &json!({"action":"exec","command":"python3","args":["-c","print('python-ok')"]}),
            )
            .await
            .unwrap();
        assert_eq!(python["stdout"], "python-ok\n");
        let node = manager
            .execute(
                &first_context,
                &json!({"action":"exec","command":"node","args":["-e","console.log('node-ok')"]}),
            )
            .await
            .unwrap();
        assert_eq!(node["stdout"], "node-ok\n");
        let write = manager
            .execute(
                &first_context,
                &json!({
                    "action":"shell",
                    "script":"printf persistent > state.txt && printf %s \"$VALUE\"",
                    "env":{"VALUE":"env-ok"}
                }),
            )
            .await
            .unwrap();
        assert_eq!(write["stdout"], "env-ok");
        let read = manager
            .execute(
                &first_context,
                &json!({"action":"exec","command":"cat","args":["state.txt"]}),
            )
            .await
            .unwrap();
        assert_eq!(read["stdout"], "persistent");

        let isolated = manager
            .execute(
                &context(Uuid::new_v4()),
                &json!({"action":"shell","script":"test -e state.txt"}),
            )
            .await
            .unwrap();
        assert_eq!(isolated["success"], false);
        let network = manager
            .execute(
                &first_context,
                &json!({"action":"exec","command":"curl","args":["-fsS","https://example.com"]}),
            )
            .await
            .unwrap();
        assert_eq!(network["success"], true);
        let timeout = manager
            .execute(
                &first_context,
                &json!({"action":"shell","script":"sleep 2","timeout_ms":50}),
            )
            .await
            .unwrap();
        assert_eq!(timeout["timed_out"], true);

        manager.destroy_all().await.unwrap();
        assert_eq!(manager.reap_orphans().await.unwrap(), 0);
    }
}
