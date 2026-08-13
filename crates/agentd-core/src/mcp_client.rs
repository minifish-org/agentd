use agentd_api::{McpServer, McpTool, McpToolInvocationTarget, McpTransport};
use anyhow::{anyhow, Result};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, ACCEPT};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;
use tokio::time::timeout;

const MCP_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MCP_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(200);
const MAX_MCP_MESSAGE_BYTES: usize = 1024 * 1024;
const LATEST_PROTOCOL_VERSION: &str = "2025-11-25";
const SUPPORTED_PROTOCOL_VERSIONS: &[&str] =
    &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

type SessionSlot = Arc<Mutex<Option<HttpSession>>>;
type SessionMap = Arc<Mutex<HashMap<String, SessionSlot>>>;

#[derive(Clone)]
pub(crate) struct McpClient {
    http: reqwest::Client,
    sessions: SessionMap,
}

#[derive(Debug)]
struct HttpSession {
    session_id: Option<String>,
    protocol_version: String,
    next_id: u64,
}

#[derive(Debug)]
struct HttpResponse {
    result: Option<Value>,
    session_id: Option<String>,
}

#[derive(Debug)]
struct HttpSessionExpired;

impl fmt::Display for HttpSessionExpired {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MCP HTTP session expired")
    }
}

impl Error for HttpSessionExpired {}

impl McpClient {
    pub(crate) fn new(request_timeout: Duration) -> Self {
        let http = reqwest::Client::builder()
            .timeout(request_timeout)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            http,
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) async fn call_tool(
        &self,
        target: &McpToolInvocationTarget,
        params: &Value,
    ) -> Result<Value> {
        match &target.server.spec.transport {
            McpTransport::Stdio { .. } => call_tool_stdio(target, params).await,
            McpTransport::Http { url, headers_from } => {
                let headers = resolve_http_headers(headers_from)?;
                let slot = self
                    .session_slot(&target.server.tenant, &target.server.name)
                    .await;
                let mut session = slot.lock().await;
                for attempt in 0..2 {
                    if session.is_none() {
                        *session = Some(initialize_http(&self.http, url, &headers).await?);
                    }
                    let (session_id, protocol_version, request_id) = {
                        let active = session.as_mut().expect("session was initialized");
                        let request_id = active.next_id;
                        active.next_id += 1;
                        (
                            active.session_id.clone(),
                            active.protocol_version.clone(),
                            request_id,
                        )
                    };
                    let request = json!({
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "method": "tools/call",
                        "params": { "name": target.tool.name, "arguments": params }
                    });
                    match post_http(
                        &self.http,
                        url,
                        &headers,
                        session_id.as_deref(),
                        Some(&protocol_version),
                        &request,
                    )
                    .await
                    {
                        Ok(response) => {
                            if let Some(updated_session_id) = response.session_id {
                                session.as_mut().expect("session exists").session_id =
                                    Some(updated_session_id);
                            }
                            let result = response
                                .result
                                .ok_or_else(|| anyhow!("MCP tools/call returned an empty body"))?;
                            return validate_tool_result(result);
                        }
                        Err(error)
                            if attempt == 0
                                && error.downcast_ref::<HttpSessionExpired>().is_some() =>
                        {
                            *session = None;
                        }
                        Err(error) => return Err(error),
                    }
                }
                unreachable!("MCP HTTP call either returns or retries once")
            }
        }
    }

    pub(crate) async fn invalidate(&self, tenant: &str, name: &str) {
        self.sessions
            .lock()
            .await
            .remove(&session_key(tenant, name));
    }

    async fn session_slot(&self, tenant: &str, name: &str) -> SessionSlot {
        self.sessions
            .lock()
            .await
            .entry(session_key(tenant, name))
            .or_insert_with(|| Arc::new(Mutex::new(None)))
            .clone()
    }
}

pub async fn discover_tools(server: &McpServer) -> Result<Vec<McpTool>> {
    match &server.spec.transport {
        McpTransport::Stdio { .. } => discover_tools_stdio(server).await,
        McpTransport::Http { url, headers_from } => discover_tools_http(url, headers_from).await,
    }
}

async fn discover_tools_http(
    url: &str,
    headers_from: &BTreeMap<String, String>,
) -> Result<Vec<McpTool>> {
    let client = reqwest::Client::builder()
        .timeout(MCP_REQUEST_TIMEOUT)
        .build()?;
    let headers = resolve_http_headers(headers_from)?;
    let mut session = initialize_http(&client, url, &headers).await?;
    let mut tools = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let params = cursor
            .as_ref()
            .map(|cursor| json!({"cursor":cursor}))
            .unwrap_or_else(|| json!({}));
        let id = session.next_id;
        session.next_id += 1;
        let response = post_http(
            &client,
            url,
            &headers,
            session.session_id.as_deref(),
            Some(&session.protocol_version),
            &json!({"jsonrpc":"2.0","id":id,"method":"tools/list","params":params}),
        )
        .await?;
        if let Some(updated_session_id) = response.session_id {
            session.session_id = Some(updated_session_id);
        }
        let (mut page, next_cursor) = parse_tools_page(
            &response
                .result
                .ok_or_else(|| anyhow!("MCP tools/list returned an empty body"))?,
        )?;
        tools.append(&mut page);
        cursor = next_cursor;
        if cursor.is_none() {
            return Ok(tools);
        }
    }
}

async fn initialize_http(
    client: &reqwest::Client,
    url: &str,
    headers: &HeaderMap,
) -> Result<HttpSession> {
    let initialize = json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {
            "protocolVersion": LATEST_PROTOCOL_VERSION, "capabilities": {},
            "clientInfo": { "name": "agentd", "version": env!("CARGO_PKG_VERSION") }
        }
    });
    let response = post_http(client, url, headers, None, None, &initialize).await?;
    let result = response
        .result
        .ok_or_else(|| anyhow!("MCP initialize returned an empty body"))?;
    let protocol_version = negotiated_protocol(&result)?;
    let mut session_id = response.session_id;
    let initialized = post_http(
        client,
        url,
        headers,
        session_id.as_deref(),
        Some(&protocol_version),
        &json!({"jsonrpc":"2.0","method":"notifications/initialized","params":{}}),
    )
    .await?;
    if initialized.session_id.is_some() {
        session_id = initialized.session_id;
    }
    Ok(HttpSession {
        session_id,
        protocol_version,
        next_id: 2,
    })
}

async fn post_http(
    client: &reqwest::Client,
    url: &str,
    headers: &HeaderMap,
    session: Option<&str>,
    protocol_version: Option<&str>,
    request: &Value,
) -> Result<HttpResponse> {
    let mut builder = client
        .post(url)
        .headers(headers.clone())
        .header(ACCEPT, "application/json, text/event-stream")
        .json(request);
    if let Some(session) = session {
        builder = builder.header("mcp-session-id", session);
    }
    if let Some(protocol_version) = protocol_version {
        builder = builder.header("mcp-protocol-version", protocol_version);
    }
    let mut response = builder.send().await?;
    let status = response.status();
    let next_session = response
        .headers()
        .get("mcp-session-id")
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string);
    let body = read_bounded_http_body(&mut response).await?;
    if status == reqwest::StatusCode::NOT_FOUND && session.is_some() {
        return Err(HttpSessionExpired.into());
    }
    if !status.is_success() {
        return Err(anyhow!("MCP http returned {status}: {body}"));
    }
    let Some(message) = select_http_response(&body, request.get("id"))? else {
        return Ok(HttpResponse {
            result: None,
            session_id: next_session,
        });
    };
    if let Some(error) = message.get("error") {
        return Err(anyhow!("MCP server returned error: {error}"));
    }
    Ok(HttpResponse {
        result: message.get("result").cloned(),
        session_id: next_session,
    })
}

async fn read_bounded_http_body(response: &mut reqwest::Response) -> Result<String> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len().saturating_add(chunk.len()) > MAX_MCP_MESSAGE_BYTES {
            return Err(anyhow!(
                "MCP HTTP response exceeds {MAX_MCP_MESSAGE_BYTES} bytes"
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    String::from_utf8(bytes).map_err(|error| anyhow!("MCP HTTP response is not UTF-8: {error}"))
}

fn select_http_response(body: &str, expected_id: Option<&Value>) -> Result<Option<Value>> {
    let body = body.trim();
    if body.is_empty() {
        return Ok(None);
    }
    if let Ok(value) = serde_json::from_str(body) {
        return select_json_rpc_message(vec![value], expected_id);
    }
    select_json_rpc_message(parse_sse_messages(body)?, expected_id)
}

fn parse_sse_messages(body: &str) -> Result<Vec<Value>> {
    fn finish_event(data: &mut Vec<String>, messages: &mut Vec<Value>) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let payload = data.join("\n");
        data.clear();
        if !payload.trim().is_empty() {
            messages.push(serde_json::from_str(&payload)?);
        }
        Ok(())
    }

    let mut messages = Vec::new();
    let mut data = Vec::new();
    for line in body.lines() {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() {
            finish_event(&mut data, &mut messages)?;
            continue;
        }
        if line.starts_with(':') {
            continue;
        }
        if let Some(value) = line.strip_prefix("data:") {
            data.push(value.strip_prefix(' ').unwrap_or(value).to_string());
        }
    }
    finish_event(&mut data, &mut messages)?;
    if messages.is_empty() {
        return Err(anyhow!("invalid MCP SSE response"));
    }
    Ok(messages)
}

fn select_json_rpc_message(
    messages: Vec<Value>,
    expected_id: Option<&Value>,
) -> Result<Option<Value>> {
    let Some(expected_id) = expected_id else {
        return Ok(messages
            .into_iter()
            .find(|message| message.get("error").is_some()));
    };
    messages
        .into_iter()
        .find(|message| message.get("id") == Some(expected_id))
        .map(Some)
        .ok_or_else(|| anyhow!("MCP HTTP response missing matching JSON-RPC id {expected_id}"))
}

async fn discover_tools_stdio(server: &McpServer) -> Result<Vec<McpTool>> {
    let McpTransport::Stdio {
        command,
        args,
        env_from,
    } = &server.spec.transport
    else {
        return Err(anyhow!("expected stdio MCP transport"));
    };
    let mut child = spawn_stdio(command, args, env_from)?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("failed to open stdin for MCP server {command}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("failed to open stdout for MCP server {command}"))?;
    let result = discover_tools_stdio_session(&mut stdin, &mut BufReader::new(stdout)).await;
    shutdown_stdio(child, stdin).await;
    result
}

async fn discover_tools_stdio_session(
    stdin: &mut ChildStdin,
    stdout: &mut BufReader<ChildStdout>,
) -> Result<Vec<McpTool>> {
    send_json(stdin, &initialize_request(1)).await?;
    let initialized = read_response(stdout, 1).await?;
    negotiated_protocol(&initialized)?;
    send_json(
        stdin,
        &json!({"jsonrpc":"2.0","method":"notifications/initialized","params":{}}),
    )
    .await?;

    let mut tools = Vec::new();
    let mut cursor: Option<String> = None;
    let mut id = 2_u64;
    loop {
        let params = cursor
            .as_ref()
            .map(|cursor| json!({"cursor":cursor}))
            .unwrap_or_else(|| json!({}));
        send_json(
            stdin,
            &json!({"jsonrpc":"2.0","id":id,"method":"tools/list","params":params}),
        )
        .await?;
        let (mut page, next_cursor) = parse_tools_page(&read_response(stdout, id).await?)?;
        tools.append(&mut page);
        cursor = next_cursor;
        if cursor.is_none() {
            return Ok(tools);
        }
        id += 1;
    }
}

async fn call_tool_stdio(target: &McpToolInvocationTarget, params: &Value) -> Result<Value> {
    let McpTransport::Stdio {
        command,
        args,
        env_from,
    } = &target.server.spec.transport
    else {
        return Err(anyhow!("expected stdio MCP transport"));
    };
    let mut child = spawn_stdio(command, args, env_from)?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("failed to open stdin for MCP server {command}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("failed to open stdout for MCP server {command}"))?;
    let mut stdout = BufReader::new(stdout);
    let result = call_tool_stdio_session(target, params, &mut stdin, &mut stdout).await;
    shutdown_stdio(child, stdin).await;
    result
}

async fn call_tool_stdio_session(
    target: &McpToolInvocationTarget,
    params: &Value,
    stdin: &mut ChildStdin,
    stdout: &mut BufReader<ChildStdout>,
) -> Result<Value> {
    send_json(stdin, &initialize_request(1)).await?;
    let initialized = read_response(stdout, 1).await?;
    negotiated_protocol(&initialized)?;
    send_json(
        stdin,
        &json!({"jsonrpc":"2.0","method":"notifications/initialized","params":{}}),
    )
    .await?;
    send_json(
        stdin,
        &json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": { "name": target.tool.name, "arguments": params }
        }),
    )
    .await?;
    validate_tool_result(read_response(stdout, 2).await?)
}

fn spawn_stdio(
    command: &str,
    args: &[String],
    env_from: &BTreeMap<String, String>,
) -> Result<Child> {
    let environment = resolve_stdio_environment(env_from)?;
    Command::new(command)
        .args(args)
        .env_clear()
        .envs(environment)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| anyhow!("failed to start MCP server {command}: {error}"))
}

fn resolve_stdio_environment(
    env_from: &BTreeMap<String, String>,
) -> Result<Vec<(String, std::ffi::OsString)>> {
    let mut environment = Vec::new();
    for name in ["HOME", "PATH", "TMPDIR", "LANG", "LC_ALL", "SYSTEMROOT"] {
        if let Some(value) = std::env::var_os(name) {
            environment.push((name.to_string(), value));
        }
    }
    for (child_name, source_name) in env_from {
        let value = std::env::var_os(source_name).ok_or_else(|| {
            anyhow!("MCP environment variable {source_name} is not set for {child_name}")
        })?;
        environment.push((child_name.clone(), value));
    }
    Ok(environment)
}

fn resolve_http_headers(headers_from: &BTreeMap<String, String>) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    for (header_name, source_name) in headers_from {
        let value = std::env::var(source_name).map_err(|_| {
            anyhow!("MCP environment variable {source_name} is not set for header {header_name}")
        })?;
        let name = HeaderName::from_bytes(header_name.as_bytes())
            .map_err(|error| anyhow!("invalid MCP HTTP header {header_name}: {error}"))?;
        let value = HeaderValue::from_str(&value).map_err(|error| {
            anyhow!("invalid value from {source_name} for MCP HTTP header {header_name}: {error}")
        })?;
        headers.insert(name, value);
    }
    Ok(headers)
}

fn initialize_request(id: u64) -> Value {
    json!({
        "jsonrpc":"2.0","id":id,"method":"initialize",
        "params": {
            "protocolVersion":LATEST_PROTOCOL_VERSION,"capabilities":{},
            "clientInfo":{"name":"agentd","version":env!("CARGO_PKG_VERSION")}
        }
    })
}

fn negotiated_protocol(result: &Value) -> Result<String> {
    let protocol_version = result
        .get("protocolVersion")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("MCP initialize result missing protocolVersion"))?;
    if !SUPPORTED_PROTOCOL_VERSIONS.contains(&protocol_version) {
        return Err(anyhow!(
            "unsupported MCP protocol version {protocol_version}; supported versions: {}",
            SUPPORTED_PROTOCOL_VERSIONS.join(", ")
        ));
    }
    Ok(protocol_version.to_string())
}

fn validate_tool_result(result: Value) -> Result<Value> {
    if result
        .get("isError")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Err(anyhow!("MCP tool returned isError: {result}"));
    }
    Ok(result)
}

async fn shutdown_stdio(mut child: Child, stdin: ChildStdin) {
    drop(stdin);
    if timeout(MCP_SHUTDOWN_TIMEOUT, child.wait()).await.is_err() {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
}

fn session_key(tenant: &str, name: &str) -> String {
    format!("{tenant}\0{name}")
}

async fn send_json(stdin: &mut ChildStdin, value: &Value) -> Result<()> {
    let mut line = serde_json::to_vec(value)?;
    line.push(b'\n');
    stdin.write_all(&line).await?;
    stdin.flush().await?;
    Ok(())
}

async fn read_response<R>(stdout: &mut R, id: u64) -> Result<serde_json::Value>
where
    R: AsyncBufRead + Unpin,
{
    loop {
        let line = timeout(MCP_REQUEST_TIMEOUT, read_bounded_line(stdout))
            .await
            .map_err(|_| anyhow!("timed out waiting for MCP response id {id}"))??
            .ok_or_else(|| anyhow!("MCP server closed stdout before response id {id}"))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(trimmed)
            .map_err(|error| anyhow!("invalid MCP JSON-RPC message: {error}"))?;
        if value.get("id").and_then(Value::as_u64) != Some(id) {
            continue;
        }
        if let Some(error) = value.get("error") {
            return Err(anyhow!("MCP server returned error for id {id}: {error}"));
        }
        return Ok(value.get("result").cloned().unwrap_or(Value::Null));
    }
}

async fn read_bounded_line<R>(reader: &mut R) -> Result<Option<String>>
where
    R: AsyncBufRead + Unpin,
{
    let mut bytes = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if bytes.is_empty() {
                return Ok(None);
            }
            break;
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|position| position + 1)
            .unwrap_or(available.len());
        if bytes.len().saturating_add(take) > MAX_MCP_MESSAGE_BYTES {
            return Err(anyhow!(
                "MCP stdio message exceeds {MAX_MCP_MESSAGE_BYTES} bytes"
            ));
        }
        bytes.extend_from_slice(&available[..take]);
        let found_newline = available[take - 1] == b'\n';
        reader.consume(take);
        if found_newline {
            break;
        }
    }
    while matches!(bytes.last(), Some(b'\n' | b'\r')) {
        bytes.pop();
    }
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|error| anyhow!("MCP stdio response is not UTF-8: {error}"))
}

fn parse_tools_page(result: &Value) -> Result<(Vec<McpTool>, Option<String>)> {
    let tools = result
        .get("tools")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("MCP tools/list result missing tools array"))?;
    let tools = tools
        .iter()
        .map(|tool| {
            Ok(McpTool {
                name: tool
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("MCP tool missing name"))?
                    .to_string(),
                description: tool
                    .get("description")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                input_schema: tool
                    .get("inputSchema")
                    .or_else(|| tool.get("input_schema"))
                    .cloned()
                    .unwrap_or_else(|| json!({"type":"object"})),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let next_cursor = result
        .get("nextCursor")
        .or_else(|| result.get("next_cursor"))
        .and_then(Value::as_str)
        .map(str::to_string);
    Ok((tools, next_cursor))
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentd_api::{McpServer, McpServerSpec, McpTool};
    use axum::{
        extract::State,
        http::StatusCode,
        response::{IntoResponse, Response},
        routing::post,
        Json, Router,
    };
    use chrono::Utc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    async fn failing_mcp(Json(request): Json<Value>) -> Json<Value> {
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let result = match request.get("method").and_then(Value::as_str) {
            Some("initialize") => json!({"protocolVersion":LATEST_PROTOCOL_VERSION}),
            Some("tools/call") => {
                json!({"isError":true,"content":[{"type":"text","text":"failed"}]})
            }
            _ => json!({}),
        };
        Json(json!({"jsonrpc":"2.0","id":id,"result":result}))
    }

    #[tokio::test]
    async fn http_tool_error_is_returned_as_failure() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, Router::new().route("/", post(failing_mcp)))
                .await
                .unwrap();
        });
        let now = Utc::now();
        let target = McpToolInvocationTarget {
            server: McpServer {
                tenant: "demo".into(),
                name: "test".into(),
                spec: McpServerSpec {
                    enabled: true,
                    transport: McpTransport::Http {
                        url: format!("http://{address}/"),
                        headers_from: BTreeMap::new(),
                    },
                    allowed_tools: None,
                },
                tools: Vec::new(),
                last_error: None,
                created_at: now,
                updated_at: now,
            },
            tool: McpTool {
                name: "fail".into(),
                description: None,
                input_schema: json!({"type":"object"}),
            },
        };

        let client = McpClient::new(MCP_REQUEST_TIMEOUT);
        let error = client.call_tool(&target, &json!({})).await.unwrap_err();
        assert!(error.to_string().contains("isError"));
    }

    #[derive(Clone, Default)]
    struct SessionServer {
        initializes: Arc<AtomicUsize>,
        calls: Arc<AtomicUsize>,
        expire_first_call: Arc<AtomicBool>,
    }

    async fn session_mcp(
        State(state): State<SessionServer>,
        headers: axum::http::HeaderMap,
        Json(request): Json<Value>,
    ) -> Response {
        let method = request.get("method").and_then(Value::as_str);
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        if method == Some("initialize") {
            assert_eq!(
                request["params"]["protocolVersion"],
                LATEST_PROTOCOL_VERSION
            );
            let number = state.initializes.fetch_add(1, Ordering::SeqCst) + 1;
            return (
                [("mcp-session-id", format!("session-{number}"))],
                Json(json!({
                    "jsonrpc":"2.0","id":id,
                    "result":{"protocolVersion":LATEST_PROTOCOL_VERSION}
                })),
            )
                .into_response();
        }
        assert_eq!(
            headers
                .get("mcp-protocol-version")
                .and_then(|value| value.to_str().ok()),
            Some(LATEST_PROTOCOL_VERSION)
        );
        if method == Some("tools/call") {
            let call = state.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 && state.expire_first_call.load(Ordering::SeqCst) {
                return StatusCode::NOT_FOUND.into_response();
            }
        }
        Json(json!({
            "jsonrpc":"2.0","id":id,
            "result":{"content":[{"type":"text","text":"ok"}]}
        }))
        .into_response()
    }

    fn http_target(address: std::net::SocketAddr, name: &str) -> McpToolInvocationTarget {
        let now = Utc::now();
        McpToolInvocationTarget {
            server: McpServer {
                tenant: "demo".into(),
                name: name.into(),
                spec: McpServerSpec {
                    enabled: true,
                    transport: McpTransport::Http {
                        url: format!("http://{address}/"),
                        headers_from: BTreeMap::new(),
                    },
                    allowed_tools: None,
                },
                tools: Vec::new(),
                last_error: None,
                created_at: now,
                updated_at: now,
            },
            tool: McpTool {
                name: "ping".into(),
                description: None,
                input_schema: json!({"type":"object"}),
            },
        }
    }

    async fn start_session_server(state: SessionServer) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", post(session_mcp))
                    .with_state(state),
            )
            .await
            .unwrap();
        });
        address
    }

    #[tokio::test]
    async fn http_session_is_reused_across_tool_calls() {
        let state = SessionServer::default();
        let address = start_session_server(state.clone()).await;
        let target = http_target(address, "reuse");
        let client = McpClient::new(MCP_REQUEST_TIMEOUT);

        client.call_tool(&target, &json!({})).await.unwrap();
        client.call_tool(&target, &json!({})).await.unwrap();

        assert_eq!(state.initializes.load(Ordering::SeqCst), 1);
        assert_eq!(state.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn expired_http_session_is_reinitialized_once() {
        let state = SessionServer::default();
        state.expire_first_call.store(true, Ordering::SeqCst);
        let address = start_session_server(state.clone()).await;
        let target = http_target(address, "expires");
        let client = McpClient::new(MCP_REQUEST_TIMEOUT);

        client.call_tool(&target, &json!({})).await.unwrap();

        assert_eq!(state.initializes.load(Ordering::SeqCst), 2);
        assert_eq!(state.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn http_message_size_is_bounded() {
        async fn oversized_response() -> String {
            "x".repeat(MAX_MCP_MESSAGE_BYTES + 1)
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, Router::new().route("/", post(oversized_response)))
                .await
                .unwrap();
        });
        let target = http_target(address, "oversized");
        let error = McpClient::new(MCP_REQUEST_TIMEOUT)
            .call_tool(&target, &json!({}))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("exceeds"));
    }

    #[test]
    fn sse_selects_the_matching_response_among_other_messages() {
        let body = concat!(
            "id: prime\n",
            "data:\n\n",
            "event: message\n",
            "data: {\"jsonrpc\":\"2.0\",\n",
            "data: \"method\":\"notifications/progress\"}\n\n",
            "event: message\n",
            "data: {\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{\"ok\":true}}\n\n",
            "event: message\n",
            "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/tools/list_changed\"}\n\n"
        );
        let message = select_http_response(body, Some(&json!(7)))
            .unwrap()
            .unwrap();
        assert_eq!(message["result"], json!({"ok":true}));
        assert!(select_http_response(body, Some(&json!(8)))
            .unwrap_err()
            .to_string()
            .contains("matching JSON-RPC id"));
    }

    #[tokio::test]
    async fn stdio_message_size_is_bounded() {
        let (mut writer, reader) = tokio::io::duplex(64 * 1024);
        let write = tokio::spawn(async move {
            writer
                .write_all(&vec![b'x'; MAX_MCP_MESSAGE_BYTES + 1])
                .await
                .unwrap();
        });
        let error = read_response(&mut BufReader::new(reader), 1)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("exceeds"));
        write.abort();
    }
}
