use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode, Method},
    response::{IntoResponse, Response},
};
use std::{collections::HashMap, path::PathBuf, process::Stdio};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::process::Command;
use tokio::sync::Notify;
use tokio_util::io::ReaderStream;

use crate::types::{spawn_idle_timeout, AppState, GitResponseStream};
use crate::utils::{
    ensure_repo_exists_async, get_dir_size_async, get_safe_repo_path, pkt_line_encode,
    update_head_if_invalid_async, validate_service,
};

// REST API: GET / (health check)
pub async fn handle_health_check() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

// Git ref advertisements and RPC results are dynamic and security-sensitive.
// Prevent reverse proxies and clients from caching them.
fn add_no_cache_headers(headers: &mut HeaderMap) {
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, max-age=0, must-revalidate"),
    );
    headers.insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
    headers.insert(
        header::EXPIRES,
        HeaderValue::from_static("Fri, 01 Jan 1980 00:00:00 GMT"),
    );
}

/// Request bodies above this size are spooled to a temp file instead of RAM.
const BODY_MEMORY_SPOOL_THRESHOLD: usize = 8 * 1024 * 1024;
static TEMP_BODY_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A fully buffered request body, either in memory or in a temp file.
/// Files are removed when the body is dropped (after being fed to git).
enum SpooledRequestBody {
    Memory(std::io::Cursor<Vec<u8>>),
    File { file: tokio::fs::File, path: PathBuf },
}

impl Drop for SpooledRequestBody {
    fn drop(&mut self) {
        if let SpooledRequestBody::File { path, .. } = self {
            let _ = std::fs::remove_file(path);
        }
    }
}

impl AsyncRead for SpooledRequestBody {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut *self {
            SpooledRequestBody::Memory(cursor) => Pin::new(cursor).poll_read(cx, buf),
            SpooledRequestBody::File { file, .. } => Pin::new(file).poll_read(cx, buf),
        }
    }
}

fn temp_body_path() -> PathBuf {
    let seq = TEMP_BODY_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "sgit-request-{}-{}-{}.tmp",
        std::process::id(),
        now,
        seq
    ))
}

/// Read the complete request body, enforcing `limit_bytes` by counting the
/// actual bytes received. `Content-Length` cannot be trusted here because git
/// uses `Transfer-Encoding: chunked`, which axum's `DefaultBodyLimit` does not
/// apply to when the handler consumes `Body` directly.
///
/// Returning a `413` is only possible before response headers are sent, so the
/// body must be fully received first. Large bodies are spooled to disk.
async fn spool_request_body(
    body: Body,
    limit_bytes: u64,
) -> Result<(SpooledRequestBody, u64), StatusCode> {
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};

    let mut stream = body.into_data_stream();
    let mut total: u64 = 0;
    let mut memory: Vec<u8> = Vec::new();
    let mut file: Option<(tokio::fs::File, PathBuf)> = None;

    while let Some(chunk) = tokio_stream::StreamExt::next(&mut stream).await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(_) => {
                if let Some((_, path)) = file.take() {
                    let _ = tokio::fs::remove_file(&path).await;
                }
                return Err(StatusCode::BAD_REQUEST);
            }
        };

        total = total.saturating_add(chunk.len() as u64);
        if total > limit_bytes {
            if let Some((_, path)) = file.take() {
                let _ = tokio::fs::remove_file(&path).await;
            }
            return Err(StatusCode::PAYLOAD_TOO_LARGE);
        }

        if let Some((writer, _)) = file.as_mut() {
            if writer.write_all(&chunk).await.is_err() {
                if let Some((_, path)) = file.take() {
                    let _ = tokio::fs::remove_file(&path).await;
                }
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        } else if memory.len().saturating_add(chunk.len()) <= BODY_MEMORY_SPOOL_THRESHOLD {
            memory.extend_from_slice(&chunk);
        } else {
            let path = temp_body_path();
            let mut options = tokio::fs::OpenOptions::new();
            options.read(true).write(true).create_new(true);
            #[cfg(unix)]
            options.mode(0o600);
            let mut writer = match options.open(&path).await {
                Ok(writer) => writer,
                Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
            };
            if writer.write_all(&memory).await.is_err()
                || writer.write_all(&chunk).await.is_err()
            {
                let _ = tokio::fs::remove_file(&path).await;
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
            memory.clear();
            memory.shrink_to_fit();
            file = Some((writer, path));
        }
    }

    if let Some((mut writer, path)) = file {
        if writer.flush().await.is_err()
            || writer
                .seek(std::io::SeekFrom::Start(0))
                .await
                .is_err()
        {
            let _ = tokio::fs::remove_file(&path).await;
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
        Ok((SpooledRequestBody::File { file: writer, path }, total))
    } else {
        Ok((SpooledRequestBody::Memory(std::io::Cursor::new(memory)), total))
    }
}

/// Strip exactly one endpoint suffix and reject non-canonical paths that still
/// end in an endpoint segment (e.g. `/repo/info/refs/info/refs`).
fn strip_endpoint_suffix<'a>(path: &'a str, suffix: &str) -> Option<&'a str> {
    let repo = path.strip_suffix(suffix)?;
    if repo.ends_with("/info/refs")
        || repo.ends_with("/git-upload-pack")
        || repo.ends_with("/git-receive-pack")
    {
        return None;
    }
    Some(repo)
}

// Catch-all Git handler supporting arbitrary nested namespaces/subfolders
pub async fn handle_git_request(
    State(state): State<AppState>,
    Path(path): Path<String>,
    method: Method,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if method == Method::GET {
        if let Some(repo) = strip_endpoint_suffix(&path, "/info/refs") {
            return handle_info_refs_internal(repo.to_string(), params, headers).await;
        }
    } else if method == Method::POST {
        if let Some(repo) = strip_endpoint_suffix(&path, "/git-upload-pack") {
            return execute_git_service_stream(
                repo.to_string(),
                "git-upload-pack",
                headers,
                body,
                state.git_semaphore.clone(),
            )
            .await;
        }
        if let Some(repo) = strip_endpoint_suffix(&path, "/git-receive-pack") {
            return execute_git_service_stream(
                repo.to_string(),
                "git-receive-pack",
                headers,
                body,
                state.git_semaphore.clone(),
            )
            .await;
        }
    }

    StatusCode::NOT_FOUND.into_response()
}

// 1. Discover refs handler
pub async fn handle_info_refs_internal(
    repo: String,
    params: HashMap<String, String>,
    headers: HeaderMap,
) -> Response {
    let Some(repo_path) = get_safe_repo_path(&repo) else {
        return (StatusCode::BAD_REQUEST, "Invalid repository name").into_response();
    };

    let Some(raw_service) = params.get("service") else {
        return (StatusCode::BAD_REQUEST, "Missing service parameter").into_response();
    };

    let Some(service) = validate_service(raw_service) else {
        return (StatusCode::FORBIDDEN, "Invalid or unsupported git service").into_response();
    };

    let is_push = service == "git-receive-pack";
    let quota_bytes = if is_push {
        crate::utils::get_repo_size_quota_bytes()
    } else {
        None
    };

    // Do not auto-create a repository that would immediately be over quota
    // (a zero-byte quota rejects even the empty repository).
    if is_push && !repo_path.exists() && quota_bytes == Some(0) {
        return (StatusCode::INSUFFICIENT_STORAGE, "Repository storage quota exceeded")
            .into_response();
    }

    if let Err(err_msg) = ensure_repo_exists_async(repo_path.clone(), is_push).await {
        let status = if err_msg.contains("does not exist") {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };
        return (status, err_msg).into_response();
    }
    update_head_if_invalid_async(repo_path.clone()).await;

    if let Some(quota_bytes) = quota_bytes {
        let current_size = match get_dir_size_async(repo_path.clone()).await {
            Ok(size) => size,
            Err(_) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to measure repository")
                    .into_response();
            }
        };
        if current_size >= quota_bytes {
            return (StatusCode::INSUFFICIENT_STORAGE, "Repository storage quota exceeded")
                .into_response();
        }
    }

    let mut cmd = Command::new(service);
    cmd.arg("--stateless-rpc")
        .arg("--advertise-refs")
        .arg(&repo_path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    // Support Git Protocol V2 if specified by client
    if let Some(protocol) = headers.get("Git-Protocol") {
        if let Ok(p_str) = protocol.to_str() {
            cmd.env("GIT_PROTOCOL", p_str);
        }
    }

    // Read timeout from environment variable SGIT_READ_TIMEOUT (default 15s)
    let read_timeout_secs = std::env::var("SGIT_READ_TIMEOUT")
        .ok()
        .and_then(|val| val.parse::<u64>().ok())
        .unwrap_or(15);

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(_) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to spawn git process")
                .into_response();
        }
    };
    let mut child_stdout = child.stdout.take().unwrap();
    let mut child_stderr = child.stderr.take().unwrap();

    // Drain both pipes concurrently so a large ref advertisement cannot fill
    // the pipe buffers and deadlock the child.
    let stdout_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        use tokio::io::AsyncReadExt;
        child_stdout.read_to_end(&mut buf).await.map(|_| buf)
    });
    let stderr_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        use tokio::io::AsyncReadExt;
        child_stderr.read_to_end(&mut buf).await.map(|_| buf)
    });

    let status = match tokio::time::timeout(
        std::time::Duration::from_secs(read_timeout_secs),
        child.wait(),
    )
    .await
    {
        Ok(Ok(status)) => status,
        Ok(Err(_)) => {
            stdout_task.abort();
            stderr_task.abort();
            return (StatusCode::INTERNAL_SERVER_ERROR, "Git execution error").into_response();
        }
        Err(_) => {
            // The wait future is dropped above, so explicitly terminate and reap
            // the child instead of leaving it running after the 504 response.
            let _ = child.start_kill();
            let _ = child.wait().await;
            // A grandchild (for example a `sleep` started by a wrapper script)
            // may still hold the pipes open. Abort the readers so the timeout
            // response is returned immediately rather than after that process
            // exits.
            stdout_task.abort();
            stderr_task.abort();
            return (StatusCode::GATEWAY_TIMEOUT, "Request timed out").into_response();
        }
    };

    let stdout = match stdout_task.await {
        Ok(Ok(buf)) => buf,
        _ => return (StatusCode::INTERNAL_SERVER_ERROR, "Git output error").into_response(),
    };
    let stderr = match stderr_task.await {
        Ok(Ok(buf)) => buf,
        _ => return (StatusCode::INTERNAL_SERVER_ERROR, "Git output error").into_response(),
    };
    if !status.success() {
        if !stderr.is_empty() {
            crate::log_stderr!(
                "Git service [{}] stderr: {}",
                service,
                String::from_utf8_lossy(&stderr)
            );
        }
        return (StatusCode::INTERNAL_SERVER_ERROR, "Git execution error").into_response();
    }
    let output = std::process::Output {
        status,
        stdout,
        stderr,
    };

    // Calculate pkt-line header using helper function
    let line_content = format!("# service={}\n", service);
    let pkt_line = pkt_line_encode(line_content.as_bytes());

    let mut body = Vec::with_capacity(pkt_line.len() + 4 + output.stdout.len());
    body.extend_from_slice(&pkt_line);
    body.extend_from_slice(b"0000"); // Flush-pkt
    body.extend_from_slice(&output.stdout);

    let mut res_headers = HeaderMap::new();
    res_headers.insert(
        header::CONTENT_TYPE,
        format!("application/x-{}-advertisement", service)
            .parse()
            .unwrap(),
    );
    add_no_cache_headers(&mut res_headers);

    (StatusCode::OK, res_headers, body).into_response()
}

// For backward compatibility/testing of older handlers
pub async fn handle_info_refs(
    Path(repo): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    handle_info_refs_internal(repo, params, headers).await
}

// 2. Upload pack handler (Fetch/Clone)
pub async fn handle_upload_pack(
    State(state): State<AppState>,
    Path(repo): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> impl IntoResponse {
    execute_git_service_stream(
        repo,
        "git-upload-pack",
        headers,
        body,
        state.git_semaphore.clone(),
    )
    .await
}

// 3. Receive pack handler (Push)
pub async fn handle_receive_pack(
    State(state): State<AppState>,
    Path(repo): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> impl IntoResponse {
    execute_git_service_stream(
        repo,
        "git-receive-pack",
        headers,
        body,
        state.git_semaphore.clone(),
    )
    .await
}

// Helper to execute Git process asynchronously with full bi-directional streaming
pub async fn execute_git_service_stream(
    repo: String,
    service: &'static str,
    headers: HeaderMap,
    body: Body,
    semaphore: Option<Arc<tokio::sync::Semaphore>>,
) -> Response {
    let Some(repo_path) = get_safe_repo_path(&repo) else {
        return (StatusCode::BAD_REQUEST, "Invalid repository name").into_response();
    };

    // Hold this permit for the complete process/body lifetime. The request
    // concurrency layer releases its permit as soon as response headers are
    // produced, which is far too early for a streaming git transfer.
    let permit = if let Some(semaphore) = semaphore {
        match semaphore.acquire_owned().await {
            Ok(permit) => Some(permit),
            Err(_) => {
                return (StatusCode::SERVICE_UNAVAILABLE, "Git service unavailable")
                    .into_response();
            }
        }
    } else {
        None
    };

    let is_push = service == "git-receive-pack";

    // Enforce Push Size Limit (Max Request Body). A value of 0 disables the limit.
    let max_request_size_mb = std::env::var("SGIT_MAX_REQUEST_SIZE_MB")
        .ok()
        .and_then(|val| val.parse::<u64>().ok())
        .unwrap_or(500);
    let limit_bytes = if max_request_size_mb == 0 {
        u64::MAX
    } else {
        max_request_size_mb.saturating_mul(1024 * 1024)
    };

    if let Some(content_length) = headers.get(header::CONTENT_LENGTH) {
        if let Ok(cl_str) = content_length.to_str() {
            if let Ok(cl) = cl_str.parse::<u64>() {
                if cl > limit_bytes {
                    return StatusCode::PAYLOAD_TOO_LARGE.into_response();
                }
            }
        }
    }

    // Buffer the complete body (or spool it to disk) so an oversized chunked
    // request can be rejected with 413 before any git process is spawned.
    let (request_body, body_size) = match spool_request_body(body, limit_bytes).await {
        Ok(body) => body,
        Err(status) => return status.into_response(),
    };

    // Enforce the per-repository quota before auto-creation and before spawning
    // git. The incoming body (including the pack for a push) is counted against
    // the remaining space so a single push cannot blow past the quota.
    let quota_bytes = if is_push {
        crate::utils::get_repo_size_quota_bytes()
    } else {
        None
    };
    let mut repo_size_before_push = 0u64;
    if let Some(quota_bytes) = quota_bytes {
        repo_size_before_push = if repo_path.exists() {
            match get_dir_size_async(repo_path.clone()).await {
                Ok(size) => size,
                Err(_) => {
                    return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to measure repository")
                        .into_response();
                }
            }
        } else {
            0
        };

        if repo_size_before_push >= quota_bytes
            || repo_size_before_push.saturating_add(body_size) > quota_bytes
        {
            return (StatusCode::INSUFFICIENT_STORAGE, "Repository storage quota exceeded")
                .into_response();
        }
    }

    if let Err(err_msg) = ensure_repo_exists_async(repo_path.clone(), is_push).await {
        let status = if err_msg.contains("does not exist") {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };
        return (status, err_msg).into_response();
    }

    let mut cmd = Command::new(service);
    cmd.arg("--stateless-rpc")
        .arg(&repo_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    // Enforce the remaining quota while receive-pack unpacks, so a pack that
    // expands beyond the advertised body size is also stopped by git.
    if let Some(remaining_bytes) =
        quota_bytes.map(|quota| quota.saturating_sub(repo_size_before_push))
    {
        let config_count = std::env::var("GIT_CONFIG_COUNT")
            .ok()
            .and_then(|val| val.parse::<usize>().ok())
            .unwrap_or(0);
        cmd.env("GIT_CONFIG_COUNT", (config_count + 1).to_string())
            .env(
                format!("GIT_CONFIG_KEY_{}", config_count),
                "receive.maxInputSize",
            )
            .env(
                format!("GIT_CONFIG_VALUE_{}", config_count),
                remaining_bytes.to_string(),
            );
    }

    // Pass Git-Protocol header if present
    if let Some(protocol) = headers.get("Git-Protocol") {
        if let Ok(p_str) = protocol.to_str() {
            cmd.env("GIT_PROTOCOL", p_str);
        }
    }

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to spawn git process",
            )
                .into_response();
        }
    };

    let mut child_stdin = child.stdin.take().unwrap();
    let child_stdout = child.stdout.take().unwrap();
    let mut child_stderr = child.stderr.take().unwrap();

    let cancellation_token = tokio_util::sync::CancellationToken::new();
    let child = Arc::new(Mutex::new(Some(child)));
    let activity = Arc::new(Notify::new());
    let timed_out = Arc::new(AtomicBool::new(false));

    // Spawn background task to read stderr and log it
    let stderr_token = cancellation_token.clone();
    tokio::spawn(async move {
        let mut buf = Vec::new();
        use tokio::io::AsyncReadExt;
        tokio::select! {
            _ = stderr_token.cancelled() => {},
            _ = child_stderr.read_to_end(&mut buf) => {
                if !buf.is_empty() {
                    let err_str = String::from_utf8_lossy(&buf);
                    crate::log_stderr!("Git service [{}] stderr: {}", service, err_str);
                }
            }
        }
    });

    // Feed the already-buffered request into process stdin. Each written chunk
    // counts as progress for the idle timeout, so a long upload is not mistaken
    // for a stalled stream.
    let stdin_token = cancellation_token.clone();
    let stdin_activity = activity.clone();
    let mut request_body = request_body;
    tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut buf = vec![0u8; 64 * 1024];

        tokio::select! {
            _ = stdin_token.cancelled() => {}
            _ = async {
                loop {
                    match request_body.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            if child_stdin.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                            stdin_activity.notify_one();
                        }
                        Err(_) => break,
                    }
                }
            } => {}
        }
    });

    // Timeout read from SGIT_STREAM_TIMEOUT, falling back to SGIT_TIMEOUT
    // (default 60s). A value of 0 disables the timeout.
    let timeout_secs = std::env::var("SGIT_STREAM_TIMEOUT")
        .ok()
        .and_then(|val| val.parse::<u64>().ok())
        .or_else(|| {
            std::env::var("SGIT_TIMEOUT")
                .ok()
                .and_then(|val| val.parse::<u64>().ok())
        })
        .unwrap_or(60);

    if timeout_secs > 0 {
        spawn_idle_timeout(
            std::time::Duration::from_secs(timeout_secs),
            child.clone(),
            activity.clone(),
            timed_out.clone(),
            cancellation_token.clone(),
        );
    }

    let response_stream = GitResponseStream {
        inner: ReaderStream::new(child_stdout),
        child,
        activity,
        timed_out,
        cancellation_token,
        // Only update HEAD if the service is git-receive-pack (Push)
        repo_path: if is_push { Some(repo_path.clone()) } else { None },
        completed: false,
        permit,
    };
    let response_body = Body::from_stream(response_stream);

    let mut res_headers = HeaderMap::new();
    res_headers.insert(
        header::CONTENT_TYPE,
        format!("application/x-{}-result", service)
            .parse()
            .unwrap(),
    );
    add_no_cache_headers(&mut res_headers);

    (StatusCode::OK, res_headers, response_body).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_spool_request_body_rejects_oversized_body() {
        let result = spool_request_body(Body::from(vec![0u8; 16]), 8).await;
        assert!(
            matches!(result, Err(StatusCode::PAYLOAD_TOO_LARGE)),
            "body above the limit must be rejected"
        );
    }

    #[tokio::test]
    async fn test_spool_request_body_spills_to_disk_and_cleans_up() {
        let data = vec![0xABu8; BODY_MEMORY_SPOOL_THRESHOLD + 1024];
        let (mut body, size) = spool_request_body(Body::from(data.clone()), u64::MAX)
            .await
            .unwrap();
        assert_eq!(size, data.len() as u64);

        let path = match &body {
            SpooledRequestBody::File { path, .. } => path.clone(),
            SpooledRequestBody::Memory(_) => panic!("large body should be spooled to a file"),
        };
        assert!(path.exists());

        let mut copied = Vec::new();
        body.read_to_end(&mut copied).await.unwrap();
        assert_eq!(copied, data);

        drop(body);
        assert!(!path.exists(), "temporary body file was not removed");
    }
}

