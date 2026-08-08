use axum::{
    body::Body,
    extract::{Path, Query},
    http::{header, HeaderMap, StatusCode, Method},
    response::{IntoResponse, Response},
};
use std::{collections::HashMap, process::Stdio};
use tokio::process::Command;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::types::GitResponseStream;
use crate::utils::{validate_service, get_safe_repo_path, ensure_repo_exists, pkt_line_encode, update_head_if_invalid};

// REST API: GET / (health check)
pub async fn handle_health_check() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

// Catch-all Git handler supporting arbitrary nested namespaces/subfolders
pub async fn handle_git_request(
    Path(path): Path<String>,
    method: Method,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if path.ends_with("/info/refs") && method == Method::GET {
        let repo = path.trim_end_matches("/info/refs").to_string();
        handle_info_refs_internal(repo, params, headers).await
    } else if path.ends_with("/git-upload-pack") && method == Method::POST {
        let repo = path.trim_end_matches("/git-upload-pack").to_string();
        execute_git_service_stream(repo, "git-upload-pack", headers, body).await
    } else if path.ends_with("/git-receive-pack") && method == Method::POST {
        let repo = path.trim_end_matches("/git-receive-pack").to_string();
        execute_git_service_stream(repo, "git-receive-pack", headers, body).await
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
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
    if let Err(err_msg) = ensure_repo_exists(&repo_path, is_push) {
        let status = if err_msg.contains("does not exist") {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };
        return (status, err_msg).into_response();
    }
    update_head_if_invalid(&repo_path);

    let mut cmd = Command::new(service);
    cmd.arg("--stateless-rpc")
        .arg("--advertise-refs")
        .arg(&repo_path);

    // Support Git Protocol V2 if specified by client
    if let Some(protocol) = headers.get("Git-Protocol") {
        if let Ok(p_str) = protocol.to_str() {
            cmd.env("GIT_PROTOCOL", p_str);
        }
    }

    let output = match cmd.output().await {
        Ok(out) if out.status.success() => out,
        _ => return (StatusCode::INTERNAL_SERVER_ERROR, "Git execution error").into_response(),
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
    Path(repo): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> impl IntoResponse {
    execute_git_service_stream(repo, "git-upload-pack", headers, body).await
}

// 3. Receive pack handler (Push)
pub async fn handle_receive_pack(
    Path(repo): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> impl IntoResponse {
    execute_git_service_stream(repo, "git-receive-pack", headers, body).await
}

// Helper to execute Git process asynchronously with full bi-directional streaming
pub async fn execute_git_service_stream(
    repo: String,
    service: &'static str,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let Some(repo_path) = get_safe_repo_path(&repo) else {
        return (StatusCode::BAD_REQUEST, "Invalid repository name").into_response();
    };

    let is_push = service == "git-receive-pack";
    if let Err(err_msg) = ensure_repo_exists(&repo_path, is_push) {
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
                    eprintln!("Git service [{}] stderr: {}", service, err_str);
                }
            }
        }
    });

    // Stream Axum Body directly into process stdin in background task
    let stdin_token = cancellation_token.clone();
    tokio::spawn(async move {
        let body_stream = body.into_data_stream();
        let mut body_reader = StreamReader::new(
            tokio_stream::StreamExt::map(body_stream, |res| {
                res.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            })
        );
        tokio::select! {
            _ = stdin_token.cancelled() => {},
            _ = tokio::io::copy(&mut body_reader, &mut child_stdin) => {}
        }
    });

    // Wrap stdout in GitResponseStream with timeout read from environment variable SGIT_TIMEOUT (default 60s)
    let timeout_secs = std::env::var("SGIT_TIMEOUT")
        .ok()
        .and_then(|val| val.parse::<u64>().ok())
        .unwrap_or(60);
    let timeout = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(timeout_secs)));

    let response_stream = GitResponseStream {
        inner: ReaderStream::new(child_stdout),
        child,
        timeout,
        cancellation_token,
        // Only update HEAD if the service is git-receive-pack (Push)
        repo_path: if is_push { Some(repo_path.clone()) } else { None },
    };
    let response_body = Body::from_stream(response_stream);

    let mut res_headers = HeaderMap::new();
    res_headers.insert(
        header::CONTENT_TYPE,
        format!("application/x-{}-result", service)
            .parse()
            .unwrap(),
    );

    (StatusCode::OK, res_headers, response_body).into_response()
}

