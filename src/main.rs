use axum::{
    body::Body,
    extract::{Path, Query},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use std::{collections::HashMap, path::PathBuf, process::Stdio};
use tokio::process::Command;
use tokio_util::io::{ReaderStream, StreamReader};

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/:repo/info/refs", get(handle_info_refs))
        .route("/:repo/git-upload-pack", post(handle_upload_pack))
        .route("/:repo/git-receive-pack", post(handle_receive_pack));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .expect("Failed to bind to port 3000");

    println!("SGit MVP Edition running at http://127.0.0.1:3000");
    axum::serve(listener, app).await.unwrap();
}

// Security Helper: Validate and map allowed git services
fn validate_service(service: &str) -> Option<&'static str> {
    match service {
        "git-upload-pack" => Some("git-upload-pack"),
        "git-receive-pack" => Some("git-receive-pack"),
        _ => None,
    }
}

// Security Helper: Sanitize repo name to prevent path traversal
fn get_safe_repo_path(repo_name: &str) -> Option<PathBuf> {
    if repo_name.is_empty()
        || repo_name.contains("..")
        || repo_name.contains('/')
        || repo_name.contains('\\')
    {
        return None;
    }
    Some(PathBuf::from("/tmp/git-repos").join(format!("{}.git", repo_name)))
}

// 1. Discover refs handler
async fn handle_info_refs(
    Path(repo): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let Some(raw_service) = params.get("service") else {
        return (StatusCode::BAD_REQUEST, "Missing service query parameter").into_response();
    };

    let Some(service) = validate_service(raw_service) else {
        return (StatusCode::FORBIDDEN, "Invalid or unsupported git service").into_response();
    };

    let Some(repo_path) = get_safe_repo_path(&repo) else {
        return (StatusCode::BAD_REQUEST, "Invalid repository name").into_response();
    };

    if !repo_path.exists() {
        return (StatusCode::NOT_FOUND, "Repository does not exist").into_response();
    }

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

    // Calculate pkt-line header
    let line_content = format!("# service={}\n", service);
    let pkt_len = line_content.len() + 4;
    let pkt_line = format!("{:04x}{}", pkt_len, line_content);

    let mut body = pkt_line.into_bytes();
    body.extend(b"0000"); // Flush-pkt
    body.extend(output.stdout);

    let mut res_headers = HeaderMap::new();
    res_headers.insert(
        header::CONTENT_TYPE,
        format!("application/x-{}-advertisement", service)
            .parse()
            .unwrap(),
    );

    (StatusCode::OK, res_headers, body).into_response()
}

// 2. Upload pack handler (Fetch/Clone)
async fn handle_upload_pack(
    Path(repo): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> impl IntoResponse {
    execute_git_service_stream(repo, "git-upload-pack", headers, body).await
}

// 3. Receive pack handler (Push)
async fn handle_receive_pack(
    Path(repo): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> impl IntoResponse {
    execute_git_service_stream(repo, "git-receive-pack", headers, body).await
}

// Helper to execute Git process asynchronously with full bi-directional streaming
async fn execute_git_service_stream(
    repo: String,
    service: &'static str,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let Some(repo_path) = get_safe_repo_path(&repo) else {
        return (StatusCode::BAD_REQUEST, "Invalid repository name").into_response();
    };

    if !repo_path.exists() {
        return (StatusCode::NOT_FOUND, "Repository does not exist").into_response();
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

    // Stream Axum Body directly into process stdin in background task
    tokio::spawn(async move {
        let body_stream = body.into_data_stream();
        let mut body_reader = StreamReader::new(
            tokio_stream::StreamExt::map(body_stream, |res| {
                res.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            })
        );
        let _ = tokio::io::copy(&mut body_reader, &mut child_stdin).await;
    });

    // Stream process stdout directly back to Axum response Body
    let response_stream = ReaderStream::new(child_stdout);
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
