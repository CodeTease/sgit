use axum::{
    body::Body,
    extract::{Path, Query},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, delete},
    Router,
    Json,
};
use std::{collections::HashMap, path::PathBuf, process::Stdio};
use tokio::process::Command;
use tokio_util::io::{ReaderStream, StreamReader};
use std::pin::Pin;
use std::task::{Context, Poll};
use futures_core::Stream;
use bytes::Bytes;
use tokio::time::Sleep;
use serde::{Serialize, Deserialize};

pub fn app() -> Router {
    Router::new()
        .route("/", get(handle_health_check))
        .route("/api/repos", get(handle_list_repos))
        .route("/api/repos/:repo", delete(handle_delete_repo))
        .route("/:repo/info/refs", get(handle_info_refs))
        .route("/:repo/git-upload-pack", post(handle_upload_pack))
        .route("/:repo/git-receive-pack", post(handle_receive_pack))
        .route("/:repo/branches", get(handle_list_branches))
        .route("/:repo/commits", get(handle_list_commits))
        .route("/:repo/tree", get(handle_get_tree_root))
        .route("/:repo/tree/*path", get(handle_get_tree))
        .route("/:repo/raw/*path", get(handle_get_raw))
        .route("/:repo/*path", get(handle_dumb_http_fallback))
}

#[tokio::main]
async fn main() {
    let app = app();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .expect("Failed to bind to port 3000");

    println!("SGit MVP Edition running at http://127.0.0.1:3000");
    
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C handler");
    println!("Shutdown signal received, shutting down gracefully...");
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

// Auto-initialize helper
fn ensure_repo_exists(repo_path: &std::path::Path, is_push: bool) -> Result<(), String> {
    if !repo_path.exists() {
        if is_push {
            if let Some(parent) = repo_path.parent() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    return Err(format!("Failed to create parent directory: {}", e));
                }
            }
            if let Err(e) = git2::Repository::init_bare(repo_path) {
                return Err(format!("Failed to initialize bare repository: {}", e));
            }
        } else {
            return Err("Repository does not exist".to_string());
        }
    }
    Ok(())
}

// Custom response stream that holds the process child to kill on drop (disconnect) or timeout
struct GitResponseStream {
    inner: ReaderStream<tokio::process::ChildStdout>,
    child: tokio::process::Child,
    timeout: Pin<Box<Sleep>>,
}

impl Stream for GitResponseStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use std::future::Future;
        // Check timeout
        if self.timeout.as_mut().poll(cx).is_ready() {
            let _ = self.child.start_kill();
            return Poll::Ready(Some(Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Git process timed out",
            ))));
        }

        // Poll stdout
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl Drop for GitResponseStream {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

// REST API: GET / (health check)
async fn handle_health_check() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

// REST API: GET /api/repos
async fn handle_list_repos() -> impl IntoResponse {
    let mut repos = Vec::new();
    let base_dir = std::path::Path::new("/tmp/git-repos");
    if base_dir.exists() {
        if let Ok(mut entries) = tokio::fs::read_dir(base_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                if path.is_dir() {
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        if name.ends_with(".git") {
                            let repo_name = &name[..name.len() - 4];
                            repos.push(repo_name.to_string());
                        }
                    }
                }
            }
        }
    }
    repos.sort();
    (StatusCode::OK, Json(repos))
}

// REST API: DELETE /api/repos/:repo
async fn handle_delete_repo(Path(repo): Path<String>) -> impl IntoResponse {
    let Some(repo_path) = get_safe_repo_path(&repo) else {
        return (StatusCode::BAD_REQUEST, "Invalid repository name").into_response();
    };

    if !repo_path.exists() {
        return (StatusCode::NOT_FOUND, "Repository does not exist").into_response();
    }

    if let Err(e) = tokio::fs::remove_dir_all(&repo_path).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to delete repository: {}", e),
        )
            .into_response();
    }

    (StatusCode::OK, "Repository deleted successfully").into_response()
}

// 1. Discover refs handler
async fn handle_info_refs(
    Path(repo): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let Some(repo_path) = get_safe_repo_path(&repo) else {
        return (StatusCode::BAD_REQUEST, "Invalid repository name").into_response();
    };

    let Some(raw_service) = params.get("service") else {
        // Dumb HTTP Protocol fallback for info/refs
        let file_path = repo_path.join("info/refs");
        if !file_path.exists() {
            return (StatusCode::NOT_FOUND, "info/refs not found").into_response();
        }
        match tokio::fs::read(&file_path).await {
            Ok(content) => {
                let mut res_headers = HeaderMap::new();
                res_headers.insert(header::CONTENT_TYPE, "text/plain".parse().unwrap());
                return (StatusCode::OK, res_headers, content).into_response();
            }
            Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to read info/refs").into_response(),
        }
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

    // Spawn background task to read stderr and log it
    tokio::spawn(async move {
        let mut buf = Vec::new();
        use tokio::io::AsyncReadExt;
        if let Ok(_) = child_stderr.read_to_end(&mut buf).await {
            if !buf.is_empty() {
                let err_str = String::from_utf8_lossy(&buf);
                eprintln!("Git service [{}] stderr: {}", service, err_str);
            }
        }
    });

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

    // Wrap stdout in GitResponseStream with timeout (60s)
    let timeout = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(60)));
    let response_stream = GitResponseStream {
        inner: ReaderStream::new(child_stdout),
        child,
        timeout,
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

// GET /:repo/branches
#[derive(Serialize, Deserialize)]
struct BranchInfo {
    name: String,
    commit_hash: String,
}

async fn handle_list_branches(Path(repo): Path<String>) -> impl IntoResponse {
    let Some(repo_path) = get_safe_repo_path(&repo) else {
        return (StatusCode::BAD_REQUEST, "Invalid repository name").into_response();
    };

    if !repo_path.exists() {
        return (StatusCode::NOT_FOUND, "Repository does not exist").into_response();
    }

    let git_repo = match git2::Repository::open(&repo_path) {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to open repository: {}", e),
            )
                .into_response()
        }
    };

    let mut branches_info = Vec::new();
    let branches = match git_repo.branches(None) {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to list branches: {}", e),
            )
                .into_response()
        }
    };

    for branch_res in branches {
        if let Ok((branch, _branch_type)) = branch_res {
            if let Ok(Some(name)) = branch.name() {
                let reference = branch.get();
                let commit_hash = if let Ok(peeled) = reference.peel_to_commit() {
                    peeled.id().to_string()
                } else {
                    "".to_string()
                };
                branches_info.push(BranchInfo {
                    name: name.to_string(),
                    commit_hash,
                });
            }
        }
    }

    (StatusCode::OK, Json(branches_info)).into_response()
}

// GET /:repo/commits
#[derive(Serialize, Deserialize)]
struct CommitInfo {
    hash: String,
    author: String,
    email: String,
    time: i64,
    message: String,
}

async fn handle_list_commits(Path(repo): Path<String>) -> impl IntoResponse {
    let Some(repo_path) = get_safe_repo_path(&repo) else {
        return (StatusCode::BAD_REQUEST, "Invalid repository name").into_response();
    };

    if !repo_path.exists() {
        return (StatusCode::NOT_FOUND, "Repository does not exist").into_response();
    }

    let git_repo = match git2::Repository::open(&repo_path) {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to open repository: {}", e),
            )
                .into_response()
        }
    };

    if git_repo.is_empty().unwrap_or(true) {
        return (StatusCode::OK, Json(Vec::<CommitInfo>::new())).into_response();
    }

    let mut revwalk = match git_repo.revwalk() {
        Ok(rw) => rw,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to create revwalk: {}", e),
            )
                .into_response()
        }
    };

    if let Err(_) = revwalk.push_head() {
        return (StatusCode::OK, Json(Vec::<CommitInfo>::new())).into_response();
    }

    let mut commits = Vec::new();
    for oid_res in revwalk.take(100) {
        if let Ok(oid) = oid_res {
            if let Ok(commit) = git_repo.find_commit(oid) {
                let author = commit.author();
                commits.push(CommitInfo {
                    hash: oid.to_string(),
                    author: author.name().unwrap_or("").to_string(),
                    email: author.email().unwrap_or("").to_string(),
                    time: commit.time().seconds(),
                    message: commit.message().unwrap_or("").to_string(),
                });
            }
        }
    }

    (StatusCode::OK, Json(commits)).into_response()
}

// Helper to resolve tree at path
fn get_tree_at_path<'a>(
    git_repo: &'a git2::Repository,
    reference_or_commit: Option<&str>,
    path_str: &str,
) -> Result<git2::Tree<'a>, String> {
    let commit = if let Some(ref_name) = reference_or_commit {
        let obj = git_repo.revparse_single(ref_name)
            .map_err(|e| format!("Could not find reference '{}': {}", ref_name, e))?;
        obj.peel_to_commit()
            .map_err(|e| format!("Reference '{}' does not resolve to a commit: {}", ref_name, e))?
    } else {
        let head_ref = git_repo.head()
            .map_err(|e| format!("Could not get HEAD reference: {}", e))?;
        head_ref.peel_to_commit()
            .map_err(|e| format!("HEAD does not resolve to a commit: {}", e))?
    };

    let root_tree = commit.tree()
        .map_err(|e| format!("Could not get commit tree: {}", e))?;

    if path_str.is_empty() {
        Ok(root_tree)
    } else {
        let entry = root_tree.get_path(std::path::Path::new(path_str))
            .map_err(|e| format!("Path '{}' not found: {}", path_str, e))?;
        let obj = entry.to_object(git_repo)
            .map_err(|e| format!("Could not convert tree entry to object: {}", e))?;
        let tree = obj.as_tree()
            .ok_or_else(|| format!("Path '{}' is not a directory/tree", path_str))?;
        Ok(tree.clone())
    }
}

// GET /:repo/tree and GET /:repo/tree/*path
#[derive(Serialize, Deserialize)]
struct TreeEntryInfo {
    name: String,
    is_dir: bool,
    sha: String,
}

async fn handle_get_tree(
    Path((repo, path_str)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    get_tree_internal(repo, Some(path_str), params).await
}

async fn handle_get_tree_root(
    Path(repo): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    get_tree_internal(repo, None, params).await
}

async fn get_tree_internal(
    repo: String,
    path_str_opt: Option<String>,
    params: HashMap<String, String>,
) -> Response {
    let Some(repo_path) = get_safe_repo_path(&repo) else {
        return (StatusCode::BAD_REQUEST, "Invalid repository name").into_response();
    };

    if !repo_path.exists() {
        return (StatusCode::NOT_FOUND, "Repository does not exist").into_response();
    }

    let git_repo = match git2::Repository::open(&repo_path) {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to open repository: {}", e),
            )
                .into_response()
        }
    };

    if git_repo.is_empty().unwrap_or(true) {
        return (StatusCode::OK, Json(Vec::<TreeEntryInfo>::new())).into_response();
    }

    let path_str = path_str_opt.unwrap_or_default();
    let ref_param = params.get("ref").map(|s| s.as_str());

    let tree = match get_tree_at_path(&git_repo, ref_param, &path_str) {
        Ok(t) => t,
        Err(e) => return (StatusCode::NOT_FOUND, e).into_response(),
    };

    let mut entries = Vec::new();
    for entry in tree.iter() {
        if let Some(name) = entry.name() {
            let is_dir = entry.kind() == Some(git2::ObjectType::Tree);
            entries.push(TreeEntryInfo {
                name: name.to_string(),
                is_dir,
                sha: entry.id().to_string(),
            });
        }
    }

    (StatusCode::OK, Json(entries)).into_response()
}

// GET /:repo/raw/*path
async fn handle_get_raw(
    Path((repo, path_str)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let Some(repo_path) = get_safe_repo_path(&repo) else {
        return (StatusCode::BAD_REQUEST, "Invalid repository name").into_response();
    };

    if !repo_path.exists() {
        return (StatusCode::NOT_FOUND, "Repository does not exist").into_response();
    }

    let git_repo = match git2::Repository::open(&repo_path) {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to open repository: {}", e),
            )
                .into_response()
        }
    };

    if git_repo.is_empty().unwrap_or(true) {
        return (StatusCode::NOT_FOUND, "Repository is empty").into_response();
    }

    let ref_param = params.get("ref").map(|s| s.as_str());

    let commit = if let Some(ref_name) = ref_param {
        let Ok(obj) = git_repo.revparse_single(ref_name) else {
            return (StatusCode::NOT_FOUND, format!("Reference '{}' not found", ref_name)).into_response();
        };
        let Ok(c) = obj.peel_to_commit() else {
            return (StatusCode::BAD_REQUEST, format!("Reference '{}' is not a commit", ref_name)).into_response();
        };
        c
    } else {
        let Ok(head_ref) = git_repo.head() else {
            return (StatusCode::NOT_FOUND, "Could not get HEAD reference").into_response();
        };
        let Ok(c) = head_ref.peel_to_commit() else {
            return (StatusCode::INTERNAL_SERVER_ERROR, "HEAD does not resolve to a commit").into_response();
        };
        c
    };

    let Ok(root_tree) = commit.tree() else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "Could not get commit tree").into_response();
    };

    let Ok(entry) = root_tree.get_path(std::path::Path::new(&path_str)) else {
        return (StatusCode::NOT_FOUND, format!("Path '{}' not found", path_str)).into_response();
    };

    let Ok(obj) = entry.to_object(&git_repo) else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "Could not resolve tree entry to object").into_response();
    };

    let Some(blob) = obj.as_blob() else {
        return (StatusCode::BAD_REQUEST, format!("Path '{}' is not a file/blob", path_str)).into_response();
    };

    let content = blob.content().to_vec();

    let mut res_headers = HeaderMap::new();
    res_headers.insert(header::CONTENT_TYPE, "application/octet-stream".parse().unwrap());

    (StatusCode::OK, res_headers, content).into_response()
}

// GET /:repo/*path (Dumb HTTP fallback for HEAD, objects/pack, etc.)
async fn handle_dumb_http_fallback(
    Path((repo, path_str)): Path<(String, String)>,
) -> impl IntoResponse {
    let Some(repo_path) = get_safe_repo_path(&repo) else {
        return (StatusCode::BAD_REQUEST, "Invalid repository name").into_response();
    };

    if !repo_path.exists() {
        return (StatusCode::NOT_FOUND, "Repository does not exist").into_response();
    }

    if path_str.contains("..") {
        return (StatusCode::BAD_REQUEST, "Invalid path").into_response();
    }

    let file_path = repo_path.join(&path_str);
    if !file_path.exists() || !file_path.is_file() {
        return (StatusCode::NOT_FOUND, "File not found").into_response();
    }

    match tokio::fs::read(&file_path).await {
        Ok(content) => {
            let mut res_headers = HeaderMap::new();
            let content_type = if path_str == "HEAD" || path_str == "info/refs" || path_str == "objects/info/packs" {
                "text/plain"
            } else if path_str.ends_with(".pack") {
                "application/x-git-packed-objects"
            } else if path_str.ends_with(".idx") {
                "application/x-git-packed-objects-toc"
            } else if path_str.contains("objects/") {
                "application/x-git-loose-object"
            } else {
                "application/octet-stream"
            };
            res_headers.insert(header::CONTENT_TYPE, content_type.parse().unwrap());
            (StatusCode::OK, res_headers, content).into_response()
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Failed to read file").into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn test_health_check() {
        let app = app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1000000)
            .await
            .unwrap();
        assert_eq!(body, "OK");
    }

    #[tokio::test]
    async fn test_repo_management_and_auto_init() {
        let repo_name = "test_auto_init_repo_123";
        let safe_path = get_safe_repo_path(repo_name).unwrap();
        if safe_path.exists() {
            let _ = std::fs::remove_dir_all(&safe_path);
        }

        let app = app();

        // 1. Check list of repos (should not contain test_auto_init_repo_123)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/repos")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1000000)
            .await
            .unwrap();
        let repos: Vec<String> = serde_json::from_slice(&body).unwrap();
        assert!(!repos.contains(&repo_name.to_string()));

        // 2. Perform GET info/refs?service=git-receive-pack to trigger auto-init on push
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/{}/info/refs?service=git-receive-pack", repo_name))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Verify repository now exists
        assert!(safe_path.exists());

        // 3. List repos again, should contain our new repo
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/repos")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1000000)
            .await
            .unwrap();
        let repos: Vec<String> = serde_json::from_slice(&body).unwrap();
        assert!(repos.contains(&repo_name.to_string()));

        // 4. Delete repo
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/repos/{}", repo_name))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(!safe_path.exists());
    }

    #[tokio::test]
    async fn test_git2_metadata_endpoints() {
        let repo_name = "test_metadata_repo_abc";
        let safe_path = get_safe_repo_path(repo_name).unwrap();
        if safe_path.exists() {
            let _ = std::fs::remove_dir_all(&safe_path);
        }

        // Initialize bare repo and create a commit
        let repo = git2::Repository::init_bare(&safe_path).unwrap();
        let sig = git2::Signature::now("Test User", "test@example.com").unwrap();
        let blob_id = repo.blob(b"Hello from test file!").unwrap();
        let mut tb = repo.treebuilder(None).unwrap();
        tb.insert("test_file.txt", blob_id, 0o100644).unwrap();
        let tree_id = tb.write().unwrap();
        let tree = repo.find_tree(tree_id).unwrap();
        let commit_id = repo.commit(
            Some("refs/heads/master"),
            &sig,
            &sig,
            "Integration test commit",
            &tree,
            &[],
        )
        .unwrap();
        repo.set_head("refs/heads/master").unwrap();

        let app = app();

        // 1. Test branches endpoint
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/{}/branches", repo_name))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1000000)
            .await
            .unwrap();
        let branches: Vec<BranchInfo> = serde_json::from_slice(&body).unwrap();
        assert_eq!(branches.len(), 1);
        assert_eq!(branches[0].name, "master");
        assert_eq!(branches[0].commit_hash, commit_id.to_string());

        // 2. Test commits endpoint
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/{}/commits", repo_name))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1000000)
            .await
            .unwrap();
        let commits: Vec<CommitInfo> = serde_json::from_slice(&body).unwrap();
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].hash, commit_id.to_string());
        assert_eq!(commits[0].message, "Integration test commit");

        // 3. Test tree endpoint (root)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/{}/tree", repo_name))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1000000)
            .await
            .unwrap();
        let tree_entries: Vec<TreeEntryInfo> = serde_json::from_slice(&body).unwrap();
        assert_eq!(tree_entries.len(), 1);
        assert_eq!(tree_entries[0].name, "test_file.txt");
        assert_eq!(tree_entries[0].is_dir, false);

        // 4. Test raw file endpoint
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/{}/raw/test_file.txt", repo_name))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1000000)
            .await
            .unwrap();
        assert_eq!(body, "Hello from test file!");

        // 5. Test Dumb HTTP fallback for HEAD file
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/{}/HEAD", repo_name))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1000000)
            .await
            .unwrap();
        let body_str = String::from_utf8_lossy(&body);
        assert!(body_str.contains("ref: refs/heads/master"));

        // Clean up
        let _ = std::fs::remove_dir_all(&safe_path);
    }
}
