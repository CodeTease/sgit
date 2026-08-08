use axum::{
    body::Body,
    extract::{Path, Query},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use std::{collections::HashMap, process::Stdio};
use tokio::process::Command;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::types::{GitResponseStream, BranchInfo, CommitInfo, TreeEntryInfo};
use crate::utils::{validate_service, get_safe_repo_path, ensure_repo_exists, get_tree_at_path};

// REST API: GET / (health check)
pub async fn handle_health_check() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

// REST API: GET /api/repos
pub async fn handle_list_repos() -> impl IntoResponse {
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
pub async fn handle_delete_repo(Path(repo): Path<String>) -> impl IntoResponse {
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
pub async fn handle_info_refs(
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

pub async fn handle_list_branches(Path(repo): Path<String>) -> impl IntoResponse {
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

pub async fn handle_list_commits(Path(repo): Path<String>) -> impl IntoResponse {
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

pub async fn handle_get_tree(
    Path((repo, path_str)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    get_tree_internal(repo, Some(path_str), params).await
}

pub async fn handle_get_tree_root(
    Path(repo): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    get_tree_internal(repo, None, params).await
}

pub async fn get_tree_internal(
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

pub async fn handle_get_raw(
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

pub async fn handle_dumb_http_fallback(
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

