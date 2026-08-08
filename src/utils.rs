use std::path::PathBuf;
use std::collections::HashMap;
use std::fs;
use serde::Deserialize;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use axum::{
    body::Body,
    http::{Request, Response, StatusCode, header, HeaderValue},
    middleware::Next,
};

// Security Helper: Validate and map allowed git services
pub fn validate_service(service: &str) -> Option<&'static str> {
    match service {
        "git-upload-pack" => Some("git-upload-pack"),
        "git-receive-pack" => Some("git-receive-pack"),
        _ => None,
    }
}

// Security Helper: Sanitize repo name to prevent path traversal
pub fn get_safe_repo_path(repo_name: &str) -> Option<PathBuf> {
    if repo_name.is_empty()
        || repo_name.contains("..")
        || repo_name.contains('\\')
        || repo_name.contains("//")
        || repo_name.starts_with('/')
        || repo_name.ends_with('/')
    {
        return None;
    }

    let base_dir = std::env::var("SGIT_DATA_DIR").unwrap_or_else(|_| {
        if cfg!(test) {
            "/tmp/git-repos".to_string()
        } else {
            "/var/lib/sgit".to_string()
        }
    });
    let base_path = PathBuf::from(base_dir);

    let normalized = if repo_name.ends_with(".git") {
        repo_name.to_string()
    } else {
        format!("{}.git", repo_name)
    };

    Some(base_path.join(normalized))
}

// Auto-initialize helper
pub fn ensure_repo_exists(repo_path: &std::path::Path, is_push: bool) -> Result<(), String> {
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

/// Encodes a data payload into Git's pkt-line format.
pub fn pkt_line_encode(data: &[u8]) -> bytes::Bytes {
    let len = data.len();
    if len == 0 {
        return bytes::Bytes::from_static(b"0000");
    }
    assert!(len <= 65520, "pkt-line payload too large: max is 65520 bytes");
    let total_len = len + 4;
    let mut buf = Vec::with_capacity(total_len);
    let hex = format!("{:04x}", total_len);
    buf.extend_from_slice(hex.as_bytes());
    buf.extend_from_slice(data);
    bytes::Bytes::from(buf)
}

pub fn update_head_if_invalid(repo_path: &std::path::Path) {
    let Ok(repo) = git2::Repository::open(repo_path) else { return; };

    if repo.head().is_ok() {
        return;
    }

    if let Ok(branches) = repo.branches(Some(git2::BranchType::Local)) {
        for (branch, _) in branches.flatten() {
            if let Ok(Some(branch_name)) = branch.name() {
                let target_ref = format!("refs/heads/{}", branch_name);
                let _ = repo.set_head(&target_ref);
                
                if branch_name == "main" || branch_name == "master" {
                    break;
                }
            }
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct UsersConfig {
    pub users: HashMap<String, String>,
}

pub fn validate_credentials(user: &str, pass: &str) -> bool {
    let users_file = std::env::var("SGIT_USERS_FILE").unwrap_or_else(|_| "users.toml".to_string());
    if let Ok(content) = fs::read_to_string(&users_file) {
        if let Ok(config) = toml::from_str::<UsersConfig>(&content) {
            if let Some(expected_pass) = config.users.get(user) {
                return expected_pass == pass;
            }
        }
    }
    false
}

pub fn check_basic_auth(auth_header: &str) -> bool {
    if !auth_header.starts_with("Basic ") {
        return false;
    }
    let encoded = &auth_header[6..];
    let Ok(decoded_bytes) = STANDARD.decode(encoded.trim()) else {
        return false;
    };
    let Ok(decoded_str) = String::from_utf8(decoded_bytes) else {
        return false;
    };
    let mut parts = decoded_str.splitn(2, ':');
    let Some(user) = parts.next() else {
        return false;
    };
    let Some(pass) = parts.next() else {
        return false;
    };
    validate_credentials(user, pass)
}

pub async fn git_auth_middleware(
    request: Request<Body>,
    next: Next,
) -> Response<Body> {
    let users_file = std::env::var("SGIT_USERS_FILE").unwrap_or_else(|_| "users.toml".to_string());
    if std::path::Path::new(&users_file).exists() {
        if let Some(auth_header) = request.headers().get(header::AUTHORIZATION) {
            if let Ok(auth_str) = auth_header.to_str() {
                if check_basic_auth(auth_str) {
                    return next.run(request).await;
                }
            }
        }
        
        let mut response = Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .body(Body::from("Unauthorized"))
            .unwrap();
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Basic realm=\"SGit\""),
        );
        return response;
    }

    next.run(request).await
}

