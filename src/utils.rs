use std::path::PathBuf;
use std::collections::HashMap;
use std::fs;
use serde::Deserialize;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use axum::{
    body::Body,
    http::{Request, Response, StatusCode, header, HeaderValue},
    middleware::Next,
    extract::{FromRequestParts, Path, Query, State},
};
use sha2::{Sha256, Digest};
use subtle::ConstantTimeEq;
use argon2::{
    password_hash::{phc::PasswordHash, PasswordHasher, PasswordVerifier},
    Argon2,
};
use crate::types::AppState;

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

/// Parsed `SGIT_MAX_REPO_SIZE_MB` (bytes). `None` means unlimited.
pub fn get_repo_size_quota_bytes() -> Option<u64> {
    std::env::var("SGIT_MAX_REPO_SIZE_MB")
        .ok()
        .and_then(|val| val.parse::<u64>().ok())
        .map(|mb| mb.saturating_mul(1024 * 1024))
}

// Auto-initialize helper
pub fn get_dir_size(path: &std::path::Path) -> std::io::Result<u64> {
    let mut total_size = 0;
    if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                total_size += get_dir_size(&path)?;
            } else {
                total_size += entry.metadata()?.len();
            }
        }
    } else {
        total_size += path.metadata()?.len();
    }
    Ok(total_size)
}

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

#[macro_export]
macro_rules! log_stderr {
    ($($arg:tt)*) => {{
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        eprintln!("[{}] {}", timestamp, format_args!($($arg)*));
    }};
}

/// Legacy unsalted SHA-256 hash (hex). Kept for backwards compatibility with
/// existing `users.toml` files. New hashes should use [`hash_password`].
pub fn hash_password_sha256(password: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    let result = hasher.finalize();
    result.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Hash a password with Argon2id and a fresh random salt.
///
/// The returned value is a versioned PHC string (`$argon2id$v=19$...`), so the
/// algorithm and parameters travel with the hash.
pub fn hash_password(password: &str) -> String {
    Argon2::default()
        .hash_password(password.as_bytes())
        .expect("Argon2id password hashing failed")
        .to_string()
}

/// Verify a password against either an Argon2id PHC hash or a legacy SHA-256
/// hex digest. Comparisons are constant-time for the stored/computed values.
pub fn verify_password(stored_hash: &str, password: &str) -> bool {
    if stored_hash.starts_with("$argon2") {
        let Ok(parsed) = PasswordHash::new(stored_hash) else {
            return false;
        };
        return Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok();
    }

    let computed = hash_password_sha256(password);
    bool::from(stored_hash.as_bytes().ct_eq(computed.as_bytes()))
}

use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};

pub fn load_users_config() -> AppState {
    let users_file = std::env::var("SGIT_USERS_FILE").unwrap_or_else(|_| "users.toml".to_string());
    let mut users = HashMap::new();
    let mut has_users_file = false;
    if std::path::Path::new(&users_file).exists() {
        has_users_file = true;
        if let Ok(content) = fs::read_to_string(&users_file) {
            if let Ok(config) = toml::from_str::<UsersConfig>(&content) {
                users = config.users;
            }
        }
    }
    AppState {
        users: Arc::new(RwLock::new(users)),
        has_users_file: Arc::new(AtomicBool::new(has_users_file)),
        git_semaphore: None,
    }
}

pub fn reload_users_config(state: &AppState) {
    let users_file = std::env::var("SGIT_USERS_FILE").unwrap_or_else(|_| "users.toml".to_string());
    let mut users = HashMap::new();
    let mut has_users_file = false;
    if std::path::Path::new(&users_file).exists() {
        has_users_file = true;
        if let Ok(content) = fs::read_to_string(&users_file) {
            if let Ok(config) = toml::from_str::<UsersConfig>(&content) {
                users = config.users;
            }
        }
    }
    if let Ok(mut lock) = state.users.write() {
        *lock = users;
    }
    state.has_users_file.store(has_users_file, Ordering::SeqCst);
}

pub fn check_basic_auth(state: &AppState, auth_header: &str) -> bool {
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
    
    if let Ok(lock) = state.users.read() {
        if let Some(expected_hash) = lock.get(user) {
            return verify_password(expected_hash, pass);
        }
    }
    false
}

pub async fn git_auth_middleware(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response<Body> {
    if state.has_users_file.load(Ordering::SeqCst) {
        // Route on the same percent-decoded values that handle_git_request sees.
        // The raw URI must never be used for authorization: an encoded form such
        // as `git%2Dreceive-pack` decodes to a write endpoint in the handler.
        let (mut parts, body) = request.into_parts();
        let decoded_path =
            Path::<String>::from_request_parts(&mut parts, &state)
                .await
                .map(|Path(path)| path);
        let decoded_service =
            Query::<HashMap<String, String>>::from_request_parts(&mut parts, &state)
                .await
                .ok()
                .and_then(|Query(params)| params.get("service").cloned());
        let request = Request::from_parts(parts, body);

        let is_write = match decoded_path {
            Ok(path) => {
                path.ends_with("/git-receive-pack")
                    || (path.ends_with("/info/refs")
                        && decoded_service.as_deref() == Some("git-receive-pack"))
            }
            // Fail closed if the request target cannot be decoded: the handler
            // cannot safely dispatch it either.
            Err(_) => true,
        };

        if !is_write {
            // Allow public clone / read
            return next.run(request).await;
        }

        if let Some(auth_header) = request.headers().get(header::AUTHORIZATION) {
            if let Ok(auth_str) = auth_header.to_str() {
                if check_basic_auth(&state, auth_str) {
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

