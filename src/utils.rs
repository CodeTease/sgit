use std::path::PathBuf;

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
        || repo_name.contains('/')
        || repo_name.contains('\\')
    {
        return None;
    }
    Some(PathBuf::from("/tmp/git-repos").join(format!("{}.git", repo_name)))
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
/// The specification defines a pkt-line as 4 hex characters representing the line length (including the 4 length bytes),
/// followed by the data payload. The maximum line length is 65524 bytes.
pub fn pkt_line_encode(data: &[u8]) -> bytes::Bytes {
    let len = data.len();
    if len == 0 {
        return bytes::Bytes::from_static(b"0000");
    }
    // pkt-line maximum length is 65524 (since 65520 + 4 = 65524, and length is represented by 4 hex chars, max FFFF is 65535, but Git spec limits payload size to 65520).
    // Let's cap and split, or chunk, but usually we just encode single packet line.
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

    // If HEAD points to a valid ref (already has a commit), no action is needed
    if repo.head().is_ok() {
        return;
    }

    // Find the list of all local branches (refs/heads/*) that were just pushed
    if let Ok(branches) = repo.branches(Some(git2::BranchType::Local)) {
        for (branch, _) in branches.flatten() {
            if let Ok(Some(branch_name)) = branch.name() {
                let target_ref = format!("refs/heads/{}", branch_name);
                // Point HEAD to this branch
                let _ = repo.set_head(&target_ref);
                
                // Prioritize selecting 'main' or 'master' if found
                if branch_name == "main" || branch_name == "master" {
                    break;
                }
            }
        }
    }
}
