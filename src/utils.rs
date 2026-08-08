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

// Helper to resolve tree at path
pub fn get_tree_at_path<'a>(
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

