pub mod types;
pub mod utils;
pub mod handlers;

use axum::{
    routing::{get, post, delete},
    Router,
};
use handlers::*;

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

#[cfg(test)]
mod tests {
    use super::*;
    use types::{BranchInfo, CommitInfo, TreeEntryInfo};
    use utils::get_safe_repo_path;
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

