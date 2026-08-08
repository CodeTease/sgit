pub mod types;
pub mod utils;
pub mod handlers;

use axum::{
    routing::{get, post},
    Router,
};
use handlers::*;

pub fn app() -> Router {
    Router::new()
        .route("/", get(handle_health_check))
        .route("/:repo/info/refs", get(handle_info_refs))
        .route("/:repo/git-upload-pack", post(handle_upload_pack))
        .route("/:repo/git-receive-pack", post(handle_receive_pack))
}

#[cfg(test)]
mod tests {
    use super::*;
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

        // 1. Perform GET info/refs?service=git-receive-pack to trigger auto-init on push
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

        // Clean up
        let _ = std::fs::remove_dir_all(&safe_path);
    }

    #[tokio::test]
    async fn test_pkt_line_encode_and_ref_advertisement() {
        let repo_name = "test_ref_advertisement_repo";
        let safe_path = get_safe_repo_path(repo_name).unwrap();
        if safe_path.exists() {
            let _ = std::fs::remove_dir_all(&safe_path);
        }

        // Initialize bare repo and create a master ref
        let repo = git2::Repository::init_bare(&safe_path).unwrap();
        let sig = git2::Signature::now("Test User", "test@example.com").unwrap();
        let blob_id = repo.blob(b"Hello world!").unwrap();
        let mut tb = repo.treebuilder(None).unwrap();
        tb.insert("hello.txt", blob_id, 0o100644).unwrap();
        let tree_id = tb.write().unwrap();
        let tree = repo.find_tree(tree_id).unwrap();
        let _commit_id = repo.commit(
            Some("refs/heads/master"),
            &sig,
            &sig,
            "Initial commit",
            &tree,
            &[],
        )
        .unwrap();

        let app = app();

        // Perform GET info/refs?service=git-upload-pack
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/{}/info/refs?service=git-upload-pack", repo_name))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        
        let content_type = response.headers().get("content-type").unwrap().to_str().unwrap();
        assert_eq!(content_type, "application/x-git-upload-pack-advertisement");

        let body = axum::body::to_bytes(response.into_body(), 1000000)
            .await
            .unwrap();
        
        // Output must start withpkt-line service line and flush packet (0000)
        let expected_prefix = utils::pkt_line_encode(b"# service=git-upload-pack\n");
        assert!(body.starts_with(&expected_prefix));
        
        let after_service = &body[expected_prefix.len()..];
        assert!(after_service.starts_with(b"0000"));

        // Clean up
        let _ = std::fs::remove_dir_all(&safe_path);
    }
}

