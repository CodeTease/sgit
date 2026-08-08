pub mod types;
pub mod utils;
pub mod handlers;

use axum::{
    routing::{any, get},
    Router,
    middleware,
};
use handlers::*;
use utils::git_auth_middleware;

pub fn app() -> Router {
    let state = utils::load_users_config();

    // Standard and catch-all routes to support arbitrarily nested subfolders.
    // Ensure all git protocol endpoints go through the git_auth_middleware layer.
    let git_routes = Router::new()
        .route("/*path", any(handle_git_request))
        .layer(middleware::from_fn_with_state(state.clone(), git_auth_middleware))
        .with_state(state);

    Router::new()
        .route("/", get(handle_health_check))
        .merge(git_routes)
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
    use std::fs;
    use base64::Engine;
    use std::sync::Mutex;

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn test_health_check() {
        let _guard = TEST_LOCK.lock().unwrap();
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
        let _guard = TEST_LOCK.lock().unwrap();
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
        let _guard = TEST_LOCK.lock().unwrap();
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
        
        // Output must start with pkt-line service line and flush packet (0000)
        let expected_prefix = utils::pkt_line_encode(b"# service=git-upload-pack\n");
        assert!(body.starts_with(&expected_prefix));
        
        let after_service = &body[expected_prefix.len()..];
        assert!(after_service.starts_with(b"0000"));

        // Clean up
        let _ = std::fs::remove_dir_all(&safe_path);
    }

    #[tokio::test]
    async fn test_path_sanitization_and_namespaces() {
        let _guard = TEST_LOCK.lock().unwrap();
        // Test namespaces and nesting
        let nested_repo = "user/nested/myrepo";
        let path = get_safe_repo_path(nested_repo).unwrap();
        assert!(path.to_str().unwrap().contains("user/nested/myrepo.git"));

        // Test redundant `.git` suffix avoidance
        let nested_git = "user/nested/myrepo.git";
        let path_git = get_safe_repo_path(nested_git).unwrap();
        assert!(path_git.to_str().unwrap().ends_with("user/nested/myrepo.git"));

        // Test traversal and malicious inputs
        assert!(get_safe_repo_path("../etc/passwd").is_none());
        assert!(get_safe_repo_path("user/../nested").is_none());
        assert!(get_safe_repo_path("\\windows\\system32").is_none());
        assert!(get_safe_repo_path("user//nested").is_none());
        assert!(get_safe_repo_path("/leading").is_none());
        assert!(get_safe_repo_path("trailing/").is_none());
        assert!(get_safe_repo_path("").is_none());

        // Test custom env variable for storage
        unsafe {
            std::env::set_var("SGIT_DATA_DIR", "/custom/data/dir");
        }
        let path_custom = get_safe_repo_path("cool-repo").unwrap();
        assert_eq!(path_custom.to_str().unwrap(), "/custom/data/dir/cool-repo.git");
        unsafe {
            std::env::remove_var("SGIT_DATA_DIR");
        }
    }

    #[tokio::test]
    async fn test_http_basic_auth_middleware() {
        let _guard = TEST_LOCK.lock().unwrap();
        let temp_users_file = "temp_users.toml";
        unsafe {
            std::env::set_var("SGIT_USERS_FILE", temp_users_file);
        }
        
        // Write mock credentials using SHA-256 hashes
        let config_toml = r#"
            [users]
            alice = "f75778f7425be4db0369d09af37a6c2b9a83dea0e53e7bd57412e4b060e607f7" # supersecret
            bob = "90c1db884b25916cb034e32321487f84b6732cfd1e2e13fa096df0709b1192e2" # pass123
        "#;
        fs::write(temp_users_file, config_toml).unwrap();

        let app = app();

        // 1. Upload-pack (read) without credentials -> 404 NOT_FOUND (because repo does not exist, but auth is bypassed!)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/myrepo/info/refs?service=git-upload-pack")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // 2. Receive-pack (write) without credentials -> 401 Unauthorized
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/myrepo/info/refs?service=git-receive-pack")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers().get("WWW-Authenticate").unwrap().to_str().unwrap(),
            "Basic realm=\"SGit\""
        );

        // 3. Receive-pack (write) with incorrect credentials -> 401 Unauthorized
        let bad_auth = format!("Basic {}", base64::engine::general_purpose::STANDARD.encode("alice:badpass"));
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/myrepo/info/refs?service=git-receive-pack")
                    .header("Authorization", bad_auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        // 4. Receive-pack (write) with correct credentials -> 200 OK (will trigger auto-init on receive-pack or mock folder)
        let good_auth = format!("Basic {}", base64::engine::general_purpose::STANDARD.encode("alice:supersecret"));
        let repo_name = "auth_test_repo_123";
        let safe_path = get_safe_repo_path(repo_name).unwrap();
        if safe_path.exists() {
            let _ = std::fs::remove_dir_all(&safe_path);
        }

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/{}/info/refs?service=git-receive-pack", repo_name))
                    .header("Authorization", good_auth.clone())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // 5. POST Upload-pack (read) without credentials -> 404 NOT_FOUND (auth is bypassed!)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/myrepo/git-upload-pack")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // 6. POST Receive-pack (write) without credentials -> 401 Unauthorized
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/myrepo/git-receive-pack")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        // Clean up
        let _ = std::fs::remove_dir_all(&safe_path);
        let _ = fs::remove_file(temp_users_file);
        unsafe {
            std::env::remove_var("SGIT_USERS_FILE");
        }
    }

    #[tokio::test]
    async fn test_env_timeout_and_port() {
        let _guard = TEST_LOCK.lock().unwrap();
        // Test timeout parsing logic from env in a simple way
        unsafe {
            std::env::set_var("SGIT_TIMEOUT", "120");
        }
        let timeout_secs = std::env::var("SGIT_TIMEOUT")
            .ok()
            .and_then(|val| val.parse::<u64>().ok())
            .unwrap_or(60);
        assert_eq!(timeout_secs, 120);
        unsafe {
            std::env::remove_var("SGIT_TIMEOUT");
        }

        // Test port parsing logic
        unsafe {
            std::env::set_var("SGIT_PORT", "8080");
        }
        let port = std::env::var("SGIT_PORT")
            .ok()
            .and_then(|val| val.parse::<u16>().ok())
            .unwrap_or(3000);
        assert_eq!(port, 8080);
        unsafe {
            std::env::remove_var("SGIT_PORT");
        }
    }
}

