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

    // SIGHUP Config Reload
    #[cfg(unix)]
    {
        let state_clone = state.clone();
        tokio::spawn(async move {
            use tokio::signal::unix::{signal, SignalKind};
            if let Ok(mut stream) = signal(SignalKind::hangup()) {
                while stream.recv().await.is_some() {
                    utils::reload_users_config(&state_clone);
                }
            }
        });
    }

    // Standard and catch-all routes to support arbitrarily nested subfolders.
    // Ensure all git protocol endpoints go through the git_auth_middleware layer.
    let git_routes = Router::new()
        .route("/*path", any(handle_git_request))
        .layer(middleware::from_fn_with_state(state.clone(), git_auth_middleware))
        .with_state(state);

    let mut app = Router::new()
        .route("/", get(handle_health_check))
        .merge(git_routes);

    // 1. Push Size Limit (Max Request Body).
    // A value of 0 disables the limit instead of rejecting every non-empty body.
    let max_request_size_mb = std::env::var("SGIT_MAX_REQUEST_SIZE_MB")
        .ok()
        .and_then(|val| val.parse::<usize>().ok())
        .unwrap_or(500);
    if max_request_size_mb > 0 {
        let limit_bytes = max_request_size_mb.saturating_mul(1024 * 1024);
        app = app.layer(axum::extract::DefaultBodyLimit::max(limit_bytes));
    } else {
        app = app.layer(axum::extract::DefaultBodyLimit::disable());
    }

    // 2. Concurrency Limit.
    // A value of 0 disables the limit; ConcurrencyLimitLayer::new(0) would
    // otherwise park every request forever.
    let max_concurrent_reqs = std::env::var("SGIT_MAX_CONCURRENT_REQS")
        .ok()
        .and_then(|val| val.parse::<usize>().ok())
        .unwrap_or(20);
    if max_concurrent_reqs > 0 {
        app = app.layer(tower::limit::ConcurrencyLimitLayer::new(max_concurrent_reqs));
    }

    // 3. Rate Limiting
    let rate_limit = std::env::var("SGIT_RATE_LIMIT_PER_IP")
        .ok()
        .and_then(|val| val.parse::<u32>().ok())
        .unwrap_or(30);

    if rate_limit > 0 {
        // Integer division truncates to zero above 60_000 requests/minute, which
        // makes GovernorConfigBuilder::finish() return None and used to panic.
        let ms_per_request = std::cmp::max(1, 60000 / rate_limit as u64);
        if let Some(config) = tower_governor::governor::GovernorConfigBuilder::default()
            .per_millisecond(ms_per_request)
            .burst_size(rate_limit)
            .finish()
        {
            app = app.layer(tower_governor::GovernorLayer {
                config: std::sync::Arc::new(config),
            });
        }
    }

    app
}

#[cfg(test)]
mod tests {
    use super::*;
    use utils::get_safe_repo_path;
    use axum::{
        body::Body,
            http::StatusCode,
    };
    use tower::util::ServiceExt;
    use std::fs;
    use base64::Engine;
    use std::sync::Mutex;

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn mock_request(method: &str, uri: &str) -> axum::http::request::Builder {
        let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 12345));
        axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .extension(addr)
            .extension(axum::extract::ConnectInfo(addr))
    }

    #[tokio::test]
    async fn test_health_check() {
        let _guard = TEST_LOCK.lock().unwrap();
        let app = app();
        let response = app
            .oneshot(
                mock_request("GET", "/")
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
                mock_request("GET", &format!("/{}/info/refs?service=git-receive-pack", repo_name))
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
    async fn test_duplicate_endpoint_suffixes_are_rejected() {
        let _guard = TEST_LOCK.lock().unwrap();
        let repo_name = "test_dupe_suffix_repo";
        let safe_path = get_safe_repo_path(repo_name).unwrap();
        if safe_path.exists() {
            let _ = std::fs::remove_dir_all(&safe_path);
        }
        let _repo = git2::Repository::init_bare(&safe_path).unwrap();

        let app = app();

        // Duplicated info/refs suffix must not be silently routed to the repository prefix.
        let response = app
            .clone()
            .oneshot(
                mock_request(
                    "GET",
                    &format!("/{}/info/refs/info/refs?service=git-upload-pack", repo_name),
                )
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // Duplicated RPC suffix must not be silently routed to the repository prefix.
        let response = app
            .clone()
            .oneshot(
                mock_request(
                    "POST",
                    &format!("/{}/git-upload-pack/git-upload-pack", repo_name),
                )
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // The canonical endpoint must still work.
        let response = app
            .oneshot(
                mock_request(
                    "GET",
                    &format!("/{}/info/refs?service=git-upload-pack", repo_name),
                )
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

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
                mock_request("GET", &format!("/{}/info/refs?service=git-upload-pack", repo_name))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        
        let content_type = response.headers().get("content-type").unwrap().to_str().unwrap();
        assert_eq!(content_type, "application/x-git-upload-pack-advertisement");
        assert_eq!(
            response.headers().get("cache-control").unwrap().to_str().unwrap(),
            "no-cache, max-age=0, must-revalidate"
        );
        assert_eq!(
            response.headers().get("pragma").unwrap().to_str().unwrap(),
            "no-cache"
        );
        assert_eq!(
            response.headers().get("expires").unwrap().to_str().unwrap(),
            "Fri, 01 Jan 1980 00:00:00 GMT"
        );

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
            bob = "9b8769a4a742959a2d0298c36fb70623f2dfacda8436237df08d8dfd5b37374c" # pass123
        "#;
        fs::write(temp_users_file, config_toml).unwrap();

        let app = app();

        // 1. Upload-pack (read) without credentials -> 404 NOT_FOUND (because repo does not exist, but auth is bypassed!)
        let response = app
            .clone()
            .oneshot(
                mock_request("GET", "/myrepo/info/refs?service=git-upload-pack")
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
                mock_request("GET", "/myrepo/info/refs?service=git-receive-pack")
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
                mock_request("GET", "/myrepo/info/refs?service=git-receive-pack")
                    .header("Authorization", bad_auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        // 3b. Receive-pack (write) with bob's wrong password -> 401 Unauthorized
        let bad_bob_auth = format!("Basic {}", base64::engine::general_purpose::STANDARD.encode("bob:wrongpass"));
        let response = app
            .clone()
            .oneshot(
                mock_request("GET", "/myrepo/info/refs?service=git-receive-pack")
                    .header("Authorization", bad_bob_auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        // 3c. Receive-pack (write) with bob's correct password -> 200 OK
        let good_bob_auth = format!("Basic {}", base64::engine::general_purpose::STANDARD.encode("bob:pass123"));
        let bob_repo_name = "bob_auth_test_repo_123";
        let bob_safe_path = get_safe_repo_path(bob_repo_name).unwrap();
        if bob_safe_path.exists() {
            let _ = std::fs::remove_dir_all(&bob_safe_path);
        }
        let response = app
            .clone()
            .oneshot(
                mock_request("GET", &format!("/{}/info/refs?service=git-receive-pack", bob_repo_name))
                    .header("Authorization", good_bob_auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

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
                mock_request("GET", &format!("/{}/info/refs?service=git-receive-pack", repo_name))
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
                mock_request("POST", "/myrepo/git-upload-pack")
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
                mock_request("POST", "/myrepo/git-receive-pack")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        // Clean up
        let _ = std::fs::remove_dir_all(&safe_path);
        let _ = std::fs::remove_dir_all(&bob_safe_path);
        let _ = fs::remove_file(temp_users_file);
        unsafe {
            std::env::remove_var("SGIT_USERS_FILE");
        }
    }

    #[tokio::test]
    async fn test_percent_encoded_receive_pack_requires_auth() {
        let _guard = TEST_LOCK.lock().unwrap();
        let temp_users_file = "temp_encoded_users.toml";
        unsafe {
            std::env::set_var("SGIT_USERS_FILE", temp_users_file);
        }
        fs::write(
            temp_users_file,
            r#"
                [users]
                alice = "f75778f7425be4db0369d09af37a6c2b9a83dea0e53e7bd57412e4b060e607f7" # supersecret
            "#,
        )
        .unwrap();

        let app = app();
        let repo_name = "encoded_auth_test_repo";
        let safe_path = get_safe_repo_path(repo_name).unwrap();
        if safe_path.exists() {
            let _ = std::fs::remove_dir_all(&safe_path);
        }

        // Percent-encoded write endpoints must be protected exactly like their
        // canonical forms (B1).
        for uri in [
            format!("/{}/git%2Dreceive-pack", repo_name),
            format!("/{}/git%2dreceive-pack", repo_name),
            format!("/{}%2Fgit-receive-pack", repo_name),
        ] {
            let response = app
                .clone()
                .oneshot(
                    mock_request("POST", &uri)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "percent-encoded write endpoint was not protected: {uri}"
            );
        }

        let response = app
            .clone()
            .oneshot(
                mock_request(
                    "GET",
                    &format!("/{}/info/refs?service=git%2Dreceive-pack", repo_name),
                )
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        // Rejected requests must not auto-create the repository.
        assert!(!safe_path.exists());

        // An authenticated encoded request must still reach the handler.
        let auth = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode("alice:supersecret")
        );
        let response = app
            .oneshot(
                mock_request("POST", &format!("/{}/git%2Dreceive-pack", repo_name))
                    .header("Authorization", auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

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

    #[tokio::test]
    async fn test_rate_limiting() {
        let _guard = TEST_LOCK.lock().unwrap();
        unsafe {
            std::env::set_var("SGIT_RATE_LIMIT_PER_IP", "3");
        }
        let app = app();

        // 3 requests should succeed
        for _ in 0..3 {
            let response = app
                .clone()
                .oneshot(
                    mock_request("GET", "/")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        // 4th request should be rate-limited
        let response = app
            .clone()
            .oneshot(
                mock_request("GET", "/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);

        unsafe {
            std::env::remove_var("SGIT_RATE_LIMIT_PER_IP");
        }
    }

    #[tokio::test]
    async fn test_large_rate_limit_does_not_panic() {
        let _guard = TEST_LOCK.lock().unwrap();
        unsafe {
            std::env::set_var("SGIT_RATE_LIMIT_PER_IP", "100000");
        }
        let app = app();

        let response = app
            .oneshot(
                mock_request("GET", "/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        unsafe {
            std::env::remove_var("SGIT_RATE_LIMIT_PER_IP");
        }
    }

    #[tokio::test]
    async fn test_zero_concurrency_and_request_size_disable_limits() {
        let _guard = TEST_LOCK.lock().unwrap();
        unsafe {
            std::env::set_var("SGIT_MAX_CONCURRENT_REQS", "0");
            std::env::set_var("SGIT_MAX_REQUEST_SIZE_MB", "0");
        }
        let app = app();

        // The request must not be parked forever by a zero-permit semaphore.
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            app.clone().oneshot(
                mock_request("GET", "/")
                    .body(Body::empty())
                    .unwrap(),
            ),
        )
        .await
        .expect("request hung with SGIT_MAX_CONCURRENT_REQS=0")
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // A non-empty body must not be rejected when the size limit is disabled.
        let repo_name = "test_zero_limit_repo";
        let safe_path = get_safe_repo_path(repo_name).unwrap();
        if safe_path.exists() {
            let _ = std::fs::remove_dir_all(&safe_path);
        }
        let _repo = git2::Repository::init_bare(&safe_path).unwrap();

        let response = app
            .oneshot(
                mock_request("POST", &format!("/{}/git-upload-pack", repo_name))
                    .header("content-length", "1024")
                    .body(Body::from(vec![0u8; 1024]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_ne!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

        let _ = std::fs::remove_dir_all(&safe_path);
        unsafe {
            std::env::remove_var("SGIT_MAX_CONCURRENT_REQS");
            std::env::remove_var("SGIT_MAX_REQUEST_SIZE_MB");
        }
    }

    #[tokio::test]
    async fn test_request_size_limit() {
        let _guard = TEST_LOCK.lock().unwrap();
        let repo_name = "test_request_size_limit_repo";
        let safe_path = get_safe_repo_path(repo_name).unwrap();
        if safe_path.exists() {
            let _ = std::fs::remove_dir_all(&safe_path);
        }
        let _repo = git2::Repository::init_bare(&safe_path).unwrap();

        unsafe {
            std::env::set_var("SGIT_MAX_REQUEST_SIZE_MB", "1");
        }
        let app = app();

        // Body larger than 1MB
        let large_bytes = vec![0u8; 1100 * 1024];
        let len = large_bytes.len();
        let response = app
            .oneshot(
                mock_request("POST", &format!("/{}/git-upload-pack", repo_name))
                    .header("content-length", len.to_string())
                    .body(Body::from(large_bytes))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

        unsafe {
            std::env::remove_var("SGIT_MAX_REQUEST_SIZE_MB");
        }
        let _ = std::fs::remove_dir_all(&safe_path);
    }

    #[tokio::test]
    async fn test_read_timeout() {
        let _guard = TEST_LOCK.lock().unwrap();
        let repo_name = "test_read_timeout_repo";
        let safe_path = get_safe_repo_path(repo_name).unwrap();
        if safe_path.exists() {
            let _ = std::fs::remove_dir_all(&safe_path);
        }
        let _repo = git2::Repository::init_bare(&safe_path).unwrap();

        unsafe {
            std::env::set_var("SGIT_READ_TIMEOUT", "0");
        }
        let app = app();

        let response = app
            .oneshot(
                mock_request("GET", &format!("/{}/info/refs?service=git-upload-pack", repo_name))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);

        unsafe {
            std::env::remove_var("SGIT_READ_TIMEOUT");
        }
        let _ = std::fs::remove_dir_all(&safe_path);
    }

    #[tokio::test]
    async fn test_stream_timeout() {
        let _guard = TEST_LOCK.lock().unwrap();
        let repo_name = "test_stream_timeout_repo";
        let safe_path = get_safe_repo_path(repo_name).unwrap();
        if safe_path.exists() {
            let _ = std::fs::remove_dir_all(&safe_path);
        }
        let _repo = git2::Repository::init_bare(&safe_path).unwrap();

        unsafe {
            std::env::set_var("SGIT_STREAM_TIMEOUT", "0");
        }
        let app = app();

        let response = app
            .oneshot(
                mock_request("POST", &format!("/{}/git-upload-pack", repo_name))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body();
        let result = axum::body::to_bytes(body, 1000000).await;
        assert!(result.is_err());

        unsafe {
            std::env::remove_var("SGIT_STREAM_TIMEOUT");
        }
        let _ = std::fs::remove_dir_all(&safe_path);
    }

    #[tokio::test]
    async fn test_sighup_config_reload() {
        let _guard = TEST_LOCK.lock().unwrap();
        let temp_users_file = "temp_sighup_users.toml";
        unsafe {
            std::env::set_var("SGIT_USERS_FILE", temp_users_file);
        }

        // Initial config with user 'alice'
        let initial_toml = r#"
            [users]
            alice = "f75778f7425be4db0369d09af37a6c2b9a83dea0e53e7bd57412e4b060e607f7" # supersecret
        "#;
        fs::write(temp_users_file, initial_toml).unwrap();

        let app_state = utils::load_users_config();
        assert!(utils::check_basic_auth(&app_state, &format!("Basic {}", base64::engine::general_purpose::STANDARD.encode("alice:supersecret"))));
        assert!(!utils::check_basic_auth(&app_state, &format!("Basic {}", base64::engine::general_purpose::STANDARD.encode("bob:pass123"))));

        // Overwrite file with new credentials (added 'bob')
        let updated_toml = r#"
            [users]
            alice = "f75778f7425be4db0369d09af37a6c2b9a83dea0e53e7bd57412e4b060e607f7" # supersecret
            bob = "9b8769a4a742959a2d0298c36fb70623f2dfacda8436237df08d8dfd5b37374c" # pass123
        "#;
        fs::write(temp_users_file, updated_toml).unwrap();

        // Reload the config manually (signal reloading is verified by compile and manual tests, but we verify reload logic here)
        utils::reload_users_config(&app_state);

        assert!(utils::check_basic_auth(&app_state, &format!("Basic {}", base64::engine::general_purpose::STANDARD.encode("alice:supersecret"))));
        assert!(utils::check_basic_auth(&app_state, &format!("Basic {}", base64::engine::general_purpose::STANDARD.encode("bob:pass123"))));

        let _ = fs::remove_file(temp_users_file);
        unsafe {
            std::env::remove_var("SGIT_USERS_FILE");
        }
    }

    #[tokio::test]
    async fn test_repo_storage_quota() {
        let _guard = TEST_LOCK.lock().unwrap();
        let repo_name = "test_quota_repo";
        let safe_path = get_safe_repo_path(repo_name).unwrap();
        if safe_path.exists() {
            let _ = std::fs::remove_dir_all(&safe_path);
        }
        let _repo = git2::Repository::init_bare(&safe_path).unwrap();

        // Write a small file in the repo to simulate disk usage
        let test_file = safe_path.join("large_dummy_file");
        fs::write(&test_file, vec![0u8; 1024 * 1024]).unwrap(); // 1MB dummy file

        // Set quota to 0 MB (exceeded)
        unsafe {
            std::env::set_var("SGIT_MAX_REPO_SIZE_MB", "0");
        }
        let app = app();

        let response = app
            .oneshot(
                mock_request("GET", &format!("/{}/info/refs?service=git-receive-pack", repo_name))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INSUFFICIENT_STORAGE);

        unsafe {
            std::env::remove_var("SGIT_MAX_REPO_SIZE_MB");
        }
        let _ = std::fs::remove_dir_all(&safe_path);
    }
}

