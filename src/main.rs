use sgit::app;

#[tokio::main]
async fn main() {
    let app = app();
    
    let host = std::env::var("SGIT_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = std::env::var("SGIT_PORT")
        .ok()
        .and_then(|val| val.parse::<u16>().ok())
        .unwrap_or(3000);

    let addr = format!("{}:{}", host, port);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|_| panic!("Failed to bind to {}", addr));

    sgit::log_stderr!("SGit server running at http://{}", addr);
    
    axum::serve(listener, app.into_make_service_with_connect_info::<std::net::SocketAddr>())
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install CTRL+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    sgit::log_stderr!("Shutdown signal received, shutting down gracefully...");
}

