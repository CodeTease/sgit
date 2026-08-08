use sgit::app;

#[tokio::main]
async fn main() {
    let app = app();

    let port = std::env::var("SGIT_PORT")
        .ok()
        .and_then(|val| val.parse::<u16>().ok())
        .unwrap_or(3000);

    let addr = format!("127.0.0.1:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|_| panic!("Failed to bind to {}", addr));

    println!("SGit server running at http://{}", addr);
    
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C handler");
    println!("Shutdown signal received, shutting down gracefully...");
}

