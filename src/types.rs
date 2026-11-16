use std::pin::Pin;
use std::task::{Context, Poll};
use std::path::PathBuf;
use futures_core::Stream;
use bytes::Bytes;
use tokio::time::Sleep;
use tokio_util::io::ReaderStream;
use crate::utils::update_head_if_invalid;
use std::sync::Arc;
use std::collections::HashMap;

use std::sync::RwLock;
use std::sync::atomic::AtomicBool;

#[derive(Clone, Debug)]
pub struct AppState {
    pub users: Arc<RwLock<HashMap<String, String>>>,
    pub has_users_file: Arc<AtomicBool>,
}

// Custom response stream that owns the git process and its stdout.
//
// The child is kept in an Option so Drop can move it into an asynchronous
// waiter. On a client disconnect the stdin writer is cancelled (closing git's
// stdin), which lets receive-pack finish its ref transaction instead of being
// killed mid-update. A grace period prevents a truly stuck process from
// lingering forever.
pub struct GitResponseStream {
    pub inner: ReaderStream<tokio::process::ChildStdout>,
    pub child: Option<tokio::process::Child>,
    pub timeout: Pin<Box<Sleep>>,
    pub cancellation_token: tokio_util::sync::CancellationToken,
    pub repo_path: Option<PathBuf>,
    /// True once stdout reached EOF, i.e. the body completed normally.
    pub completed: bool,
}

impl Stream for GitResponseStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use std::future::Future;
        // Check timeout
        if self.timeout.as_mut().poll(cx).is_ready() {
            self.cancellation_token.cancel();
            if let Some(child) = self.child.as_mut() {
                let _ = child.start_kill();
            }
            return Poll::Ready(Some(Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Git process timed out",
            ))));
        }

        // Poll stdout
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_next(cx);
        if matches!(result, Poll::Ready(None)) {
            this.completed = true;
        }
        result
    }
}

impl Drop for GitResponseStream {
    fn drop(&mut self) {
        self.cancellation_token.cancel();

        let Some(mut child) = self.child.take() else {
            return;
        };
        let repo_path = self.repo_path.clone();
        let completed_normally = self.completed;

        // Drop is synchronous, so move the child into a task that can wait for
        // it. Never SIGKILL immediately: receive-pack may be committing refs or
        // running hooks.
        tokio::spawn(async move {
            let status = match tokio::time::timeout(
                std::time::Duration::from_secs(60),
                child.wait(),
            )
            .await
            {
                Ok(Ok(status)) => Some(status),
                Ok(Err(_)) => None,
                Err(_) => {
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    None
                }
            };
            let exited_successfully = status.map(|s| s.success()).unwrap_or(false);

            if let Some(path) = repo_path {
                // Automatically check and update HEAD after the git process
                // completes (also when the client disconnected after sending
                // a complete push).
                update_head_if_invalid(&path);

                if exited_successfully {
                    // Spawn a background task to run git gc --auto.
                    let path_clone = path.clone();
                    tokio::spawn(async move {
                        let mut gc_cmd = tokio::process::Command::new("git");
                        gc_cmd
                            .arg("-C")
                            .arg(&path_clone)
                            .arg("gc")
                            .arg("--auto")
                            .stdin(std::process::Stdio::null())
                            .stdout(std::process::Stdio::null())
                            .stderr(std::process::Stdio::null());
                        let _ = gc_cmd.status().await;
                    });
                }
            }

            // Kept for clarity: a normal EOF is the common path. Aborted
            // uploads also reach here after stdin closes; both are awaited
            // before any fallback kill.
            let _ = completed_normally;
        });
    }
}

