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

#[derive(Clone, Debug)]
pub struct AppState {
    pub users: Arc<HashMap<String, String>>,
    pub has_users_file: bool,
}

// Custom response stream that holds the process child to kill on drop (disconnect) or timeout
pub struct GitResponseStream {
    pub inner: ReaderStream<tokio::process::ChildStdout>,
    pub child: tokio::process::Child,
    pub timeout: Pin<Box<Sleep>>,
    pub cancellation_token: tokio_util::sync::CancellationToken,
    pub repo_path: Option<PathBuf>,
}

impl Stream for GitResponseStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use std::future::Future;
        // Check timeout
        if self.timeout.as_mut().poll(cx).is_ready() {
            self.cancellation_token.cancel();
            let _ = self.child.start_kill();
            return Poll::Ready(Some(Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Git process timed out",
            ))));
        }

        // Poll stdout
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl Drop for GitResponseStream {
    fn drop(&mut self) {
        self.cancellation_token.cancel();
        
        // Wait (or kill if we disconnect prematurely), but try to check child's success status
        // Drop is synchronous, so we can only invoke try_wait or start_kill.
        // We'll see if the child has already exited, and if it exited with success, trigger GC
        let exited_successfully = match self.child.try_wait() {
            Ok(Some(status)) => status.success(),
            _ => {
                let _ = self.child.start_kill();
                false
            }
        };

        // Automatically check and update HEAD after the git stream process completes
        if let Some(path) = &self.repo_path {
            update_head_if_invalid(path);
            
            if exited_successfully {
                // Spawn an async background task to run git gc --auto
                let path_clone = path.clone();
                tokio::spawn(async move {
                    let mut gc_cmd = tokio::process::Command::new("git");
                    gc_cmd.arg("-C")
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
    }
}

