use std::pin::Pin;
use std::task::{Context, Poll};
use std::path::PathBuf;
use futures_core::Stream;
use bytes::Bytes;
use tokio_util::io::ReaderStream;
use crate::utils::{
    get_dir_size_async, get_repo_size_quota_bytes, update_head_if_invalid_async,
};
use std::sync::Arc;
use std::collections::HashMap;
use std::time::Duration;

use std::sync::{Mutex, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

#[derive(Clone, Debug)]
pub struct AppState {
    pub users: Arc<RwLock<HashMap<String, String>>>,
    pub has_users_file: Arc<AtomicBool>,
    /// Bounds concurrently running git processes. A permit is held for the
    /// entire response-body lifetime, not just until response headers.
    pub git_semaphore: Option<Arc<tokio::sync::Semaphore>>,
}

type SharedChild = Arc<Mutex<Option<tokio::process::Child>>>;

// Custom response stream that owns the git process and its stdout.
//
// The child is shared with an independent idle-timeout task. This matters
// because hyper polls a response body only when the client socket can accept
// more data: a slow/non-reading client must not prevent the timeout from
// firing (and the git process from being killed).
//
// On a client disconnect the stdin writer is cancelled (closing git's stdin),
// which lets receive-pack finish its ref transaction instead of being killed
// mid-update. A grace period prevents a truly stuck process from lingering
// forever.
pub struct GitResponseStream {
    pub inner: ReaderStream<tokio::process::ChildStdout>,
    pub child: SharedChild,
    /// Notified whenever stdout makes progress (resets the idle timeout).
    pub activity: Arc<Notify>,
    /// Set by the idle-timeout task right before it kills the child.
    pub timed_out: Arc<AtomicBool>,
    pub cancellation_token: tokio_util::sync::CancellationToken,
    pub repo_path: Option<PathBuf>,
    /// True once stdout reached EOF, i.e. the body completed normally.
    pub completed: bool,
    /// Concurrency permit held until the git process is reaped.
    pub permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl Stream for GitResponseStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.timed_out.load(Ordering::SeqCst) {
            self.cancellation_token.cancel();
            return Poll::Ready(Some(Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Git process timed out",
            ))));
        }

        // Poll stdout
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_next(cx);
        match &result {
            Poll::Ready(Some(Ok(_))) => this.activity.notify_one(),
            Poll::Ready(None) => this.completed = true,
            _ => {}
        }
        result
    }
}

impl Drop for GitResponseStream {
    fn drop(&mut self) {
        self.cancellation_token.cancel();

        let permit = self.permit.take();
        let child = match self.child.lock() {
            Ok(mut slot) => slot.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        let Some(mut child) = child else {
            return;
        };
        let repo_path = self.repo_path.clone();

        // Drop is synchronous, so move the child into a task that can wait for
        // it. Never SIGKILL immediately: receive-pack may be committing refs or
        // running hooks.
        tokio::spawn(async move {
            let status = match tokio::time::timeout(Duration::from_secs(60), child.wait()).await {
                Ok(Ok(status)) => Some(status),
                Ok(Err(_)) => None,
                Err(_) => {
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    None
                }
            };
            let exited_successfully = status.map(|s| s.success()).unwrap_or(false);

            // Release the concurrency slot only after the process has really
            // exited, not when the client disconnected.
            drop(permit);

            if let Some(path) = repo_path {
                // Automatically check and update HEAD after the git process
                // completes (also when the client disconnected after sending
                // a complete push).
                update_head_if_invalid_async(path.clone()).await;

                // Re-check the quota after the push. A push accepted near the
                // limit can still grow the repository beyond it; subsequent
                // pushes are then rejected by the pre-push check.
                if let Some(quota_bytes) = get_repo_size_quota_bytes() {
                    if let Ok(size) = get_dir_size_async(path.clone()).await {
                        if size > quota_bytes {
                            crate::log_stderr!(
                                "Repository {} is over quota ({} bytes > {} bytes)",
                                path.display(),
                                size,
                                quota_bytes
                            );
                        }
                    }
                }

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
        });
    }
}

/// Enforce an idle timeout independently of response-body polling.
///
/// The timer only resets when either stdout makes progress or stdin receives
/// request data; a client that stops reading (or a git process that stops
/// producing output) is killed once the idle period elapses.
pub fn spawn_idle_timeout(
    idle_timeout: Duration,
    child: SharedChild,
    activity: Arc<Notify>,
    timed_out: Arc<AtomicBool>,
    cancellation_token: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                // The body was consumed or dropped; the Drop waiter handles
                // the child from here.
                _ = cancellation_token.cancelled() => return,
                // Progress reset the idle deadline.
                _ = activity.notified() => continue,
                _ = tokio::time::sleep(idle_timeout) => {
                    timed_out.store(true, Ordering::SeqCst);
                    cancellation_token.cancel();
                    let mut slot = match child.lock() {
                        Ok(slot) => slot,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    if let Some(child) = slot.as_mut() {
                        let _ = child.start_kill();
                    }
                    return;
                }
            }
        }
    });
}

