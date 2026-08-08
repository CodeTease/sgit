use std::pin::Pin;
use std::task::{Context, Poll};
use futures_core::Stream;
use bytes::Bytes;
use tokio::time::Sleep;
use tokio_util::io::ReaderStream;
use serde::{Serialize, Deserialize};

// Custom response stream that holds the process child to kill on drop (disconnect) or timeout
pub struct GitResponseStream {
    pub inner: ReaderStream<tokio::process::ChildStdout>,
    pub child: tokio::process::Child,
    pub timeout: Pin<Box<Sleep>>,
}

impl Stream for GitResponseStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use std::future::Future;
        // Check timeout
        if self.timeout.as_mut().poll(cx).is_ready() {
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
        let _ = self.child.start_kill();
    }
}

// GET /:repo/branches
#[derive(Serialize, Deserialize)]
pub struct BranchInfo {
    pub name: String,
    pub commit_hash: String,
}

// GET /:repo/commits
#[derive(Serialize, Deserialize)]
pub struct CommitInfo {
    pub hash: String,
    pub author: String,
    pub email: String,
    pub time: i64,
    pub message: String,
}

// GET /:repo/tree and GET /:repo/tree/*path
#[derive(Serialize, Deserialize)]
pub struct TreeEntryInfo {
    pub name: String,
    pub is_dir: bool,
    pub sha: String,
}

