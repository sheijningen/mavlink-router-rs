use std::time::Duration;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::warn;

pub async fn watch_for_shutdown_signal(token: CancellationToken) {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            warn!(error = %e, "ctrl-c handler failed; signal source unavailable");
            std::future::pending::<()>().await
        }
    };

    #[cfg(unix)]
    let sigterm = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                warn!(error = %e, "SIGTERM handler install failed");
                std::future::pending::<()>().await
            }
        }
    };

    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = sigterm => {}
        _ = token.cancelled() => return,
    }
    token.cancel();
}

pub async fn shutdown<T: Send + 'static>(mut tasks: JoinSet<T>, grace: Duration) {
    if tasks.is_empty() {
        return;
    }
    let deadline = tokio::time::Instant::now() + grace;
    loop {
        if tasks.is_empty() {
            return;
        }
        tokio::select! {
            biased;
            joined = tasks.join_next() => {
                if joined.is_none() {
                    return;
                }
            }
            () = tokio::time::sleep_until(deadline) => {
                let remaining = tasks.len();
                warn!(remaining, "shutdown grace exceeded; aborting remaining tasks");
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn empty_joinset_returns_immediately() {
        let tasks: JoinSet<()> = JoinSet::new();
        let start = tokio::time::Instant::now();
        shutdown(tasks, Duration::from_secs(5)).await;
        assert!(start.elapsed() < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn waits_for_tasks_that_finish_within_grace() {
        let mut tasks: JoinSet<()> = JoinSet::new();
        let counter = Arc::new(AtomicUsize::new(0));
        for _ in 0..3 {
            let c = counter.clone();
            tasks.spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                c.fetch_add(1, Ordering::Relaxed);
            });
        }
        shutdown(tasks, Duration::from_secs(5)).await;
        assert_eq!(counter.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn aborts_tasks_that_outlast_grace() {
        let mut tasks: JoinSet<()> = JoinSet::new();
        let counter = Arc::new(AtomicUsize::new(0));
        for _ in 0..2 {
            let c = counter.clone();
            tasks.spawn(async move {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                c.fetch_add(1, Ordering::Relaxed);
            });
        }
        let start = tokio::time::Instant::now();
        shutdown(tasks, Duration::from_millis(100)).await;
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(100));
        assert!(elapsed < Duration::from_secs(2));
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn watch_returns_when_token_already_cancelled() {
        let token = CancellationToken::new();
        token.cancel();
        tokio::time::timeout(Duration::from_millis(100), watch_for_shutdown_signal(token))
            .await
            .expect("watch returned when token already cancelled");
    }
}
