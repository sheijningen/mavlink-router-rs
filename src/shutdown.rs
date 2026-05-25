use std::time::Duration;

#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// CLAUDE.md "Per-task 2s shutdown drain": each top-level `JoinSet` entry
/// gets up to this long after the cancellation token trips to complete its
/// current operation (final TX flush, final frame write, `PeerRemoved`
/// emission). Tasks still alive at this point are aborted; the overall
/// wall-clock budget [`SHUTDOWN_GRACE`] then bounds how long we wait for
/// the abort to land.
const PER_TASK_DRAIN: Duration = Duration::from_secs(2);

/// CLAUDE.md "Shutdown timing": overall wall-clock budget on shutdown.
/// Hardcoded — operator policy doesn't apply here, the constant is a
/// liveness bound (after this we abort + drop), not a tunable.
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

pub async fn watch_for_shutdown_signal(token: CancellationToken) {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            warn!(error = %err, "ctrl-c handler failed; signal source unavailable");
            std::future::pending::<()>().await
        }
    };

    #[cfg(unix)]
    let sigterm = async {
        match signal(SignalKind::terminate()) {
            Ok(mut sigterm_stream) => {
                sigterm_stream.recv().await;
            }
            Err(err) => {
                warn!(error = %err, "SIGTERM handler install failed");
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

/// Two-tier drain. Phase 1 waits up to [`PER_TASK_DRAIN`] (capped by
/// `wall_clock`) for tasks to return naturally. Phase 2 calls
/// [`JoinSet::shutdown`] and waits up to the remaining wall-clock budget
/// for the aborts to land; tasks still hanging at the wall-clock deadline
/// log at WARN and are dropped.
pub async fn shutdown<T: Send + 'static>(mut tasks: JoinSet<T>, wall_clock: Duration) {
    if tasks.is_empty() {
        return;
    }

    let start = tokio::time::Instant::now();
    let drain_budget = PER_TASK_DRAIN.min(wall_clock);
    let drained = tokio::time::timeout(drain_budget, async {
        while tasks.join_next().await.is_some() {}
    })
    .await;

    if drained.is_ok() {
        return;
    }

    warn!(
        remaining = tasks.len(),
        "per-task drain budget exceeded; aborting remaining tasks"
    );

    let abort_budget = wall_clock.saturating_sub(start.elapsed());
    if tokio::time::timeout(abort_budget, tasks.shutdown())
        .await
        .is_err()
    {
        warn!(
            remaining = tasks.len(),
            "wall-clock shutdown budget exceeded after abort; giving up"
        );
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
            let counter_clone = counter.clone();
            tasks.spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                counter_clone.fetch_add(1, Ordering::Relaxed);
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
            let counter_clone = counter.clone();
            tasks.spawn(async move {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                counter_clone.fetch_add(1, Ordering::Relaxed);
            });
        }
        let start = tokio::time::Instant::now();
        // Sub-PER_TASK_DRAIN grace clamps phase 1 to the wall-clock, so the
        // total elapsed stays bounded by the caller's chosen budget.
        shutdown(tasks, Duration::from_millis(100)).await;
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(100));
        assert!(elapsed < Duration::from_secs(2));
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn aborts_tasks_exceeding_per_task_drain() {
        // A task that sleeps far longer than the 2s per-task drain must
        // be aborted at the 2s mark and the shutdown call must return
        // shortly after — well before the 5s wall-clock budget elapses.
        let mut tasks: JoinSet<()> = JoinSet::new();
        let counter = Arc::new(AtomicUsize::new(0));
        for _ in 0..2 {
            let counter_clone = counter.clone();
            tasks.spawn(async move {
                tokio::time::sleep(Duration::from_secs(60)).await;
                counter_clone.fetch_add(1, Ordering::Relaxed);
            });
        }
        let start = tokio::time::Instant::now();
        shutdown(tasks, Duration::from_secs(5)).await;
        let elapsed = start.elapsed();
        assert!(
            elapsed >= PER_TASK_DRAIN,
            "shutdown returned before per-task drain elapsed: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(3),
            "shutdown lingered past per-task drain (should abort): {elapsed:?}"
        );
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn task_completing_within_per_task_drain_is_not_aborted() {
        // 100 ms is well inside the 2s budget; the counter must reach 3.
        let mut tasks: JoinSet<()> = JoinSet::new();
        let counter = Arc::new(AtomicUsize::new(0));
        for _ in 0..3 {
            let counter_clone = counter.clone();
            tasks.spawn(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                counter_clone.fetch_add(1, Ordering::Relaxed);
            });
        }
        let start = tokio::time::Instant::now();
        shutdown(tasks, Duration::from_secs(5)).await;
        let elapsed = start.elapsed();
        assert!(
            elapsed < PER_TASK_DRAIN,
            "well-behaved tasks should drain before the per-task deadline"
        );
        assert_eq!(counter.load(Ordering::Relaxed), 3);
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
