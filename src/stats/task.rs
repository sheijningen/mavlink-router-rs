//! The stats task itself: the run loop over cancel, interval ticks, router
//! events, and the stdout writer, plus its post-cancel drain.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use tokio::io::AsyncWrite;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use super::handle::StatsInbox;
use super::queue::{QueueEntry, warn_on_new_drops};
use super::registry::{RegisteredEndpoint, emit_interval_lines, handle_event};
use super::writer::LineWriter;
use crate::endpoint::EndpointId;

/// Stats-task bounded queue depth; drop-oldest on slow/dead stdout consumer.
pub const DEFAULT_STATS_QUEUE_LINES: usize = 256;

/// Knobs handed to [`run`] by the spawner — `enabled` mirrors `--stats`,
/// `interval` mirrors `--stats-interval-secs`, and `queue_capacity` is the
/// bounded internal buffer between interval-tick production and async
/// stdout writes.
#[derive(Debug, Clone, Copy)]
pub struct StatsRunConfig {
    pub enabled: bool,
    pub interval: Duration,
    pub queue_capacity: usize,
}

/// Run the stats task until the cancellation token fires. Maintains the
/// registry mirror from router-emitted `Register`/`Finalize`
/// events, and — when `cfg.enabled` is true — emits one JSON line per
/// registered endpoint per interval to `writer`. The writer is parameterised
/// so production wires `tokio::io::stdout()` while tests pass an in-memory
/// duplex stream.
pub async fn run<W>(inbox: StatsInbox, cancel: CancellationToken, cfg: StatsRunConfig, writer: W)
where
    W: AsyncWrite + Send + Unpin,
{
    let mut event_rx = inbox.rx;
    let mut registry: HashMap<EndpointId, RegisteredEndpoint> = HashMap::new();
    let mut queue: VecDeque<QueueEntry> = VecDeque::new();
    let mut total_dropped: u64 = 0;
    let mut last_warned_dropped: u64 = 0;
    let mut line_writer = LineWriter::new(writer);

    // Delay the first tick so registrations land first; `Skip` avoids a
    // post-stall burst of catch-up lines.
    let mut interval = cfg.enabled.then(|| {
        let start = tokio::time::Instant::now() + cfg.interval;
        let mut timer = tokio::time::interval_at(start, cfg.interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        timer
    });

    // Writing is its own select arm so a stalled consumer never blocks event
    // intake, and the last arm because a starved intake corrupts the registry.
    loop {
        line_writer.stage_next(&mut queue);
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            tick = interval_tick(interval.as_mut()) => {
                let _: () = tick;
                emit_interval_lines(&registry, &mut queue, cfg.queue_capacity, &mut total_dropped);
                warn_on_new_drops(total_dropped, &mut last_warned_dropped);
            }
            received = event_rx.recv() => match received {
                Some(event) => {
                    handle_event(&mut registry, &mut queue, cfg.queue_capacity, &mut total_dropped, cfg.enabled, event);
                }
                None => {
                    debug!("stats_event channel closed; stats task exiting");
                    break;
                }
            },
            () = line_writer.write_step(), if line_writer.has_pending() => {}
        }
    }

    // `recv().await` (not `try_recv`) is load-bearing: shutdown_sweep
    // emits Finalizes AFTER its own cancel observation, so a tight
    // try_recv would race past them and leave endpoints stuck on an
    // interval-line as their last observable state.
    let drain_deadline = tokio::time::Instant::now() + POST_CANCEL_DRAIN;
    loop {
        let now = tokio::time::Instant::now();
        if now >= drain_deadline {
            break;
        }
        let remaining = drain_deadline - now;
        match tokio::time::timeout(remaining, event_rx.recv()).await {
            Ok(Some(event)) => handle_event(
                &mut registry,
                &mut queue,
                cfg.queue_capacity,
                &mut total_dropped,
                cfg.enabled,
                event,
            ),
            Ok(None) | Err(_) => break,
        }
    }
    line_writer.drain(&mut queue).await;
}

/// Post-cancel slice of the per-task 2s drain budget the harness honours
/// (see [`crate::shutdown::PER_TASK_DRAIN`]). 1500 ms for processing
/// in-flight Register/Finalize events leaves ~500 ms of slack for the
/// final `LineWriter::drain` writes to land before the harness aborts.
///
/// MUST stay strictly smaller than [`crate::shutdown::PER_TASK_DRAIN`] —
/// if these two equal each other, the harness aborts before the final
/// `LineWriter::drain` writes get any wall-clock slack and authoritative
/// synthetic lines vanish.
const POST_CANCEL_DRAIN: Duration = Duration::from_millis(1500);

/// Either await the interval timer or park forever on a `pending` future.
/// `interval.as_mut()` lets the same select! arm work whether stats are
/// enabled or not without a second copy of the loop.
async fn interval_tick(interval: Option<&mut tokio::time::Interval>) {
    match interval {
        Some(timer) => {
            timer.tick().await;
        }
        None => std::future::pending::<()>().await,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, duplex};
    use tokio::sync::mpsc;

    use super::*;
    use crate::endpoint::EndpointIdAllocator;
    use crate::endpoint::stats::{EndpointState, EndpointStats};
    use crate::stats::StatsEvent;

    fn make_register(id: EndpointId, name: &str, stats: Arc<EndpointStats>) -> StatsEvent {
        StatsEvent::Register {
            id,
            name: name.to_string(),
            stats,
            routable: true,
        }
    }

    fn make_register_listener(id: EndpointId, name: &str, stats: Arc<EndpointStats>) -> StatsEvent {
        StatsEvent::Register {
            id,
            name: name.to_string(),
            stats,
            routable: false,
        }
    }

    fn disabled_config() -> StatsRunConfig {
        StatsRunConfig {
            enabled: false,
            interval: Duration::from_secs(60),
            queue_capacity: DEFAULT_STATS_QUEUE_LINES,
        }
    }

    fn enabled_config(interval_ms: u64, queue_capacity: usize) -> StatsRunConfig {
        StatsRunConfig {
            enabled: true,
            interval: Duration::from_millis(interval_ms),
            queue_capacity,
        }
    }

    #[tokio::test]
    async fn task_exits_on_cancel_with_sender_alive_disabled() {
        // 3s join budget covers POST_CANCEL_DRAIN (1.5s) plus slack.
        let (tx, rx) = mpsc::channel::<StatsEvent>(4);
        let cancel = CancellationToken::new();
        let (writer, _reader) = duplex(1024);
        let handle = tokio::spawn(run(
            StatsInbox { rx },
            cancel.clone(),
            disabled_config(),
            writer,
        ));
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(3), handle)
            .await
            .expect("stats task did not exit on cancel within drain budget")
            .expect("stats task panicked");
        drop(tx);
    }

    #[tokio::test]
    async fn post_cancel_drain_processes_finalize_arriving_after_cancel() {
        let (tx, rx) = mpsc::channel::<StatsEvent>(8);
        let cancel = CancellationToken::new();
        let (writer, mut reader) = duplex(4096);
        let cfg = enabled_config(60_000, 32); // interval long enough not to fire
        let handle = tokio::spawn(run(StatsInbox { rx }, cancel.clone(), cfg, writer));

        let alloc = EndpointIdAllocator::new();
        let id = alloc.alloc();
        let stats = Arc::new(EndpointStats::default());
        stats.store_state(EndpointState::Connected);
        tx.send(make_register(id, "ep", stats.clone()))
            .await
            .expect("register");

        // Cancel first, then send Finalize — mirrors shutdown_sweep.
        cancel.cancel();
        tokio::task::yield_now().await;
        stats.store_state(EndpointState::Down);
        tx.send(StatsEvent::Finalize { id })
            .await
            .expect("finalize after cancel");
        drop(tx);

        tokio::time::timeout(Duration::from_secs(3), handle)
            .await
            .expect("stats task did not exit")
            .expect("stats task panicked");

        let mut buf = vec![0u8; 4096];
        let bytes_read = reader.read(&mut buf).await.expect("read");
        assert!(bytes_read > 0, "no synthetic line emitted post-cancel");
        let line = std::str::from_utf8(&buf[..bytes_read]).unwrap().trim_end();
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(value["endpoint"], "ep");
        assert_eq!(value["state"], "down");
    }

    #[tokio::test]
    async fn task_exits_when_router_drops_sender() {
        let (tx, rx) = mpsc::channel::<StatsEvent>(4);
        let cancel = CancellationToken::new();
        let (writer, _reader) = duplex(1024);
        let handle = tokio::spawn(run(StatsInbox { rx }, cancel, disabled_config(), writer));
        drop(tx);
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("stats task did not exit when channel closed")
            .expect("stats task panicked");
    }

    #[tokio::test]
    async fn disabled_task_emits_no_lines_on_finalize() {
        let (tx, rx) = mpsc::channel::<StatsEvent>(8);
        let cancel = CancellationToken::new();
        let (writer, mut reader) = duplex(4096);
        let handle = tokio::spawn(run(
            StatsInbox { rx },
            cancel.clone(),
            disabled_config(),
            writer,
        ));

        let alloc = EndpointIdAllocator::new();
        let id = alloc.alloc();
        let stats = Arc::new(EndpointStats::default());
        stats.store_state(EndpointState::Connected);
        tx.send(make_register(id, "ep", stats.clone()))
            .await
            .expect("register");
        tx.send(StatsEvent::Finalize { id })
            .await
            .expect("finalize");

        tokio::time::sleep(Duration::from_millis(20)).await;
        // Drop the sender so the post-cancel drain exits via `Ok(None)`
        // without burning the drain budget.
        drop(tx);
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("stats task did not exit")
            .expect("stats task panicked");

        let mut buf = vec![0u8; 1024];
        let bytes_read = reader.read(&mut buf).await.expect("read");
        assert_eq!(
            bytes_read, 0,
            "disabled stats wrote {bytes_read} bytes to stdout"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn enabled_task_emits_one_line_per_endpoint_per_interval() {
        let (tx, rx) = mpsc::channel::<StatsEvent>(8);
        let cancel = CancellationToken::new();
        let (writer, mut reader) = duplex(8192);
        let cfg = enabled_config(100, 32);
        let handle = tokio::spawn(run(StatsInbox { rx }, cancel.clone(), cfg, writer));

        let alloc = EndpointIdAllocator::new();
        let id_a = alloc.alloc();
        let id_b = alloc.alloc();
        let stats_a = Arc::new(EndpointStats::default());
        let stats_b = Arc::new(EndpointStats::default());
        stats_a.add_rx_frame(10);
        stats_b.add_tx_frame(20);
        stats_a.store_state(EndpointState::Connected);
        stats_b.store_state(EndpointState::Reconnecting);
        tx.send(make_register(id_a, "alpha", stats_a))
            .await
            .expect("register a");
        tx.send(make_register(id_b, "beta", stats_b))
            .await
            .expect("register b");

        // Advance past one interval so the timer fires once.
        tokio::time::sleep(Duration::from_millis(150)).await;

        let mut buf = vec![0u8; 4096];
        let bytes_read = tokio::time::timeout(Duration::from_secs(1), reader.read(&mut buf))
            .await
            .expect("reader timeout")
            .expect("reader read");
        let text = std::str::from_utf8(&buf[..bytes_read]).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2, "expected 2 lines, got: {text}");
        let mut endpoints: Vec<String> = lines
            .iter()
            .map(|line| {
                let value: serde_json::Value = serde_json::from_str(line).unwrap();
                value["endpoint"].as_str().unwrap().to_string()
            })
            .collect();
        endpoints.sort();
        assert_eq!(endpoints, vec!["alpha", "beta"]);

        // Drop the sender so the post-cancel drain exits via `Ok(None)`
        // (under `start_paused`, timeout expiry needs `time::advance`).
        drop(tx);
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("stats task did not exit")
            .expect("stats task panicked");
    }

    #[tokio::test(start_paused = true)]
    async fn enabled_task_emits_state_only_line_for_listener_endpoint() {
        let (tx, rx) = mpsc::channel::<StatsEvent>(8);
        let cancel = CancellationToken::new();
        let (writer, mut reader) = duplex(4096);
        let cfg = enabled_config(100, 32);
        let handle = tokio::spawn(run(StatsInbox { rx }, cancel.clone(), cfg, writer));

        let alloc = EndpointIdAllocator::new();
        let id = alloc.alloc();
        let stats = Arc::new(EndpointStats::default());
        stats.store_state(EndpointState::Connected);
        // Bump a counter that must NOT appear in the listener's emitted line.
        stats.rx_frames.fetch_add(42, Ordering::Relaxed);
        tx.send(make_register_listener(id, "input", stats))
            .await
            .expect("register listener");

        tokio::time::sleep(Duration::from_millis(150)).await;

        let mut buf = vec![0u8; 4096];
        let bytes_read = tokio::time::timeout(Duration::from_secs(1), reader.read(&mut buf))
            .await
            .expect("reader timeout")
            .expect("reader read");
        let text = std::str::from_utf8(&buf[..bytes_read]).unwrap();
        let line = text.lines().next().expect("at least one line");
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        let object = value.as_object().expect("object");
        assert_eq!(
            object.len(),
            3,
            "listener line had unexpected fields: {object:?}"
        );
        assert_eq!(object["endpoint"], "input");
        assert_eq!(object["state"], "connected");
        assert!(!object.contains_key("rx_frames"));

        drop(tx);
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("stats task did not exit")
            .expect("stats task panicked");
    }

    #[tokio::test(start_paused = true)]
    async fn finalize_emits_synthetic_line_with_terminal_state() {
        let (tx, rx) = mpsc::channel::<StatsEvent>(8);
        let cancel = CancellationToken::new();
        let (writer, mut reader) = duplex(4096);
        // High interval so only the synthetic Finalize line lands on stdout
        // (the cancel drain flushes the queue regardless of interval).
        let cfg = enabled_config(60_000, 32);
        let handle = tokio::spawn(run(StatsInbox { rx }, cancel.clone(), cfg, writer));

        let alloc = EndpointIdAllocator::new();
        let id = alloc.alloc();
        let stats = Arc::new(EndpointStats::default());
        stats.store_state(EndpointState::Connected);
        tx.send(make_register(id, "ep", stats.clone()))
            .await
            .expect("register");
        // Router writes terminal state before forwarding Finalize.
        stats.store_state(EndpointState::Down);
        tx.send(StatsEvent::Finalize { id })
            .await
            .expect("finalize");

        // Give the task a chance to drain the event and write the line.
        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut buf = vec![0u8; 4096];
        let bytes_read = tokio::time::timeout(Duration::from_secs(1), reader.read(&mut buf))
            .await
            .expect("reader timeout")
            .expect("reader read");
        let line = std::str::from_utf8(&buf[..bytes_read]).unwrap().trim_end();
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(value["endpoint"], "ep");
        assert_eq!(value["state"], "down");

        drop(tx);
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("stats task did not exit")
            .expect("stats task panicked");
    }

    #[tokio::test(start_paused = true)]
    async fn broken_pipe_logs_once_and_swallows_subsequent_writes() {
        let (tx, rx) = mpsc::channel::<StatsEvent>(8);
        let cancel = CancellationToken::new();
        let (writer, reader) = duplex(64);
        drop(reader); // Closing the reader half forces BrokenPipe on the next write.
        let cfg = enabled_config(10, 32);
        let handle = tokio::spawn(run(StatsInbox { rx }, cancel.clone(), cfg, writer));

        let alloc = EndpointIdAllocator::new();
        let id = alloc.alloc();
        let stats = Arc::new(EndpointStats::default());
        tx.send(make_register(id, "ep", stats)).await.expect("reg");

        // Multiple intervals so the BrokenPipe branch fires repeatedly; the
        // task must keep ticking despite the writer being dead.
        tokio::time::sleep(Duration::from_millis(80)).await;

        drop(tx);
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("stats task did not exit")
            .expect("stats task panicked");
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_stdout_does_not_block_event_intake() {
        // No reader: the first line jams the 16-byte duplex, and a stats task
        // wedged on it would wedge the 2-slot sender too.
        let (tx, rx) = mpsc::channel::<StatsEvent>(2);
        let cancel = CancellationToken::new();
        let (writer, reader) = duplex(16);
        let cfg = enabled_config(100, 8);
        let handle = tokio::spawn(run(StatsInbox { rx }, cancel.clone(), cfg, writer));

        let alloc = EndpointIdAllocator::new();
        let id = alloc.alloc();
        let stats = Arc::new(EndpointStats::default());
        tx.send(make_register(id, "ep", stats))
            .await
            .expect("register");

        // Two ticks: the first line jams the writer, the second queues.
        tokio::time::sleep(Duration::from_millis(250)).await;

        for _ in 0..16 {
            let peer = alloc.alloc();
            let peer_stats = Arc::new(EndpointStats::default());
            tokio::time::timeout(
                Duration::from_millis(500),
                tx.send(make_register(peer, "peer", peer_stats)),
            )
            .await
            .expect("stats task stopped taking events while stdout was stalled")
            .expect("register peer");
            tokio::time::timeout(
                Duration::from_millis(500),
                tx.send(StatsEvent::Finalize { id: peer }),
            )
            .await
            .expect("stats task stopped taking events while stdout was stalled")
            .expect("finalize peer");
        }

        // Closing the reader turns the stuck write into BrokenPipe so the
        // final drain can finish.
        drop(reader);
        drop(tx);
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(3), handle)
            .await
            .expect("stats task did not exit")
            .expect("stats task panicked");
    }

    #[tokio::test]
    async fn partial_writes_resume_without_corrupting_lines() {
        // A 16-byte duplex splits every line into many partial writes; each
        // must still arrive whole, once, in order.
        let (tx, rx) = mpsc::channel::<StatsEvent>(64);
        let cancel = CancellationToken::new();
        let (writer, mut reader) = duplex(16);
        let cfg = enabled_config(60_000, 64);
        let handle = tokio::spawn(run(StatsInbox { rx }, cancel.clone(), cfg, writer));
        let collector = tokio::spawn(async move {
            let mut out = Vec::new();
            reader.read_to_end(&mut out).await.expect("read_to_end");
            out
        });

        let alloc = EndpointIdAllocator::new();
        for index in 0..20 {
            let id = alloc.alloc();
            let stats = Arc::new(EndpointStats::default());
            stats.store_state(EndpointState::Down);
            tx.send(make_register(id, &format!("ep{index}"), stats))
                .await
                .expect("register");
            tx.send(StatsEvent::Finalize { id })
                .await
                .expect("finalize");
        }
        // Closing the channel ends the task without the post-cancel wait.
        drop(tx);
        tokio::time::timeout(Duration::from_secs(3), handle)
            .await
            .expect("stats task did not exit")
            .expect("stats task panicked");
        let out = tokio::time::timeout(Duration::from_secs(3), collector)
            .await
            .expect("collector did not finish")
            .expect("collector panicked");

        let text = std::str::from_utf8(&out).expect("utf8");
        let names: Vec<String> = text
            .lines()
            .map(|line| {
                let value: serde_json::Value = serde_json::from_str(line)
                    .unwrap_or_else(|err| panic!("corrupt line {line:?}: {err}"));
                assert_eq!(value["state"], "down");
                value["endpoint"].as_str().unwrap().to_string()
            })
            .collect();
        let expected: Vec<String> = (0..20).map(|index| format!("ep{index}")).collect();
        assert_eq!(names, expected);
    }
}
