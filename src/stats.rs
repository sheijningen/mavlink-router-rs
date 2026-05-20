//! Dedicated stats task: registry mirror, interval timer, and JSON-Lines
//! stdout sink.
//!
//! Owns the [`EndpointId → (name, Arc<EndpointStats>)`] registry mirror fed
//! by the router over a [`StatsEvent`] channel. On each
//! `stats_interval_secs` tick it walks the registry, builds one
//! [`StatsLine`] per endpoint, and writes it as a single JSON object
//! followed by `\n` to the supplied writer (production: `tokio::io::stdout`).
//!
//! Internally the task keeps a bounded `VecDeque` so a slow or broken stdout
//! consumer cannot stall registry maintenance. Drop-oldest on overflow per
//! CLAUDE.md; `stats_dropped` accumulates and produces at most one WARN per
//! interval. `BrokenPipe` on the writer logs once at WARN and suppresses
//! subsequent writes — the registry still ticks so the cancel-path final
//! synthetic lines aren't piling forever.
//!
//! `Finalize` (from `PeerRemoved` and the router shutdown sweep) emits a
//! final synthetic line taking a **bypass path**: it evicts the oldest
//! regular line to make room when the queue is full, so the authoritative
//! terminal state is never silently lost behind a flood of interval lines.
//! A synthetic line displaced by another synthetic line (bounded by
//! registered-endpoint count, so vanishingly rare in practice) gets its own
//! dedicated WARN naming the lost endpoint.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

use crate::endpoint::EndpointId;
use crate::endpoint::stats::{EndpointState, EndpointStats};

/// CLAUDE.md "Defaults" → `stats_queue_lines` (Stats-task bounded queue
/// depth; drop-oldest on slow/dead stdout consumer).
pub const DEFAULT_STATS_QUEUE_LINES: usize = 256;

/// One lifecycle message from the router to the stats task. Forwarded
/// fire-and-forget per CLAUDE.md — the router never awaits a stats-task
/// acknowledgement.
#[derive(Debug)]
pub enum StatsEvent {
    Register {
        id: EndpointId,
        name: String,
        stats: Arc<EndpointStats>,
    },
    Finalize {
        id: EndpointId,
    },
}

/// One row of the stats task's registry mirror — the name + stats handle
/// the task snapshots on every interval tick.
#[derive(Debug)]
struct RegisteredEndpoint {
    name: String,
    stats: Arc<EndpointStats>,
}

/// Per-endpoint stats emitted as one JSON-Lines object on stdout. Schema is
/// pinned by CLAUDE.md's "Stats schema" example; every field corresponds 1:1
/// to a counter on [`EndpointStats`], plus the timestamp and endpoint name.
#[derive(Debug, Serialize)]
struct StatsLine {
    ts: String,
    endpoint: String,
    state: &'static str,
    rx_frames: u64,
    tx_frames: u64,
    rx_bytes: u64,
    tx_bytes: u64,
    dropped_tx: u64,
    crc_errors: u64,
    resync_bytes: u64,
    rx_lost_est: u64,
    in_filter_drops: u64,
    out_filter_drops: u64,
    dedup_drops: u64,
    learn_entries: u64,
}

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

/// Stable lowercase label for the stats JSON's `state` field — pinned by
/// CLAUDE.md's "Stats `state` field" decision (`connected | reconnecting |
/// idle | down`).
fn state_label(state: EndpointState) -> &'static str {
    match state {
        EndpointState::Reconnecting => "reconnecting",
        EndpointState::Connected => "connected",
        EndpointState::Idle => "idle",
        EndpointState::Down => "down",
        EndpointState::Unknown => "unknown",
    }
}

/// RFC 3339 UTC timestamp truncated to whole seconds — matches the
/// `"2026-05-15T19:00:00Z"` example in CLAUDE.md.
fn rfc3339_now() -> String {
    let now = OffsetDateTime::now_utc()
        .replace_nanosecond(0)
        .expect("0 is a valid nanosecond");
    now.format(&Rfc3339)
        .expect("Rfc3339 format succeeds for any OffsetDateTime")
}

fn build_line(name: &str, stats: &EndpointStats, ts: String) -> StatsLine {
    StatsLine {
        ts,
        endpoint: name.to_string(),
        state: state_label(stats.load_state()),
        rx_frames: stats.rx_frames.load(Ordering::Relaxed),
        tx_frames: stats.tx_frames.load(Ordering::Relaxed),
        rx_bytes: stats.rx_bytes.load(Ordering::Relaxed),
        tx_bytes: stats.tx_bytes.load(Ordering::Relaxed),
        dropped_tx: stats.dropped_tx.load(Ordering::Relaxed),
        crc_errors: stats.crc_errors.load(Ordering::Relaxed),
        resync_bytes: stats.resync_bytes.load(Ordering::Relaxed),
        rx_lost_est: stats.rx_lost_est.load(Ordering::Relaxed),
        in_filter_drops: stats.in_filter_drops.load(Ordering::Relaxed),
        out_filter_drops: stats.out_filter_drops.load(Ordering::Relaxed),
        dedup_drops: stats.dedup_drops.load(Ordering::Relaxed),
        learn_entries: stats.learn_entries.load(Ordering::Relaxed),
    }
}

/// Run the stats task until the cancellation token fires. Maintains the
/// registry mirror in lockstep with router-emitted `Register`/`Finalize`
/// events, and — when `cfg.enabled` is true — emits one JSON line per
/// registered endpoint per interval to `writer`. The writer is parameterised
/// so production wires `tokio::io::stdout()` while tests pass an in-memory
/// duplex stream.
pub async fn run<W>(
    mut event_rx: mpsc::Receiver<StatsEvent>,
    cancel: CancellationToken,
    cfg: StatsRunConfig,
    mut writer: W,
) where
    W: AsyncWrite + Send + Unpin,
{
    let mut registry: HashMap<EndpointId, RegisteredEndpoint> = HashMap::new();
    let mut queue: VecDeque<QueueEntry> = VecDeque::new();
    let mut total_dropped: u64 = 0;
    let mut last_warned_dropped: u64 = 0;
    let mut broken_pipe_warned = false;

    // Delay the first tick by `interval` so we don't emit before any
    // endpoint has had a chance to register; subsequent ticks fire on the
    // regular schedule. `Skip` keeps us from catching up if the loop ever
    // falls behind (a long stdout stall would otherwise produce a burst of
    // back-to-back lines on recovery).
    let mut interval = cfg.enabled.then(|| {
        let start = tokio::time::Instant::now() + cfg.interval;
        let mut timer = tokio::time::interval_at(start, cfg.interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        timer
    });

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            tick = interval_tick(interval.as_mut()) => {
                let _: () = tick;
                emit_interval_lines(&registry, &mut queue, cfg.queue_capacity, &mut total_dropped);
                warn_on_new_drops(total_dropped, &mut last_warned_dropped);
                drain_queue(&mut queue, &mut writer, &mut broken_pipe_warned).await;
            }
            received = event_rx.recv() => match received {
                Some(event) => {
                    handle_event(&mut registry, &mut queue, cfg.queue_capacity, &mut total_dropped, cfg.enabled, event);
                    drain_queue(&mut queue, &mut writer, &mut broken_pipe_warned).await;
                }
                None => {
                    debug!("stats_event channel closed; stats task exiting");
                    break;
                }
            },
        }
    }

    // After cancel: keep processing events until the channel closes
    // (every sender — router + sub-endpoint tasks — drops their handle
    // when they finish their own drain) OR we burn the producer's slice
    // of the per-task drain budget. Using `recv().await` (not `try_recv`)
    // is load-bearing: the router emits its shutdown-sweep Finalizes
    // AFTER its own cancel observation, so a tight try_recv loop here
    // would race those emissions and exit before they arrived, leaving
    // every top-level endpoint with an interval-line as its last
    // observable state instead of the authoritative Down.
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
    drain_queue(&mut queue, &mut writer, &mut broken_pipe_warned).await;
    let _ = writer.flush().await;
}

/// Post-cancel slice of the per-task 2s drain budget the harness honours
/// (see [`crate::shutdown::PER_TASK_DRAIN`]). 1500 ms for processing
/// in-flight Register/Finalize events leaves ~500 ms of slack for the
/// final `drain_queue` writes to land before the harness aborts.
///
/// MUST stay strictly smaller than [`crate::shutdown::PER_TASK_DRAIN`] —
/// if these two equal each other, the harness aborts before the final
/// `drain_queue` writes get any wall-clock slack and authoritative
/// synthetic lines vanish.
const POST_CANCEL_DRAIN: Duration = Duration::from_millis(1500);

/// Queued line tagged with its origin so the bypass path can preferentially
/// evict regular interval lines before touching a synthetic Finalize line.
struct QueueEntry {
    line: StatsLine,
    synthetic: bool,
}

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

fn handle_event(
    registry: &mut HashMap<EndpointId, RegisteredEndpoint>,
    queue: &mut VecDeque<QueueEntry>,
    queue_capacity: usize,
    total_dropped: &mut u64,
    emit_lines: bool,
    event: StatsEvent,
) {
    match event {
        StatsEvent::Register { id, name, stats } => {
            trace!(%id, %name, "stats: register");
            registry.insert(id, RegisteredEndpoint { name, stats });
        }
        StatsEvent::Finalize { id } => {
            trace!(%id, "stats: finalize");
            let Some(entry) = registry.remove(&id) else {
                return;
            };
            // When stats output is disabled the registry mirror still
            // serves its role (router fire-and-forget needs a consumer),
            // but no synthetic line should reach stdout. Skipping the
            // enqueue here also means `--stats=false` never grows the
            // queue or burns the BrokenPipe path.
            if !emit_lines {
                return;
            }
            let line = build_line(&entry.name, &entry.stats, rfc3339_now());
            enqueue_synthetic(queue, queue_capacity, line, total_dropped);
        }
    }
}

fn emit_interval_lines(
    registry: &HashMap<EndpointId, RegisteredEndpoint>,
    queue: &mut VecDeque<QueueEntry>,
    queue_capacity: usize,
    total_dropped: &mut u64,
) {
    if registry.is_empty() {
        return;
    }
    let ts = rfc3339_now();
    for entry in registry.values() {
        let line = build_line(&entry.name, &entry.stats, ts.clone());
        enqueue_regular(queue, queue_capacity, line, total_dropped);
    }
}

/// Regular interval-tick line: drop-oldest on overflow, increment the
/// rate-limited `stats_dropped` counter.
fn enqueue_regular(
    queue: &mut VecDeque<QueueEntry>,
    queue_capacity: usize,
    line: StatsLine,
    total_dropped: &mut u64,
) {
    if queue_capacity == 0 {
        *total_dropped = total_dropped.saturating_add(1);
        return;
    }
    if queue.len() >= queue_capacity {
        queue.pop_front();
        *total_dropped = total_dropped.saturating_add(1);
    }
    queue.push_back(QueueEntry {
        line,
        synthetic: false,
    });
}

/// Finalize bypass per CLAUDE.md "Stats sink architecture": authoritative
/// end-state lines never silently disappear. Evict the oldest regular
/// interval line to make room; if every queued entry is itself a synthetic
/// line, fall back to dropping the oldest synthetic and emit a dedicated
/// WARN naming the lost endpoint (not rate-limited — this is supposed to be
/// vanishingly rare).
fn enqueue_synthetic(
    queue: &mut VecDeque<QueueEntry>,
    queue_capacity: usize,
    line: StatsLine,
    total_dropped: &mut u64,
) {
    if queue_capacity == 0 {
        warn!(
            endpoint = %line.endpoint,
            "stats Finalize line dropped: queue capacity is zero"
        );
        *total_dropped = total_dropped.saturating_add(1);
        return;
    }
    if queue.len() >= queue_capacity {
        if let Some(pos) = queue.iter().position(|entry| !entry.synthetic) {
            queue.remove(pos);
            *total_dropped = total_dropped.saturating_add(1);
        } else if let Some(evicted) = queue.pop_front() {
            warn!(
                evicted_endpoint = %evicted.line.endpoint,
                replacing_endpoint = %line.endpoint,
                "stats Finalize line evicted by another Finalize line; terminal state lost"
            );
            *total_dropped = total_dropped.saturating_add(1);
        }
    }
    queue.push_back(QueueEntry {
        line,
        synthetic: true,
    });
}

fn warn_on_new_drops(total_dropped: u64, last_warned: &mut u64) {
    if total_dropped > *last_warned {
        let delta = total_dropped - *last_warned;
        warn!(
            stats_dropped = total_dropped,
            delta_this_interval = delta,
            "stats queue at capacity; oldest line(s) dropped"
        );
        *last_warned = total_dropped;
    }
}

async fn drain_queue<W>(
    queue: &mut VecDeque<QueueEntry>,
    writer: &mut W,
    broken_pipe_warned: &mut bool,
) where
    W: AsyncWrite + Unpin,
{
    while let Some(entry) = queue.pop_front() {
        if *broken_pipe_warned {
            // Stdout is gone; discard quietly. We keep ticking so the post-
            // cancel drain doesn't grow the queue forever, and the WARN was
            // already emitted once.
            continue;
        }
        let line = &entry.line;
        let mut json = match serde_json::to_vec(line) {
            Ok(bytes) => bytes,
            Err(err) => {
                debug!(error = %err, "stats: serialize failed; dropping line");
                continue;
            }
        };
        json.push(b'\n');
        match writer.write_all(&json).await {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => {
                warn!("stats stdout broken pipe; suppressing further stats output");
                *broken_pipe_warned = true;
            }
            Err(err) => {
                debug!(error = %err, "stats: write error");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::EndpointIdAllocator;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, duplex};

    fn make_register(id: EndpointId, name: &str, stats: Arc<EndpointStats>) -> StatsEvent {
        StatsEvent::Register {
            id,
            name: name.to_string(),
            stats,
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

    #[test]
    fn build_line_captures_every_counter_documented_in_claude_md() {
        let stats = EndpointStats::default();
        stats.rx_frames.fetch_add(7, Ordering::Relaxed);
        stats.tx_frames.fetch_add(8, Ordering::Relaxed);
        stats.rx_bytes.fetch_add(9, Ordering::Relaxed);
        stats.tx_bytes.fetch_add(10, Ordering::Relaxed);
        stats.dropped_tx.fetch_add(11, Ordering::Relaxed);
        stats.crc_errors.fetch_add(12, Ordering::Relaxed);
        stats.resync_bytes.fetch_add(13, Ordering::Relaxed);
        stats.rx_lost_est.fetch_add(14, Ordering::Relaxed);
        stats.in_filter_drops.fetch_add(15, Ordering::Relaxed);
        stats.out_filter_drops.fetch_add(16, Ordering::Relaxed);
        stats.dedup_drops.fetch_add(17, Ordering::Relaxed);
        stats.learn_entries.store(18, Ordering::Relaxed);
        stats.store_state(EndpointState::Connected);

        let line = build_line("vehicle", &stats, "2026-05-15T19:00:00Z".to_string());
        let json: serde_json::Value =
            serde_json::from_slice(&serde_json::to_vec(&line).unwrap()).unwrap();
        assert_eq!(json["ts"], "2026-05-15T19:00:00Z");
        assert_eq!(json["endpoint"], "vehicle");
        assert_eq!(json["state"], "connected");
        assert_eq!(json["rx_frames"], 7);
        assert_eq!(json["tx_frames"], 8);
        assert_eq!(json["rx_bytes"], 9);
        assert_eq!(json["tx_bytes"], 10);
        assert_eq!(json["dropped_tx"], 11);
        assert_eq!(json["crc_errors"], 12);
        assert_eq!(json["resync_bytes"], 13);
        assert_eq!(json["rx_lost_est"], 14);
        assert_eq!(json["in_filter_drops"], 15);
        assert_eq!(json["out_filter_drops"], 16);
        assert_eq!(json["dedup_drops"], 17);
        assert_eq!(json["learn_entries"], 18);
    }

    #[test]
    fn state_label_covers_every_variant_with_claude_md_strings() {
        assert_eq!(state_label(EndpointState::Reconnecting), "reconnecting");
        assert_eq!(state_label(EndpointState::Connected), "connected");
        assert_eq!(state_label(EndpointState::Idle), "idle");
        assert_eq!(state_label(EndpointState::Down), "down");
        assert_eq!(state_label(EndpointState::Unknown), "unknown");
    }

    #[test]
    fn rfc3339_now_has_no_subseconds() {
        // CLAUDE.md example has Z directly after seconds — must not include
        // a fractional component.
        let ts = rfc3339_now();
        assert!(ts.ends_with('Z'), "ts = {ts}");
        assert!(!ts.contains('.'), "ts = {ts}");
    }

    fn make_line(endpoint: &str) -> StatsLine {
        build_line(endpoint, &EndpointStats::default(), "t".to_string())
    }

    fn entry_endpoints(queue: &VecDeque<QueueEntry>) -> Vec<&str> {
        queue
            .iter()
            .map(|entry| entry.line.endpoint.as_str())
            .collect()
    }

    #[test]
    fn regular_enqueue_drops_oldest_when_capacity_exceeded() {
        let mut queue = VecDeque::new();
        let mut dropped = 0u64;
        enqueue_regular(&mut queue, 2, make_line("a"), &mut dropped);
        enqueue_regular(&mut queue, 2, make_line("b"), &mut dropped);
        enqueue_regular(&mut queue, 2, make_line("c"), &mut dropped);
        assert_eq!(dropped, 1);
        assert_eq!(entry_endpoints(&queue), vec!["b", "c"]);
    }

    #[test]
    fn regular_enqueue_zero_capacity_drops_everything() {
        let mut queue = VecDeque::new();
        let mut dropped = 0u64;
        enqueue_regular(&mut queue, 0, make_line("a"), &mut dropped);
        assert_eq!(dropped, 1);
        assert!(queue.is_empty());
    }

    #[test]
    fn synthetic_enqueue_evicts_oldest_regular_first() {
        // Bypass path: a synthetic Finalize line entering a full queue must
        // displace an interval line, not another synthetic line.
        let mut queue = VecDeque::new();
        let mut dropped = 0u64;
        enqueue_regular(&mut queue, 3, make_line("reg-a"), &mut dropped);
        enqueue_synthetic(&mut queue, 3, make_line("syn-x"), &mut dropped);
        enqueue_regular(&mut queue, 3, make_line("reg-b"), &mut dropped);
        // Queue is now [reg-a (R), syn-x (S), reg-b (R)] at cap 3.
        enqueue_synthetic(&mut queue, 3, make_line("syn-y"), &mut dropped);
        // syn-y enters via bypass → reg-a (oldest regular) is evicted, syn-x stays.
        assert_eq!(dropped, 1);
        assert_eq!(entry_endpoints(&queue), vec!["syn-x", "reg-b", "syn-y"]);
    }

    #[test]
    fn synthetic_enqueue_falls_back_to_oldest_synthetic_when_no_regular() {
        let mut queue = VecDeque::new();
        let mut dropped = 0u64;
        enqueue_synthetic(&mut queue, 2, make_line("syn-a"), &mut dropped);
        enqueue_synthetic(&mut queue, 2, make_line("syn-b"), &mut dropped);
        // Queue is now [syn-a, syn-b] at cap 2 — every entry is synthetic.
        enqueue_synthetic(&mut queue, 2, make_line("syn-c"), &mut dropped);
        // syn-c displaces syn-a (oldest synthetic) and the lost endpoint is
        // named in a dedicated WARN by `enqueue_synthetic`.
        assert_eq!(dropped, 1);
        assert_eq!(entry_endpoints(&queue), vec!["syn-b", "syn-c"]);
    }

    #[test]
    fn synthetic_enqueue_zero_capacity_drops_and_warns() {
        let mut queue = VecDeque::new();
        let mut dropped = 0u64;
        enqueue_synthetic(&mut queue, 0, make_line("syn-a"), &mut dropped);
        assert_eq!(dropped, 1);
        assert!(queue.is_empty());
    }

    #[test]
    fn warn_on_new_drops_only_fires_when_total_advances() {
        let mut last = 0u64;
        warn_on_new_drops(0, &mut last);
        assert_eq!(last, 0);
        warn_on_new_drops(3, &mut last);
        assert_eq!(last, 3);
        // Same total → no new WARN, `last` stays put.
        warn_on_new_drops(3, &mut last);
        assert_eq!(last, 3);
        warn_on_new_drops(5, &mut last);
        assert_eq!(last, 5);
    }

    #[tokio::test]
    async fn task_exits_on_cancel_with_sender_alive_disabled() {
        // With the sender held alive, the post-cancel drain awaits more
        // events for up to `POST_CANCEL_DRAIN` (1.5s) before timing out
        // and exiting. The 3s join budget covers that drain plus slack.
        let (tx, rx) = mpsc::channel::<StatsEvent>(4);
        let cancel = CancellationToken::new();
        let (writer, _reader) = duplex(1024);
        let handle = tokio::spawn(run(rx, cancel.clone(), disabled_config(), writer));
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(3), handle)
            .await
            .expect("stats task did not exit on cancel within drain budget")
            .expect("stats task panicked");
        drop(tx);
    }

    #[tokio::test]
    async fn post_cancel_drain_processes_finalize_arriving_after_cancel() {
        // Regression: the prior `try_recv()` drain raced the router's
        // shutdown_sweep — a Finalize emitted AFTER the stats task
        // observed cancel was missed, and the final synthetic line never
        // reached stdout. The bounded `recv().await` drain catches the
        // late event; verify by reading the synthetic line off the
        // duplex writer after cancel.
        let (tx, rx) = mpsc::channel::<StatsEvent>(8);
        let cancel = CancellationToken::new();
        let (writer, mut reader) = duplex(4096);
        let cfg = enabled_config(60_000, 32); // interval long enough not to fire
        let handle = tokio::spawn(run(rx, cancel.clone(), cfg, writer));

        let alloc = EndpointIdAllocator::new();
        let id = alloc.alloc();
        let stats = Arc::new(EndpointStats::default());
        stats.store_state(EndpointState::Connected);
        tx.send(make_register(id, "ep", stats.clone()))
            .await
            .expect("register");

        // Cancel FIRST, then send Finalize — simulating the router's
        // shutdown_sweep which emits Finalize after observing cancel.
        cancel.cancel();
        // Yield so the stats task observes cancel before our send lands.
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
        let handle = tokio::spawn(run(rx, cancel, disabled_config(), writer));
        drop(tx);
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("stats task did not exit when channel closed")
            .expect("stats task panicked");
    }

    #[tokio::test]
    async fn disabled_task_emits_no_lines_on_finalize() {
        // Regression: when `--stats` is off, the registry mirror still runs
        // (the router fire-and-forwards Register/Finalize), but no bytes
        // must reach stdout. Test fixtures that build a `Config` with
        // `stats: false` rely on this so their stdout stays clean.
        let (tx, rx) = mpsc::channel::<StatsEvent>(8);
        let cancel = CancellationToken::new();
        let (writer, mut reader) = duplex(4096);
        let handle = tokio::spawn(run(rx, cancel.clone(), disabled_config(), writer));

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

        // Give the task time to process events; with stats disabled, no
        // lines should land on the duplex reader.
        tokio::time::sleep(Duration::from_millis(20)).await;
        // Drop the sender before cancel so the post-cancel drain's
        // `recv().await` returns `Ok(None)` immediately and the task
        // exits without burning the drain budget.
        drop(tx);
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("stats task did not exit")
            .expect("stats task panicked");

        // Reader side: every byte the task wrote is buffered in the duplex.
        // We close the writer half by dropping it (already done when the
        // task returned), so `read` returns 0 cleanly at EOF.
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
        let handle = tokio::spawn(run(rx, cancel.clone(), cfg, writer));

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

        // Read whatever was written so far. duplex buffer is in-process so
        // bytes are available immediately after the write.
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
        // immediately. Under `start_paused`, the alternative (timeout
        // expiry) would require manual `tokio::time::advance` calls.
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
        let handle = tokio::spawn(run(rx, cancel.clone(), cfg, writer));

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
        let handle = tokio::spawn(run(rx, cancel.clone(), cfg, writer));

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
}
