//! JSON-Lines schema of one emitted stats row and its construction.

use std::sync::atomic::Ordering;

use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::endpoint::stats::{EndpointState, EndpointStats};

/// Per-endpoint stats emitted as one JSON-Lines object on stdout. Parent
/// listeners (`tcps:` / `udps:`) carry no frames, so they emit the
/// counter-less `NonRoutable` shape.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub(super) enum StatsLine {
    Routable {
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
    },
    NonRoutable {
        ts: String,
        endpoint: String,
        state: &'static str,
    },
}

impl StatsLine {
    pub(super) fn endpoint(&self) -> &str {
        match self {
            StatsLine::Routable { endpoint, .. } | StatsLine::NonRoutable { endpoint, .. } => {
                endpoint
            }
        }
    }
}

/// Stable lowercase label for the stats JSON's `state` field.
fn state_label(state: EndpointState) -> &'static str {
    match state {
        EndpointState::Reconnecting => "reconnecting",
        EndpointState::Connected => "connected",
        EndpointState::Idle => "idle",
        EndpointState::Down => "down",
        EndpointState::Unknown => "unknown",
    }
}

/// RFC 3339 UTC timestamp truncated to whole seconds.
pub(super) fn rfc3339_now() -> String {
    let now = OffsetDateTime::now_utc()
        .replace_nanosecond(0)
        .expect("0 is a valid nanosecond");
    now.format(&Rfc3339)
        .expect("Rfc3339 format succeeds for any OffsetDateTime")
}

pub(super) fn build_line(
    name: &str,
    stats: &EndpointStats,
    ts: String,
    routable: bool,
) -> StatsLine {
    let state = state_label(stats.load_state());
    let endpoint = name.to_string();
    if !routable {
        return StatsLine::NonRoutable {
            ts,
            endpoint,
            state,
        };
    }
    StatsLine::Routable {
        ts,
        endpoint,
        state,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_line_serialises_full_counter_schema() {
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

        let line = build_line("vehicle", &stats, "2026-05-15T19:00:00Z".to_string(), true);
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
    fn build_line_for_listener_omits_counter_fields() {
        let stats = EndpointStats::default();
        stats.rx_frames.fetch_add(99, Ordering::Relaxed);
        stats.store_state(EndpointState::Connected);

        let line = build_line("input", &stats, "2026-05-15T19:00:00Z".to_string(), false);
        let value: serde_json::Value =
            serde_json::from_slice(&serde_json::to_vec(&line).unwrap()).unwrap();
        let object = value
            .as_object()
            .expect("NonRoutable line serializes as a JSON object");
        assert_eq!(object.len(), 3);
        assert_eq!(object["ts"], "2026-05-15T19:00:00Z");
        assert_eq!(object["endpoint"], "input");
        assert_eq!(object["state"], "connected");
        for counter in [
            "rx_frames",
            "tx_frames",
            "rx_bytes",
            "tx_bytes",
            "dropped_tx",
            "crc_errors",
            "resync_bytes",
            "rx_lost_est",
            "in_filter_drops",
            "out_filter_drops",
            "dedup_drops",
            "learn_entries",
        ] {
            assert!(
                !object.contains_key(counter),
                "listener line must not carry `{counter}`"
            );
        }
    }

    #[test]
    fn state_label_covers_every_variant() {
        assert_eq!(state_label(EndpointState::Reconnecting), "reconnecting");
        assert_eq!(state_label(EndpointState::Connected), "connected");
        assert_eq!(state_label(EndpointState::Idle), "idle");
        assert_eq!(state_label(EndpointState::Down), "down");
        assert_eq!(state_label(EndpointState::Unknown), "unknown");
    }

    #[test]
    fn rfc3339_now_has_no_subseconds() {
        let ts = rfc3339_now();
        assert!(ts.ends_with('Z'), "ts = {ts}");
        assert!(!ts.contains('.'), "ts = {ts}");
    }
}
