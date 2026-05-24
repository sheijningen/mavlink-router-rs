//! CLI deserialisation.
//!
//! Parses argv via clap into a [`Cli`], then [`Cli::into_cli_config`] turns
//! the raw argv values into a typed [`CliConfig`]. Globals on `CliConfig`
//! are `Option<T>` so the merge step in [`crate::config`] can distinguish
//! "operator omitted `--log-level`" from "operator passed `--log-level info`":
//! the former falls through to the TOML value (if any) and then to the
//! default, the latter overrides both.
//!
//! Endpoint argv strings are parsed by [`EndpointSpec::parse`] — the same
//! body / query / applier machinery the TOML side reaches via
//! [`EndpointSpec::build`]. Neither input layer references the other.

use std::path::PathBuf;

use clap::Parser;

use crate::config::{LogFormat, LogLevel};
use crate::endpoint::spec::EndpointSpec;
use crate::error::Error;

/// Endpoint mini-guide shown by `rmr --help` (clap's `after_long_help`).
/// Documents the spec grammar, the five schemes, and the most-used query
/// keys so the binary is self-documenting at the terminal without a man
/// page or a separate hosted docs site. See the project README for the
/// full design rationale.
const ENDPOINT_GUIDE: &str = "Endpoints:
  ENDPOINT := SCHEME:BODY[#name][?key1=val1&key2=val2&...]

  scheme         body                         use for
  serial:        path:baud (or path,baud)     UART to a flight controller
  udps:          host:port                    UDP server (learns peers)
  udpc:          host:port                    UDP client (latches on reply)
  tcps:          host:port                    TCP server (accepts many)
  tcpc:          host:port                    TCP client (dials + reconnects)

Example:
  rmr tcps:0.0.0.0:5760#vehicle?block_msgid_in=33,100-150

  The `#name` is an optional identifier of the endpoint used for logs and stats.

  Per-endpoint options are passed as a URL-style query string appended
  to the spec: a leading `?`, then `key=val` pairs joined by `&`.

Query keys — any scheme:
  group=NAME              share a learn-set so members never forward
                          to each other (e.g. redundant parallel links)
  sniffer=true            receive all routed traffic, bypassing target
                          match, loop prevention, and out-filters

Query keys — scheme-specific:
  flow_control=rtscts     serial: only — enable RTS/CTS (default none)
  idle_secs=N             udps: only — peer expiry on inactivity
                          (default 60)
  latch_idle_secs=N       udpc: only — revert to configured host after
                          N s of silence from the latched peer
                          (default 30)

Filter query keys — any scheme:
  Same `?key=val` syntax as the keys above. Values are comma-separated
  decimal integers and inclusive `lo-hi` ranges:
    allow_msgid_in     / block_msgid_in       ingress, by msgid
    allow_msgid_out    / block_msgid_out      egress,  by msgid
    allow_src_sys_in   / block_src_sys_in     ingress, by source sysid
    allow_src_sys_out  / block_src_sys_out    egress,  by source sysid
    allow_src_comp_in  / block_src_comp_in    ingress, by source compid
    allow_src_comp_out / block_src_comp_out   egress,  by source compid

  `allow_*` is a whitelist (empty = allow all; non-empty = only these
  pass). `block_*` is a blacklist. Blacklist wins on overlap. `*_in`
  is applied on incoming traffic at the source endpoint; `*_out`
  is applied on outgoing traffic per destination endpoint.

Worked examples, design doc, and full TOML config reference:
  https://github.com/sheijningen/mavlink-router-rs
";

/// Parsed argv. Every global is `Option<T>` because clap has no
/// `default_value_t` set on them — operator-omitted flags arrive as `None`
/// and the merge step in [`crate::config::Config::merge`] picks the
/// fall-through value (TOML, then default).
#[derive(Parser, Debug)]
#[command(
    name = "rmr",
    version = env!("RMR_VERSION_STRING"),
    about = "Rust MAVLink Router",
    after_long_help = ENDPOINT_GUIDE,
)]
pub struct Cli {
    /// TOML config file with endpoints and globals
    #[arg(short, long, value_name = "FILE")]
    pub config: Option<PathBuf>,

    /// Log verbosity (default: info)
    #[arg(long, value_enum, value_name = "LEVEL")]
    pub log_level: Option<LogLevel>,

    /// Log format (default: text)
    #[arg(long, value_enum, value_name = "FMT")]
    pub log_format: Option<LogFormat>,

    /// Emit periodic per-endpoint stats (JSON-Lines on stdout)
    #[arg(long)]
    pub stats: bool,

    /// Stats output interval in seconds (default: 5)
    #[arg(long, value_name = "N")]
    pub stats_interval_secs: Option<u64>,

    /// Duplicate suppression window in milliseconds (default: 0, off)
    #[arg(long, value_name = "N")]
    pub dedup_ms: Option<u64>,

    /// Suppress the INFO dump of the resolved config at startup
    #[arg(long)]
    pub skip_config_log: bool,

    /// Print the fully-resolved config (after CLI+TOML merge) to stdout and
    /// exit without binding any sockets. For verifying a setup before
    /// running it.
    #[arg(long)]
    pub dry_run: bool,

    /// One or more endpoint specifications (scheme:body[#name][?key=val&...])
    #[arg(value_name = "ENDPOINT")]
    pub endpoints: Vec<String>,
}

/// CLI-side input to the [`crate::config::Config::merge`] step. Mirrors the
/// shape of [`crate::parsers::toml::TomlConfig`] so both sources funnel into the
/// same merge surface. `Option<T>` globals mean "operator did not pass this
/// flag"; the merge step replaces `None` with the TOML value (if any) and
/// finally with the documented default.
///
/// `Default` produces a `CliConfig` with every global `None` and no endpoints
/// — useful for test fixtures and the "operator only passed `--config <FILE>`"
/// branch. An empty endpoint Vec is accepted at parse time; the "must have at
/// least one endpoint" check runs post-merge in [`crate::main`] so a TOML
/// file that defines no endpoints fails the same way as `rmr` with no args.
#[derive(Debug, Clone, Default)]
pub struct CliConfig {
    pub log_level: Option<LogLevel>,
    pub log_format: Option<LogFormat>,
    pub stats: Option<bool>,
    pub stats_interval_secs: Option<u64>,
    pub dedup_ms: Option<u64>,
    pub skip_config_log: Option<bool>,
    pub endpoints: Vec<EndpointSpec>,
}

impl Cli {
    /// Convert argv into a typed [`CliConfig`]. The `--config <FILE>` path is
    /// **not** read here — the caller (typically [`crate::main`]) is
    /// responsible for loading the TOML file and passing the resulting
    /// [`crate::parsers::toml::TomlConfig`] to
    /// [`crate::config::Config::merge`] alongside this `CliConfig`. Keeping
    /// the file-load out of this method preserves the
    /// CLI-doesn't-reference-TOML invariant.
    pub fn into_cli_config(self) -> Result<CliConfig, Error> {
        let endpoints = parse_specs(&self.endpoints)?;
        // `--stats` is a clap flag (no value), so absence is `false`, not
        // `None`. To preserve "unset means defer to TOML / default", treat
        // operator-set-to-true as Some(true) and absent as None. Same for
        // `--skip-config-log`: absent stays `None`, present means "force off".
        let stats = if self.stats { Some(true) } else { None };
        let skip_config_log = if self.skip_config_log {
            Some(true)
        } else {
            None
        };
        Ok(CliConfig {
            log_level: self.log_level,
            log_format: self.log_format,
            stats,
            stats_interval_secs: self.stats_interval_secs,
            dedup_ms: self.dedup_ms,
            skip_config_log,
            endpoints,
        })
    }
}

/// Turn a slice of CLI-style endpoint strings into typed [`EndpointSpec`]s
/// via [`EndpointSpec::parse`] — the same parser the TOML side eventually
/// reaches via [`EndpointSpec::build`]. Spec errors are wrapped in
/// [`Error::SpecInArg`] so the operator sees which positional argument
/// failed. Duplicate-name detection is left to
/// [`crate::config::Config::validate`] after the merge runs.
pub fn parse_specs(raw: &[String]) -> Result<Vec<EndpointSpec>, Error> {
    let mut specs = Vec::with_capacity(raw.len());
    for (index, text) in raw.iter().enumerate() {
        let spec = EndpointSpec::parse(text).map_err(|source| Error::SpecInArg {
            index,
            arg: text.clone(),
            source,
        })?;
        specs.push(spec);
    }
    Ok(specs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal() {
        let cli = Cli::try_parse_from(["rmr", "udps:0.0.0.0:14550"]).unwrap();
        assert_eq!(cli.endpoints, vec!["udps:0.0.0.0:14550".to_string()]);
        assert!(cli.log_level.is_none());
        assert!(cli.log_format.is_none());
        assert!(!cli.stats);
        assert!(cli.stats_interval_secs.is_none());
        assert!(cli.dedup_ms.is_none());
        assert!(cli.config.is_none());
    }

    #[test]
    fn parse_log_options() {
        let cli = Cli::try_parse_from([
            "rmr",
            "--log-level",
            "debug",
            "--log-format",
            "json",
            "udps:0.0.0.0:1",
        ])
        .unwrap();
        assert_eq!(cli.log_level, Some(LogLevel::Debug));
        assert_eq!(cli.log_format, Some(LogFormat::Json));
    }

    #[test]
    fn parse_stats_flags() {
        let cli = Cli::try_parse_from([
            "rmr",
            "--stats",
            "--stats-interval-secs",
            "10",
            "--dedup-ms",
            "250",
            "udps:0.0.0.0:1",
        ])
        .unwrap();
        assert!(cli.stats);
        assert_eq!(cli.stats_interval_secs, Some(10));
        assert_eq!(cli.dedup_ms, Some(250));
    }

    #[test]
    fn parse_multiple_endpoints() {
        let cli = Cli::try_parse_from([
            "rmr",
            "udps:0.0.0.0:1",
            "tcpc:host:2",
            "serial:/dev/ttyUSB0:115200",
        ])
        .unwrap();
        assert_eq!(cli.endpoints.len(), 3);
    }

    #[test]
    fn no_endpoints_no_config_parses_ok() {
        // Clap accepts zero positional endpoints without `--config`; the
        // "at least one endpoint" gate runs post-merge in `main` so the
        // empty-CLI and empty-TOML paths share one error surface.
        let cli = Cli::try_parse_from(["rmr"]).unwrap();
        assert!(cli.endpoints.is_empty());
        assert!(cli.config.is_none());
    }

    #[test]
    fn config_only_ok() {
        let cli = Cli::try_parse_from(["rmr", "--config", "rmr.toml"]).unwrap();
        assert!(cli.endpoints.is_empty());
        assert_eq!(
            cli.config
                .as_ref()
                .map(|path| path.to_string_lossy().into_owned()),
            Some("rmr.toml".to_string())
        );
    }

    #[test]
    fn config_file_and_cli_endpoints_both_ok() {
        // `--config <FILE>` + CLI endpoints is a supported combination:
        // the TOML file provides a base config, CLI overrides patch it via
        // `Config::merge`. Pin that argv parsing accepts the combination.
        let cli =
            Cli::try_parse_from(["rmr", "--config", "rmr.toml", "udps:0.0.0.0:14550"]).unwrap();
        assert_eq!(cli.endpoints.len(), 1);
        assert!(cli.config.is_some());
    }

    #[test]
    fn into_cli_config_builds_typed_config() {
        let cli = Cli::try_parse_from([
            "rmr",
            "--stats",
            "--dedup-ms",
            "300",
            "udps:0.0.0.0:14550#bus",
            "tcpc:gcs.local:5760#vehicle",
        ])
        .unwrap();
        let cfg = cli.into_cli_config().expect("conversion must succeed");
        assert_eq!(cfg.stats, Some(true));
        assert_eq!(cfg.dedup_ms, Some(300));
        assert_eq!(cfg.endpoints.len(), 2);
        assert_eq!(cfg.endpoints[0].name, "bus");
        assert_eq!(cfg.endpoints[1].name, "vehicle");
    }

    #[test]
    fn into_cli_config_leaves_unset_globals_as_none() {
        // Operator passed neither --log-level nor --log-format: both should
        // arrive at the merge step as `None` so the TOML value / default
        // wins. `--stats` is absent too, so it stays `None` (not
        // `Some(false)`), preserving "operator didn't choose" semantics.
        let cli = Cli::try_parse_from(["rmr", "udps:0.0.0.0:1#a"]).unwrap();
        let cfg = cli.into_cli_config().expect("conversion must succeed");
        assert!(cfg.log_level.is_none());
        assert!(cfg.log_format.is_none());
        assert!(cfg.stats.is_none());
        assert!(cfg.stats_interval_secs.is_none());
        assert!(cfg.dedup_ms.is_none());
        assert!(cfg.skip_config_log.is_none());
    }

    #[test]
    fn dry_run_flag_parses() {
        // --dry-run is a binary-only flag (handled by main, not merged into
        // CliConfig), so the test stays on the `Cli` struct.
        let cli = Cli::try_parse_from(["rmr", "--dry-run", "udps:0.0.0.0:1#a"]).unwrap();
        assert!(cli.dry_run);
        let absent = Cli::try_parse_from(["rmr", "udps:0.0.0.0:1#a"]).unwrap();
        assert!(!absent.dry_run);
    }

    #[test]
    fn skip_config_log_flag_present_becomes_some_true() {
        // `--skip-config-log` is a bool flag that, when present, suppresses
        // the merged-config INFO event. Absence stays `None` so the TOML
        // value (if any) can win.
        let cli = Cli::try_parse_from(["rmr", "--skip-config-log", "udps:0.0.0.0:1#a"]).unwrap();
        let cfg = cli.into_cli_config().expect("conversion must succeed");
        assert_eq!(cfg.skip_config_log, Some(true));
    }

    #[test]
    fn into_cli_config_propagates_spec_error() {
        let cli = Cli::try_parse_from(["rmr", "bogus-not-an-endpoint"]).unwrap();
        match cli.into_cli_config() {
            Err(Error::SpecInArg { index, arg, .. }) => {
                assert_eq!(index, 0);
                assert_eq!(arg, "bogus-not-an-endpoint");
            }
            other => panic!("expected SpecInArg, got {other:?}"),
        }
    }

    #[test]
    fn parse_specs_ok() {
        let raw = vec![
            "udps:0.0.0.0:14550#bus".to_string(),
            "tcpc:gcs.local:5760#vehicle".to_string(),
        ];
        let specs = parse_specs(&raw).unwrap();
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].name, "bus");
        assert_eq!(specs[1].name, "vehicle");
    }

    #[test]
    fn parse_specs_propagates_spec_error() {
        let raw = vec!["bogus-not-an-endpoint".to_string()];
        match parse_specs(&raw) {
            Err(Error::SpecInArg { index, arg, .. }) => {
                assert_eq!(index, 0);
                assert_eq!(arg, "bogus-not-an-endpoint");
            }
            other => panic!("expected SpecInArg, got {other:?}"),
        }
    }

    #[test]
    fn parse_specs_reports_offending_arg_position() {
        let raw = vec![
            "udps:0.0.0.0:14550".to_string(),
            "tcpc:bad-no-port".to_string(),
            "serial:/dev/ttyUSB0:115200".to_string(),
        ];
        match parse_specs(&raw) {
            Err(Error::SpecInArg { index, arg, .. }) => {
                assert_eq!(index, 1);
                assert_eq!(arg, "tcpc:bad-no-port");
            }
            other => panic!("expected SpecInArg at index 1, got {other:?}"),
        }
    }

    #[test]
    fn spec_in_arg_message_includes_position_and_raw_arg() {
        let raw = vec!["tcpc:bad-no-port".to_string()];
        let err = parse_specs(&raw).expect_err("must fail");
        let rendered = err.to_string();
        assert!(rendered.contains("#1"), "missing position: {rendered}");
        assert!(
            rendered.contains("'tcpc:bad-no-port'"),
            "missing raw arg: {rendered}"
        );
    }

    #[test]
    fn version_string_starts_with_cargo_pkg_version() {
        // build.rs emits RMR_VERSION_STRING — either bare `<X.Y.Z>` on a
        // tagged-commit build, or `<X.Y.Z> (sha …, built …)` otherwise.
        // Both shapes must start with the Cargo.toml version so callers
        // can `cut -d' ' -f1` to recover the semver.
        let version_string = env!("RMR_VERSION_STRING");
        let pkg_version = env!("CARGO_PKG_VERSION");
        assert!(
            version_string.starts_with(pkg_version),
            "RMR_VERSION_STRING={version_string:?} does not start with CARGO_PKG_VERSION={pkg_version:?}",
        );
    }

    #[test]
    fn version_flag_exposes_emitted_string() {
        use clap::CommandFactory;
        let cmd = Cli::command();
        let rendered = cmd.get_version().expect("version present");
        assert_eq!(rendered, env!("RMR_VERSION_STRING"));
    }

    #[test]
    fn long_help_contains_endpoint_guide() {
        use clap::CommandFactory;

        let mut cmd = Cli::command();
        let mut buf = Vec::new();
        cmd.write_long_help(&mut buf).expect("write_long_help");
        let help = String::from_utf8(buf).expect("utf8 help");
        assert!(help.contains("ENDPOINT := SCHEME:BODY"), "grammar missing");
        for scheme in ["serial:", "udps:", "udpc:", "tcps:", "tcpc:"] {
            assert!(help.contains(scheme), "{scheme} missing from help");
        }
        for key in [
            "group=",
            "sniffer=",
            "flow_control=",
            "idle_secs=",
            "latch_idle_secs=",
            "allow_msgid_in",
            "block_msgid_in",
        ] {
            assert!(help.contains(key), "query key {key} missing");
        }
        assert!(
            help.contains("github.com/sheijningen/mavlink-router-rs"),
            "repo link missing"
        );
    }
}
