//! Phase 7 tests:
//!   - every example under `examples/simple/` and `examples/advanced/`
//!     is a runnable TOML config whose trailing `# rmr ...` block
//!     produces the same merged [`rmr::config::Config`] as the TOML
//!     body itself ("each example TOML's trailing `# rmr ...` CLI
//!     block parses identically to the TOML it accompanies").
//!   - `rmr --help` renders the endpoint mini-guide ("`rmr --help`
//!     renders the endpoint mini-guide").
//!
//! Example TOML convention enforced here:
//!   - The TOML body is a normal TOML document.
//!   - The file ends with a contiguous run of `#`-prefixed comment lines.
//!   - The first such line (top of the block) starts with `# rmr ` (or
//!     is exactly `# rmr`); the rest are continuations.
//!   - A trailing backslash inside a comment body joins the next line
//!     onto the same command (shell-style line continuation).
//!   - No shell quoting is required — every CLI value in our examples
//!     is a single whitespace-free token.

use std::path::{Path, PathBuf};

use assert_cmd::Command;
use clap::Parser;
use predicates::prelude::*;
use rmr::config::Config;
use rmr::parsers::cli::Cli;
use rmr::parsers::toml::TomlConfig;

/// Every `*.toml` under `examples/simple/` and `examples/advanced/` must
/// be a valid example: its body parses as a TOML config and its trailing
/// `# rmr ...` block produces the same merged [`Config`] as the TOML
/// body itself. Multi-RMR scenarios (e.g. paired drone+ground
/// deployments) carry one TOML per role under the same example dir, so
/// the test walks every `.toml` file rather than a single
/// `config.toml`.
#[test]
fn every_example_toml_matches_its_cli_block() {
    for path in example_toml_files() {
        verify_one(&path);
    }
}

/// Sanity-check: every example dir CLAUDE.md Phase 7 enumerates must
/// carry the TOML(s) it advertises. The canary catches "we shipped
/// Phase 7 with empty placeholder dirs".
#[test]
fn all_expected_examples_are_populated() {
    let single: &[(&str, &str)] = &[
        ("simple", "fc-network"),
        ("simple", "fc-network-windows"),
        ("simple", "fc-sniffer"),
        ("simple", "ipv6-and-hostname"),
        ("advanced", "companion-microservices"),
        ("advanced", "ground-side-local-service"),
        ("advanced", "fleet-aggregator"),
        ("advanced", "egress-bandwidth-shaping"),
        ("advanced", "fc-safety-filter"),
    ];
    for (tier, name) in single {
        let path = Path::new("examples")
            .join(tier)
            .join(name)
            .join("config.toml");
        assert!(path.exists(), "expected example {}", path.display());
    }
    let paired: &[(&str, &str, &[&str])] = &[(
        "advanced",
        "redundant-links",
        &["drone.toml", "ground.toml"],
    )];
    for (tier, name, files) in paired {
        for file in *files {
            let path = Path::new("examples").join(tier).join(name).join(file);
            assert!(path.exists(), "expected example {}", path.display());
        }
    }
}

/// `rmr --help` must render the endpoint mini-guide added via clap's
/// `after_long_help`. Spot-checks anchors that are unique to the
/// extended help (the scheme table headers and the per-scheme query
/// keys) so a regression that drops the guide is loud.
#[test]
fn help_renders_endpoint_mini_guide() {
    Command::cargo_bin("rmr")
        .expect("cargo bin")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Endpoints:"))
        .stdout(predicate::str::contains("ENDPOINT := SCHEME:BODY[#name]"))
        .stdout(predicate::str::contains("Query keys — any scheme:"))
        .stdout(predicate::str::contains("Query keys — scheme-specific:"))
        .stdout(predicate::str::contains("Filter query keys"))
        .stdout(predicate::str::contains("flow_control=rtscts"))
        .stdout(predicate::str::contains("idle_secs=N"))
        .stdout(predicate::str::contains("latch_idle_secs=N"));
}

fn verify_one(path: &Path) {
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));

    let toml_config = TomlConfig::parse_str(&text)
        .unwrap_or_else(|err| panic!("TOML parse {}: {err}", path.display()));

    let argv = extract_cli_argv(&text)
        .unwrap_or_else(|| panic!("no trailing `# rmr ...` block in {}", path.display()));
    let cli = Cli::try_parse_from(&argv)
        .unwrap_or_else(|err| panic!("CLI parse {}: {err}", path.display()));
    let cli_config = cli
        .into_cli_config()
        .unwrap_or_else(|err| panic!("CLI specs {}: {err}", path.display()));

    let from_toml = Config::merge(Some(toml_config), Default::default())
        .unwrap_or_else(|err| panic!("merge TOML-only {}: {err}", path.display()))
        .config;
    let from_cli = Config::merge(None, cli_config)
        .unwrap_or_else(|err| panic!("merge CLI-only {}: {err}", path.display()))
        .config;

    assert_eq!(
        from_toml.endpoints,
        from_cli.endpoints,
        "endpoints diverge between TOML and CLI in {}",
        path.display()
    );
    assert_eq!(
        (
            from_toml.log_level,
            from_toml.log_format,
            from_toml.stats,
            from_toml.stats_interval_secs,
            from_toml.dedup_ms,
            from_toml.skip_config_log,
        ),
        (
            from_cli.log_level,
            from_cli.log_format,
            from_cli.stats,
            from_cli.stats_interval_secs,
            from_cli.dedup_ms,
            from_cli.skip_config_log,
        ),
        "globals diverge between TOML and CLI in {}",
        path.display()
    );
}

fn example_toml_files() -> Vec<PathBuf> {
    let mut out = Vec::new();
    for parent in ["examples/simple", "examples/advanced"] {
        let Ok(entries) = std::fs::read_dir(parent) else {
            continue;
        };
        for entry in entries {
            let entry = entry.expect("read_dir entry");
            let dir = entry.path();
            if !dir.is_dir() {
                continue;
            }
            let Ok(inner) = std::fs::read_dir(&dir) else {
                continue;
            };
            for file in inner {
                let file = file.expect("read_dir entry");
                let path = file.path();
                if path.extension().and_then(|s| s.to_str()) == Some("toml") {
                    out.push(path);
                }
            }
        }
    }
    out.sort();
    out
}

/// Extract the argv vector embedded in the trailing `# rmr ...` block of a
/// TOML file. Returns `None` if the file has no such block.
fn extract_cli_argv(toml_text: &str) -> Option<Vec<String>> {
    let lines: Vec<&str> = toml_text.lines().collect();
    let mut start = None;
    for (idx, line) in lines.iter().enumerate().rev() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("# rmr ") || trimmed == "# rmr" {
            start = Some(idx);
            break;
        }
    }
    let start = start?;

    let mut joined = String::new();
    for line in &lines[start..] {
        let trimmed = line.trim_start();
        if !trimmed.starts_with('#') {
            break;
        }
        let body = trimmed.trim_start_matches('#').trim();
        if body.is_empty() {
            continue;
        }
        if let Some(stripped) = body.strip_suffix('\\') {
            joined.push_str(stripped.trim_end());
            joined.push(' ');
        } else {
            joined.push_str(body);
            joined.push(' ');
        }
    }

    Some(joined.split_whitespace().map(str::to_string).collect())
}
