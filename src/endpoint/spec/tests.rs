use super::parse::{sanitize_for_name, validate_name};
use super::query::{COMMON_KEYS, levenshtein};
use super::*;

fn parse_ok(input: &str) -> EndpointSpec {
    EndpointSpec::parse(input).unwrap_or_else(|e| panic!("expected ok for {input:?}, got {e}"))
}

fn parse_err(input: &str) -> SpecError {
    EndpointSpec::parse(input).expect_err(&format!("expected err for {input:?}"))
}

fn as_udps(spec: &EndpointSpec) -> &UdpServerEndpoint {
    match &spec.kind {
        EndpointKind::UdpServer(e) => e,
        other => panic!("expected udps, got {other:?}"),
    }
}

fn as_udpc(spec: &EndpointSpec) -> &UdpClientEndpoint {
    match &spec.kind {
        EndpointKind::UdpClient(e) => e,
        other => panic!("expected udpc, got {other:?}"),
    }
}

fn as_tcps(spec: &EndpointSpec) -> &TcpServerEndpoint {
    match &spec.kind {
        EndpointKind::TcpServer(e) => e,
        other => panic!("expected tcps, got {other:?}"),
    }
}

fn as_tcpc(spec: &EndpointSpec) -> &TcpClientEndpoint {
    match &spec.kind {
        EndpointKind::TcpClient(e) => e,
        other => panic!("expected tcpc, got {other:?}"),
    }
}

fn as_serial(spec: &EndpointSpec) -> &SerialEndpoint {
    match &spec.kind {
        EndpointKind::Serial(e) => e,
        other => panic!("expected serial, got {other:?}"),
    }
}

#[test]
fn serial_colon_form() {
    let s = parse_ok("serial:/dev/ttyUSB0:921600");
    let e = as_serial(&s);
    assert_eq!(e.path, "/dev/ttyUSB0");
    assert_eq!(e.baud, 921600);
    assert_eq!(s.name, "serial-_dev_ttyUSB0-921600");
    assert!(!s.explicit_name);
}

#[test]
fn serial_comma_form() {
    let s = parse_ok("serial:/dev/ttyUSB0,921600");
    let e = as_serial(&s);
    assert_eq!(e.path, "/dev/ttyUSB0");
    assert_eq!(e.baud, 921600);
}

#[test]
fn serial_windows_com_colon() {
    let e = as_serial(&parse_ok("serial:COM3:115200")).clone();
    assert_eq!(e.path, "COM3");
    assert_eq!(e.baud, 115200);
}

#[test]
fn serial_windows_com_comma() {
    let e = as_serial(&parse_ok("serial:COM3,115200")).clone();
    assert_eq!(e.path, "COM3");
    assert_eq!(e.baud, 115200);
}

#[test]
fn serial_windows_unc_path() {
    let e = as_serial(&parse_ok(r"serial:\\.\COM10:115200")).clone();
    assert_eq!(e.path, r"\\.\COM10");
    assert_eq!(e.baud, 115200);
}

#[test]
fn serial_by_id_symlink() {
    let e = as_serial(&parse_ok("serial:/dev/serial/by-id/usb-FTDI-port0:57600")).clone();
    assert_eq!(e.path, "/dev/serial/by-id/usb-FTDI-port0");
    assert_eq!(e.baud, 57600);
}

#[test]
fn serial_with_explicit_name() {
    let s = parse_ok("serial:/dev/ttyUSB0:921600#vehicle");
    assert_eq!(s.name, "vehicle");
    assert!(s.explicit_name);
}

#[test]
fn serial_no_separator_fails() {
    assert!(matches!(
        parse_err("serial:COM3"),
        SpecError::MalformedBody {
            scheme: "serial",
            ..
        }
    ));
}

#[test]
fn serial_non_numeric_baud_fails() {
    assert!(matches!(
        parse_err("serial:/dev/foo:abc"),
        SpecError::MalformedBody {
            scheme: "serial",
            ..
        }
    ));
}

#[test]
fn serial_empty_path_fails() {
    assert!(matches!(
        parse_err("serial::921600"),
        SpecError::MalformedBody {
            scheme: "serial",
            ..
        }
    ));
}

#[test]
fn serial_zero_baud_fails() {
    assert!(matches!(
        parse_err("serial:/dev/foo:0"),
        SpecError::MalformedBody {
            scheme: "serial",
            ..
        }
    ));
}

#[test]
fn serial_reopen_ms_typed() {
    let e = as_serial(&parse_ok("serial:/dev/foo:9600?serial_reopen_ms=750")).clone();
    assert_eq!(e.serial_reopen_ms, Some(750));
}

#[test]
fn udps_ipv4() {
    let s = parse_ok("udps:0.0.0.0:14550");
    let e = as_udps(&s);
    assert_eq!(e.host, "0.0.0.0");
    assert_eq!(e.port, 14550);
    assert_eq!(s.name, "udps-0_0_0_0-14550");
}

#[test]
fn udps_ipv6_dual_stack() {
    let s = parse_ok("udps:[::]:14550");
    let e = as_udps(&s);
    assert_eq!(e.host, "::");
    assert_eq!(e.port, 14550);
    assert_eq!(s.name, "udps-__-14550");
}

#[test]
fn udpc_ipv4() {
    let s = parse_ok("udpc:192.168.1.5:14550");
    let e = as_udpc(&s);
    assert_eq!(e.host, "192.168.1.5");
    assert_eq!(e.port, 14550);
}

#[test]
fn tcps_ipv6_bracketed() {
    let s = parse_ok("tcps:[2001:db8::1]:5760");
    let e = as_tcps(&s);
    assert_eq!(e.host, "2001:db8::1");
    assert_eq!(e.port, 5760);
}

#[test]
fn tcpc_hostname_explicit_name() {
    let s = parse_ok("tcpc:companion.local:5760#vehicle");
    let e = as_tcpc(&s);
    assert_eq!(e.host, "companion.local");
    assert_eq!(e.port, 5760);
    assert_eq!(s.name, "vehicle");
}

#[test]
fn tcpc_with_group() {
    let s = parse_ok("tcpc:companion.local:5760#vehicle?group=uplink");
    let e = as_tcpc(&s);
    assert_eq!(e.common.group.as_deref(), Some("uplink"));
    assert_eq!(s.name, "vehicle");
}

#[test]
fn udps_with_sniffer_query() {
    let s = parse_ok("udps:0.0.0.0:14551#tap?sniffer=true");
    let e = as_udps(&s);
    assert_eq!(s.name, "tap");
    assert!(e.common.sniffer);
}

#[test]
fn sniffer_false_explicit() {
    let e = as_udps(&parse_ok("udps:0.0.0.0:1?sniffer=false")).clone();
    assert!(!e.common.sniffer);
}

#[test]
fn sniffer_default_is_false() {
    let e = as_udps(&parse_ok("udps:0.0.0.0:1")).clone();
    assert!(!e.common.sniffer);
}

#[test]
fn sniffer_invalid_value_rejected() {
    match parse_err("udps:0.0.0.0:1?sniffer=yes") {
        SpecError::InvalidQueryValue { key, .. } => assert_eq!(key, "sniffer"),
        other => panic!("wrong error: {other:?}"),
    }
}

#[test]
fn msgid_filter_list_typed() {
    let s = parse_ok("tcpc:gcs.local:5760?block_msgid_in=33,100-150,32");
    let e = as_tcpc(&s);
    let list = e.common.block_msgid_in.as_deref().unwrap();
    assert_eq!(list.len(), 3);
    assert_eq!(list[0], MsgIdRange::single(33));
    assert_eq!(list[1], MsgIdRange { lo: 100, hi: 150 });
    assert_eq!(list[2], MsgIdRange::single(32));
}

#[test]
fn msgid_filter_list_with_whitespace() {
    let s = parse_ok("tcpc:gcs.local:5760?allow_msgid_out=1, 2 , 3-5");
    let e = as_tcpc(&s);
    let list = e.common.allow_msgid_out.as_deref().unwrap();
    assert_eq!(list.len(), 3);
    assert_eq!(list[2], MsgIdRange { lo: 3, hi: 5 });
}

#[test]
fn msgid_filter_list_lo_gt_hi_rejected() {
    match parse_err("tcpc:gcs.local:5760?block_msgid_in=10-5") {
        SpecError::InvalidQueryValue { key, .. } => assert_eq!(key, "block_msgid_in"),
        other => panic!("wrong error: {other:?}"),
    }
}

#[test]
fn msgid_filter_list_hex_rejected() {
    match parse_err("tcpc:gcs.local:5760?block_msgid_in=0x21") {
        SpecError::InvalidQueryValue { key, .. } => assert_eq!(key, "block_msgid_in"),
        other => panic!("wrong error: {other:?}"),
    }
}

#[test]
fn msgid_filter_list_symbolic_name_rejected() {
    match parse_err("tcpc:gcs.local:5760?block_msgid_in=HEARTBEAT") {
        SpecError::InvalidQueryValue { key, .. } => assert_eq!(key, "block_msgid_in"),
        other => panic!("wrong error: {other:?}"),
    }
}

#[test]
fn msgid_filter_empty_entry_rejected() {
    match parse_err("tcpc:gcs.local:5760?block_msgid_in=1,,2") {
        SpecError::InvalidQueryValue { key, .. } => assert_eq!(key, "block_msgid_in"),
        other => panic!("wrong error: {other:?}"),
    }
}

#[test]
fn src_sys_filter_typed_as_u8() {
    let s = parse_ok("tcpc:gcs.local:5760?allow_src_sys_out=1,5-10,200");
    let e = as_tcpc(&s);
    let list = e.common.allow_src_sys_out.as_deref().unwrap();
    assert_eq!(list.len(), 3);
    assert_eq!(list[0], U8Range::single(1));
    assert_eq!(list[1], U8Range { lo: 5, hi: 10 });
    assert_eq!(list[2], U8Range::single(200));
}

#[test]
fn src_sys_filter_overflow_rejected() {
    match parse_err("tcpc:gcs.local:5760?allow_src_sys_out=256") {
        SpecError::InvalidQueryValue { key, .. } => assert_eq!(key, "allow_src_sys_out"),
        other => panic!("wrong error: {other:?}"),
    }
}

#[test]
fn idle_secs_typed_on_udps() {
    let e = as_udps(&parse_ok("udps:0.0.0.0:14550?idle_secs=30")).clone();
    assert_eq!(e.idle_secs, Some(30));
}

#[test]
fn udpc_specific_key_on_udps_is_unknown() {
    match parse_err("udps:0.0.0.0:14550?latch_idle_secs=15") {
        SpecError::UnknownQueryKey { key, scheme, .. } => {
            assert_eq!(key, "latch_idle_secs");
            assert_eq!(scheme, "udps");
        }
        other => panic!("wrong error: {other:?}"),
    }
}

#[test]
fn udps_specific_key_on_udpc_is_unknown() {
    match parse_err("udpc:1.2.3.4:14550?idle_secs=15") {
        SpecError::UnknownQueryKey { key, scheme, .. } => {
            assert_eq!(key, "idle_secs");
            assert_eq!(scheme, "udpc");
        }
        other => panic!("wrong error: {other:?}"),
    }
}

#[test]
fn tcpc_specific_key_on_tcps_is_unknown() {
    match parse_err("tcps:0.0.0.0:5760?reconnect_initial_ms=250") {
        SpecError::UnknownQueryKey { key, scheme, .. } => {
            assert_eq!(key, "reconnect_initial_ms");
            assert_eq!(scheme, "tcps");
        }
        other => panic!("wrong error: {other:?}"),
    }
}

#[test]
fn serial_specific_key_on_udps_is_unknown() {
    match parse_err("udps:0.0.0.0:1?serial_reopen_ms=1000") {
        SpecError::UnknownQueryKey { key, scheme, .. } => {
            assert_eq!(key, "serial_reopen_ms");
            assert_eq!(scheme, "udps");
        }
        other => panic!("wrong error: {other:?}"),
    }
}

#[test]
fn name_max_64_ok() {
    let n = "a".repeat(64);
    let s = parse_ok(&format!("udps:0.0.0.0:1#{n}"));
    assert_eq!(s.name, n);
}

#[test]
fn name_too_long_fails() {
    let n = "a".repeat(65);
    assert!(matches!(
        parse_err(&format!("udps:0.0.0.0:1#{n}")),
        SpecError::InvalidName(_)
    ));
}

#[test]
fn name_empty_fails() {
    assert!(matches!(
        parse_err("udps:0.0.0.0:1#"),
        SpecError::InvalidName(_)
    ));
}

#[test]
fn name_with_dot_fails() {
    assert!(matches!(
        parse_err("udps:0.0.0.0:1#a.b"),
        SpecError::InvalidName(_)
    ));
}

#[test]
fn name_with_slash_fails() {
    assert!(matches!(
        parse_err("udps:0.0.0.0:1#a/b"),
        SpecError::InvalidName(_)
    ));
}

#[test]
fn name_with_underscore_hyphen_digit_ok() {
    let s = parse_ok("udps:0.0.0.0:1#abc_DEF-123");
    assert_eq!(s.name, "abc_DEF-123");
}

#[test]
fn unknown_query_key_with_close_suggestion() {
    match parse_err("udps:0.0.0.0:1?snifer=true") {
        SpecError::UnknownQueryKey {
            key, suggestion, ..
        } => {
            assert_eq!(key, "snifer");
            assert_eq!(suggestion, Some("sniffer"));
        }
        other => panic!("wrong error: {other:?}"),
    }
}

#[test]
fn unknown_query_key_with_near_match() {
    match parse_err("udps:0.0.0.0:1?tx_queue_frame=10") {
        SpecError::UnknownQueryKey {
            key,
            suggestion: Some("tx_queue_frames"),
            ..
        } => {
            assert_eq!(key, "tx_queue_frame");
        }
        other => panic!("wrong error: {other:?}"),
    }
}

#[test]
fn unknown_query_key_no_suggestion_when_distant() {
    match parse_err("udps:0.0.0.0:1?xyz=1") {
        SpecError::UnknownQueryKey {
            key,
            suggestion: None,
            ..
        } => assert_eq!(key, "xyz"),
        other => panic!("wrong error: {other:?}"),
    }
}

#[test]
fn missing_scheme_fails() {
    assert!(matches!(
        parse_err("nothing-here"),
        SpecError::MissingScheme(_)
    ));
}

#[test]
fn empty_scheme_fails() {
    assert!(matches!(parse_err(":body"), SpecError::MissingScheme(_)));
}

#[test]
fn unknown_scheme_fails() {
    assert!(matches!(
        parse_err("http:host:80"),
        SpecError::UnknownScheme(_)
    ));
}

#[test]
fn empty_input_fails() {
    assert!(matches!(parse_err(""), SpecError::MissingScheme(_)));
}

#[test]
fn udps_no_port_fails() {
    assert!(matches!(
        parse_err("udps:nohost"),
        SpecError::MalformedBody { .. }
    ));
}

#[test]
fn udps_bad_port_fails() {
    assert!(matches!(
        parse_err("udps:foo:abc"),
        SpecError::MalformedBody { .. }
    ));
}

#[test]
fn udps_port_overflow_fails() {
    assert!(matches!(
        parse_err("udps:foo:99999"),
        SpecError::MalformedBody { .. }
    ));
}

#[test]
fn udps_empty_host_fails() {
    assert!(matches!(
        parse_err("udps::14550"),
        SpecError::MalformedBody { .. }
    ));
}

#[test]
fn ipv6_unclosed_fails() {
    assert!(matches!(
        parse_err("udps:[::1"),
        SpecError::MalformedBody { .. }
    ));
}

#[test]
fn ipv6_no_port_after_bracket_fails() {
    assert!(matches!(
        parse_err("udps:[::1]"),
        SpecError::MalformedBody { .. }
    ));
}

#[test]
fn ipv6_garbage_after_bracket_fails() {
    assert!(matches!(
        parse_err("udps:[::1]x14550"),
        SpecError::MalformedBody { .. }
    ));
}

#[test]
fn duplicate_query_key_fails() {
    assert!(matches!(
        parse_err("udps:0.0.0.0:1?sniffer=true&sniffer=false"),
        SpecError::DuplicateQueryKey(k) if k == "sniffer"
    ));
}

#[test]
fn malformed_query_no_eq_fails() {
    assert!(matches!(
        parse_err("udps:0.0.0.0:1?just_a_key"),
        SpecError::MalformedQuery(_)
    ));
}

#[test]
fn malformed_query_empty_key_fails() {
    assert!(matches!(
        parse_err("udps:0.0.0.0:1?=value"),
        SpecError::MalformedQuery(_)
    ));
}

#[test]
fn empty_query_after_question_mark_ok() {
    let s = parse_ok("udps:0.0.0.0:1?");
    let e = as_udps(&s);
    assert!(e.common.group.is_none());
    assert!(!e.common.sniffer);
}

#[test]
fn trailing_ampersand_tolerated() {
    let e = as_udps(&parse_ok("udps:0.0.0.0:1?sniffer=true&")).clone();
    assert!(e.common.sniffer);
}

#[test]
fn empty_value_for_group_ok() {
    let e = as_udps(&parse_ok("udps:0.0.0.0:1?group=")).clone();
    assert_eq!(e.common.group.as_deref(), Some(""));
}

#[test]
fn value_with_embedded_equals_kept_intact() {
    let e = as_udps(&parse_ok("udps:0.0.0.0:1?group=a=b")).clone();
    assert_eq!(e.common.group.as_deref(), Some("a=b"));
}

#[test]
fn fragment_before_query_order_locked() {
    let s = parse_ok("udps:0.0.0.0:1#name?group=g");
    assert_eq!(s.name, "name");
    let e = as_udps(&s);
    assert_eq!(e.common.group.as_deref(), Some("g"));
}

#[test]
fn query_before_fragment_rejected() {
    match parse_err("udps:0.0.0.0:1?group=val#nope") {
        SpecError::MalformedQuery(_) => {}
        other => panic!("wrong error: {other:?}"),
    }
}

#[test]
fn all_common_filter_keys_accepted_on_tcpc() {
    let q = "allow_msgid_in=1&block_msgid_in=2&allow_msgid_out=3&block_msgid_out=4\
             &allow_src_sys_in=5&block_src_sys_in=6&allow_src_sys_out=7&block_src_sys_out=8\
             &allow_src_comp_in=9&block_src_comp_in=10&allow_src_comp_out=11&block_src_comp_out=12";
    let e = as_tcpc(&parse_ok(&format!("tcpc:x:1?{q}"))).clone();
    assert!(e.common.allow_msgid_in.is_some());
    assert!(e.common.block_msgid_in.is_some());
    assert!(e.common.allow_msgid_out.is_some());
    assert!(e.common.block_msgid_out.is_some());
    assert!(e.common.allow_src_sys_in.is_some());
    assert!(e.common.block_src_sys_in.is_some());
    assert!(e.common.allow_src_sys_out.is_some());
    assert!(e.common.block_src_sys_out.is_some());
    assert!(e.common.allow_src_comp_in.is_some());
    assert!(e.common.block_src_comp_in.is_some());
    assert!(e.common.allow_src_comp_out.is_some());
    assert!(e.common.block_src_comp_out.is_some());
}

#[test]
fn endpoint_kind_scheme_roundtrip() {
    for input in &[
        "serial:/dev/ttyUSB0:115200",
        "udps:0.0.0.0:1",
        "udpc:1.2.3.4:1",
        "tcps:0.0.0.0:1",
        "tcpc:host:1",
    ] {
        let s = parse_ok(input);
        assert!(input.starts_with(&format!("{}:", s.kind.scheme())));
    }
}

#[test]
fn levenshtein_known_pairs() {
    assert_eq!(levenshtein("", ""), 0);
    assert_eq!(levenshtein("", "abc"), 3);
    assert_eq!(levenshtein("abc", ""), 3);
    assert_eq!(levenshtein("abc", "abc"), 0);
    assert_eq!(levenshtein("kitten", "sitting"), 3);
    assert_eq!(levenshtein("snifer", "sniffer"), 1);
}

#[test]
fn auto_names_satisfy_explicit_name_regex() {
    for input in &[
        "serial:/dev/ttyUSB0:921600",
        r"serial:\\.\COM10:115200",
        "udps:0.0.0.0:14550",
        "udps:[::]:14550",
        "tcps:[2001:db8::1]:5760",
        "tcpc:gcs.local:5760",
        "udpc:companion.local:14550",
    ] {
        let s = parse_ok(input);
        validate_name(&s.name)
            .unwrap_or_else(|e| panic!("auto-name {:?} fails name regex: {e}", s.name));
    }
}

#[test]
fn sanitize_for_name_replaces_disallowed_chars() {
    assert_eq!(sanitize_for_name("0.0.0.0"), "0_0_0_0");
    assert_eq!(sanitize_for_name("/dev/ttyUSB0"), "_dev_ttyUSB0");
    assert_eq!(sanitize_for_name("::1"), "__1");
    assert_eq!(sanitize_for_name("gcs.local"), "gcs_local");
    assert_eq!(sanitize_for_name(r"\\.\COM10"), "____COM10");
    assert_eq!(sanitize_for_name("abc_DEF-123"), "abc_DEF-123");
}

#[test]
fn common_keys_sorted_and_unique() {
    let mut copy: Vec<&&str> = COMMON_KEYS.iter().collect();
    copy.sort();
    copy.dedup();
    assert_eq!(copy.len(), COMMON_KEYS.len(), "duplicates present");
    for w in COMMON_KEYS.windows(2) {
        assert!(w[0] < w[1], "not sorted: {} >= {}", w[0], w[1]);
    }
}
