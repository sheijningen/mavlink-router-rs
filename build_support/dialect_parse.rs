// Pure transforms over MAVLink dialect parse results. `include!`d into
// build.rs (the only production user) and into tests/build_support.rs
// (which drives the unit tests below). No I/O, no XML parsing —
// quick-xml lives only in build-dependencies.
//
// Must remain self-contained (no `use crate::*`, no sibling `mod`) so
// the include! works in both contexts.

/// One MAVLink `<field>` parsed from a dialect XML. Array suffix (`uint8_t[16]`)
/// has been split into `type_name = "uint8_t"` + `array_length = 16`.
#[derive(Debug, Clone)]
pub(crate) struct ParsedField {
    pub name: String,
    pub type_name: String,
    pub array_length: u8,
    pub is_extension: bool,
}

/// One MAVLink `<message>` parsed from a dialect XML. `fields` is in
/// declaration order; size-sorting for wire layout happens downstream.
#[derive(Debug)]
pub(crate) struct ParsedMessage {
    pub id: u32,
    pub name: String,
    pub fields: Vec<ParsedField>,
}

/// Staging form of a msgid-table row used while build.rs is walking dialects.
/// Owned `String` names (vs. the `&'static str` in the runtime `MsgEntry`)
/// because the generated table is materialised by writing string literals.
#[derive(Debug, Clone)]
pub(crate) struct MsgEntryGen {
    pub name: String,
    pub crc_extra: u8,
    pub min_payload_len: u16,
    pub target_sys_offset: Option<u16>,
    pub target_comp_offset: Option<u16>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ParseArrayError {
    Malformed(String),
    BadLength(String),
    ZeroLength(String),
}

impl std::fmt::Display for ParseArrayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Malformed(t) => write!(f, "malformed array type '{t}'"),
            Self::BadLength(t) => write!(f, "bad array length in '{t}'"),
            Self::ZeroLength(t) => write!(f, "zero-length array in '{t}'"),
        }
    }
}

pub(crate) fn parse_array_suffix(t: &str) -> Result<(String, u8), ParseArrayError> {
    let Some(lb) = t.find('[') else {
        return Ok((t.to_string(), 0));
    };
    let rb = t
        .find(']')
        .ok_or_else(|| ParseArrayError::Malformed(t.to_string()))?;
    let elem = t[..lb].to_string();
    let len: u8 = t[lb + 1..rb]
        .parse()
        .map_err(|_| ParseArrayError::BadLength(t.to_string()))?;
    if len == 0 {
        return Err(ParseArrayError::ZeroLength(t.to_string()));
    }
    Ok((elem, len))
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MergeError {
    CrcExtraConflict {
        id: u32,
        prev_name: String,
        new_name: String,
        prev_crc: u8,
        new_crc: u8,
    },
    NameConflict {
        id: u32,
        prev_name: String,
        new_name: String,
    },
}

impl std::fmt::Display for MergeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CrcExtraConflict {
                id,
                prev_name,
                new_name,
                prev_crc,
                new_crc,
            } => write!(
                f,
                "crc_extra conflict for msgid {id} ({prev_name} vs {new_name}): {prev_crc} != {new_crc}"
            ),
            Self::NameConflict {
                id,
                prev_name,
                new_name,
            } => write!(
                f,
                "msgid {id} declared under two names: '{prev_name}' and '{new_name}'"
            ),
        }
    }
}

pub(crate) fn merge_entry(
    msg_id: u32,
    msg_name: String,
    crc_extra: u8,
    min_payload_len: u16,
    target_sys_offset: Option<u16>,
    target_comp_offset: Option<u16>,
    entries: &mut std::collections::HashMap<u32, MsgEntryGen>,
) -> Result<(), MergeError> {
    if let Some(prev) = entries.get(&msg_id) {
        if prev.crc_extra != crc_extra {
            return Err(MergeError::CrcExtraConflict {
                id: msg_id,
                prev_name: prev.name.clone(),
                new_name: msg_name,
                prev_crc: prev.crc_extra,
                new_crc: crc_extra,
            });
        }
        if prev.name != msg_name {
            return Err(MergeError::NameConflict {
                id: msg_id,
                prev_name: prev.name.clone(),
                new_name: msg_name,
            });
        }
        return Ok(());
    }
    entries.insert(
        msg_id,
        MsgEntryGen {
            name: msg_name,
            crc_extra,
            min_payload_len,
            target_sys_offset,
            target_comp_offset,
        },
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // ---- parse_array_suffix ----

    #[test]
    fn scalar_returns_zero_length() {
        assert_eq!(
            parse_array_suffix("uint8_t").unwrap(),
            ("uint8_t".to_string(), 0)
        );
    }

    #[test]
    fn typical_u8_array() {
        assert_eq!(
            parse_array_suffix("uint8_t[16]").unwrap(),
            ("uint8_t".to_string(), 16)
        );
    }

    #[test]
    fn char_array_50() {
        assert_eq!(
            parse_array_suffix("char[50]").unwrap(),
            ("char".to_string(), 50)
        );
    }

    #[test]
    fn array_length_at_u8_boundary() {
        assert_eq!(
            parse_array_suffix("uint8_t[255]").unwrap(),
            ("uint8_t".to_string(), 255)
        );
    }

    #[test]
    fn missing_closing_bracket_is_malformed() {
        let err = parse_array_suffix("uint8_t[16").unwrap_err();
        assert_eq!(err, ParseArrayError::Malformed("uint8_t[16".to_string()));
        assert!(err.to_string().contains("malformed array type"));
        assert!(err.to_string().contains("uint8_t[16"));
    }

    #[test]
    fn non_numeric_length_is_bad_length() {
        let err = parse_array_suffix("uint8_t[abc]").unwrap_err();
        assert_eq!(err, ParseArrayError::BadLength("uint8_t[abc]".to_string()));
        assert!(err.to_string().contains("bad array length"));
    }

    #[test]
    fn length_exceeding_u8_is_bad_length() {
        let err = parse_array_suffix("uint8_t[256]").unwrap_err();
        assert_eq!(err, ParseArrayError::BadLength("uint8_t[256]".to_string()));
    }

    #[test]
    fn zero_length_is_rejected() {
        let err = parse_array_suffix("uint8_t[0]").unwrap_err();
        assert_eq!(err, ParseArrayError::ZeroLength("uint8_t[0]".to_string()));
        assert!(err.to_string().contains("zero-length"));
    }

    // ---- merge_entry ----

    fn seeded() -> HashMap<u32, MsgEntryGen> {
        let mut m = HashMap::new();
        m.insert(
            0,
            MsgEntryGen {
                name: "HEARTBEAT".to_string(),
                crc_extra: 50,
                min_payload_len: 9,
                target_sys_offset: None,
                target_comp_offset: None,
            },
        );
        m
    }

    #[test]
    fn merge_inserts_into_empty() {
        let mut entries: HashMap<u32, MsgEntryGen> = HashMap::new();
        merge_entry(0, "HEARTBEAT".to_string(), 50, 9, None, None, &mut entries).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[&0].name, "HEARTBEAT");
        assert_eq!(entries[&0].crc_extra, 50);
    }

    #[test]
    fn merge_identical_is_idempotent() {
        let mut entries = seeded();
        merge_entry(0, "HEARTBEAT".to_string(), 50, 9, None, None, &mut entries).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn merge_with_different_crc_extra_is_fatal() {
        let mut entries = seeded();
        let err =
            merge_entry(0, "HEARTBEAT".to_string(), 99, 9, None, None, &mut entries).unwrap_err();
        match err {
            MergeError::CrcExtraConflict {
                id,
                prev_crc,
                new_crc,
                ..
            } => {
                assert_eq!(id, 0);
                assert_eq!(prev_crc, 50);
                assert_eq!(new_crc, 99);
            }
            other => panic!("expected CrcExtraConflict, got {other:?}"),
        }
        // Original entry is preserved on error.
        assert_eq!(entries[&0].crc_extra, 50);
    }

    #[test]
    fn merge_with_same_id_different_name_is_fatal() {
        let mut entries = seeded();
        let err = merge_entry(0, "OTHER".to_string(), 50, 9, None, None, &mut entries).unwrap_err();
        match err {
            MergeError::NameConflict {
                id,
                prev_name,
                new_name,
            } => {
                assert_eq!(id, 0);
                assert_eq!(prev_name, "HEARTBEAT");
                assert_eq!(new_name, "OTHER");
            }
            other => panic!("expected NameConflict, got {other:?}"),
        }
        assert_eq!(entries[&0].name, "HEARTBEAT");
    }

    #[test]
    fn merge_distinct_ids_both_inserted() {
        let mut entries = seeded();
        merge_entry(
            1,
            "SYS_STATUS".to_string(),
            124,
            31,
            None,
            None,
            &mut entries,
        )
        .unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[&0].name, "HEARTBEAT");
        assert_eq!(entries[&1].name, "SYS_STATUS");
    }

    #[test]
    fn merge_targets_are_preserved() {
        let mut entries: HashMap<u32, MsgEntryGen> = HashMap::new();
        merge_entry(
            4,
            "PING".to_string(),
            237,
            14,
            Some(12),
            Some(13),
            &mut entries,
        )
        .unwrap();
        assert_eq!(entries[&4].target_sys_offset, Some(12));
        assert_eq!(entries[&4].target_comp_offset, Some(13));
    }

    #[test]
    fn merge_error_display_includes_key_facts() {
        let crc = MergeError::CrcExtraConflict {
            id: 42,
            prev_name: "FOO".to_string(),
            new_name: "BAR".to_string(),
            prev_crc: 11,
            new_crc: 22,
        };
        let s = crc.to_string();
        assert!(s.contains("42"));
        assert!(s.contains("FOO"));
        assert!(s.contains("BAR"));
        assert!(s.contains("11"));
        assert!(s.contains("22"));

        let name = MergeError::NameConflict {
            id: 7,
            prev_name: "OLD".to_string(),
            new_name: "NEW".to_string(),
        };
        let s = name.to_string();
        assert!(s.contains("7"));
        assert!(s.contains("OLD"));
        assert!(s.contains("NEW"));
    }
}
