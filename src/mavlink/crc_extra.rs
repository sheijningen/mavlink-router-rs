// Single source of truth for the CRC-16-MCRF4XX (MAVLink CRC) update step
// and the `crc_extra` byte computation. Used both in the project and in `build.rs`.

pub(crate) const CRC_INIT: u16 = 0xFFFF;

#[inline]
pub(crate) fn crc16_update(crc: &mut u16, b: u8) {
    let tmp = b ^ ((*crc & 0xFF) as u8);
    let tmp = tmp ^ (tmp << 4);
    let tmp16 = tmp as u16;
    *crc = (*crc >> 8) ^ (tmp16 << 8) ^ (tmp16 << 3) ^ (tmp16 >> 4);
}

pub(crate) fn crc16_mcrf4xx(bytes: &[u8]) -> u16 {
    let mut crc = CRC_INIT;
    for &b in bytes {
        crc16_update(&mut crc, b);
    }
    crc
}

/// Field description passed to `crc_extra_for_message`. Borrowed strings keep
/// the caller (build.rs) in charge of XML lifetime; the algorithm only reads.
#[derive(Debug, Clone)]
pub(crate) struct CrcExtraField<'a> {
    pub name: &'a str,
    /// XML type, as-is. `uint8_t_mavlink_version` is stripped to `uint8_t`
    /// internally for the CRC computation, matching the reference algorithm.
    pub type_name: &'a str,
    /// 0 for scalars, >0 for arrays. For an array, the element type is
    /// `type_name` and the array length is mixed into the CRC as a single byte.
    pub array_length: u8,
    /// Extension fields are excluded from `crc_extra` and from size-sorting.
    pub is_extension: bool,
}

fn type_size(type_name: &str) -> u8 {
    match type_name {
        "char" | "int8_t" | "uint8_t" | "uint8_t_mavlink_version" => 1,
        "int16_t" | "uint16_t" => 2,
        "int32_t" | "uint32_t" | "float" => 4,
        "int64_t" | "uint64_t" | "double" => 8,
        _ => panic!("unknown MAVLink type for crc_extra: {type_name}"),
    }
}

fn crc_type(type_name: &str) -> &str {
    if type_name == "uint8_t_mavlink_version" {
        "uint8_t"
    } else {
        type_name
    }
}

pub(crate) fn crc_extra_for_message(msg_name: &str, fields: &[CrcExtraField<'_>]) -> u8 {
    let mut base: Vec<&CrcExtraField<'_>> = fields.iter().filter(|f| !f.is_extension).collect();
    // Stable sort by element size, descending.
    base.sort_by(|a, b| type_size(b.type_name).cmp(&type_size(a.type_name)));

    let mut crc = CRC_INIT;
    for &b in msg_name.as_bytes() {
        crc16_update(&mut crc, b);
    }
    crc16_update(&mut crc, b' ');

    for f in base {
        let t = crc_type(f.type_name);
        for &b in t.as_bytes() {
            crc16_update(&mut crc, b);
        }
        crc16_update(&mut crc, b' ');
        for &b in f.name.as_bytes() {
            crc16_update(&mut crc, b);
        }
        crc16_update(&mut crc, b' ');
        if f.array_length > 0 {
            crc16_update(&mut crc, f.array_length);
        }
    }

    ((crc & 0xFF) ^ (crc >> 8)) as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(name: &'static str, type_name: &'static str) -> CrcExtraField<'static> {
        CrcExtraField {
            name,
            type_name,
            array_length: 0,
            is_extension: false,
        }
    }
    fn fx(name: &'static str, type_name: &'static str) -> CrcExtraField<'static> {
        CrcExtraField {
            name,
            type_name,
            array_length: 0,
            is_extension: true,
        }
    }
    fn fa(name: &'static str, type_name: &'static str, len: u8) -> CrcExtraField<'static> {
        CrcExtraField {
            name,
            type_name,
            array_length: len,
            is_extension: false,
        }
    }

    // CRC-16-MCRF4XX check vector: 0x6F91 for ASCII "123456789".
    // (X.25 with init 0xFFFF, reflected, no final XOR.)
    #[test]
    fn crc16_check_vector() {
        assert_eq!(crc16_mcrf4xx(b"123456789"), 0x6F91);
    }

    #[test]
    fn crc16_init_empty() {
        assert_eq!(crc16_mcrf4xx(b""), CRC_INIT);
    }

    // HEARTBEAT (id 0): published crc_extra = 50.
    #[test]
    fn crc_extra_heartbeat() {
        let fields = &[
            f("type", "uint8_t"),
            f("autopilot", "uint8_t"),
            f("base_mode", "uint8_t"),
            f("custom_mode", "uint32_t"),
            f("system_status", "uint8_t"),
            f("mavlink_version", "uint8_t_mavlink_version"),
        ];
        assert_eq!(crc_extra_for_message("HEARTBEAT", fields), 50);
    }

    // SYS_STATUS (id 1): published crc_extra = 124. Mixes 4/2/1-byte fields and
    // has extension fields, exercising both stable size-sort and extension skip.
    #[test]
    fn crc_extra_sys_status() {
        let fields = &[
            f("onboard_control_sensors_present", "uint32_t"),
            f("onboard_control_sensors_enabled", "uint32_t"),
            f("onboard_control_sensors_health", "uint32_t"),
            f("load", "uint16_t"),
            f("voltage_battery", "uint16_t"),
            f("current_battery", "int16_t"),
            f("battery_remaining", "int8_t"),
            f("drop_rate_comm", "uint16_t"),
            f("errors_comm", "uint16_t"),
            f("errors_count1", "uint16_t"),
            f("errors_count2", "uint16_t"),
            f("errors_count3", "uint16_t"),
            f("errors_count4", "uint16_t"),
            fx("onboard_control_sensors_present_extended", "uint32_t"),
            fx("onboard_control_sensors_enabled_extended", "uint32_t"),
            fx("onboard_control_sensors_health_extended", "uint32_t"),
        ];
        assert_eq!(crc_extra_for_message("SYS_STATUS", fields), 124);
    }

    // SYSTEM_TIME (id 2): published crc_extra = 137.
    #[test]
    fn crc_extra_system_time() {
        let fields = &[
            f("time_unix_usec", "uint64_t"),
            f("time_boot_ms", "uint32_t"),
        ];
        assert_eq!(crc_extra_for_message("SYSTEM_TIME", fields), 137);
    }

    // PING (id 4): published crc_extra = 237. Has target_system / target_component
    // — the exact shape the router relies on for targeted forwarding.
    #[test]
    fn crc_extra_ping() {
        let fields = &[
            f("time_usec", "uint64_t"),
            f("seq", "uint32_t"),
            f("target_system", "uint8_t"),
            f("target_component", "uint8_t"),
        ];
        assert_eq!(crc_extra_for_message("PING", fields), 237);
    }

    // ATTITUDE (id 30): published crc_extra = 39. All same-size fields,
    // confirms stable sort preserves XML order when sizes tie.
    #[test]
    fn crc_extra_attitude() {
        let fields = &[
            f("time_boot_ms", "uint32_t"),
            f("roll", "float"),
            f("pitch", "float"),
            f("yaw", "float"),
            f("rollspeed", "float"),
            f("pitchspeed", "float"),
            f("yawspeed", "float"),
        ];
        assert_eq!(crc_extra_for_message("ATTITUDE", fields), 39);
    }

    // GPS_STATUS (id 25): published crc_extra = 23. Several uint8_t arrays —
    // exercises the per-field array_length byte mixed into the CRC.
    #[test]
    fn crc_extra_gps_status_arrays() {
        let fields = &[
            f("satellites_visible", "uint8_t"),
            fa("satellite_prn", "uint8_t", 20),
            fa("satellite_used", "uint8_t", 20),
            fa("satellite_elevation", "uint8_t", 20),
            fa("satellite_azimuth", "uint8_t", 20),
            fa("satellite_snr", "uint8_t", 20),
        ];
        assert_eq!(crc_extra_for_message("GPS_STATUS", fields), 23);
    }
}
