//! Typed MAVLink frame fixtures for integration tests.
//!
//! Two layers:
//! - [`TestFrame`] is a fluent builder that owns the wire layout: STX, header
//!   byte order (v1 vs v2), payload-length byte, CRC computation, and the
//!   optional signature trailer. Tests describe a frame by its fields; the
//!   builder produces the bytes.
//! - [`MavPayload`] is a tiny trait implemented by the few typed payload
//!   structs we need ([`Heartbeat`], [`Ping`], [`SysStatus`]). Each payload
//!   carries its `MSGID` and `CRC_EXTRA` as associated constants and knows how
//!   to serialise itself to the size-sorted byte layout MAVLink prescribes.
//!
//! The `crc_extra` constants here are anchored by the build-support unit
//! tests in `build_support/crc_extra.rs` (which compute them via the same
//! algorithm `build.rs` runs at build time and compare against the published
//! reference values). The CRC algorithm itself comes from the production
//! [`rmr::mavlink::crc::Crc16`] type — no second implementation in the test
//! fixtures.

use rmr::mavlink::crc::Crc16;
use rmr::mavlink::frame::{STX_V1, STX_V2, V2_IFLAG_SIGNED, V2_SIGNATURE_LEN};

/// A MAVLink payload that knows its msgid, its `crc_extra`, and how to
/// serialise itself to the size-sorted byte layout.
pub trait MavPayload {
    const MSGID: u32;
    const CRC_EXTRA: u8;
    fn to_bytes(&self) -> Vec<u8>;
}

#[derive(Debug, Clone, Copy)]
enum FrameVersion {
    V1,
    V2,
}

/// Fluent builder for a complete MAVLink frame (v1 or v2, signed or not).
/// Constructors default seq/sysid/compid to zero/one; chain setters to
/// override what the test cares about, then call [`TestFrame::build`].
pub struct TestFrame {
    version: FrameVersion,
    msgid: u32,
    crc_extra: u8,
    seq: u8,
    sysid: u8,
    compid: u8,
    incompat_flags: u8,
    compat_flags: u8,
    payload: Vec<u8>,
    signature: Option<[u8; V2_SIGNATURE_LEN]>,
}

impl TestFrame {
    /// Start a v1 frame for an arbitrary msgid + crc_extra. Use
    /// [`TestFrame::v1_message`] when the test has a typed payload available.
    pub fn v1(msgid: u32, crc_extra: u8) -> Self {
        Self {
            version: FrameVersion::V1,
            msgid,
            crc_extra,
            seq: 0,
            sysid: 1,
            compid: 1,
            incompat_flags: 0,
            compat_flags: 0,
            payload: Vec::new(),
            signature: None,
        }
    }

    /// Start a v2 frame for an arbitrary msgid + crc_extra. Use
    /// [`TestFrame::v2_message`] when the test has a typed payload available.
    pub fn v2(msgid: u32, crc_extra: u8) -> Self {
        Self {
            version: FrameVersion::V2,
            ..Self::v1(msgid, crc_extra)
        }
    }

    /// Start a v1 frame from a typed payload. The msgid and `crc_extra`
    /// come from the [`MavPayload`] impl; the payload is pre-serialised.
    pub fn v1_message<M: MavPayload>(msg: &M) -> Self {
        let mut t = Self::v1(M::MSGID, M::CRC_EXTRA);
        t.payload = msg.to_bytes();
        t
    }

    /// Start a v2 frame from a typed payload. See [`TestFrame::v1_message`].
    pub fn v2_message<M: MavPayload>(msg: &M) -> Self {
        let mut t = Self::v2(M::MSGID, M::CRC_EXTRA);
        t.payload = msg.to_bytes();
        t
    }

    pub fn seq(mut self, n: u8) -> Self {
        self.seq = n;
        self
    }

    pub fn sysid(mut self, n: u8) -> Self {
        self.sysid = n;
        self
    }

    pub fn compid(mut self, n: u8) -> Self {
        self.compid = n;
        self
    }

    /// Replace the payload bytes. Useful for the unknown-msgid path where
    /// the payload is arbitrary test data.
    pub fn payload(mut self, p: impl Into<Vec<u8>>) -> Self {
        self.payload = p.into();
        self
    }

    /// Mark a v2 frame as signed and append the given signature trailer.
    /// Panics if called on a v1 frame — v1 has no signature trailer.
    pub fn signed(mut self, sig: [u8; V2_SIGNATURE_LEN]) -> Self {
        assert!(
            matches!(self.version, FrameVersion::V2),
            "signing is v2-only"
        );
        self.signature = Some(sig);
        self.incompat_flags |= V2_IFLAG_SIGNED;
        self
    }

    /// Override the incompat-flags byte (v2 only). The [`TestFrame::signed`]
    /// setter manages the signed bit; use this for testing rejection of
    /// unknown incompat flags.
    pub fn incompat_flags(mut self, flags: u8) -> Self {
        self.incompat_flags = flags;
        self
    }

    /// Serialise the frame. CRC is computed over the header + payload using
    /// [`Crc16`] and the configured `crc_extra`. Signature trailer (if any)
    /// is appended unchanged.
    pub fn build(self) -> Vec<u8> {
        let (frame, _crc_lo) = self.into_bytes_with_crc_position();
        frame
    }

    /// Like [`TestFrame::build`] but flips the high CRC byte so the frame
    /// fails CRC validation. Used by the "corrupted frame in stream" test.
    pub fn build_with_corrupted_crc(self) -> Vec<u8> {
        let has_sig = self.signature.is_some();
        let mut frame = self.build();
        let sig_len = if has_sig { V2_SIGNATURE_LEN } else { 0 };
        let crc_hi = frame.len() - 1 - sig_len;
        frame[crc_hi] ^= 0xFF;
        frame
    }

    /// Like [`TestFrame::build`] but stamps an arbitrary CRC instead of the
    /// computed one. Used by the unknown-msgid test to assert the framer
    /// forwards a frame whose CRC it has no way to validate.
    pub fn build_with_crc_override(self, crc: u16) -> Vec<u8> {
        let has_sig = self.signature.is_some();
        let mut frame = self.build();
        let sig_len = if has_sig { V2_SIGNATURE_LEN } else { 0 };
        let crc_lo = frame.len() - 2 - sig_len;
        frame[crc_lo] = (crc & 0xFF) as u8;
        frame[crc_lo + 1] = (crc >> 8) as u8;
        frame
    }

    /// Lay out STX + header + payload, then the computed CRC, then signature
    /// (if any). Returns the assembled bytes plus the byte offset of the low
    /// CRC byte so the public `build_with_*` wrappers can mutate it.
    fn into_bytes_with_crc_position(self) -> (Vec<u8>, usize) {
        let payload_len = self.payload.len();
        assert!(payload_len <= u8::MAX as usize, "payload too long");

        let header_len = match self.version {
            FrameVersion::V1 => 6,
            FrameVersion::V2 => 10,
        };
        let sig_len = self.signature.map(|_| V2_SIGNATURE_LEN).unwrap_or(0);
        let total = header_len + payload_len + 2 + sig_len;

        let mut frame = Vec::with_capacity(total);
        match self.version {
            FrameVersion::V1 => {
                assert!(self.msgid <= u8::MAX as u32, "v1 msgid > 255");
                frame.push(STX_V1);
                frame.push(payload_len as u8);
                frame.push(self.seq);
                frame.push(self.sysid);
                frame.push(self.compid);
                frame.push(self.msgid as u8);
            }
            FrameVersion::V2 => {
                assert!(self.msgid <= 0xFF_FFFF, "v2 msgid > 24 bits");
                frame.push(STX_V2);
                frame.push(payload_len as u8);
                frame.push(self.incompat_flags);
                frame.push(self.compat_flags);
                frame.push(self.seq);
                frame.push(self.sysid);
                frame.push(self.compid);
                frame.push((self.msgid & 0xFF) as u8);
                frame.push(((self.msgid >> 8) & 0xFF) as u8);
                frame.push(((self.msgid >> 16) & 0xFF) as u8);
            }
        }
        frame.extend_from_slice(&self.payload);

        let mut crc = Crc16::new();
        crc.update_slice(&frame[1..]); // skip STX
        crc.update(self.crc_extra);
        let crc_value = crc.finalize();

        let crc_lo = frame.len();
        frame.push((crc_value & 0xFF) as u8);
        frame.push((crc_value >> 8) as u8);

        if let Some(sig) = self.signature {
            frame.extend_from_slice(&sig);
        }

        (frame, crc_lo)
    }
}

/// MAVLink `HEARTBEAT` (msgid 0). Fields are the standard `common.xml` layout
/// in size-sorted order. Defaults produce the same content the previous
/// hand-built `heartbeat_payload(custom_mode)` helper did.
#[derive(Debug, Default, Clone, Copy)]
pub struct Heartbeat {
    pub custom_mode: u32,
    pub mav_type: u8,
    pub autopilot: u8,
    pub base_mode: u8,
    pub system_status: u8,
    pub mavlink_version: u8,
}

impl MavPayload for Heartbeat {
    const MSGID: u32 = 0;
    const CRC_EXTRA: u8 = 50;

    fn to_bytes(&self) -> Vec<u8> {
        let mut p = Vec::with_capacity(9);
        p.extend_from_slice(&self.custom_mode.to_le_bytes());
        p.push(self.mav_type);
        p.push(self.autopilot);
        p.push(self.base_mode);
        p.push(self.system_status);
        p.push(self.mavlink_version);
        p
    }
}

/// MAVLink `PING` (msgid 4). Carries `target_system` / `target_component` —
/// useful for exercising the framer's typed-target extraction.
#[derive(Debug, Default, Clone, Copy)]
pub struct Ping {
    pub time_usec: u64,
    pub seq: u32,
    pub target_system: u8,
    pub target_component: u8,
}

impl MavPayload for Ping {
    const MSGID: u32 = 4;
    const CRC_EXTRA: u8 = 237;

    fn to_bytes(&self) -> Vec<u8> {
        let mut p = Vec::with_capacity(14);
        p.extend_from_slice(&self.time_usec.to_le_bytes());
        p.extend_from_slice(&self.seq.to_le_bytes());
        p.push(self.target_system);
        p.push(self.target_component);
        p
    }
}

/// MAVLink `SYS_STATUS` (msgid 1). 31-byte payload; fields in size-sorted
/// order (3×u32 → 9×u16/i16 → 1×i8).
#[derive(Debug, Default, Clone, Copy)]
pub struct SysStatus {
    pub onboard_control_sensors_present: u32,
    pub onboard_control_sensors_enabled: u32,
    pub onboard_control_sensors_health: u32,
    pub load: u16,
    pub voltage_battery: u16,
    pub current_battery: i16,
    pub drop_rate_comm: u16,
    pub errors_comm: u16,
    pub errors_count1: u16,
    pub errors_count2: u16,
    pub errors_count3: u16,
    pub errors_count4: u16,
    pub battery_remaining: i8,
}

impl MavPayload for SysStatus {
    const MSGID: u32 = 1;
    const CRC_EXTRA: u8 = 124;

    fn to_bytes(&self) -> Vec<u8> {
        let mut p = Vec::with_capacity(31);
        p.extend_from_slice(&self.onboard_control_sensors_present.to_le_bytes());
        p.extend_from_slice(&self.onboard_control_sensors_enabled.to_le_bytes());
        p.extend_from_slice(&self.onboard_control_sensors_health.to_le_bytes());
        p.extend_from_slice(&self.load.to_le_bytes());
        p.extend_from_slice(&self.voltage_battery.to_le_bytes());
        p.extend_from_slice(&self.current_battery.to_le_bytes());
        p.extend_from_slice(&self.drop_rate_comm.to_le_bytes());
        p.extend_from_slice(&self.errors_comm.to_le_bytes());
        p.extend_from_slice(&self.errors_count1.to_le_bytes());
        p.extend_from_slice(&self.errors_count2.to_le_bytes());
        p.extend_from_slice(&self.errors_count3.to_le_bytes());
        p.extend_from_slice(&self.errors_count4.to_le_bytes());
        p.push(self.battery_remaining as u8);
        p
    }
}
