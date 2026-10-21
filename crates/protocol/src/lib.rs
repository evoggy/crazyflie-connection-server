//! # Crazyflie connection protocol
//!
//! Messages exchanged between the Crazyflie connection server and its clients.
//! Over the network every message is one JSON text frame on a WebSocket. A
//! server in the client's process gets the same types over channels, without
//! serializing them.
//!
//! A client sends [ClientMessage]s, each with an `id` it picks. The server
//! answers every request with exactly one [ServerMessage::Reply] carrying the
//! same `id`. Streams (log data, param changes, console) are opened with a
//! subscribe request carrying a `sub` number, also picked by the client, and
//! arrive as [ServerMessage::Event]s until the client sends
//! [Request::Unsubscribe] or the stream ends with [Event::Ended].
//!
//! ``` text
//! → {"id":7,"call":"param_set","args":{"uri":"radio:///80/2M/E7E7E7E701","name":"led.bitmask","value":3.0}}
//! ← {"type":"reply","id":7,"ok":{"kind":"done"}}
//! → {"id":8,"call":"log_subscribe","args":{"uri":"radio:///80/2M/E7E7E7E701","sub":1,"vars":["pm.vbat"],"period_ms":100}}
//! ← {"type":"reply","id":8,"ok":{"kind":"done"}}
//! ← {"type":"event","sub":1,"event":{"kind":"log","timestamp":123456,"values":{"pm.vbat":3.92}}}
//! ```
//!
//! Crazyflies are addressed by URI. The server keys them on the normalized
//! form returned by [normalize_uri], so `radio://0/80/2M/E7E7E7E701` and
//! `radio:///80/2M/E7E7E7E701` are the same Crazyflie.
//!
//! The repository's `docs/` has a guide to the protocol, a reference of every
//! call with examples, and the JSON Schema of the messages.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Protocol version, sent in [Response::Hello]
pub const PROTOCOL_VERSION: u32 = 1;

/// TCP port the server listens on by default
pub const DEFAULT_PORT: u16 = 7777;

/// Radio address used when a URI does not give one
pub const DEFAULT_ADDRESS: &str = "E7E7E7E7E7";

/// A request from a client
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct ClientMessage {
    /// Picked by the client, echoed in the reply
    pub id: u64,
    #[serde(flatten)]
    pub request: Request,
}

/// Request payloads
///
/// Every call on a Crazyflie (`uri`) needs the client to be attached to it,
/// except [Request::Attach], [Request::Detach], [Request::ScanSelected],
/// [Request::Flash] and [Request::Platform]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "call", content = "args", rename_all = "snake_case")]
pub enum Request {
    /// Name this client, shown to other clients in scan and list results
    Hello { name: String },
    /// Scan all channels for Crazyflies at an address. Refused while a job runs
    Scan {
        /// 10 hex digits, E7E7E7E7E7 when not given
        address: Option<String>,
    },
    /// Check which of these Crazyflies answer, one packet each, without
    /// connecting. Connected ones answer without radio traffic
    ScanSelected { uris: Vec<String> },
    /// List the Crazyflies the server is connected to
    List,
    /// Attach to a Crazyflie, connecting it if no other client has
    Attach { uri: String },
    /// Detach from a Crazyflie. It is disconnected a while (the server's
    /// linger time) after its last client detaches
    Detach { uri: String },

    /// Read a param from the Crazyflie
    ParamGet { uri: String, name: String },
    /// Set a param. Every value is a number, converted to the param's type
    ParamSet { uri: String, name: String, value: f64 },
    /// Stream of [Event::ParamChanged] for every param set by any client
    ParamWatch { uri: String, sub: u64 },
    /// Whether a param can be stored in the EEPROM. Replies [Response::Bool].
    /// Asks the Crazyflie, so it isn't part of [AttachInfo]
    ParamIsPersistent { uri: String, name: String },
    /// Default and stored value of a persistent param
    ParamPersistentState { uri: String, name: String },
    /// Store the current value of a persistent param in the Crazyflie's EEPROM
    ParamPersistentStore { uri: String, name: String },
    /// Clear the stored value of a persistent param
    ParamPersistentClear { uri: String, name: String },

    /// Stream of [Event::Log] with the given variables. Variables that don't fit
    /// one log block (26 bytes) are spread over several, each arriving as its own event
    LogSubscribe {
        uri: String,
        sub: u64,
        /// Log variable names, `group.name`
        vars: Vec<String>,
        /// Milliseconds between samples, 10 to 2550
        period_ms: u64,
    },

    /// Stream of [Event::Console] lines
    ConsoleSubscribe {
        uri: String,
        sub: u64,
        /// Start with the lines printed since the Crazyflie was connected
        history: bool,
    },

    /// Stream of [Event::LighthouseAngles]. The Crazyflie sends them only while
    /// the `locSrv.enLhAngleStream` param is 1
    LighthouseAngleSubscribe { uri: String, sub: u64 },

    /// Stop a stream opened with `sub`
    Unsubscribe { sub: u64 },

    /// Take off from the current position. The reply comes when the command is
    /// sent, not when the Crazyflie is up
    TakeOff {
        uri: String,
        /// Meters above the world origin
        height: f32,
        /// Radians, the current yaw when not given
        yaw: Option<f32>,
        /// Seconds to reach `height`
        duration: f32,
        /// The groups of Crazyflies that obey, all when not given
        group_mask: Option<u8>,
    },
    /// Land at the current position
    Land {
        uri: String,
        /// Meters above the world origin
        height: f32,
        /// Radians, the current yaw when not given
        yaw: Option<f32>,
        /// Seconds to reach `height`
        duration: f32,
        /// The groups of Crazyflies that obey, all when not given
        group_mask: Option<u8>,
    },
    /// Stop the high-level command and the motors
    Stop {
        uri: String,
        /// The groups of Crazyflies that obey, all when not given
        group_mask: Option<u8>,
    },
    /// Fly to a position
    GoTo {
        uri: String,
        /// Meters
        x: f32,
        /// Meters
        y: f32,
        /// Meters
        z: f32,
        /// Radians
        yaw: f32,
        /// Seconds to get there
        duration: f32,
        /// The position and yaw are relative to the current ones
        relative: bool,
        /// A straight line at constant speed instead of a smooth polynomial
        linear: bool,
        /// The groups of Crazyflies that obey, all when not given
        group_mask: Option<u8>,
    },
    /// Define trajectory `id` as `num_pieces` pieces at `memory_offset` in the trajectory memory
    DefineTrajectory {
        uri: String,
        id: u8,
        /// Bytes into the trajectory memory
        memory_offset: u32,
        num_pieces: u8,
        /// 0: uncompressed (the default), 1: compressed
        trajectory_type: Option<u8>,
    },
    /// Fly trajectory `id`
    StartTrajectory {
        uri: String,
        id: u8,
        /// 1 flies it as defined, 2 twice as slow
        time_scale: f32,
        /// Shift it to start at the current position
        relative_position: bool,
        /// Turn it to start at the current yaw
        relative_yaw: bool,
        /// Fly it backwards
        reversed: bool,
        /// The groups of Crazyflies that obey, all when not given
        group_mask: Option<u8>,
    },

    /// Low-level setpoint, sent repeatedly. The thrust is locked until a
    /// setpoint with thrust 0 is sent
    SetpointRpyt {
        uri: String,
        /// Degrees
        roll: f32,
        /// Degrees
        pitch: f32,
        /// Degrees/s
        yawrate: f32,
        /// 0 to 65535
        thrust: u16,
    },
    /// End the low-level setpoints, so that the high-level commander takes over
    NotifySetpointStop {
        uri: String,
        /// How long the last setpoint still holds, 0 for no time
        remain_valid_ms: u32,
    },

    /// Arm or disarm the motors
    ArmingRequest { uri: String, arm: bool },
    /// Stop the motors at once. The Crazyflie has to be restarted to fly again
    EmergencyStop { uri: String },
    /// Recover a Crazyflie that crashed (tumbled)
    CrashRecovery { uri: String },

    /// The radio link's statistics
    LinkStatistics { uri: String },

    /// Restart the Crazyflies into their bootloaders, flash the images to each
    /// and restart them into their firmware, as a job on stream `sub`:
    /// [Event::Progress], one [Event::Flashed] or [Event::FlashFailed] per
    /// Crazyflie, then [Event::Done]. The images are for one platform. The
    /// Crazyflies whose bootloaders take broadcast get each image at the same
    /// time, the others one after another. Clients attached to them are
    /// disconnected, and attaching and scanning are refused until the job is
    /// done. The job finishes even if the client that started it goes away
    Flash { uris: Vec<String>, sub: u64, images: Vec<FlashImage> },

    /// Stream of [Event::Busy] and [Event::Available]: the Crazyflies that jobs
    /// take over and give back
    Watch { sub: u64 },

    /// Read `length` bytes of memory `id` (see [AttachInfo::memories]) from `address`
    MemoryRead { uri: String, id: u8, address: u32, length: u32 },
    /// Write `data` to memory `id` at `address`
    MemoryWrite {
        uri: String,
        id: u8,
        address: u32,
        /// The bytes, base64 encoded
        data: String,
    },
    /// Read a deck's 1-wire memory `id`: its ids and elements (board name, revision, …)
    OneWireRead { uri: String, id: u8 },
    /// Read the EEPROM config memory `id`
    EepromConfigRead { uri: String, id: u8 },
    /// Write the EEPROM config memory `id`. The firmware applies it at boot
    EepromConfigWrite { uri: String, id: u8, config: EepromConfig },
    /// Read the geometry of base station `bs` from lighthouse memory `id`
    LighthouseGeometryRead { uri: String, id: u8, bs: u8 },
    /// Write the geometry of base station `bs` to lighthouse memory `id`. The
    /// Crazyflie uses it until it restarts, see [Request::LighthousePersist]
    LighthouseGeometryWrite { uri: String, id: u8, bs: u8, geometry: LighthouseBsGeometry },
    /// Read the calibration of base station `bs` from lighthouse memory `id`
    LighthouseCalibrationRead { uri: String, id: u8, bs: u8 },
    /// Write the calibration of base station `bs` to lighthouse memory `id`.
    /// The Crazyflie uses it until it restarts, see [Request::LighthousePersist]
    LighthouseCalibrationWrite { uri: String, id: u8, bs: u8, calibration: LighthouseBsCalibration },
    /// Write uncompressed trajectory segments to trajectory memory `id` at
    /// `start_addr`. Replies [Response::Written]
    TrajectoryWriteUncompressed { uri: String, id: u8, start_addr: u32, segments: Vec<Poly4D> },
    /// Write a compressed trajectory to trajectory memory `id` at `start_addr`.
    /// Replies [Response::Written]
    TrajectoryWriteCompressed {
        uri: String,
        id: u8,
        start_addr: u32,
        start: CompressedStart,
        segments: Vec<CompressedSegment>,
    },
    /// The anchor ids in Loco memory `id`: all configured ones, or the
    /// `active` ones. Replies [Response::Ids]
    LocoReadIdList { uri: String, id: u8, active: bool },
    /// The position of `anchor` in Loco memory `id`
    LocoReadAnchor { uri: String, id: u8, anchor: u8 },
    /// Send a Loco Positioning Protocol packet to anchor `dest_id`, through the
    /// Crazyflie (e.g. an anchor position)
    SendShortLppPacket {
        uri: String,
        dest_id: u8,
        /// The packet's bytes
        data: Vec<u8>,
    },
    /// Store the geometries and calibrations of these base stations in the
    /// Crazyflie's flash. Replies [Response::Bool]: whether the Crazyflie did
    LighthousePersist {
        uri: String,
        /// Base stations whose geometry to store
        geometries: Vec<u8>,
        /// Base stations whose calibration to store
        calibrations: Vec<u8>,
    },

    /// A command the Crazyflie's nRF51 handles, sent without connecting to it.
    /// Its clients are disconnected, except by [PlatformCommand::PowerOnStm32Domain].
    /// Fails if it doesn't answer
    Platform {
        uri: String,
        command: PlatformCommand,
        /// With [PlatformCommand::Reboot], where the Crazyflie comes back when
        /// its radio channel or address was changed in its EEPROM config: the
        /// reply waits for its firmware there, it isn't connected again, and
        /// [Event::Available] says where it moved
        #[serde(default, skip_serializing_if = "Option::is_none")]
        new_uri: Option<String>,
    },
}

/// Commands for [Request::Platform], named after the crazyflie-lib functions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum PlatformCommand {
    /// Restart the firmware. The reply comes once the firmware answers again;
    /// meanwhile the Crazyflie is busy ([Event::Busy]), and a client that
    /// was attached gets it back connected ([Event::Available])
    Reboot,
    /// Power off the whole Crazyflie, as the power button does. Only the
    /// button turns it on again
    PowerOffAll,
    /// Power off the STM32 and decks (sleep). The nRF51 stays on the radio.
    /// The Crazyflie is announced busy, and attaching fails until it is woken up
    PowerOffStm32Domain,
    /// Power the STM32 and decks on again (wake up). The reply comes once the
    /// firmware answers; the Crazyflie is then announced available
    PowerOnStm32Domain,
}

/// A firmware image for [Request::Flash]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct FlashImage {
    /// `stm32` or `nrf51`
    pub target: String,
    /// Where to write the image; the start of the firmware area when not given
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<FlashStart>,
    /// The image, base64 encoded
    pub data: String,
}

/// Where a [FlashImage] is written
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum FlashStart {
    Address(u32),
    Page(u16),
}

/// A message from the server
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    /// Answer to the request with the same `id`. Exactly one of `ok` and `error` is set
    Reply {
        id: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ok: Option<Response>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    /// Data on a stream opened by the client with `sub`
    Event { sub: u64, event: Event },
    /// A Crazyflie this client is attached to was disconnected
    Disconnected { uri: String, reason: String },
}

/// Reply payloads
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Response {
    /// Done, nothing to return
    Done,
    Hello {
        /// The server's name
        server: String,
        /// The protocol version, [PROTOCOL_VERSION]
        protocol: u32,
    },
    /// The Crazyflies that answered
    Scan { found: Vec<CfInfo> },
    /// The Crazyflies the server is connected to
    List { connected: Vec<CfInfo> },
    /// The Crazyflie attached to
    Attached(AttachInfo),
    /// A param value
    Value { value: f64 },
    PersistentState(PersistentParamState),
    LinkStatistics(LinkStatistics),
    /// Bytes read
    Data {
        /// base64 encoded
        data: String,
    },
    Bool { value: bool },
    OneWire(OneWireMemory),
    EepromConfig(EepromConfig),
    LighthouseGeometry(LighthouseBsGeometry),
    LighthouseCalibration(LighthouseBsCalibration),
    /// Bytes written
    Written { bytes: u64 },
    /// Loco anchor ids
    Ids { ids: Vec<u8> },
    LocoAnchor(LocoAnchorData),
}

/// Stream payloads
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Event {
    /// A client set a param
    ParamChanged { name: String, value: f64 },
    /// Samples of a log block
    Log {
        /// Milliseconds since the Crazyflie started, when sampled
        timestamp: u32,
        /// By variable name
        values: BTreeMap<String, f64>,
    },
    /// A line the Crazyflie printed, without the newline
    Console { line: String },
    /// Sweep angles of the four sensors, in radians, from one base station
    LighthouseAngles { base_station: u8, x: [f32; 4], y: [f32; 4] },
    /// How far a job is: `done` of `total` for its current `step`, on the
    /// Crazyflie at `uri`, or on all of them (e.g. a broadcast) without one
    Progress {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        uri: Option<String>,
        step: String,
        done: u64,
        total: u64,
    },
    /// A Crazyflie of a flash job was flashed, `how`: e.g. `broadcast`
    Flashed { uri: String, how: String },
    /// Flashing a Crazyflie of a flash job failed
    FlashFailed { uri: String, reason: String },
    /// A job finished
    Done,
    /// A job took the Crazyflie over (e.g. flashing): attaching to it fails
    /// until it is [Event::Available] again
    Busy { uri: String, reason: String },
    /// The job on the Crazyflie is done
    Available {
        uri: String,
        /// The server connected it again, so attaching to it is immediate
        connected: bool,
        /// It was rebooted onto another channel or address, and answers there now
        #[serde(default, skip_serializing_if = "Option::is_none")]
        moved_to: Option<String>,
    },
    /// The stream ended on the server side (disconnect, log error, …)
    Ended { reason: String },
}

/// A Crazyflie found by a scan or connected on the server
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct CfInfo {
    /// Normalized URI, see [normalize_uri]
    pub uri: String,
    /// Names of the clients attached to it
    pub clients: Vec<String>,
    /// The server's Crazyradio it is connected or answered on, left out for USB
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub radio: Option<usize>,
}

/// What a client learns when it attaches
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct AttachInfo {
    /// Normalized URI
    pub uri: String,
    /// e.g. `2025.12`, when the firmware tells
    pub firmware_version: Option<String>,
    /// e.g. `Crazyflie 2.1`, when the firmware tells
    pub device_type: Option<String>,
    /// The firmware's CRTP protocol version
    #[serde(default)]
    pub protocol_version: Option<u8>,
    /// The param TOC
    pub params: Vec<TocItem>,
    /// The log TOC
    pub log_vars: Vec<TocItem>,
    /// The Crazyflie's memories, as listed when it was connected
    #[serde(default)]
    pub memories: Vec<MemoryInfo>,
}

/// One memory of a Crazyflie, see `crazyflie_lib::subsystems::memory::MemoryDevice`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct MemoryInfo {
    /// The `id` memory calls take
    pub id: u8,
    #[serde(rename = "type")]
    pub memory_type: MemoryType,
    /// Size in bytes
    pub size: u32,
    /// Serial number, e.g. a deck's 1-wire ROM id, as hex
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub serial: Option<String>,
}

/// Memory types, named as in `crazyflie_lib::subsystems::memory::MemoryType`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
#[allow(clippy::upper_case_acronyms)]
pub enum MemoryType {
    #[serde(rename = "eeprom_config")]
    EEPROMConfig,
    OneWire,
    DriverLed,
    Loco,
    Trajectory,
    Loco2,
    Lighthouse,
    MemoryTester,
    #[serde(rename = "micro_sd")]
    MicroSD,
    DriverLedTiming,
    App,
    DeckMemory,
    #[serde(rename = "deck_ctrl_dfu")]
    DeckCtrlDFU,
    DeckCtrl,
    DeckMultiranger,
    DeckPaa3905,
    /// A memory type this version doesn't know
    #[serde(other, rename = "unknown")]
    UNKNOWN,
}

/// A deck's 1-wire memory, see `crazyflie_lib::subsystems::memory::OwMemory`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct OneWireMemory {
    /// Bitmask of the deck pins the deck uses
    pub used_pins: u32,
    /// Vendor id
    pub vid: u8,
    /// Product id
    pub pid: u8,
    /// e.g. `boardName`, `revision`
    pub elements: BTreeMap<String, String>,
}

/// The EEPROM config, see `crazyflie_lib::subsystems::memory::EEPROMConfigMemory`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct EepromConfig {
    /// 0 to 125
    pub radio_channel: u8,
    /// 0: 250 kbit/s, 1: 1 Mbit/s, 2: 2 Mbit/s
    pub radio_speed: u8,
    /// 10 hex digits
    pub radio_address: String,
    pub pitch_trim: f32,
    pub roll_trim: f32,
}

/// Where a lighthouse base station is, see
/// `crazyflie_lib::subsystems::memory::LighthouseBsGeometry`
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct LighthouseBsGeometry {
    /// Meters
    pub origin: [f32; 3],
    /// Rows
    pub rotation_matrix: [[f32; 3]; 3],
    pub valid: bool,
}

/// A lighthouse base station's calibration, see
/// `crazyflie_lib::subsystems::memory::LighthouseBsCalibration`
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct LighthouseBsCalibration {
    pub sweeps: [LighthouseCalibrationSweep; 2],
    /// The base station's id
    pub uid: u32,
    pub valid: bool,
}

/// A polynomial of up to 8 coefficients, see
/// `crazyflie_lib::subsystems::memory::Poly`
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct Poly {
    pub values: [f32; 8],
}

impl Poly {
    pub fn new(values: [f32; 8]) -> Self {
        Self { values }
    }

    /// The first 8 values, the rest zero
    pub fn from_slice(values: &[f32]) -> Self {
        let mut poly = Self::default();
        let len = values.len().min(8);
        poly.values[..len].copy_from_slice(&values[..len]);
        poly
    }
}

/// An uncompressed trajectory segment, see
/// `crazyflie_lib::subsystems::memory::Poly4D`
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct Poly4D {
    /// Seconds
    pub duration: f32,
    pub x: Poly,
    pub y: Poly,
    pub z: Poly,
    pub yaw: Poly,
}

impl Poly4D {
    pub fn new(duration: f32, x: Poly, y: Poly, z: Poly, yaw: Poly) -> Self {
        Self { duration, x, y, z, yaw }
    }
}

/// Where a compressed trajectory starts, see
/// `crazyflie_lib::subsystems::memory::CompressedStart`
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct CompressedStart {
    /// Meters
    pub x: f32,
    /// Meters
    pub y: f32,
    /// Meters
    pub z: f32,
    /// Radians
    pub yaw: f32,
}

impl CompressedStart {
    pub fn new(x: f32, y: f32, z: f32, yaw: f32) -> Self {
        Self { x, y, z, yaw }
    }
}

/// A compressed trajectory segment: the Bézier control points of each axis,
/// 0, 1, 3 or 7 of them, see `crazyflie_lib::subsystems::memory::CompressedSegment`
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct CompressedSegment {
    /// Seconds
    pub duration: f32,
    pub x: Vec<f32>,
    pub y: Vec<f32>,
    pub z: Vec<f32>,
    pub yaw: Vec<f32>,
}

impl CompressedSegment {
    /// Fails unless every axis has 0, 1, 3 or 7 control points
    pub fn new(duration: f32, x: Vec<f32>, y: Vec<f32>, z: Vec<f32>, yaw: Vec<f32>) -> Result<Self, String> {
        for axis in [&x, &y, &z, &yaw] {
            if ![0, 1, 3, 7].contains(&axis.len()) {
                return Err(format!("A compressed segment axis has {} control points, not 0, 1, 3 or 7", axis.len()));
            }
        }
        Ok(Self { duration, x, y, z, yaw })
    }
}

/// A Loco Positioning anchor, see `crazyflie_lib::subsystems::memory::LocoAnchorData`
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct LocoAnchorData {
    /// Meters
    pub position: [f32; 3],
    pub is_valid: bool,
}

/// The calibration of one of a base station's two sweeps, see
/// `crazyflie_lib::subsystems::memory::LighthouseCalibrationSweep`
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct LighthouseCalibrationSweep {
    pub phase: f32,
    pub tilt: f32,
    pub curve: f32,
    pub gibmag: f32,
    pub gibphase: f32,
    pub ogeemag: f32,
    pub ogeephase: f32,
}

/// One param or log variable
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct TocItem {
    pub name: String,
    /// Firmware type, e.g. `u8` or `f32`
    #[serde(rename = "type")]
    pub value_type: String,
    /// Always `true` for log variables
    pub writable: bool,
}

/// Default and stored value of a persistent param, see
/// `crazyflie_lib::subsystems::param::PersistentParamState`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct PersistentParamState {
    /// A value is stored in the EEPROM
    pub is_stored: bool,
    /// The firmware's default
    pub default_value: f64,
    /// The stored value, when `is_stored`
    pub stored_value: Option<f64>,
}

/// Radio link statistics, see `crazyflie_lib::subsystems::link_service::Statistics`.
/// Every field is `null` over USB, `rssi` also with a Crazyradio that doesn't measure it
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct LinkStatistics {
    /// Share of packets acked, 0 to 1
    pub link_quality: Option<f32>,
    /// Data packets sent per second, without the empty ones that poll the Crazyflie
    pub uplink_rate: Option<f32>,
    /// Packets received per second
    pub downlink_rate: Option<f32>,
    /// Packets sent per second, with the empty ones
    pub radio_send_rate: Option<f32>,
    /// Retries per acked packet
    pub avg_retries: Option<f32>,
    /// Share of acks where the nRF24 power detector triggered, 0 to 1
    pub power_detector_rate: Option<f32>,
    /// dBm, measured by the Crazyradio on acks
    pub rssi: Option<f32>,
}

/// A URI split into the key the server uses and the radio it pins, if any
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedUri {
    /// `radio:///<channel>/<datarate>/<ADDRESS>` for radio URIs, the URI itself otherwise
    pub key: String,
    /// Radio index given in the URI (`radio://1/…`), `None` for `radio:///…`
    pub radio: Option<usize>,
    /// Query string without the `?`, e.g. `timeout=3000`
    pub options: Option<String>,
}

impl NormalizedUri {
    /// The channel of a radio URI
    pub fn channel(&self) -> Option<u8> {
        self.key.strip_prefix("radio:///")?.split('/').next()?.parse().ok()
    }
}

/// Normalize a Crazyflie URI
///
/// Radio URIs drop the radio index and options and get the default datarate
/// (2M) and address filled in, so every way of writing the same Crazyflie
/// gives the same key. Other URIs (e.g. `usb://0`) are kept as they are.
///
/// ```
/// let n = crazyflie_connection_protocol::normalize_uri("radio://1/80/2M/e7e7e7e701?timeout=3000").unwrap();
/// assert_eq!(n.key, "radio:///80/2M/E7E7E7E701");
/// assert_eq!(n.radio, Some(1));
/// assert_eq!(n.options.as_deref(), Some("timeout=3000"));
/// ```
pub fn normalize_uri(uri: &str) -> Result<NormalizedUri, String> {
    let Some(rest) = uri.strip_prefix("radio://") else {
        return Ok(NormalizedUri { key: uri.to_owned(), radio: None, options: None });
    };
    let (path, options) = match rest.split_once('?') {
        Some((path, options)) => (path, Some(options.to_owned())),
        None => (rest, None),
    };
    let mut parts = path.split('/');
    let radio = match parts.next() {
        Some("") | None => None,
        Some(index) => Some(index.parse().map_err(|_| format!("Invalid radio index in {uri}"))?),
    };
    let channel: u8 = parts
        .next()
        .filter(|c| !c.is_empty())
        .ok_or_else(|| format!("Missing channel in {uri}"))?
        .parse()
        .map_err(|_| format!("Invalid channel in {uri}"))?;
    let datarate = match parts.next() {
        Some("") | None => "2M".to_owned(),
        Some(rate) => rate.to_uppercase(),
    };
    let address = match parts.next() {
        Some("") | None => DEFAULT_ADDRESS.to_owned(),
        Some(address) if address.len() == 10 && address.chars().all(|c| c.is_ascii_hexdigit()) => {
            address.to_uppercase()
        }
        Some(_) => return Err(format!("Invalid address in {uri}")),
    };
    Ok(NormalizedUri { key: format!("radio:///{channel}/{datarate}/{address}"), radio, options })
}

#[cfg(test)]
mod reference;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_json_shape() {
        let msg = ClientMessage {
            id: 7,
            request: Request::ParamSet { uri: "radio:///80/2M/E7E7E7E701".into(), name: "led.bitmask".into(), value: 3.0 },
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert_eq!(
            json,
            r#"{"id":7,"call":"param_set","args":{"uri":"radio:///80/2M/E7E7E7E701","name":"led.bitmask","value":3.0}}"#
        );
        let back: ClientMessage = serde_json::from_str(&json).unwrap();
        assert!(matches!(back.request, Request::ParamSet { .. }));

        let list: ClientMessage = serde_json::from_str(r#"{"id":1,"call":"list"}"#).unwrap();
        assert!(matches!(list.request, Request::List));
    }

    #[test]
    fn reply_json_shape() {
        let msg = ServerMessage::Reply { id: 7, ok: Some(Response::Done), error: None };
        assert_eq!(serde_json::to_string(&msg).unwrap(), r#"{"type":"reply","id":7,"ok":{"kind":"done"}}"#);
        let msg: ServerMessage = serde_json::from_str(r#"{"type":"reply","id":3,"error":"nope"}"#).unwrap();
        assert!(matches!(msg, ServerMessage::Reply { id: 3, ok: None, error: Some(_) }));
    }

    #[test]
    fn normalize() {
        let n = normalize_uri("radio:///80").unwrap();
        assert_eq!(n.key, "radio:///80/2M/E7E7E7E7E7");
        assert_eq!(n.radio, None);
        assert_eq!(normalize_uri("radio://0/80/2m/E7E7E7E701").unwrap().key, "radio:///80/2M/E7E7E7E701");
        assert_eq!(normalize_uri("usb://0").unwrap().key, "usb://0");
        assert!(normalize_uri("radio://x/80").is_err());
        assert!(normalize_uri("radio:///80/2M/E7").is_err());
        assert_eq!(normalize_uri("radio://1/88/2M/ABAD1DEA01").unwrap().channel(), Some(88));
        assert_eq!(normalize_uri("usb://0").unwrap().channel(), None);
    }

    #[test]
    fn memory_types_by_name() {
        let info: MemoryInfo = serde_json::from_str(r#"{"id":3,"type":"eeprom_config","size":21}"#).unwrap();
        assert_eq!(info.memory_type, MemoryType::EEPROMConfig);
        let info: MemoryInfo = serde_json::from_str(r#"{"id":4,"type":"something_new","size":1}"#).unwrap();
        assert_eq!(info.memory_type, MemoryType::UNKNOWN);
        assert_eq!(serde_json::to_string(&MemoryType::DeckCtrlDFU).unwrap(), r#""deck_ctrl_dfu""#);
    }

    #[test]
    fn platform_json_shape() {
        let msg = ClientMessage {
            id: 2,
            request: Request::Platform {
                uri: "radio:///80/2M/E7E7E7E7E7".into(),
                command: PlatformCommand::PowerOffStm32Domain,
                new_uri: None,
            },
        };
        assert_eq!(
            serde_json::to_string(&msg).unwrap(),
            r#"{"id":2,"call":"platform","args":{"uri":"radio:///80/2M/E7E7E7E7E7","command":"power_off_stm32_domain"}}"#
        );
    }
}

