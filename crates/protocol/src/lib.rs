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
pub struct ClientMessage {
    /// Picked by the client, echoed in the reply
    pub id: u64,
    #[serde(flatten)]
    pub request: Request,
}

/// Request payloads
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "call", content = "args", rename_all = "snake_case")]
pub enum Request {
    /// Name this client, shown to other clients in scan and list results
    Hello { name: String },
    /// Scan all channels for Crazyflies at an address (default E7E7E7E7E7)
    Scan { address: Option<String> },
    /// List the Crazyflies the server is connected to
    List,
    /// Attach to a Crazyflie, connecting it if no other client has
    Attach { uri: String },
    /// Detach from a Crazyflie. It is disconnected when its last client detaches
    Detach { uri: String },

    ParamGet { uri: String, name: String },
    ParamSet { uri: String, name: String, value: f64 },
    /// Stream of [Event::ParamChanged] for every param set by any client
    ParamWatch { uri: String, sub: u64 },
    /// Default and stored value of a persistent param
    ParamPersistentState { uri: String, name: String },
    /// Store the current value of a persistent param in the Crazyflie's EEPROM
    ParamPersistentStore { uri: String, name: String },
    /// Clear the stored value of a persistent param
    ParamPersistentClear { uri: String, name: String },

    /// Stream of [Event::Log] with the given variables
    LogSubscribe { uri: String, sub: u64, vars: Vec<String>, period_ms: u64 },

    /// Stream of [Event::Console] lines, optionally starting with the history
    ConsoleSubscribe { uri: String, sub: u64, history: bool },

    /// Stop a stream opened with `sub`
    Unsubscribe { sub: u64 },

    TakeOff { uri: String, height: f32, yaw: Option<f32>, duration: f32, group_mask: Option<u8> },
    Land { uri: String, height: f32, yaw: Option<f32>, duration: f32, group_mask: Option<u8> },
    Stop { uri: String, group_mask: Option<u8> },
    GoTo {
        uri: String,
        x: f32,
        y: f32,
        z: f32,
        yaw: f32,
        duration: f32,
        relative: bool,
        linear: bool,
        group_mask: Option<u8>,
    },

    ArmingRequest { uri: String, arm: bool },
    EmergencyStop { uri: String },

    LinkStatistics { uri: String },
}

/// A message from the server
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Response {
    Done,
    Hello { server: String, protocol: u32 },
    Scan { found: Vec<CfInfo> },
    List { connected: Vec<CfInfo> },
    Attached(AttachInfo),
    Value { value: f64 },
    PersistentState(PersistentParamState),
    LinkStatistics(LinkStatistics),
}

/// Stream payloads
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Event {
    ParamChanged { name: String, value: f64 },
    Log { timestamp: u32, values: BTreeMap<String, f64> },
    Console { line: String },
    /// The stream ended on the server side (disconnect, log error, …)
    Ended { reason: String },
}

/// A Crazyflie found by a scan or connected on the server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CfInfo {
    /// Normalized URI, see [normalize_uri]
    pub uri: String,
    /// Names of the clients attached to it
    pub clients: Vec<String>,
}

/// What a client learns when it attaches
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttachInfo {
    pub uri: String,
    pub firmware_version: Option<String>,
    pub device_type: Option<String>,
    #[serde(default)]
    pub protocol_version: Option<u8>,
    pub params: Vec<TocItem>,
    pub log_vars: Vec<TocItem>,
}

/// One param or log variable
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TocItem {
    pub name: String,
    /// Firmware type, e.g. `u8` or `f32`
    #[serde(rename = "type")]
    pub value_type: String,
    /// Always `true` for log variables
    pub writable: bool,
    /// Param that can be stored in the EEPROM. Always `false` for log variables
    #[serde(default)]
    pub persistent: bool,
}

/// Default and stored value of a persistent param, see
/// `crazyflie_lib::subsystems::param::PersistentParamState`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentParamState {
    pub is_stored: bool,
    pub default_value: f64,
    pub stored_value: Option<f64>,
}

/// Radio link statistics, see `crazyflie_lib::subsystems::link_service::Statistics`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LinkStatistics {
    pub link_quality: Option<f32>,
    pub uplink_rate: Option<f32>,
    pub downlink_rate: Option<f32>,
    pub radio_send_rate: Option<f32>,
    pub avg_retries: Option<f32>,
    pub power_detector_rate: Option<f32>,
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
    }
}
