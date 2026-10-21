//! The protocol reference in `docs/`, generated from the messages' JSON Schema
//! and the examples below
//!
//! `cargo test` writes `docs/protocol-reference.md` and
//! `docs/protocol.schema.json`, and fails when they changed so that they are
//! committed with the protocol change. Every call needs an example here, and
//! every reply and event has to be in one.

use super::*;
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;

const URI: &str = "radio:///80/2M/E7E7E7E7E7";
const URI2: &str = "radio:///80/2M/E7E7E7E701";
/// Where URI2 comes back after its channel was changed
const MOVED: &str = "radio:///90/2M/E7E7E7E701";

/// A call as the reference shows it: the request, its reply and the events on its stream
struct Example {
    request: Request,
    reply: Response,
    events: Vec<Event>,
}

fn call(request: Request, reply: Response) -> Example {
    Example { request, reply, events: Vec::new() }
}

fn stream(request: Request, reply: Response, events: Vec<Event>) -> Example {
    Example { request, reply, events }
}

fn uri() -> String {
    URI.to_owned()
}

/// A group of calls: its heading, a line about it and its calls in order
struct Group {
    title: &'static str,
    about: &'static str,
    examples: Vec<Example>,
}

/// The calls, in the order of the reference
fn groups() -> Vec<Group> {
    let cf_info = |uri: &str, clients: &[&str]| CfInfo {
        uri: uri.to_owned(),
        clients: clients.iter().map(|&client| client.to_owned()).collect(),
        radio: Some(0),
    };
    let toc = |name: &str, value_type: &str, writable: bool| TocItem {
        name: name.to_owned(),
        value_type: value_type.to_owned(),
        writable,
    };
    let attach_info = AttachInfo {
        uri: uri(),
        firmware_version: Some("2025.09".into()),
        device_type: Some("Crazyflie 2.1".into()),
        protocol_version: Some(10),
        params: vec![toc("led.bitmask", "u8", true), toc("pm.lowVoltage", "f32", true)],
        log_vars: vec![toc("pm.vbat", "f32", true)],
        memories: vec![
            MemoryInfo { id: 0, memory_type: MemoryType::EEPROMConfig, size: 21, serial: None },
            MemoryInfo { id: 1, memory_type: MemoryType::OneWire, size: 112, serial: Some("F1A2C3D4E5F60718".into()) },
        ],
    };
    let eeprom = EepromConfig {
        radio_channel: 80,
        radio_speed: 2,
        radio_address: "E7E7E7E7E7".into(),
        pitch_trim: 0.0,
        roll_trim: 0.0,
    };
    let geometry = LighthouseBsGeometry {
        origin: [-1.92, 2.13, 2.51],
        rotation_matrix: [[0.68, -0.48, 0.55], [0.47, 0.87, 0.16], [-0.56, 0.13, 0.82]],
        valid: true,
    };
    let sweep = LighthouseCalibrationSweep {
        phase: 0.0,
        tilt: -0.047,
        curve: 0.21,
        gibmag: -0.006,
        gibphase: 1.14,
        ogeemag: -0.12,
        ogeephase: 0.82,
    };
    let calibration =
        LighthouseBsCalibration { sweeps: [sweep, LighthouseCalibrationSweep { tilt: 0.046, ..sweep }], uid: 450938338, valid: true };
    let segment = Poly4D::new(
        2.0,
        Poly::from_slice(&[0.0, 0.0, 0.0, 0.0, 0.94, -0.47, 0.078]),
        Poly::default(),
        Poly::from_slice(&[0.5]),
        Poly::default(),
    );

    vec![
        Group {
            title: "Clients and Crazyflies",
            about: "Finding Crazyflies and attaching to them.",
            examples: vec![
                call(
                    Request::Hello { name: "fly.py".into() },
                    Response::Hello { server: "crazyflie-connection-server".into(), protocol: PROTOCOL_VERSION },
                ),
                call(Request::Scan { address: None }, Response::Scan { found: vec![cf_info(URI, &[])] }),
                call(
                    Request::ScanSelected { uris: vec![uri(), URI2.into()] },
                    Response::Scan { found: vec![cf_info(URI, &["swarmkeeper"])] },
                ),
                call(Request::List, Response::List { connected: vec![cf_info(URI, &["swarmkeeper", "fly.py"])] }),
                call(Request::Attach { uri: uri() }, Response::Attached(attach_info)),
                call(Request::Detach { uri: uri() }, Response::Done),
                call(
                    Request::LinkStatistics { uri: uri() },
                    Response::LinkStatistics(LinkStatistics {
                        link_quality: Some(0.98),
                        uplink_rate: Some(12.0),
                        downlink_rate: Some(105.0),
                        radio_send_rate: Some(110.0),
                        avg_retries: Some(0.05),
                        power_detector_rate: Some(0.0),
                        rssi: Some(-48.0),
                    }),
                ),
            ],
        },
        Group {
            title: "Params",
            about: "Params are numbers on the wire, whatever their type in the firmware.",
            examples: vec![
                call(Request::ParamGet { uri: uri(), name: "pm.lowVoltage".into() }, Response::Value { value: 3.2 }),
                call(Request::ParamSet { uri: uri(), name: "led.bitmask".into(), value: 3.0 }, Response::Done),
                stream(
                    Request::ParamWatch { uri: uri(), sub: 1 },
                    Response::Done,
                    vec![Event::ParamChanged { name: "led.bitmask".into(), value: 3.0 }],
                ),
                call(
                    Request::ParamIsPersistent { uri: uri(), name: "ring.effect".into() },
                    Response::Bool { value: true },
                ),
                call(
                    Request::ParamPersistentState { uri: uri(), name: "ring.effect".into() },
                    Response::PersistentState(PersistentParamState {
                        is_stored: true,
                        default_value: 6.0,
                        stored_value: Some(7.0),
                    }),
                ),
                call(Request::ParamPersistentStore { uri: uri(), name: "ring.effect".into() }, Response::Done),
                call(Request::ParamPersistentClear { uri: uri(), name: "ring.effect".into() }, Response::Done),
            ],
        },
        Group {
            title: "Streams",
            about: "Each of these opens a stream with the `sub` it is given; [`unsubscribe`](#unsubscribe) closes it.",
            examples: vec![
                stream(
                    Request::LogSubscribe {
                        uri: uri(),
                        sub: 2,
                        vars: vec!["pm.vbat".into(), "stateEstimate.z".into()],
                        period_ms: 100,
                    },
                    Response::Done,
                    vec![
                        Event::Log {
                            timestamp: 123400,
                            values: [("pm.vbat".into(), 3.92), ("stateEstimate.z".into(), 0.51)].into(),
                        },
                        Event::Log {
                            timestamp: 123500,
                            values: [("pm.vbat".into(), 3.91), ("stateEstimate.z".into(), 0.5)].into(),
                        },
                        Event::Ended { reason: "Crazyflie disconnected".into() },
                    ],
                ),
                stream(
                    Request::ConsoleSubscribe { uri: uri(), sub: 3, history: true },
                    Response::Done,
                    vec![Event::Console { line: "SYS: Crazyflie 2.1 is up and running!".into() }],
                ),
                stream(
                    Request::LighthouseAngleSubscribe { uri: uri(), sub: 4 },
                    Response::Done,
                    vec![Event::LighthouseAngles {
                        base_station: 0,
                        x: [-0.312, -0.309, -0.317, -0.315],
                        y: [0.127, 0.131, 0.129, 0.133],
                    }],
                ),
                call(Request::Unsubscribe { sub: 2 }, Response::Done),
            ],
        },
        Group {
            title: "Flying",
            about: "The replies come when the commands are sent; the Crazyflie flies them on its own.",
            examples: vec![
                call(
                    Request::TakeOff { uri: uri(), height: 0.5, yaw: None, duration: 2.0, group_mask: None },
                    Response::Done,
                ),
                call(
                    Request::Land { uri: uri(), height: 0.0, yaw: None, duration: 2.0, group_mask: None },
                    Response::Done,
                ),
                call(Request::Stop { uri: uri(), group_mask: None }, Response::Done),
                call(
                    Request::GoTo {
                        uri: uri(),
                        x: 1.0,
                        y: 0.0,
                        z: 0.5,
                        yaw: 0.0,
                        duration: 3.0,
                        relative: false,
                        linear: false,
                        group_mask: None,
                    },
                    Response::Done,
                ),
                call(
                    Request::DefineTrajectory { uri: uri(), id: 1, memory_offset: 0, num_pieces: 1, trajectory_type: None },
                    Response::Done,
                ),
                call(
                    Request::StartTrajectory {
                        uri: uri(),
                        id: 1,
                        time_scale: 1.0,
                        relative_position: true,
                        relative_yaw: false,
                        reversed: false,
                        group_mask: None,
                    },
                    Response::Done,
                ),
                call(
                    Request::SetpointRpyt { uri: uri(), roll: 0.0, pitch: 0.0, yawrate: 0.0, thrust: 0 },
                    Response::Done,
                ),
                call(Request::NotifySetpointStop { uri: uri(), remain_valid_ms: 0 }, Response::Done),
                call(Request::ArmingRequest { uri: uri(), arm: true }, Response::Done),
                call(Request::EmergencyStop { uri: uri() }, Response::Done),
                call(Request::CrashRecovery { uri: uri() }, Response::Done),
            ],
        },
        Group {
            title: "Memories",
            about: "Memories are addressed by the `id` in the [`attached`](#attached-reply) reply's `memories`.",
            examples: vec![
                call(
                    Request::MemoryRead { uri: uri(), id: 1, address: 0, length: 4 },
                    Response::Data { data: "6wAAAA==".into() },
                ),
                call(Request::MemoryWrite { uri: uri(), id: 1, address: 0, data: "AQID".into() }, Response::Done),
                call(
                    Request::OneWireRead { uri: uri(), id: 1 },
                    Response::OneWire(OneWireMemory {
                        used_pins: 0x0c,
                        vid: 0xbc,
                        pid: 0x10,
                        elements: [("boardName".into(), "bcLighthouse4".into()), ("revision".into(), "C".into())].into(),
                    }),
                ),
                call(Request::EepromConfigRead { uri: uri(), id: 0 }, Response::EepromConfig(eeprom.clone())),
                call(
                    Request::EepromConfigWrite { uri: uri(), id: 0, config: EepromConfig { radio_channel: 90, ..eeprom } },
                    Response::Done,
                ),
                call(
                    Request::LighthouseGeometryRead { uri: uri(), id: 2, bs: 0 },
                    Response::LighthouseGeometry(geometry.clone()),
                ),
                call(Request::LighthouseGeometryWrite { uri: uri(), id: 2, bs: 0, geometry }, Response::Done),
                call(
                    Request::LighthouseCalibrationRead { uri: uri(), id: 2, bs: 0 },
                    Response::LighthouseCalibration(calibration.clone()),
                ),
                call(Request::LighthouseCalibrationWrite { uri: uri(), id: 2, bs: 0, calibration }, Response::Done),
                call(
                    Request::TrajectoryWriteUncompressed { uri: uri(), id: 3, start_addr: 0, segments: vec![segment] },
                    Response::Written { bytes: 132 },
                ),
                call(
                    Request::TrajectoryWriteCompressed {
                        uri: uri(),
                        id: 3,
                        start_addr: 0,
                        start: CompressedStart::new(0.0, 0.0, 0.5, 0.0),
                        segments: vec![
                            CompressedSegment::new(2.0, vec![1.0], vec![], vec![], vec![]).expect("valid segment"),
                        ],
                    },
                    Response::Written { bytes: 13 },
                ),
                call(
                    Request::LocoReadIdList { uri: uri(), id: 4, active: false },
                    Response::Ids { ids: vec![0, 1, 2, 3, 4, 5, 6, 7] },
                ),
                call(
                    Request::LocoReadAnchor { uri: uri(), id: 4, anchor: 0 },
                    Response::LocoAnchor(LocoAnchorData { position: [0.0, 0.0, 0.2], is_valid: true }),
                ),
            ],
        },
        Group {
            title: "Positioning",
            about: "Lighthouse and Loco Positioning system setup through a Crazyflie.",
            examples: vec![
                call(
                    Request::SendShortLppPacket {
                        uri: uri(),
                        dest_id: 0,
                        data: vec![1, 0, 0, 0, 0, 0, 0, 0, 0, 0xcd, 0xcc, 0x4c, 0x3e],
                    },
                    Response::Done,
                ),
                call(
                    Request::LighthousePersist { uri: uri(), geometries: vec![0, 1], calibrations: vec![0, 1] },
                    Response::Bool { value: true },
                ),
            ],
        },
        Group {
            title: "Jobs and power",
            about: "Flashing, rebooting and powering Crazyflies, and watching which of them jobs take over.",
            examples: vec![
                stream(
                    Request::Flash {
                        uris: vec![uri(), URI2.into()],
                        sub: 5,
                        images: vec![FlashImage { target: "stm32".into(), start: None, data: "AAAAIA==".into() }],
                    },
                    Response::Done,
                    vec![
                        Event::Progress {
                            uri: Some(uri()),
                            step: "Restarting into the bootloader".into(),
                            done: 0,
                            total: 1,
                        },
                        Event::Progress {
                            uri: None,
                            step: "Broadcasting stm32 to 2 Crazyflies".into(),
                            done: 81920,
                            total: 245760,
                        },
                        Event::Flashed { uri: uri(), how: "broadcast".into() },
                        Event::FlashFailed {
                            uri: URI2.into(),
                            reason: "Restarting into the firmware: no answer; left in its bootloader at E7E7E7E701"
                                .into(),
                        },
                        Event::Done,
                    ],
                ),
                stream(
                    Request::Watch { sub: 6 },
                    Response::Done,
                    vec![
                        Event::Busy { uri: uri(), reason: format!("{URI} is being flashed by fly.py") },
                        Event::Available { uri: uri(), connected: true, moved_to: None },
                        Event::Busy { uri: URI2.into(), reason: format!("{URI2} is restarting, rebooted by fly.py") },
                        Event::Available { uri: URI2.into(), connected: false, moved_to: Some(MOVED.into()) },
                    ],
                ),
                call(
                    Request::Platform { uri: URI2.into(), command: PlatformCommand::Reboot, new_uri: Some(MOVED.into()) },
                    Response::Done,
                ),
            ],
        },
    ]
}

/// The JSON Schemas of the client's and the server's messages, with their
/// definitions merged
struct Schema {
    client: Value,
    server: Value,
    defs: Map<String, Value>,
}

impl Schema {
    fn new() -> Self {
        let mut client = serde_json::to_value(schemars::schema_for!(ClientMessage)).unwrap();
        let mut server = serde_json::to_value(schemars::schema_for!(ServerMessage)).unwrap();
        let mut defs = Map::new();
        for schema in [&mut client, &mut server] {
            let schema = schema.as_object_mut().unwrap();
            schema.remove("$schema");
            schema.remove("title");
            if let Some(Value::Object(own)) = schema.remove("$defs") {
                for (name, def) in own {
                    if let Some(existing) = defs.get(&name) {
                        assert_eq!(existing, &def, "{name} has two definitions");
                    }
                    defs.insert(name, def);
                }
            }
        }
        Self { client, server, defs }
    }

    /// The variants of a tagged enum's schema, by tag
    fn variants<'a>(schema: &'a Value, tag: &str) -> Vec<(&'a str, &'a Value)> {
        schema["oneOf"]
            .as_array()
            .unwrap()
            .iter()
            .map(|variant| (variant["properties"][tag]["const"].as_str().unwrap(), variant))
            .collect()
    }

    fn calls(&self) -> Vec<(&str, &Value)> {
        Self::variants(&self.client, "call")
    }

    fn replies(&self) -> Vec<(&str, &Value)> {
        Self::variants(&self.defs["Response"], "kind")
    }

    fn events(&self) -> Vec<(&str, &Value)> {
        Self::variants(&self.defs["Event"], "kind")
    }

    /// The schema file: both directions' messages and their definitions
    fn file(&self) -> String {
        let mut defs = Map::new();
        defs.insert("ClientMessage".into(), self.client.clone());
        defs.insert("ServerMessage".into(), self.server.clone());
        defs.extend(self.defs.clone());
        let mut file = json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Crazyflie connection protocol",
            "description": format!(
                "Version {PROTOCOL_VERSION}. The messages between the Crazyflie connection server and its clients, \
                 each one JSON text frame on a WebSocket. Generated from crates/protocol/src/lib.rs"
            ),
            "anyOf": [{"$ref": "#/$defs/ClientMessage"}, {"$ref": "#/$defs/ServerMessage"}],
            "$defs": defs,
        });
        plain_descriptions(&mut file);
        serde_json::to_string_pretty(&file).unwrap() + "\n"
    }
}

/// The descriptions of a schema without rustdoc links
fn plain_descriptions(schema: &mut Value) {
    match schema {
        Value::Object(map) => {
            for (key, value) in map.iter_mut() {
                match value {
                    Value::String(text) if key == "description" => *text = doc(text, false),
                    _ => plain_descriptions(value),
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(plain_descriptions),
        _ => {}
    }
}

/// `PowerOnStm32Domain` as `power_on_stm32_domain`, as serde names variants
fn snake_case(name: &str) -> String {
    let mut snake = String::new();
    for (i, c) in name.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            snake.push('_');
        }
        snake.push(c.to_ascii_lowercase());
    }
    snake
}

/// A doc comment as text for the reference (`links`) or the schema: rustdoc
/// links become wire names, and lines are joined into paragraphs
fn doc(text: &str, links: bool) -> String {
    let link = |target: &str| -> String {
        let (text, anchor) = match target.split_once("::") {
            Some(("Request", variant)) => (format!("`{}`", snake_case(variant)), snake_case(variant)),
            Some(("Response", variant)) => (format!("`{}` reply", snake_case(variant)), format!("{}-reply", snake_case(variant))),
            Some(("Event", variant)) => (format!("`{}` event", snake_case(variant)), format!("{}-event", snake_case(variant))),
            Some((ty, member)) => (format!("`{}`", snake_case(member)), ty.to_lowercase()),
            None if target == "normalize_uri" => ("URIs".to_owned(), "protocol.md#uris".to_owned()),
            None if target == "PROTOCOL_VERSION" => return format!("`{PROTOCOL_VERSION}`"),
            None => (format!("`{target}`"), target.to_lowercase()),
        };
        match links {
            true if anchor.contains(".md") => format!("[{text}]({anchor})"),
            true => format!("[{text}](#{anchor})"),
            false => text,
        }
    };

    let mut out = String::new();
    let mut rest = text;
    while let Some(start) = rest.find('[') {
        let (before, from) = rest.split_at(start);
        out.push_str(before);
        let target = from[1..].split(']').next().unwrap_or("");
        let is_link = !target.is_empty()
            && from.len() > target.len() + 1
            && target.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
            && !from[target.len() + 2..].starts_with('(');
        if is_link {
            out.push_str(&link(target));
            rest = &from[target.len() + 2..];
        } else {
            out.push('[');
            rest = &from[1..];
        }
    }
    out.push_str(rest);
    out.split("\n\n").map(|paragraph| paragraph.replace('\n', " ")).collect::<Vec<_>>().join("\n\n")
}

fn description(schema: &Value) -> Option<String> {
    schema.get("description").and_then(Value::as_str).map(|text| doc(text, true))
}

/// A description as a paragraph, ending with a full stop
fn paragraph(text: &str) -> String {
    match text.ends_with(['.', ':', '!', '?']) {
        true => text.to_owned(),
        false => format!("{text}."),
    }
}

/// A field's type, e.g. `u8`, `number[3]`, or a link to its definition
fn type_name(schema: &Value) -> String {
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        return match reference.trim_start_matches("#/$defs/") {
            "Response" => "[reply](#replies)".to_owned(),
            "Event" => "[event](#events)".to_owned(),
            name => format!("[`{name}`](#{})", name.to_lowercase()),
        };
    }
    if let Some(any) = schema.get("anyOf").and_then(Value::as_array) {
        let types: Vec<String> = any.iter().filter(|s| s["type"] != "null").map(type_name).collect();
        return types.join(" or ");
    }
    let ty = match &schema["type"] {
        Value::String(ty) => ty.as_str(),
        Value::Array(types) => types.iter().filter_map(Value::as_str).find(|&ty| ty != "null").unwrap_or("null"),
        _ => "any",
    };
    match ty {
        "integer" => match schema.get("format").and_then(Value::as_str) {
            Some(format @ ("uint8" | "uint16" | "uint32" | "uint64")) => format!("u{}", &format[4..]),
            _ => "integer".to_owned(),
        },
        "array" => {
            let items = type_name(&schema["items"]);
            match (schema.get("minItems"), schema.get("maxItems")) {
                (Some(min), Some(max)) if min == max => format!("{items}[{min}]"),
                _ => format!("{items}[]"),
            }
        }
        "object" => match schema.get("additionalProperties") {
            Some(values @ Value::Object(_)) => format!("object of {}", type_name(values)),
            _ => "object".to_owned(),
        },
        ty => ty.to_owned(),
    }
}

/// A table of an object schema's fields, without `skip` (the tag)
fn fields(out: &mut String, heading: &str, schema: &Value, skip: &str) {
    let Some(properties) = schema.get("properties").and_then(Value::as_object) else { return };
    let required: Vec<&str> =
        schema.get("required").and_then(Value::as_array).map(|r| r.iter().filter_map(Value::as_str).collect()).unwrap_or_default();
    let rows: Vec<(String, String, String)> = properties
        .iter()
        .filter(|(name, _)| *name != skip)
        .map(|(name, field)| {
            let mut ty = type_name(field);
            if !required.contains(&name.as_str()) {
                ty.push_str(" (optional)");
            }
            let description = match (description(field), name.as_str()) {
                (Some(text), _) => text.replace('\n', " "),
                (None, "uri") if heading == "Argument" => "The Crazyflie, see [URIs](protocol.md#uris)".to_owned(),
                (None, "sub") if heading == "Argument" => {
                    "Stream number, picked by the client, see [streams](protocol.md#streams)".to_owned()
                }
                (None, _) => String::new(),
            };
            (format!("`{name}`"), ty, description)
        })
        .collect();
    if rows.is_empty() {
        return;
    }
    if rows.iter().any(|(_, _, description)| !description.is_empty()) {
        writeln!(out, "| {heading} | Type | Description |\n|---|---|---|").unwrap();
        for (name, ty, description) in rows {
            writeln!(out, "| {name} | {ty} | {description} |").unwrap();
        }
    } else {
        writeln!(out, "| {heading} | Type |\n|---|---|").unwrap();
        for (name, ty, _) in rows {
            writeln!(out, "| {name} | {ty} |").unwrap();
        }
    }
    out.push('\n');
}

/// The fields of a reply or event, or the definition a newtype variant has them from
fn variant_fields(out: &mut String, schema: &Value) {
    if schema.get("$ref").is_some() {
        writeln!(out, "The fields of {}.\n", type_name(schema)).unwrap();
    } else if schema["properties"].as_object().is_some_and(|properties| properties.len() > 1) {
        fields(out, "Field", schema, "kind");
    } else {
        writeln!(out, "No fields.\n").unwrap();
    }
}

fn kind(value: &impl Serialize, tag: &str) -> String {
    serde_json::to_value(value).unwrap()[tag].as_str().unwrap().to_owned()
}

fn links(names: &[String], suffix: &str) -> String {
    names.iter().map(|name| format!("[`{name}`](#{name}{suffix})")).collect::<Vec<_>>().join(", ")
}

/// The event kinds a call's stream sends: those of its example, and `ended`
/// on the streams of a Crazyflie
fn event_kinds(example: &Example) -> Vec<String> {
    let mut kinds: Vec<String> = Vec::new();
    for event in &example.events {
        let event = kind(event, "kind");
        if !kinds.contains(&event) {
            kinds.push(event);
        }
    }
    let args = &serde_json::to_value(&example.request).unwrap()["args"];
    if args.get("uri").is_some() && args.get("sub").is_some() && !kinds.iter().any(|k| k == "ended") {
        kinds.push("ended".to_owned());
    }
    kinds
}

/// The example's messages, as they go over the WebSocket
fn transcript(id: u64, example: &Example) -> String {
    let mut lines = Vec::new();
    let request = ClientMessage { id, request: example.request.clone() };
    lines.push(format!("→ {}", serde_json::to_string(&request).unwrap()));
    let reply = ServerMessage::Reply { id, ok: Some(example.reply.clone()), error: None };
    lines.push(format!("← {}", serde_json::to_string(&reply).unwrap()));
    let sub = serde_json::to_value(&example.request).unwrap()["args"]["sub"].as_u64();
    for event in &example.events {
        let event = ServerMessage::Event { sub: sub.expect("a stream's request has a sub"), event: event.clone() };
        lines.push(format!("← {}", serde_json::to_string(&event).unwrap()));
    }
    format!("```text\n{}\n```\n", lines.join("\n"))
}

/// docs/protocol-reference.md
fn reference(schema: &Schema) -> String {
    let groups = groups();
    let calls: BTreeMap<&str, &Value> = schema.calls().into_iter().collect();
    // The calls that reply each reply kind, and that send each event kind
    let mut replied_by: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut sent_on: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for example in groups.iter().flat_map(|group| &group.examples) {
        let call = kind(&example.request, "call");
        replied_by.entry(kind(&example.reply, "kind")).or_default().push(call.clone());
        for event in event_kinds(example) {
            sent_on.entry(event).or_default().push(call.clone());
        }
    }

    let mut out = String::new();
    writeln!(
        out,
        "# Protocol reference\n\n\
         <!-- Generated from crates/protocol/src/lib.rs by `cargo test -p crazyflie-connection-protocol`. \
         Change the code, not this file -->\n\n\
         Protocol version {PROTOCOL_VERSION}. [The protocol guide](protocol.md) explains how requests, replies and \
         streams work; [protocol.schema.json](protocol.schema.json) has the same messages as JSON Schema.\n\n\
         Types: `u8` to `u64` are unsigned integers of that many bits, `T[]` is an array and `T[4]` one of exactly 4. \
         An optional field can be left out or `null`. The examples show the messages as they go over the WebSocket: \
         `→` from the client, `←` from the server.\n"
    )
    .unwrap();

    writeln!(out, "| | |\n|---|---|").unwrap();
    for group in &groups {
        let names: Vec<String> = group.examples.iter().map(|example| kind(&example.request, "call")).collect();
        writeln!(out, "| [{}](#{}) | {} |", group.title, group.title.to_lowercase().replace(' ', "-"), links(&names, "")).unwrap();
    }
    let replies: Vec<String> = schema.replies().iter().map(|(name, _)| name.to_string()).collect();
    let events: Vec<String> = schema.events().iter().map(|(name, _)| name.to_string()).collect();
    let mut types: Vec<&String> = type_defs(schema);
    types.sort();
    writeln!(out, "| [Replies](#replies) | {} |", links(&replies, "-reply")).unwrap();
    writeln!(out, "| [Events](#events) | {} |", links(&events, "-event")).unwrap();
    writeln!(
        out,
        "| [Types](#types) | {} |\n",
        types.iter().map(|name| format!("[`{name}`](#{})", name.to_lowercase())).collect::<Vec<_>>().join(", ")
    )
    .unwrap();

    writeln!(out, "## Messages\n").unwrap();
    writeln!(
        out,
        "A client sends requests: `{{\"id\": …, \"call\": \"<call>\", \"args\": {{…}}}}`, with `args` left out for \
         a call without any.\n"
    )
    .unwrap();
    writeln!(out, "| Field | Type | Description |\n|---|---|---|").unwrap();
    let id = &schema.client["properties"]["id"];
    writeln!(out, "| `id` | {} | {} |", type_name(id), description(id).unwrap_or_default()).unwrap();
    writeln!(out, "| `call` | string | One of the [calls](#clients-and-crazyflies) below |").unwrap();
    writeln!(out, "| `args` | object | The call's arguments |\n").unwrap();
    writeln!(out, "The server sends three kinds of messages, told apart by their `type`.\n").unwrap();
    for (name, message) in Schema::variants(&schema.server, "type") {
        writeln!(out, "### `{name}`\n").unwrap();
        if let Some(text) = description(message) {
            writeln!(out, "{}\n", paragraph(&text)).unwrap();
        }
        fields(&mut out, "Field", message, "type");
    }

    let mut id = 0;
    for group in &groups {
        writeln!(out, "## {}\n\n{}\n", group.title, group.about).unwrap();
        for example in &group.examples {
            id += 1;
            let name = kind(&example.request, "call");
            let call = calls[name.as_str()];
            writeln!(out, "### `{name}`\n").unwrap();
            if let Some(text) = description(call) {
                writeln!(out, "{}\n", paragraph(&text)).unwrap();
            }
            match call["properties"].get("args") {
                Some(args) => fields(&mut out, "Argument", args, ""),
                None => writeln!(out, "No arguments.\n").unwrap(),
            }
            let reply = kind(&example.reply, "kind");
            writeln!(out, "Reply: {}\n", links(&[reply], "-reply")).unwrap();
            let events = event_kinds(example);
            if !events.is_empty() {
                writeln!(out, "Events on `sub`: {}\n", links(&events, "-event")).unwrap();
            }
            out.push_str(&transcript(id, example));
            out.push('\n');
        }
    }

    writeln!(out, "## Replies\n\nThe `ok` of a [`reply`](#reply). Its `kind` tells which.\n").unwrap();
    for (name, reply) in schema.replies() {
        writeln!(out, "### `{name}` reply\n").unwrap();
        if let Some(text) = description(reply) {
            writeln!(out, "{}\n", paragraph(&text)).unwrap();
        }
        variant_fields(&mut out, reply);
        writeln!(out, "Replies to {}.\n", links(&replied_by[name], "")).unwrap();
    }

    writeln!(out, "## Events\n\nThe `event` of an [`event`](#event) message. Its `kind` tells which.\n").unwrap();
    for (name, event) in schema.events() {
        writeln!(out, "### `{name}` event\n").unwrap();
        if let Some(text) = description(event) {
            writeln!(out, "{}\n", paragraph(&text)).unwrap();
        }
        variant_fields(&mut out, event);
        writeln!(out, "On the streams of {}.\n", links(&sent_on[name], "")).unwrap();
    }

    writeln!(out, "## Types\n").unwrap();
    for name in types {
        let def = &schema.defs[name];
        writeln!(out, "### `{name}`\n").unwrap();
        if let Some(text) = description(def) {
            writeln!(out, "{}\n", paragraph(&text)).unwrap();
        }
        if let Some(values) = def.get("enum").and_then(Value::as_array) {
            let values: Vec<String> = values.iter().map(|value| format!("`{value}`")).collect();
            writeln!(out, "A string: {}.\n", values.join(", ")).unwrap();
        } else if let Some(variants) = def.get("oneOf").and_then(Value::as_array) {
            // Named values, those with a description each on their own
            if variants.iter().all(|variant| variant.get("const").is_some() || variant.get("enum").is_some()) {
                writeln!(out, "| Value | Description |\n|---|---|").unwrap();
                for variant in variants {
                    let text = description(variant).unwrap_or_default().replace('\n', " ");
                    let values = match variant.get("enum").and_then(Value::as_array) {
                        Some(values) => values.clone(),
                        None => vec![variant["const"].clone()],
                    };
                    for value in values {
                        writeln!(out, "| `{value}` | {text} |").unwrap();
                    }
                }
                out.push('\n');
            } else {
                writeln!(out, "One of:\n").unwrap();
                for variant in variants {
                    let shape: Vec<String> = variant["properties"]
                        .as_object()
                        .unwrap()
                        .iter()
                        .map(|(field, schema)| format!("\"{field}\": {}", type_name(schema)))
                        .collect();
                    writeln!(out, "- `{{{}}}`", shape.join(", ")).unwrap();
                }
                out.push('\n');
            }
        } else {
            fields(&mut out, "Field", def, "");
        }
    }
    out.truncate(out.trim_end().len());
    out.push('\n');
    out
}

/// The definitions the Types section lists: all but the reply and event payloads
fn type_defs(schema: &Schema) -> Vec<&String> {
    schema.defs.keys().filter(|name| !matches!(name.as_str(), "Response" | "Event")).collect()
}

#[test]
fn every_call_reply_and_event_has_an_example() {
    let schema = Schema::new();
    let groups = groups();
    let examples: Vec<&Example> = groups.iter().flat_map(|group| &group.examples).collect();
    let calls: Vec<String> = examples.iter().map(|example| kind(&example.request, "call")).collect();
    for (call, _) in schema.calls() {
        let count = calls.iter().filter(|c| *c == call).count();
        assert_eq!(count, 1, "`{call}` needs one example in reference.rs, it has {count}");
    }
    let replies: Vec<String> = examples.iter().map(|example| kind(&example.reply, "kind")).collect();
    for (reply, _) in schema.replies() {
        assert!(replies.iter().any(|r| r == reply), "No example in reference.rs replies `{reply}`");
    }
    let events: Vec<String> = examples.iter().flat_map(|example| &example.events).map(|event| kind(event, "kind")).collect();
    for (event, _) in schema.events() {
        assert!(events.iter().any(|e| e == event), "No example in reference.rs has the `{event}` event");
    }
}

#[test]
fn examples_read_back() {
    fn same<T: Serialize + for<'de> Deserialize<'de>>(value: &T) {
        let json = serde_json::to_string(value).unwrap();
        let back: T = serde_json::from_str(&json).unwrap();
        assert_eq!(serde_json::to_string(&back).unwrap(), json);
    }
    for example in groups().iter().flat_map(|group| &group.examples) {
        same(&ClientMessage { id: 1, request: example.request.clone() });
        same(&ServerMessage::Reply { id: 1, ok: Some(example.reply.clone()), error: None });
        for event in &example.events {
            same(&ServerMessage::Event { sub: 1, event: event.clone() });
        }
    }
}

#[test]
fn docs_are_up_to_date() {
    let docs = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs");
    let schema = Schema::new();
    let mut changed = Vec::new();
    for (name, text) in [("protocol-reference.md", reference(&schema)), ("protocol.schema.json", schema.file())] {
        let path = docs.join(name);
        if std::fs::read_to_string(&path).ok().as_deref() != Some(text.as_str()) {
            std::fs::create_dir_all(&docs).unwrap();
            std::fs::write(&path, text).unwrap();
            changed.push(format!("docs/{name}"));
        }
    }
    assert!(changed.is_empty(), "The protocol changed, so {} were written again. Check and commit them", changed.join(" and "));
}
