# Protocol reference

<!-- Generated from crates/protocol/src/lib.rs by `cargo test -p crazyflie-connection-protocol`. Change the code, not this file -->

Protocol version 1. [The protocol guide](protocol.md) explains how requests, replies and streams work; [protocol.schema.json](protocol.schema.json) has the same messages as JSON Schema.

Types: `u8` to `u64` are unsigned integers of that many bits, `T[]` is an array and `T[4]` one of exactly 4. An optional field can be left out or `null`. The examples show the messages as they go over the WebSocket: `→` from the client, `←` from the server.

| | |
|---|---|
| [Clients and Crazyflies](#clients-and-crazyflies) | [`hello`](#hello), [`scan`](#scan), [`scan_selected`](#scan_selected), [`list`](#list), [`radios`](#radios), [`attach`](#attach), [`detach`](#detach), [`link_statistics`](#link_statistics) |
| [Params](#params) | [`param_get`](#param_get), [`param_set`](#param_set), [`param_watch`](#param_watch), [`param_is_persistent`](#param_is_persistent), [`param_persistent_state`](#param_persistent_state), [`param_persistent_store`](#param_persistent_store), [`param_persistent_clear`](#param_persistent_clear) |
| [Streams](#streams) | [`log_subscribe`](#log_subscribe), [`console_subscribe`](#console_subscribe), [`lighthouse_angle_subscribe`](#lighthouse_angle_subscribe), [`unsubscribe`](#unsubscribe) |
| [Flying](#flying) | [`take_off`](#take_off), [`land`](#land), [`stop`](#stop), [`go_to`](#go_to), [`define_trajectory`](#define_trajectory), [`start_trajectory`](#start_trajectory), [`setpoint_rpyt`](#setpoint_rpyt), [`notify_setpoint_stop`](#notify_setpoint_stop), [`arming_request`](#arming_request), [`emergency_stop`](#emergency_stop), [`crash_recovery`](#crash_recovery) |
| [Memories](#memories) | [`memory_read`](#memory_read), [`memory_write`](#memory_write), [`one_wire_read`](#one_wire_read), [`eeprom_config_read`](#eeprom_config_read), [`eeprom_config_write`](#eeprom_config_write), [`lighthouse_geometry_read`](#lighthouse_geometry_read), [`lighthouse_geometry_write`](#lighthouse_geometry_write), [`lighthouse_calibration_read`](#lighthouse_calibration_read), [`lighthouse_calibration_write`](#lighthouse_calibration_write), [`trajectory_write_uncompressed`](#trajectory_write_uncompressed), [`trajectory_write_compressed`](#trajectory_write_compressed), [`loco_read_id_list`](#loco_read_id_list), [`loco_read_anchor`](#loco_read_anchor) |
| [Positioning](#positioning) | [`send_short_lpp_packet`](#send_short_lpp_packet), [`lighthouse_persist`](#lighthouse_persist) |
| [Jobs and power](#jobs-and-power) | [`flash`](#flash), [`watch`](#watch), [`platform`](#platform) |
| [Replies](#replies) | [`done`](#done-reply), [`hello`](#hello-reply), [`scan`](#scan-reply), [`list`](#list-reply), [`radios`](#radios-reply), [`attached`](#attached-reply), [`value`](#value-reply), [`persistent_state`](#persistent_state-reply), [`link_statistics`](#link_statistics-reply), [`data`](#data-reply), [`bool`](#bool-reply), [`one_wire`](#one_wire-reply), [`eeprom_config`](#eeprom_config-reply), [`lighthouse_geometry`](#lighthouse_geometry-reply), [`lighthouse_calibration`](#lighthouse_calibration-reply), [`written`](#written-reply), [`ids`](#ids-reply), [`loco_anchor`](#loco_anchor-reply) |
| [Events](#events) | [`param_changed`](#param_changed-event), [`log`](#log-event), [`console`](#console-event), [`lighthouse_angles`](#lighthouse_angles-event), [`progress`](#progress-event), [`flashed`](#flashed-event), [`flash_failed`](#flash_failed-event), [`done`](#done-event), [`busy`](#busy-event), [`available`](#available-event), [`ended`](#ended-event) |
| [Types](#types) | [`AttachInfo`](#attachinfo), [`CfInfo`](#cfinfo), [`CompressedSegment`](#compressedsegment), [`CompressedStart`](#compressedstart), [`EepromConfig`](#eepromconfig), [`FlashImage`](#flashimage), [`FlashStart`](#flashstart), [`LighthouseBsCalibration`](#lighthousebscalibration), [`LighthouseBsGeometry`](#lighthousebsgeometry), [`LighthouseCalibrationSweep`](#lighthousecalibrationsweep), [`LinkStatistics`](#linkstatistics), [`LocoAnchorData`](#locoanchordata), [`MemoryInfo`](#memoryinfo), [`MemoryType`](#memorytype), [`OneWireMemory`](#onewirememory), [`PersistentParamState`](#persistentparamstate), [`PlatformCommand`](#platformcommand), [`Poly`](#poly), [`Poly4D`](#poly4d), [`RadioInfo`](#radioinfo), [`RadioLink`](#radiolink), [`TocItem`](#tocitem) |

## Messages

A client sends requests: `{"id": …, "call": "<call>", "args": {…}}`, with `args` left out for a call without any.

| Field | Type | Description |
|---|---|---|
| `id` | u64 | Picked by the client, echoed in the reply |
| `call` | string | One of the [calls](#clients-and-crazyflies) below |
| `args` | object | The call's arguments |

The server sends three kinds of messages, told apart by their `type`.

### `reply`

Answer to the request with the same `id`. Exactly one of `ok` and `error` is set.

| Field | Type |
|---|---|
| `id` | u64 |
| `ok` | [reply](#replies) (optional) |
| `error` | string (optional) |

### `event`

Data on a stream opened by the client with `sub`.

| Field | Type |
|---|---|
| `sub` | u64 |
| `event` | [event](#events) |

### `disconnected`

A Crazyflie this client is attached to was disconnected.

| Field | Type |
|---|---|
| `uri` | string |
| `reason` | string |

## Clients and Crazyflies

Finding Crazyflies and attaching to them.

### `hello`

Name this client, shown to other clients in scan and list results.

| Argument | Type |
|---|---|
| `name` | string |

Reply: [`hello`](#hello-reply)

```text
→ {"id":1,"call":"hello","args":{"name":"fly.py"}}
← {"type":"reply","id":1,"ok":{"kind":"hello","server":"crazyflie-connection-server","protocol":1}}
```

### `scan`

Scan all channels for Crazyflies at an address. Refused while a job runs.

| Argument | Type | Description |
|---|---|---|
| `address` | string (optional) | 10 hex digits, E7E7E7E7E7 when not given |

Reply: [`scan`](#scan-reply)

```text
→ {"id":2,"call":"scan","args":{"address":null}}
← {"type":"reply","id":2,"ok":{"kind":"scan","found":[{"uri":"radio:///80/2M/E7E7E7E7E7","clients":[],"radio":0}]}}
```

### `scan_selected`

Check which of these Crazyflies answer, one packet each, without connecting. Connected ones answer without radio traffic.

| Argument | Type |
|---|---|
| `uris` | string[] |

Reply: [`scan`](#scan-reply)

```text
→ {"id":3,"call":"scan_selected","args":{"uris":["radio:///80/2M/E7E7E7E7E7","radio:///80/2M/E7E7E7E701"]}}
← {"type":"reply","id":3,"ok":{"kind":"scan","found":[{"uri":"radio:///80/2M/E7E7E7E7E7","clients":["swarmkeeper"],"radio":0}]}}
```

### `list`

List the Crazyflies the server is connected to.

No arguments.

Reply: [`list`](#list-reply)

```text
→ {"id":4,"call":"list"}
← {"type":"reply","id":4,"ok":{"kind":"list","connected":[{"uri":"radio:///80/2M/E7E7E7E7E7","clients":["swarmkeeper","fly.py"],"radio":0}]}}
```

### `radios`

The server's Crazyradios, with the link statistics of every Crazyflie connected on each, whichever clients are attached to it. Crazyflies a program connected itself and handed to an in-process server (`adopt`) are left out, since the server doesn't know their radio.

No arguments.

Reply: [`radios`](#radios-reply)

```text
→ {"id":5,"call":"radios"}
← {"type":"reply","id":5,"ok":{"kind":"radios","radios":[{"index":0,"links":[{"uri":"radio:///80/2M/E7E7E7E7E7","clients":["swarmkeeper","fly.py"],"statistics":{"link_quality":0.98,"uplink_rate":12.0,"downlink_rate":105.0,"radio_send_rate":110.0,"avg_retries":0.05,"power_detector_rate":0.0,"rssi":-48.0}},{"uri":"radio:///80/2M/E7E7E7E701","clients":[],"statistics":{"link_quality":0.91,"uplink_rate":0.0,"downlink_rate":2.0,"radio_send_rate":100.0,"avg_retries":0.2,"power_detector_rate":0.0,"rssi":-61.0}}]},{"index":1,"links":[]}]}}
```

### `attach`

Attach to a Crazyflie, connecting it if no other client has.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |

Reply: [`attached`](#attached-reply)

```text
→ {"id":6,"call":"attach","args":{"uri":"radio:///80/2M/E7E7E7E7E7"}}
← {"type":"reply","id":6,"ok":{"kind":"attached","uri":"radio:///80/2M/E7E7E7E7E7","firmware_version":"2025.09","device_type":"Crazyflie 2.1","protocol_version":10,"params":[{"name":"led.bitmask","type":"u8","writable":true},{"name":"pm.lowVoltage","type":"f32","writable":true}],"log_vars":[{"name":"pm.vbat","type":"f32","writable":true}],"memories":[{"id":0,"type":"eeprom_config","size":21},{"id":1,"type":"one_wire","size":112,"serial":"F1A2C3D4E5F60718"}]}}
```

### `detach`

Detach from a Crazyflie. It is disconnected a while (the server's linger time) after its last client detaches.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |

Reply: [`done`](#done-reply)

```text
→ {"id":7,"call":"detach","args":{"uri":"radio:///80/2M/E7E7E7E7E7"}}
← {"type":"reply","id":7,"ok":{"kind":"done"}}
```

### `link_statistics`

The radio link's statistics.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |

Reply: [`link_statistics`](#link_statistics-reply)

```text
→ {"id":8,"call":"link_statistics","args":{"uri":"radio:///80/2M/E7E7E7E7E7"}}
← {"type":"reply","id":8,"ok":{"kind":"link_statistics","link_quality":0.98,"uplink_rate":12.0,"downlink_rate":105.0,"radio_send_rate":110.0,"avg_retries":0.05,"power_detector_rate":0.0,"rssi":-48.0}}
```

## Params

Params are numbers on the wire, whatever their type in the firmware.

### `param_get`

Read a param from the Crazyflie.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `name` | string |  |

Reply: [`value`](#value-reply)

```text
→ {"id":9,"call":"param_get","args":{"uri":"radio:///80/2M/E7E7E7E7E7","name":"pm.lowVoltage"}}
← {"type":"reply","id":9,"ok":{"kind":"value","value":3.2}}
```

### `param_set`

Set a param. Every value is a number, converted to the param's type.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `name` | string |  |
| `value` | number |  |

Reply: [`done`](#done-reply)

```text
→ {"id":10,"call":"param_set","args":{"uri":"radio:///80/2M/E7E7E7E7E7","name":"led.bitmask","value":3.0}}
← {"type":"reply","id":10,"ok":{"kind":"done"}}
```

### `param_watch`

Stream of [`param_changed` event](#param_changed-event) for every param set by any client.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `sub` | u64 | Stream number, picked by the client, see [streams](protocol.md#streams) |

Reply: [`done`](#done-reply)

Events on `sub`: [`param_changed`](#param_changed-event), [`ended`](#ended-event)

```text
→ {"id":11,"call":"param_watch","args":{"uri":"radio:///80/2M/E7E7E7E7E7","sub":1}}
← {"type":"reply","id":11,"ok":{"kind":"done"}}
← {"type":"event","sub":1,"event":{"kind":"param_changed","name":"led.bitmask","value":3.0}}
```

### `param_is_persistent`

Whether a param can be stored in the EEPROM. Replies [`bool` reply](#bool-reply). Asks the Crazyflie, so it isn't part of [`AttachInfo`](#attachinfo).

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `name` | string |  |

Reply: [`bool`](#bool-reply)

```text
→ {"id":12,"call":"param_is_persistent","args":{"uri":"radio:///80/2M/E7E7E7E7E7","name":"ring.effect"}}
← {"type":"reply","id":12,"ok":{"kind":"bool","value":true}}
```

### `param_persistent_state`

Default and stored value of a persistent param.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `name` | string |  |

Reply: [`persistent_state`](#persistent_state-reply)

```text
→ {"id":13,"call":"param_persistent_state","args":{"uri":"radio:///80/2M/E7E7E7E7E7","name":"ring.effect"}}
← {"type":"reply","id":13,"ok":{"kind":"persistent_state","is_stored":true,"default_value":6.0,"stored_value":7.0}}
```

### `param_persistent_store`

Store the current value of a persistent param in the Crazyflie's EEPROM.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `name` | string |  |

Reply: [`done`](#done-reply)

```text
→ {"id":14,"call":"param_persistent_store","args":{"uri":"radio:///80/2M/E7E7E7E7E7","name":"ring.effect"}}
← {"type":"reply","id":14,"ok":{"kind":"done"}}
```

### `param_persistent_clear`

Clear the stored value of a persistent param.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `name` | string |  |

Reply: [`done`](#done-reply)

```text
→ {"id":15,"call":"param_persistent_clear","args":{"uri":"radio:///80/2M/E7E7E7E7E7","name":"ring.effect"}}
← {"type":"reply","id":15,"ok":{"kind":"done"}}
```

## Streams

Each of these opens a stream with the `sub` it is given; [`unsubscribe`](#unsubscribe) closes it.

### `log_subscribe`

Stream of [`log` event](#log-event) with the given variables. Variables that don't fit one log block (26 bytes) are spread over several, each arriving as its own event.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `sub` | u64 | Stream number, picked by the client, see [streams](protocol.md#streams) |
| `vars` | string[] | Log variable names, `group.name` |
| `period_ms` | u64 | Milliseconds between samples, 10 to 2550 |

Reply: [`done`](#done-reply)

Events on `sub`: [`log`](#log-event), [`ended`](#ended-event)

```text
→ {"id":16,"call":"log_subscribe","args":{"uri":"radio:///80/2M/E7E7E7E7E7","sub":2,"vars":["pm.vbat","stateEstimate.z"],"period_ms":100}}
← {"type":"reply","id":16,"ok":{"kind":"done"}}
← {"type":"event","sub":2,"event":{"kind":"log","timestamp":123400,"values":{"pm.vbat":3.92,"stateEstimate.z":0.51}}}
← {"type":"event","sub":2,"event":{"kind":"log","timestamp":123500,"values":{"pm.vbat":3.91,"stateEstimate.z":0.5}}}
← {"type":"event","sub":2,"event":{"kind":"ended","reason":"Crazyflie disconnected"}}
```

### `console_subscribe`

Stream of [`console` event](#console-event) lines.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `sub` | u64 | Stream number, picked by the client, see [streams](protocol.md#streams) |
| `history` | boolean | Start with the lines printed since the Crazyflie was connected |

Reply: [`done`](#done-reply)

Events on `sub`: [`console`](#console-event), [`ended`](#ended-event)

```text
→ {"id":17,"call":"console_subscribe","args":{"uri":"radio:///80/2M/E7E7E7E7E7","sub":3,"history":true}}
← {"type":"reply","id":17,"ok":{"kind":"done"}}
← {"type":"event","sub":3,"event":{"kind":"console","line":"SYS: Crazyflie 2.1 is up and running!"}}
```

### `lighthouse_angle_subscribe`

Stream of [`lighthouse_angles` event](#lighthouse_angles-event). The Crazyflie sends them only while the `locSrv.enLhAngleStream` param is 1.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `sub` | u64 | Stream number, picked by the client, see [streams](protocol.md#streams) |

Reply: [`done`](#done-reply)

Events on `sub`: [`lighthouse_angles`](#lighthouse_angles-event), [`ended`](#ended-event)

```text
→ {"id":18,"call":"lighthouse_angle_subscribe","args":{"uri":"radio:///80/2M/E7E7E7E7E7","sub":4}}
← {"type":"reply","id":18,"ok":{"kind":"done"}}
← {"type":"event","sub":4,"event":{"kind":"lighthouse_angles","base_station":0,"x":[-0.312,-0.309,-0.317,-0.315],"y":[0.127,0.131,0.129,0.133]}}
```

### `unsubscribe`

Stop a stream opened with `sub`.

| Argument | Type | Description |
|---|---|---|
| `sub` | u64 | Stream number, picked by the client, see [streams](protocol.md#streams) |

Reply: [`done`](#done-reply)

```text
→ {"id":19,"call":"unsubscribe","args":{"sub":2}}
← {"type":"reply","id":19,"ok":{"kind":"done"}}
```

## Flying

The replies come when the commands are sent; the Crazyflie flies them on its own.

### `take_off`

Take off from the current position. The reply comes when the command is sent, not when the Crazyflie is up.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `height` | number | Meters above the world origin |
| `yaw` | number (optional) | Radians, the current yaw when not given |
| `duration` | number | Seconds to reach `height` |
| `group_mask` | u8 (optional) | The groups of Crazyflies that obey, all when not given |

Reply: [`done`](#done-reply)

```text
→ {"id":20,"call":"take_off","args":{"uri":"radio:///80/2M/E7E7E7E7E7","height":0.5,"yaw":null,"duration":2.0,"group_mask":null}}
← {"type":"reply","id":20,"ok":{"kind":"done"}}
```

### `land`

Land at the current position.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `height` | number | Meters above the world origin |
| `yaw` | number (optional) | Radians, the current yaw when not given |
| `duration` | number | Seconds to reach `height` |
| `group_mask` | u8 (optional) | The groups of Crazyflies that obey, all when not given |

Reply: [`done`](#done-reply)

```text
→ {"id":21,"call":"land","args":{"uri":"radio:///80/2M/E7E7E7E7E7","height":0.0,"yaw":null,"duration":2.0,"group_mask":null}}
← {"type":"reply","id":21,"ok":{"kind":"done"}}
```

### `stop`

Stop the high-level command and the motors.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `group_mask` | u8 (optional) | The groups of Crazyflies that obey, all when not given |

Reply: [`done`](#done-reply)

```text
→ {"id":22,"call":"stop","args":{"uri":"radio:///80/2M/E7E7E7E7E7","group_mask":null}}
← {"type":"reply","id":22,"ok":{"kind":"done"}}
```

### `go_to`

Fly to a position.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `x` | number | Meters |
| `y` | number | Meters |
| `z` | number | Meters |
| `yaw` | number | Radians |
| `duration` | number | Seconds to get there |
| `relative` | boolean | The position and yaw are relative to the current ones |
| `linear` | boolean | A straight line at constant speed instead of a smooth polynomial |
| `group_mask` | u8 (optional) | The groups of Crazyflies that obey, all when not given |

Reply: [`done`](#done-reply)

```text
→ {"id":23,"call":"go_to","args":{"uri":"radio:///80/2M/E7E7E7E7E7","x":1.0,"y":0.0,"z":0.5,"yaw":0.0,"duration":3.0,"relative":false,"linear":false,"group_mask":null}}
← {"type":"reply","id":23,"ok":{"kind":"done"}}
```

### `define_trajectory`

Define trajectory `id` as `num_pieces` pieces at `memory_offset` in the trajectory memory.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `id` | u8 |  |
| `memory_offset` | u32 | Bytes into the trajectory memory |
| `num_pieces` | u8 |  |
| `trajectory_type` | u8 (optional) | 0: uncompressed (the default), 1: compressed |

Reply: [`done`](#done-reply)

```text
→ {"id":24,"call":"define_trajectory","args":{"uri":"radio:///80/2M/E7E7E7E7E7","id":1,"memory_offset":0,"num_pieces":1,"trajectory_type":null}}
← {"type":"reply","id":24,"ok":{"kind":"done"}}
```

### `start_trajectory`

Fly trajectory `id`.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `id` | u8 |  |
| `time_scale` | number | 1 flies it as defined, 2 twice as slow |
| `relative_position` | boolean | Shift it to start at the current position |
| `relative_yaw` | boolean | Turn it to start at the current yaw |
| `reversed` | boolean | Fly it backwards |
| `group_mask` | u8 (optional) | The groups of Crazyflies that obey, all when not given |

Reply: [`done`](#done-reply)

```text
→ {"id":25,"call":"start_trajectory","args":{"uri":"radio:///80/2M/E7E7E7E7E7","id":1,"time_scale":1.0,"relative_position":true,"relative_yaw":false,"reversed":false,"group_mask":null}}
← {"type":"reply","id":25,"ok":{"kind":"done"}}
```

### `setpoint_rpyt`

Low-level setpoint, sent repeatedly. The thrust is locked until a setpoint with thrust 0 is sent.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `roll` | number | Degrees |
| `pitch` | number | Degrees |
| `yawrate` | number | Degrees/s |
| `thrust` | u16 | 0 to 65535 |

Reply: [`done`](#done-reply)

```text
→ {"id":26,"call":"setpoint_rpyt","args":{"uri":"radio:///80/2M/E7E7E7E7E7","roll":0.0,"pitch":0.0,"yawrate":0.0,"thrust":0}}
← {"type":"reply","id":26,"ok":{"kind":"done"}}
```

### `notify_setpoint_stop`

End the low-level setpoints, so that the high-level commander takes over.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `remain_valid_ms` | u32 | How long the last setpoint still holds, 0 for no time |

Reply: [`done`](#done-reply)

```text
→ {"id":27,"call":"notify_setpoint_stop","args":{"uri":"radio:///80/2M/E7E7E7E7E7","remain_valid_ms":0}}
← {"type":"reply","id":27,"ok":{"kind":"done"}}
```

### `arming_request`

Arm or disarm the motors.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `arm` | boolean |  |

Reply: [`done`](#done-reply)

```text
→ {"id":28,"call":"arming_request","args":{"uri":"radio:///80/2M/E7E7E7E7E7","arm":true}}
← {"type":"reply","id":28,"ok":{"kind":"done"}}
```

### `emergency_stop`

Stop the motors at once. The Crazyflie has to be restarted to fly again.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |

Reply: [`done`](#done-reply)

```text
→ {"id":29,"call":"emergency_stop","args":{"uri":"radio:///80/2M/E7E7E7E7E7"}}
← {"type":"reply","id":29,"ok":{"kind":"done"}}
```

### `crash_recovery`

Recover a Crazyflie that crashed (tumbled).

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |

Reply: [`done`](#done-reply)

```text
→ {"id":30,"call":"crash_recovery","args":{"uri":"radio:///80/2M/E7E7E7E7E7"}}
← {"type":"reply","id":30,"ok":{"kind":"done"}}
```

## Memories

Memories are addressed by the `id` in the [`attached`](#attached-reply) reply's `memories`.

### `memory_read`

Read `length` bytes of memory `id` (see [`memories`](#attachinfo)) from `address`.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `id` | u8 |  |
| `address` | u32 |  |
| `length` | u32 |  |

Reply: [`data`](#data-reply)

```text
→ {"id":31,"call":"memory_read","args":{"uri":"radio:///80/2M/E7E7E7E7E7","id":1,"address":0,"length":4}}
← {"type":"reply","id":31,"ok":{"kind":"data","data":"6wAAAA=="}}
```

### `memory_write`

Write `data` to memory `id` at `address`.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `id` | u8 |  |
| `address` | u32 |  |
| `data` | string | The bytes, base64 encoded |

Reply: [`done`](#done-reply)

```text
→ {"id":32,"call":"memory_write","args":{"uri":"radio:///80/2M/E7E7E7E7E7","id":1,"address":0,"data":"AQID"}}
← {"type":"reply","id":32,"ok":{"kind":"done"}}
```

### `one_wire_read`

Read a deck's 1-wire memory `id`: its ids and elements (board name, revision, …).

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `id` | u8 |  |

Reply: [`one_wire`](#one_wire-reply)

```text
→ {"id":33,"call":"one_wire_read","args":{"uri":"radio:///80/2M/E7E7E7E7E7","id":1}}
← {"type":"reply","id":33,"ok":{"kind":"one_wire","used_pins":12,"vid":188,"pid":16,"elements":{"boardName":"bcLighthouse4","revision":"C"}}}
```

### `eeprom_config_read`

Read the EEPROM config memory `id`.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `id` | u8 |  |

Reply: [`eeprom_config`](#eeprom_config-reply)

```text
→ {"id":34,"call":"eeprom_config_read","args":{"uri":"radio:///80/2M/E7E7E7E7E7","id":0}}
← {"type":"reply","id":34,"ok":{"kind":"eeprom_config","radio_channel":80,"radio_speed":2,"radio_address":"E7E7E7E7E7","pitch_trim":0.0,"roll_trim":0.0}}
```

### `eeprom_config_write`

Write the EEPROM config memory `id`. The firmware applies it at boot.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `id` | u8 |  |
| `config` | [`EepromConfig`](#eepromconfig) |  |

Reply: [`done`](#done-reply)

```text
→ {"id":35,"call":"eeprom_config_write","args":{"uri":"radio:///80/2M/E7E7E7E7E7","id":0,"config":{"radio_channel":90,"radio_speed":2,"radio_address":"E7E7E7E7E7","pitch_trim":0.0,"roll_trim":0.0}}}
← {"type":"reply","id":35,"ok":{"kind":"done"}}
```

### `lighthouse_geometry_read`

Read the geometry of base station `bs` from lighthouse memory `id`.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `id` | u8 |  |
| `bs` | u8 |  |

Reply: [`lighthouse_geometry`](#lighthouse_geometry-reply)

```text
→ {"id":36,"call":"lighthouse_geometry_read","args":{"uri":"radio:///80/2M/E7E7E7E7E7","id":2,"bs":0}}
← {"type":"reply","id":36,"ok":{"kind":"lighthouse_geometry","origin":[-1.92,2.13,2.51],"rotation_matrix":[[0.68,-0.48,0.55],[0.47,0.87,0.16],[-0.56,0.13,0.82]],"valid":true}}
```

### `lighthouse_geometry_write`

Write the geometry of base station `bs` to lighthouse memory `id`. The Crazyflie uses it until it restarts, see [`lighthouse_persist`](#lighthouse_persist).

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `id` | u8 |  |
| `bs` | u8 |  |
| `geometry` | [`LighthouseBsGeometry`](#lighthousebsgeometry) |  |

Reply: [`done`](#done-reply)

```text
→ {"id":37,"call":"lighthouse_geometry_write","args":{"uri":"radio:///80/2M/E7E7E7E7E7","id":2,"bs":0,"geometry":{"origin":[-1.92,2.13,2.51],"rotation_matrix":[[0.68,-0.48,0.55],[0.47,0.87,0.16],[-0.56,0.13,0.82]],"valid":true}}}
← {"type":"reply","id":37,"ok":{"kind":"done"}}
```

### `lighthouse_calibration_read`

Read the calibration of base station `bs` from lighthouse memory `id`.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `id` | u8 |  |
| `bs` | u8 |  |

Reply: [`lighthouse_calibration`](#lighthouse_calibration-reply)

```text
→ {"id":38,"call":"lighthouse_calibration_read","args":{"uri":"radio:///80/2M/E7E7E7E7E7","id":2,"bs":0}}
← {"type":"reply","id":38,"ok":{"kind":"lighthouse_calibration","sweeps":[{"phase":0.0,"tilt":-0.047,"curve":0.21,"gibmag":-0.006,"gibphase":1.14,"ogeemag":-0.12,"ogeephase":0.82},{"phase":0.0,"tilt":0.046,"curve":0.21,"gibmag":-0.006,"gibphase":1.14,"ogeemag":-0.12,"ogeephase":0.82}],"uid":450938338,"valid":true}}
```

### `lighthouse_calibration_write`

Write the calibration of base station `bs` to lighthouse memory `id`. The Crazyflie uses it until it restarts, see [`lighthouse_persist`](#lighthouse_persist).

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `id` | u8 |  |
| `bs` | u8 |  |
| `calibration` | [`LighthouseBsCalibration`](#lighthousebscalibration) |  |

Reply: [`done`](#done-reply)

```text
→ {"id":39,"call":"lighthouse_calibration_write","args":{"uri":"radio:///80/2M/E7E7E7E7E7","id":2,"bs":0,"calibration":{"sweeps":[{"phase":0.0,"tilt":-0.047,"curve":0.21,"gibmag":-0.006,"gibphase":1.14,"ogeemag":-0.12,"ogeephase":0.82},{"phase":0.0,"tilt":0.046,"curve":0.21,"gibmag":-0.006,"gibphase":1.14,"ogeemag":-0.12,"ogeephase":0.82}],"uid":450938338,"valid":true}}}
← {"type":"reply","id":39,"ok":{"kind":"done"}}
```

### `trajectory_write_uncompressed`

Write uncompressed trajectory segments to trajectory memory `id` at `start_addr`. Replies [`written` reply](#written-reply).

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `id` | u8 |  |
| `start_addr` | u32 |  |
| `segments` | [`Poly4D`](#poly4d)[] |  |

Reply: [`written`](#written-reply)

```text
→ {"id":40,"call":"trajectory_write_uncompressed","args":{"uri":"radio:///80/2M/E7E7E7E7E7","id":3,"start_addr":0,"segments":[{"duration":2.0,"x":{"values":[0.0,0.0,0.0,0.0,0.94,-0.47,0.078,0.0]},"y":{"values":[0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]},"z":{"values":[0.5,0.0,0.0,0.0,0.0,0.0,0.0,0.0]},"yaw":{"values":[0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]}}]}}
← {"type":"reply","id":40,"ok":{"kind":"written","bytes":132}}
```

### `trajectory_write_compressed`

Write a compressed trajectory to trajectory memory `id` at `start_addr`. Replies [`written` reply](#written-reply).

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `id` | u8 |  |
| `start_addr` | u32 |  |
| `start` | [`CompressedStart`](#compressedstart) |  |
| `segments` | [`CompressedSegment`](#compressedsegment)[] |  |

Reply: [`written`](#written-reply)

```text
→ {"id":41,"call":"trajectory_write_compressed","args":{"uri":"radio:///80/2M/E7E7E7E7E7","id":3,"start_addr":0,"start":{"x":0.0,"y":0.0,"z":0.5,"yaw":0.0},"segments":[{"duration":2.0,"x":[1.0],"y":[],"z":[],"yaw":[]}]}}
← {"type":"reply","id":41,"ok":{"kind":"written","bytes":13}}
```

### `loco_read_id_list`

The anchor ids in Loco memory `id`: all configured ones, or the `active` ones. Replies [`ids` reply](#ids-reply).

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `id` | u8 |  |
| `active` | boolean |  |

Reply: [`ids`](#ids-reply)

```text
→ {"id":42,"call":"loco_read_id_list","args":{"uri":"radio:///80/2M/E7E7E7E7E7","id":4,"active":false}}
← {"type":"reply","id":42,"ok":{"kind":"ids","ids":[0,1,2,3,4,5,6,7]}}
```

### `loco_read_anchor`

The position of `anchor` in Loco memory `id`.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `id` | u8 |  |
| `anchor` | u8 |  |

Reply: [`loco_anchor`](#loco_anchor-reply)

```text
→ {"id":43,"call":"loco_read_anchor","args":{"uri":"radio:///80/2M/E7E7E7E7E7","id":4,"anchor":0}}
← {"type":"reply","id":43,"ok":{"kind":"loco_anchor","position":[0.0,0.0,0.2],"is_valid":true}}
```

## Positioning

Lighthouse and Loco Positioning system setup through a Crazyflie.

### `send_short_lpp_packet`

Send a Loco Positioning Protocol packet to anchor `dest_id`, through the Crazyflie (e.g. an anchor position).

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `dest_id` | u8 |  |
| `data` | u8[] | The packet's bytes |

Reply: [`done`](#done-reply)

```text
→ {"id":44,"call":"send_short_lpp_packet","args":{"uri":"radio:///80/2M/E7E7E7E7E7","dest_id":0,"data":[1,0,0,0,0,0,0,0,0,205,204,76,62]}}
← {"type":"reply","id":44,"ok":{"kind":"done"}}
```

### `lighthouse_persist`

Store the geometries and calibrations of these base stations in the Crazyflie's flash. Replies [`bool` reply](#bool-reply): whether the Crazyflie did.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `geometries` | u8[] | Base stations whose geometry to store |
| `calibrations` | u8[] | Base stations whose calibration to store |

Reply: [`bool`](#bool-reply)

```text
→ {"id":45,"call":"lighthouse_persist","args":{"uri":"radio:///80/2M/E7E7E7E7E7","geometries":[0,1],"calibrations":[0,1]}}
← {"type":"reply","id":45,"ok":{"kind":"bool","value":true}}
```

## Jobs and power

Flashing, rebooting and powering Crazyflies, and watching which of them jobs take over.

### `flash`

Restart the Crazyflies into their bootloaders, flash the images to each and restart them into their firmware, as a job on stream `sub`: [`progress` event](#progress-event), one [`flashed` event](#flashed-event) or [`flash_failed` event](#flash_failed-event) per Crazyflie, then [`done` event](#done-event). The images are for one platform. The Crazyflies whose bootloaders take broadcast get each image at the same time, the others one after another. Clients attached to them are disconnected, and attaching and scanning are refused until the job is done. The job finishes even if the client that started it goes away.

| Argument | Type | Description |
|---|---|---|
| `uris` | string[] |  |
| `sub` | u64 | Stream number, picked by the client, see [streams](protocol.md#streams) |
| `images` | [`FlashImage`](#flashimage)[] |  |

Reply: [`done`](#done-reply)

Events on `sub`: [`progress`](#progress-event), [`flashed`](#flashed-event), [`flash_failed`](#flash_failed-event), [`done`](#done-event)

```text
→ {"id":46,"call":"flash","args":{"uris":["radio:///80/2M/E7E7E7E7E7","radio:///80/2M/E7E7E7E701"],"sub":5,"images":[{"target":"stm32","data":"AAAAIA=="}]}}
← {"type":"reply","id":46,"ok":{"kind":"done"}}
← {"type":"event","sub":5,"event":{"kind":"progress","uri":"radio:///80/2M/E7E7E7E7E7","step":"Restarting into the bootloader","done":0,"total":1}}
← {"type":"event","sub":5,"event":{"kind":"progress","step":"Broadcasting stm32 to 2 Crazyflies","done":81920,"total":245760}}
← {"type":"event","sub":5,"event":{"kind":"flashed","uri":"radio:///80/2M/E7E7E7E7E7","how":"broadcast"}}
← {"type":"event","sub":5,"event":{"kind":"flash_failed","uri":"radio:///80/2M/E7E7E7E701","reason":"Restarting into the firmware: no answer; left in its bootloader at E7E7E7E701"}}
← {"type":"event","sub":5,"event":{"kind":"done"}}
```

### `watch`

Stream of [`busy` event](#busy-event) and [`available` event](#available-event): the Crazyflies that jobs take over and give back.

| Argument | Type | Description |
|---|---|---|
| `sub` | u64 | Stream number, picked by the client, see [streams](protocol.md#streams) |

Reply: [`done`](#done-reply)

Events on `sub`: [`busy`](#busy-event), [`available`](#available-event)

```text
→ {"id":47,"call":"watch","args":{"sub":6}}
← {"type":"reply","id":47,"ok":{"kind":"done"}}
← {"type":"event","sub":6,"event":{"kind":"busy","uri":"radio:///80/2M/E7E7E7E7E7","reason":"radio:///80/2M/E7E7E7E7E7 is being flashed by fly.py"}}
← {"type":"event","sub":6,"event":{"kind":"available","uri":"radio:///80/2M/E7E7E7E7E7","connected":true}}
← {"type":"event","sub":6,"event":{"kind":"busy","uri":"radio:///80/2M/E7E7E7E701","reason":"radio:///80/2M/E7E7E7E701 is restarting, rebooted by fly.py"}}
← {"type":"event","sub":6,"event":{"kind":"available","uri":"radio:///80/2M/E7E7E7E701","connected":false,"moved_to":"radio:///90/2M/E7E7E7E701"}}
```

### `platform`

A command the Crazyflie's nRF51 handles, sent without connecting to it. Its clients are disconnected, except by [`power_on_stm32_domain`](#platformcommand). Fails if it doesn't answer.

| Argument | Type | Description |
|---|---|---|
| `uri` | string | The Crazyflie, see [URIs](protocol.md#uris) |
| `command` | [`PlatformCommand`](#platformcommand) |  |
| `new_uri` | string (optional) | With [`reboot`](#platformcommand), where the Crazyflie comes back when its radio channel or address was changed in its EEPROM config: the reply waits for its firmware there, it isn't connected again, and [`available` event](#available-event) says where it moved |

Reply: [`done`](#done-reply)

```text
→ {"id":48,"call":"platform","args":{"uri":"radio:///80/2M/E7E7E7E701","command":"reboot","new_uri":"radio:///90/2M/E7E7E7E701"}}
← {"type":"reply","id":48,"ok":{"kind":"done"}}
```

## Replies

The `ok` of a [`reply`](#reply). Its `kind` tells which.

### `done` reply

Done, nothing to return.

No fields.

Replies to [`detach`](#detach), [`param_set`](#param_set), [`param_watch`](#param_watch), [`param_persistent_store`](#param_persistent_store), [`param_persistent_clear`](#param_persistent_clear), [`log_subscribe`](#log_subscribe), [`console_subscribe`](#console_subscribe), [`lighthouse_angle_subscribe`](#lighthouse_angle_subscribe), [`unsubscribe`](#unsubscribe), [`take_off`](#take_off), [`land`](#land), [`stop`](#stop), [`go_to`](#go_to), [`define_trajectory`](#define_trajectory), [`start_trajectory`](#start_trajectory), [`setpoint_rpyt`](#setpoint_rpyt), [`notify_setpoint_stop`](#notify_setpoint_stop), [`arming_request`](#arming_request), [`emergency_stop`](#emergency_stop), [`crash_recovery`](#crash_recovery), [`memory_write`](#memory_write), [`eeprom_config_write`](#eeprom_config_write), [`lighthouse_geometry_write`](#lighthouse_geometry_write), [`lighthouse_calibration_write`](#lighthouse_calibration_write), [`send_short_lpp_packet`](#send_short_lpp_packet), [`flash`](#flash), [`watch`](#watch), [`platform`](#platform).

### `hello` reply

| Field | Type | Description |
|---|---|---|
| `server` | string | The server's name |
| `protocol` | u32 | The protocol version, `1` |

Replies to [`hello`](#hello).

### `scan` reply

The Crazyflies that answered.

| Field | Type |
|---|---|
| `found` | [`CfInfo`](#cfinfo)[] |

Replies to [`scan`](#scan), [`scan_selected`](#scan_selected).

### `list` reply

The Crazyflies the server is connected to.

| Field | Type |
|---|---|
| `connected` | [`CfInfo`](#cfinfo)[] |

Replies to [`list`](#list).

### `radios` reply

The Crazyradios the server holds or has used, by index.

| Field | Type |
|---|---|
| `radios` | [`RadioInfo`](#radioinfo)[] |

Replies to [`radios`](#radios).

### `attached` reply

The Crazyflie attached to.

The fields of [`AttachInfo`](#attachinfo).

Replies to [`attach`](#attach).

### `value` reply

A param value.

| Field | Type |
|---|---|
| `value` | number |

Replies to [`param_get`](#param_get).

### `persistent_state` reply

The fields of [`PersistentParamState`](#persistentparamstate).

Replies to [`param_persistent_state`](#param_persistent_state).

### `link_statistics` reply

The fields of [`LinkStatistics`](#linkstatistics).

Replies to [`link_statistics`](#link_statistics).

### `data` reply

Bytes read.

| Field | Type | Description |
|---|---|---|
| `data` | string | base64 encoded |

Replies to [`memory_read`](#memory_read).

### `bool` reply

| Field | Type |
|---|---|
| `value` | boolean |

Replies to [`param_is_persistent`](#param_is_persistent), [`lighthouse_persist`](#lighthouse_persist).

### `one_wire` reply

The fields of [`OneWireMemory`](#onewirememory).

Replies to [`one_wire_read`](#one_wire_read).

### `eeprom_config` reply

The fields of [`EepromConfig`](#eepromconfig).

Replies to [`eeprom_config_read`](#eeprom_config_read).

### `lighthouse_geometry` reply

The fields of [`LighthouseBsGeometry`](#lighthousebsgeometry).

Replies to [`lighthouse_geometry_read`](#lighthouse_geometry_read).

### `lighthouse_calibration` reply

The fields of [`LighthouseBsCalibration`](#lighthousebscalibration).

Replies to [`lighthouse_calibration_read`](#lighthouse_calibration_read).

### `written` reply

Bytes written.

| Field | Type |
|---|---|
| `bytes` | u64 |

Replies to [`trajectory_write_uncompressed`](#trajectory_write_uncompressed), [`trajectory_write_compressed`](#trajectory_write_compressed).

### `ids` reply

Loco anchor ids.

| Field | Type |
|---|---|
| `ids` | u8[] |

Replies to [`loco_read_id_list`](#loco_read_id_list).

### `loco_anchor` reply

The fields of [`LocoAnchorData`](#locoanchordata).

Replies to [`loco_read_anchor`](#loco_read_anchor).

## Events

The `event` of an [`event`](#event) message. Its `kind` tells which.

### `param_changed` event

A client set a param.

| Field | Type |
|---|---|
| `name` | string |
| `value` | number |

On the streams of [`param_watch`](#param_watch).

### `log` event

Samples of a log block.

| Field | Type | Description |
|---|---|---|
| `timestamp` | u32 | Milliseconds since the Crazyflie started, when sampled |
| `values` | object of number | By variable name |

On the streams of [`log_subscribe`](#log_subscribe).

### `console` event

A line the Crazyflie printed, without the newline.

| Field | Type |
|---|---|
| `line` | string |

On the streams of [`console_subscribe`](#console_subscribe).

### `lighthouse_angles` event

Sweep angles of the four sensors, in radians, from one base station.

| Field | Type |
|---|---|
| `base_station` | u8 |
| `x` | number[4] |
| `y` | number[4] |

On the streams of [`lighthouse_angle_subscribe`](#lighthouse_angle_subscribe).

### `progress` event

How far a job is: `done` of `total` for its current `step`, on the Crazyflie at `uri`, or on all of them (e.g. a broadcast) without one.

| Field | Type |
|---|---|
| `uri` | string (optional) |
| `step` | string |
| `done` | u64 |
| `total` | u64 |

On the streams of [`flash`](#flash).

### `flashed` event

A Crazyflie of a flash job was flashed, `how`: e.g. `broadcast`.

| Field | Type |
|---|---|
| `uri` | string |
| `how` | string |

On the streams of [`flash`](#flash).

### `flash_failed` event

Flashing a Crazyflie of a flash job failed.

| Field | Type |
|---|---|
| `uri` | string |
| `reason` | string |

On the streams of [`flash`](#flash).

### `done` event

A job finished.

No fields.

On the streams of [`flash`](#flash).

### `busy` event

A job took the Crazyflie over (e.g. flashing): attaching to it fails until it is [`available` event](#available-event) again.

| Field | Type |
|---|---|
| `uri` | string |
| `reason` | string |

On the streams of [`watch`](#watch).

### `available` event

The job on the Crazyflie is done.

| Field | Type | Description |
|---|---|---|
| `uri` | string |  |
| `connected` | boolean | The server connected it again, so attaching to it is immediate |
| `moved_to` | string (optional) | It was rebooted onto another channel or address, and answers there now |

On the streams of [`watch`](#watch).

### `ended` event

The stream ended on the server side (disconnect, log error, …).

| Field | Type |
|---|---|
| `reason` | string |

On the streams of [`param_watch`](#param_watch), [`log_subscribe`](#log_subscribe), [`console_subscribe`](#console_subscribe), [`lighthouse_angle_subscribe`](#lighthouse_angle_subscribe).

## Types

### `AttachInfo`

What a client learns when it attaches.

| Field | Type | Description |
|---|---|---|
| `uri` | string | Normalized URI |
| `firmware_version` | string (optional) | e.g. `2025.12`, when the firmware tells |
| `device_type` | string (optional) | e.g. `Crazyflie 2.1`, when the firmware tells |
| `protocol_version` | u8 (optional) | The firmware's CRTP protocol version |
| `params` | [`TocItem`](#tocitem)[] | The param TOC |
| `log_vars` | [`TocItem`](#tocitem)[] | The log TOC |
| `memories` | [`MemoryInfo`](#memoryinfo)[] (optional) | The Crazyflie's memories, as listed when it was connected |

### `CfInfo`

A Crazyflie found by a scan or connected on the server.

| Field | Type | Description |
|---|---|---|
| `uri` | string | Normalized URI, see [URIs](protocol.md#uris) |
| `clients` | string[] | Names of the clients attached to it |
| `radio` | integer (optional) | The server's Crazyradio it is connected or answered on, left out for USB |

### `CompressedSegment`

A compressed trajectory segment: the Bézier control points of each axis, 0, 1, 3 or 7 of them, see `crazyflie_lib::subsystems::memory::CompressedSegment`.

| Field | Type | Description |
|---|---|---|
| `duration` | number | Seconds |
| `x` | number[] |  |
| `y` | number[] |  |
| `z` | number[] |  |
| `yaw` | number[] |  |

### `CompressedStart`

Where a compressed trajectory starts, see `crazyflie_lib::subsystems::memory::CompressedStart`.

| Field | Type | Description |
|---|---|---|
| `x` | number | Meters |
| `y` | number | Meters |
| `z` | number | Meters |
| `yaw` | number | Radians |

### `EepromConfig`

The EEPROM config, see `crazyflie_lib::subsystems::memory::EEPROMConfigMemory`.

| Field | Type | Description |
|---|---|---|
| `radio_channel` | u8 | 0 to 125 |
| `radio_speed` | u8 | 0: 250 kbit/s, 1: 1 Mbit/s, 2: 2 Mbit/s |
| `radio_address` | string | 10 hex digits |
| `pitch_trim` | number |  |
| `roll_trim` | number |  |

### `FlashImage`

A firmware image for [`flash`](#flash).

| Field | Type | Description |
|---|---|---|
| `target` | string | `stm32` or `nrf51` |
| `start` | [`FlashStart`](#flashstart) (optional) | Where to write the image; the start of the firmware area when not given |
| `data` | string | The image, base64 encoded |

### `FlashStart`

Where a [`FlashImage`](#flashimage) is written.

One of:

- `{"address": u32}`
- `{"page": u16}`

### `LighthouseBsCalibration`

A lighthouse base station's calibration, see `crazyflie_lib::subsystems::memory::LighthouseBsCalibration`.

| Field | Type | Description |
|---|---|---|
| `sweeps` | [`LighthouseCalibrationSweep`](#lighthousecalibrationsweep)[2] |  |
| `uid` | u32 | The base station's id |
| `valid` | boolean |  |

### `LighthouseBsGeometry`

Where a lighthouse base station is, see `crazyflie_lib::subsystems::memory::LighthouseBsGeometry`.

| Field | Type | Description |
|---|---|---|
| `origin` | number[3] | Meters |
| `rotation_matrix` | number[3][3] | Rows |
| `valid` | boolean |  |

### `LighthouseCalibrationSweep`

The calibration of one of a base station's two sweeps, see `crazyflie_lib::subsystems::memory::LighthouseCalibrationSweep`.

| Field | Type |
|---|---|
| `phase` | number |
| `tilt` | number |
| `curve` | number |
| `gibmag` | number |
| `gibphase` | number |
| `ogeemag` | number |
| `ogeephase` | number |

### `LinkStatistics`

Radio link statistics over the last second, see `crazyflie_lib::subsystems::link_service::Statistics`. Every field is `null` over USB, `rssi` also with a Crazyradio that doesn't measure it.

| Field | Type | Description |
|---|---|---|
| `link_quality` | number (optional) | Share of packets acked, 0 to 1 |
| `uplink_rate` | number (optional) | Data packets sent per second, without the empty ones that poll the Crazyflie |
| `downlink_rate` | number (optional) | Packets received per second |
| `radio_send_rate` | number (optional) | Packets sent per second, with the empty ones |
| `avg_retries` | number (optional) | Retries per acked packet |
| `power_detector_rate` | number (optional) | Share of acks where the nRF24 power detector triggered, 0 to 1 |
| `rssi` | number (optional) | dBm, measured by the Crazyradio on acks |

### `LocoAnchorData`

A Loco Positioning anchor, see `crazyflie_lib::subsystems::memory::LocoAnchorData`.

| Field | Type | Description |
|---|---|---|
| `position` | number[3] | Meters |
| `is_valid` | boolean |  |

### `MemoryInfo`

One memory of a Crazyflie, see `crazyflie_lib::subsystems::memory::MemoryDevice`.

| Field | Type | Description |
|---|---|---|
| `id` | u8 | The `id` memory calls take |
| `type` | [`MemoryType`](#memorytype) |  |
| `size` | u32 | Size in bytes |
| `serial` | string (optional) | Serial number, e.g. a deck's 1-wire ROM id, as hex |

### `MemoryType`

Memory types, named as in `crazyflie_lib::subsystems::memory::MemoryType`.

| Value | Description |
|---|---|
| `"eeprom_config"` |  |
| `"one_wire"` |  |
| `"driver_led"` |  |
| `"loco"` |  |
| `"trajectory"` |  |
| `"loco2"` |  |
| `"lighthouse"` |  |
| `"memory_tester"` |  |
| `"micro_sd"` |  |
| `"driver_led_timing"` |  |
| `"app"` |  |
| `"deck_memory"` |  |
| `"deck_ctrl_dfu"` |  |
| `"deck_ctrl"` |  |
| `"deck_multiranger"` |  |
| `"deck_paa3905"` |  |
| `"unknown"` | A memory type this version doesn't know |

### `OneWireMemory`

A deck's 1-wire memory, see `crazyflie_lib::subsystems::memory::OwMemory`.

| Field | Type | Description |
|---|---|---|
| `used_pins` | u32 | Bitmask of the deck pins the deck uses |
| `vid` | u8 | Vendor id |
| `pid` | u8 | Product id |
| `elements` | object of string | e.g. `boardName`, `revision` |

### `PersistentParamState`

Default and stored value of a persistent param, see `crazyflie_lib::subsystems::param::PersistentParamState`.

| Field | Type | Description |
|---|---|---|
| `is_stored` | boolean | A value is stored in the EEPROM |
| `default_value` | number | The firmware's default |
| `stored_value` | number (optional) | The stored value, when `is_stored` |

### `PlatformCommand`

Commands for [`platform`](#platform), named after the crazyflie-lib functions.

| Value | Description |
|---|---|
| `"reboot"` | Restart the firmware. The reply comes once the firmware answers again; meanwhile the Crazyflie is busy ([`busy` event](#busy-event)), and a client that was attached gets it back connected ([`available` event](#available-event)) |
| `"power_off_all"` | Power off the whole Crazyflie, as the power button does. Only the button turns it on again |
| `"power_off_stm32_domain"` | Power off the STM32 and decks (sleep). The nRF51 stays on the radio. The Crazyflie is announced busy, and attaching fails until it is woken up |
| `"power_on_stm32_domain"` | Power the STM32 and decks on again (wake up). The reply comes once the firmware answers; the Crazyflie is then announced available |

### `Poly`

A polynomial of up to 8 coefficients, see `crazyflie_lib::subsystems::memory::Poly`.

| Field | Type |
|---|---|
| `values` | number[8] |

### `Poly4D`

An uncompressed trajectory segment, see `crazyflie_lib::subsystems::memory::Poly4D`.

| Field | Type | Description |
|---|---|---|
| `duration` | number | Seconds |
| `x` | [`Poly`](#poly) |  |
| `y` | [`Poly`](#poly) |  |
| `z` | [`Poly`](#poly) |  |
| `yaw` | [`Poly`](#poly) |  |

### `RadioInfo`

One of the server's Crazyradios, see [`radios`](#radios).

| Field | Type | Description |
|---|---|---|
| `index` | integer | The index in `radio://<index>/…` URIs, and the `radio` of a [`CfInfo`](#cfinfo) |
| `links` | [`RadioLink`](#radiolink)[] | The Crazyflies connected on it |

### `RadioLink`

A Crazyflie connected on one of the server's Crazyradios.

| Field | Type | Description |
|---|---|---|
| `uri` | string | Normalized URI |
| `clients` | string[] | Names of the clients attached to it, empty while it lingers |
| `statistics` | [`LinkStatistics`](#linkstatistics) | How the link is doing |

### `TocItem`

One param or log variable.

| Field | Type | Description |
|---|---|---|
| `name` | string |  |
| `type` | string | Firmware type, e.g. `u8` or `f32` |
| `writable` | boolean | Always `true` for log variables |
