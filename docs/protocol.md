# Protocol guide

The connection server speaks JSON over a WebSocket. Any language with a
WebSocket library can use it; Rust programs can use the `Client` in
`crazyflie-connection-server` instead.

This page covers how requests, replies and streams work. The [protocol
reference](protocol-reference.md) lists every call, reply, event and type with
an example. [protocol.schema.json](protocol.schema.json) has the same messages
as a JSON Schema (draft 2020-12), for validating messages or generating types.
Both are generated from `crates/protocol/src/lib.rs`.

## Connecting

Start a server with `cargo run --release --example server` in this
repository. It listens on `ws://127.0.0.1:7777/`. To take clients from other
machines, start it with `-- --listen 0.0.0.0:7777`.
Swarmkeeper also serves on `127.0.0.1:7777` while it runs, and shares its
connections.

There is no authentication yet: anyone who reaches the port can fly the
Crazyflies. Use it on a trusted network or over a tunnel.

Every message, both ways, is one JSON object in one text frame. Binary frames
are ignored.

## A session

```text
→ {"id":1,"call":"hello","args":{"name":"fly.py"}}
← {"type":"reply","id":1,"ok":{"kind":"hello","server":"crazyflie-connection-server","protocol":1}}
→ {"id":2,"call":"attach","args":{"uri":"radio:///80/2M/E7E7E7E7E7"}}
← {"type":"reply","id":2,"ok":{"kind":"attached","uri":"radio:///80/2M/E7E7E7E7E7","params":[…],"log_vars":[…],…}}
→ {"id":3,"call":"log_subscribe","args":{"uri":"radio:///80/2M/E7E7E7E7E7","sub":1,"vars":["pm.vbat"],"period_ms":100}}
← {"type":"reply","id":3,"ok":{"kind":"done"}}
← {"type":"event","sub":1,"event":{"kind":"log","timestamp":123400,"values":{"pm.vbat":3.92}}}
← {"type":"event","sub":1,"event":{"kind":"log","timestamp":123500,"values":{"pm.vbat":3.91}}}
→ {"id":4,"call":"unsubscribe","args":{"sub":1}}
← {"type":"reply","id":4,"ok":{"kind":"done"}}
→ {"id":5,"call":"detach","args":{"uri":"radio:///80/2M/E7E7E7E7E7"}}
← {"type":"reply","id":5,"ok":{"kind":"done"}}
```

[`examples/python/raw_client.py`](../examples/python/raw_client.py) does this
in about 40 lines.

## Requests and replies

A request is `{"id": …, "call": "…", "args": {…}}`:

- `id` is any unsigned 64-bit number the client picks. The server echoes it in
  the reply and doesn't look at it otherwise. Use a counter.
- `args` is needed for every call except `list`, which takes none. Sending
  `"args": {}` with `list` is an error. A call whose arguments are all
  optional (`scan`) takes `"args": {}`.
- Optional arguments can be left out or `null`. The server ignores arguments
  it doesn't know.

The server answers every request with exactly one `reply` carrying the same
`id`, with either `ok` (the result, its `kind` telling which) or `error` (a
message for a human):

```text
← {"type":"reply","id":7,"ok":{"kind":"value","value":3.2}}
← {"type":"reply","id":8,"error":"Not attached to radio:///80/2M/E7E7E7E7E7"}
```

Requests run concurrently, so replies can come in a different order than the
requests. Match them by `id`. A message that isn't a valid request gets an
error reply if it has an `id`, and is dropped otherwise.

## Attaching

Calls on a Crazyflie (those with a `uri`) need the client to be attached to
it first. The exceptions are `attach` and `detach`, `scan_selected`, `flash`
and `platform`. Without attaching, the call fails with `Not attached to …`.

`attach` connects the Crazyflie if no other client has; otherwise the client
shares the connection that is already there. The reply has the param and log
TOCs and the memory list, so a client doesn't download anything itself. Over
radio, `attach` fails within a second when the Crazyflie doesn't answer, or
when only its nRF51 does (it is asleep or in its bootloader). Every
client attached to a Crazyflie can send it every call: the server does not
lock params or flying to one client.

`detach` lets go of it. The server disconnects a Crazyflie a while (10 s by
default) after its last client detaches, so a client that comes back
quickly finds it still connected.

Closing the WebSocket detaches the client from everything and closes its
streams. Jobs it started go on.

When a Crazyflie the client is attached to is lost (out of range, rebooted,
taken over by a job), the client gets a `disconnected` message and is no
longer attached:

```text
← {"type":"disconnected","uri":"radio:///80/2M/E7E7E7E7E7","reason":"Rebooted by fly.py"}
```

## Streams

Some calls open a stream: `param_watch`, `log_subscribe`,
`console_subscribe`, `lighthouse_angle_subscribe`, `watch` and `flash`. The
client picks the stream's number, `sub`, and the server sends the stream's
data as `event` messages with that number:

```text
← {"type":"event","sub":1,"event":{"kind":"log","timestamp":123400,"values":{"pm.vbat":3.92}}}
```

- `sub` numbers belong to the connection. Opening a stream with a number
  that is already open closes the old stream.
- Events can arrive before the reply to the call that opened the stream
  (e.g. the history of `console_subscribe`).
- `unsubscribe` closes a stream. Events already on their way can still
  arrive after it, so drop events for streams you closed.
- The server ends a stream with an `ended` event when it can't go on (log
  error). The streams on a Crazyflie also end when it disconnects, with or
  without an `ended` event.
- `watch` isn't tied to a Crazyflie, and a `flash` job's stream ends with a
  `done` event.

## Jobs

A job takes Crazyflies over for a while. Flashing (`flash`) is one, and so are
`platform` commands that reboot a Crazyflie or put it to sleep. While a job
runs:

- the Crazyflies it took over are disconnected, and attaching to them fails
  with the job's reason;
- `scan` is refused, because a scan holds the Crazyradio too long for the
  bootloaders.

`watch` opens a stream that tells when jobs take Crazyflies over (`busy`) and
give them back (`available`). `available` says whether the server connected
the Crazyflie again: after a reboot it does if a client was attached, so
attaching again is immediate.

A Crazyflie whose radio channel or address was changed in its EEPROM config
takes the new one when it restarts. Reboot it with `new_uri` set to where it
will be: the reply then waits for its firmware to answer there, the server
doesn't connect it again, and `available` says where it went with `moved_to`.

A `flash` job reports on the stream it was started with: `progress` events,
one `flashed` or `flash_failed` per Crazyflie, then `done`. It finishes even
if the client that started it goes away.

## URIs

Crazyflies are addressed by the usual URIs, `radio://<radio>/<channel>/<datarate>/<address>`.
The server normalizes them, so different ways of writing the same Crazyflie
mean the same one:

| Part | |
|---|---|
| radio | Optional: `radio:///80` lets the server pick a Crazyradio, `radio://1/80` asks for the second one |
| channel | Required, 0 to 125 |
| datarate | `250K`, `1M` or `2M` (the default), any case |
| address | 10 hex digits, any case, `E7E7E7E7E7` by default |
| options | `?…` after the address are passed to the link |

`radio://1/80/2m/e7e7e7e701?safelink=0` and `radio:///80/2M/E7E7E7E701` are the
same Crazyflie, and replies always use the normalized form
`radio:///80/2M/E7E7E7E701`. Other URIs, like `usb://0`, are used as they are.
`flash` and `platform` only work with radio URIs.

## Values

- Params and log variables are numbers on the wire whatever their type in the
  firmware. The server converts them.
- Memory contents (`memory_read`, `memory_write`) and firmware images
  (`flash`) are base64 strings. The LPP packets of `send_short_lpp_packet` are
  arrays of byte values.
- Units are in the reference: meters, seconds and radians for the high-level
  commander, degrees for `setpoint_rpyt`.

## Versions

The `hello` reply has the protocol version, now `1`. The protocol is still a
prototype and can change; check the version, and ignore fields and message
kinds you don't know so that additions don't break your client.
