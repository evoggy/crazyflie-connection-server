# crazyflie-connection-server

A connection server: share Crazyradios and Crazyflie connections between
several tools, on this machine or across the network. The server owns the
radios and keeps one [crazyflie-lib](https://github.com/bitcraze/crazyflie-lib-rs) connection per
Crazyflie; clients attach to the same Crazyflie at the same time (e.g.
Swarmkeeper monitoring while a script flies).

| Crate | What |
|---|---|
| `crazyflie-connection-server` | The client API, named like crazyflie-lib, and the server. A program uses `Client` in every mode; with the `server` feature (on by default) the server can run inside it |
| `crazyflie-connection-protocol` | The messages, JSON over WebSocket. Depends on serde only |

A client that only talks to a server elsewhere can leave out crazyflie-lib,
libusb and axum:

```toml
crazyflie-connection-server = { version = "0.1", default-features = false }
```

## Try it

```sh
cfcli server run                                       # listens on 127.0.0.1:7777
cargo run --example cf -- scan                         # auto: uses the server if it runs, else direct
cargo run --example cf -- --name watcher watch-params radio:///80
cargo run --example cf -- set radio:///80 led.bitmask 1     # the watcher sees it
cargo run --example cf -- list                         # Crazyflies and their clients
cargo run --example cf -- --connection direct log radio:///80 pm.vbat
```

The mode comes from `--connection` or `CRAZYFLIE_CONNECTION`:

| Mode | The Crazyradios are driven by |
|---|---|
| `direct` | this program, through a server in-process that nobody else reaches |
| `shared` | the same, and other programs attach to it on 127.0.0.1:7777 while this program runs |
| `local` | a server already running on this machine |
| `host`, `host:port`, `ws://…` | a server on another machine |
| `auto` (default) | `local` if a server runs, otherwise `direct` (an app can choose `shared`, as Swarmkeeper does) |

URIs are the usual `radio://` ones. `radio:///80/2M/E7E7E7E7E7` (no radio
index) lets the server pick the radio.

A standalone server is started with `cfcli server run`. For remote clients,
start it with `--listen 0.0.0.0:7777`. There is no authentication yet; use it
on a trusted network or over a tunnel.

## Other languages

Any language with a WebSocket library can use the server: every message is
one JSON text frame, defined in `crates/protocol/src/lib.rs`.
[`examples/python/raw_client.py`](examples/python/raw_client.py) attaches,
reads a parameter and subscribes to a log variable in about 40 lines.

## Status

Prototype. Supported: scan, list, attach, params (get/set/watch, persistent
state/store/clear, shared cache), log subscriptions (spread over as many log
blocks as they need), console, high-level commander (take-off, land, go-to,
stop, define and start trajectories), roll/pitch/yaw-rate/thrust setpoints,
arming, emergency stop, crash recovery, lighthouse sweep angles, link
statistics, and flashing the STM32 and nRF51 of one Crazyflie as a job that
shares the Crazyradio with the other connections. Clients can watch which
Crazyflies jobs take over and give back. Every client can send every command.

For tools with their own connection code (cfcli's swarm runner),
`Client::adopt` serves a Crazyflie the tool connected itself, and in direct
and shared mode `Crazyflie::lib()` gives the crazyflie-lib
`Crazyflie` behind a handle for what the API does not cover yet.
