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

```sh
cargo add crazyflie-connection-server
```

A client that only talks to a server elsewhere can leave out crazyflie-lib,
libusb and axum:

```toml
crazyflie-connection-server = { version = "0.2", default-features = false }
```

The API is documented on [docs.rs](https://docs.rs/crazyflie-connection-server).

## Run a server

The `server` example is a standalone server. It holds this computer's
Crazyradios until Ctrl-C:

```sh
cargo run --release --example server                            # ws://127.0.0.1:7777
cargo run --release --example server -- --listen 0.0.0.0:7777   # other computers too
cargo run --release --example server -- --toc-cache ~/.cache/crazyflie-connection-server
```

| Option | Default | |
|---|---|---|
| `--listen` | `127.0.0.1:7777` | Address to listen on |
| `--linger` | `10` | Seconds a Crazyflie stays connected after its last client is gone |
| `--toc-cache DIR` | in memory | Keep the TOCs on disk, so that a restart doesn't download them again |

There is no authentication yet: listen on other addresses than 127.0.0.1
only on a trusted network or over a tunnel. To serve from your own program,
see [examples/server.rs](https://github.com/evoggy/crazyflie-connection-server/blob/main/crates/server/examples/server.rs):
`Server::new` with a `ServerConfig`, then `serve_on`.

## Try it

With the server running in another terminal:

```sh
cargo run --example cf -- scan                         # auto: uses the server if it runs, else direct
cargo run --example cf -- --name watcher watch-params radio:///80
cargo run --example cf -- set radio:///80 led.bitmask 1     # the watcher sees it
cargo run --example cf -- list                         # Crazyflies and their clients
cargo run --example cf -- radios                       # the server's Crazyradios and their links
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

## Other languages

Any language with a WebSocket library can use the server: every message is
one JSON text frame. [The protocol guide](https://github.com/evoggy/crazyflie-connection-server/blob/main/docs/protocol.md) explains requests,
replies and streams, [the reference](https://github.com/evoggy/crazyflie-connection-server/blob/main/docs/protocol-reference.md) has every
call with an example, and [protocol.schema.json](https://github.com/evoggy/crazyflie-connection-server/blob/main/docs/protocol.schema.json)
is the JSON Schema of the messages.
[`examples/python/raw_client.py`](https://github.com/evoggy/crazyflie-connection-server/blob/main/examples/python/raw_client.py) attaches,
reads a parameter and subscribes to a log variable in about 40 lines.

The reference and the schema are generated from `crates/protocol/src/lib.rs`:
`cargo test` writes them again and fails when they changed, until they are
committed. A new call needs an example in `crates/protocol/src/reference.rs`.

## Status

Early: the protocol and the API can still change between 0.x versions.
Supported: scan (all channels, or which of a list of Crazyflies
answer), list, attach, params (get/set/watch, persistent state/store/clear,
shared cache), log subscriptions (spread over as many log blocks as they
need), console, high-level commander (take-off, land, go-to, stop, define and
start trajectories), roll/pitch/yaw-rate/thrust setpoints, arming, emergency
stop, crash recovery, lighthouse sweep angles, link statistics (of one
Crazyflie, or of every link on each of the server's Crazyradios), reboot and
power off/sleep/wake up, memories (the list, raw reads and writes, decks'
1-wire memories, the EEPROM config, lighthouse geometries and calibrations,
trajectories and Loco anchors, named as in crazyflie-lib), Loco Positioning
packets to the anchors, and flashing the STM32 and nRF51 as a job that
shares the Crazyradio with the other connections: one Crazyflie, or a swarm
of one platform, by broadcast to the Crazyflies whose bootloaders take it.
Clients can watch which Crazyflies jobs take over and give back. Every client
can send every command.

Connecting a swarm: Crazyflies on (nearly) the same channel share a
Crazyradio, all of them connect at the same time, and a firmware's TOCs are
downloaded from one Crazyflie while the others with that firmware wait for
the cache. `FileTocCache` keeps the TOCs on disk across restarts (the
`server` example's `--toc-cache DIR`). Whether a param is persistent is asked for when a client needs
it, not while connecting.

For tools with their own connection code, `Client::adopt` serves a Crazyflie
the tool connected itself, and in direct and shared mode `Crazyflie::lib()`
gives the crazyflie-lib `Crazyflie` behind a handle for what the API does not
cover yet.

## License

Licensed under either of [Apache License, Version 2.0](https://github.com/evoggy/crazyflie-connection-server/blob/main/LICENSE-APACHE)
or [MIT license](https://github.com/evoggy/crazyflie-connection-server/blob/main/LICENSE-MIT) at your option.
