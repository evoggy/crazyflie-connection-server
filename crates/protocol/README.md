# crazyflie-connection-protocol

The messages between the Crazyflie connection server and its clients: JSON
text frames over a WebSocket. Depends on serde only.

Rust programs use the client in
[crazyflie-connection-server](https://crates.io/crates/crazyflie-connection-server)
and don't need this crate directly. It is for other servers or clients in
Rust that want the message types without the client API.

For other languages, [the protocol guide](https://github.com/evoggy/crazyflie-connection-server/blob/main/docs/protocol.md)
explains requests, replies and streams, [the reference](https://github.com/evoggy/crazyflie-connection-server/blob/main/docs/protocol-reference.md)
has every call with an example, and
[protocol.schema.json](https://github.com/evoggy/crazyflie-connection-server/blob/main/docs/protocol.schema.json)
is the JSON Schema of the messages.

## License

Licensed under either of [Apache License, Version 2.0](https://github.com/evoggy/crazyflie-connection-server/blob/main/LICENSE-APACHE)
or [MIT license](https://github.com/evoggy/crazyflie-connection-server/blob/main/LICENSE-MIT) at your option.
