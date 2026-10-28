# CLAUDE.md

## Keep the protocol docs up to date

`docs/` documents the WebSocket protocol. Update it in the same commit as any
change to the protocol or to how the server behaves towards clients.

- `docs/protocol-reference.md` and `docs/protocol.schema.json` are generated
  from `crates/protocol/src/lib.rs`. `cargo test` writes them again and fails
  when they changed: check them and commit them with the change.
- A new call needs one example in `crates/protocol/src/reference.rs`, and new
  replies and events have to show up in some example. The tests fail otherwise.
- The doc comments on calls, arguments, replies, events and fields are the
  reference's text. Give units (meters, seconds, radians, …).
- `docs/protocol.md` is written by hand and no test checks it. Update it when
  clients would see a difference: attaching, streams, jobs, errors and
  timeouts, URIs, values.

## Formatting

The code is rustfmt-formatted and CI checks it: run `cargo fmt --all` before
committing.

## Releasing

Both crates share the workspace version and are published together.

1. Bump `version` in `[workspace.package]` and the `crazyflie-connection-protocol`
   version in `[workspace.dependencies]` (root `Cargo.toml`), update
   `Cargo.lock` (`cargo check`), commit as "Version X.Y.Z".
2. Tag that commit on main with the bare version (`X.Y.Z`, no `v`) and push the tag.
3. Run the "Publish to crates.io" workflow on the tag (Actions, Run workflow,
   pick the tag). It checks the tag against the versions and publishes the
   protocol crate first, through crates.io trusted publishing.
