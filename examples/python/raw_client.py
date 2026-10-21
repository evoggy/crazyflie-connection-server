"""Use a connection server from Python with nothing but a WebSocket.

    pip install websockets
    cfcli server run          # or Swarmkeeper, which shares its connections
    python raw_client.py radio:///80/2M/E7E7E7E7E7 [ws://host:7777/]

Attaches to the Crazyflie, reads a parameter and prints a few log samples.
The messages are described in docs/protocol.md and docs/protocol-reference.md.
"""

import asyncio
import itertools
import json
import sys

import websockets

DEFAULT_SERVER = "ws://127.0.0.1:7777/"


async def main(uri, server):
    async with websockets.connect(server) as ws:
        ids = itertools.count(1)

        async def call(method, /, **args):
            """Send a request and wait for its reply. Stream events that arrive meanwhile are dropped."""
            request_id = next(ids)
            request = {"id": request_id, "call": method}
            if args:
                request["args"] = args
            await ws.send(json.dumps(request))
            while True:
                message = json.loads(await ws.recv())
                if message["type"] == "reply" and message["id"] == request_id:
                    if "error" in message:
                        raise RuntimeError(message["error"])
                    return message["ok"]

        print(await call("hello", name="python example"))
        await call("attach", uri=uri)
        print(await call("param_get", uri=uri, name="pm.lowVoltage"))
        await call("log_subscribe", uri=uri, sub=1, vars=["pm.vbat"], period_ms=100)
        for _ in range(5):
            message = json.loads(await ws.recv())
            if message["type"] == "event" and message["sub"] == 1:
                print(message["event"])


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else DEFAULT_SERVER))
