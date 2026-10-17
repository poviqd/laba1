import asyncio
from websockets.asyncio.server import serve

CLIENTS = set()

DISCOVERY_PORT = 8766
WEBSOCKET_PORT = 8765

COLORS = {"red", "green", "blue", "yellow", "purple", "orange", "pink", "cyan"}

async def handle_client(websocket):
    CLIENTS.add(websocket)
    print("sr cnnctd")
    try:
        async for color in websocket:
            if color not in COLORS:
                continue

            others = [client for client in CLIENTS if client is not websocket]
            await asyncio.gather(
                *(client.send(color) for client in others),
                return_exceptions=True,
            )
    finally:
        CLIENTS.discard(websocket)

class DiscoveryProtocol(asyncio.DatagramProtocol):
    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        if data == b"COLOR_APP_DISCOVER":
            self.transport.sendto(b"COLOR_APP_HERE", addr)


async def main():
    loop = asyncio.get_running_loop()

    udp_transport, _ = await loop.create_datagram_endpoint(
        DiscoveryProtocol,
        local_addr=("0.0.0.0", DISCOVERY_PORT),
    )

    try:
        async with serve(handle_client, "0.0.0.0", WEBSOCKET_PORT):
            print("Server started")
            await asyncio.Future()
    finally:
        udp_transport.close()


asyncio.run(main())