import asyncio
from websockets.asyncio.server import serve

ROOM = {"p1": None, "p2": None}
SCORES = {"p1": 0, "p2": 0}
GAME_OVER = False

DISCOVERY_PORT = 8766
WEBSOCKET_PORT = 8765

async def handle_client(websocket):
    global GAME_OVER

    if ROOM["p1"] is None:
        my_role = "p1"
    elif ROOM["p2"] is None:
        my_role = "p2"
    else:
        await websocket.close()
        return

    ROOM[my_role] = websocket
    SCORES[my_role] = 0

    print(f"sr cnnctd as {my_role}")

    if ROOM["p1"] is not None and ROOM["p2"] is not None:
        GAME_OVER = False
        SCORES["p1"] = 0
        SCORES["p2"] = 0
        await ROOM["p1"].send("state:0:0:1:none")
        await ROOM["p2"].send("state:0:0:1:none")

    try:
        async for msg in websocket:
            if GAME_OVER:
                continue
            if ROOM["p1"] is None or ROOM["p2"] is None:
                continue

            if msg.startswith("click:"):
                _, color = msg.split(":")
                SCORES[my_role] += 1

                v1 = SCORES["p1"]
                v2 = SCORES["p2"]

                bg_to_p1 = color if my_role == "p2" else "none"
                bg_to_p2 = color if my_role == "p1" else "none"

                await ROOM["p1"].send(f"state:{v1}:{v2}:1:{bg_to_p1}")
                await ROOM["p2"].send(f"state:{v1}:{v2}:2:{bg_to_p2}")
                
                if SCORES[my_role] >= 100:
                    GAME_OVER = True
                    p_num = "Игрок 1" if my_role == "p1" else "Игрок 2"

                    role_p1 = "win" if my_role == "p1" else "lose"
                    role_p2 = "win" if my_role == "p2" else "lose"

                    await ROOM["p1"].send(f"win:{p_num}:{role_p1}")
                    await ROOM["p2"].send(f"win:{p_num}:{role_p2}")

                    SCORES["p1"] = 0
                    SCORES["p2"] = 0
                    await asyncio.sleep(0.5)
                    GAME_OVER = False
    except Exception as e:
        print(f"Ошибка сессии: {e}")
    finally:
        ROOM[my_role] = None
        SCORES[my_role] = 0
        GAME_OVER = False

        other_role = "p2" if my_role == "p1" else "p1"
        if ROOM[other_role] is not None:
            SCORES[other_role] = 0
            try:
                await ROOM[other_role].send("leave")
            except:
                pass

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