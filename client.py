import asyncio
import queue
import socket
import threading
from tkinter import *
from tkinter import ttk
import random

from websockets.asyncio.client import connect


COLORS = ["red", "green", "blue", "yellow", "purple", "orange", "pink", "cyan"]

ui_queue = queue.Queue()
loop_ref = {}
websocket_ref = {}

DISCOVERY_PORT = 8766
WEBSOCKET_PORT = 8765


async def discover_server():
    loop = asyncio.get_running_loop()

    broadcast_addresses = ["255.255.255.255"]
    try:
        hostname = socket.gethostname()
        _, _, ip_addresses = socket.gethostbyname_ex(hostname)

        for ip in ip_addresses:
            if ip.startswith("192.168.") or ip.startswith("10.") or ip.startswith("172."):
                parts = ip.split(".")
                parts[-1] = "255"
                broadcast_addresses.append(".".join(parts))
    except Exception as e:
        print(f"Ошибка определения интерфейсов: {e}")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.bind(("", 0))
    sock.setblocking(False)

    try:
        for target_ip in set(broadcast_addresses):
            await loop.sock_sendto(
                sock,
                b"COLOR_APP_DISCOVER",
                (target_ip, DISCOVERY_PORT),
            )
        try:
            data, addr = await asyncio.wait_for(
                loop.sock_recvfrom(sock, 1024),
                timeout=5,
            )
        except asyncio.TimeoutError:
            raise RuntimeError("Сервер не найден в локальной сети")

        if data != b"COLOR_APP_HERE":
            raise RuntimeError("Получен неизвестный ответ")

        return f"ws://{addr[0]}:{WEBSOCKET_PORT}"
    finally:
        sock.close()


async def network_client():
    try:
        server_url = await discover_server()
        print("Подключение к", server_url)

        async with connect(server_url) as websocket:
            websocket_ref["ws"] = websocket

            async for color in websocket:
                ui_queue.put(("color", color))
    finally:
        websocket_ref.pop("ws", None)


def network_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop_ref["loop"] = loop

    try:
        loop.run_until_complete(network_client())
    finally:
        loop.close()

def change_color():
    websocket = websocket_ref.get("ws")
    loop = loop_ref.get("loop")

    if websocket is None or loop is None:
        return

    color = random.choice(COLORS)

    asyncio.run_coroutine_threadsafe(websocket.send(color), loop)


def __ui_queue__():
    while True:
        try:
            message_type, value = ui_queue.get_nowait()
        except queue.Empty:
            break

        if message_type == "color":
            root.configure(bg=value)

    root.after(60, __ui_queue__)

def entered(event):
    btn["text"] = "Click Me!"

def left(event):
    btn["text"] = ""

root = Tk()
root.title("Button")
root.geometry("300x250+800+200")


btn = ttk.Button(root, text="", command=change_color    )
btn.pack(expand=True, ipadx=10, ipady=10)
btn.bind("<Enter>", entered)
btn.bind("<Leave>", left)

threading.Thread(target=network_thread, daemon=True).start()

root.after(50, __ui_queue__)
root.mainloop()