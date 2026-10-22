import asyncio
import queue
import socket
import threading
from tkinter import *
from tkinter import ttk
from tkinter import messagebox
import random

from websockets.asyncio.client import connect
from websockets.asyncio.server import serve


COLORS = ["green", "yellow", "purple", "orange", "pink", "cyan"]

DISCOVERY_PORT = 8766
WEBSOCKET_PORT = 8765


ROOM = {"p1": None, "p2": None}
SCORES = {"p1": 0, "p2": 0}
GAME_OVER = False


def reset_server_state():
    global GAME_OVER
    ROOM["p1"] = None
    ROOM["p2"] = None
    SCORES["p1"] = 0
    SCORES["p2"] = 0
    GAME_OVER = False


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

        await ROOM["p2"].send("state:0:0:2:none")

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

                    if ROOM["p1"] is not None and ROOM["p2"] is not None:
                        await ROOM["p1"].send("state:0:0:1:none")
                        await ROOM["p2"].send("state:0:0:2:none")

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


class ServerHandle:
    def __init__(self):
        self.ready = threading.Event()
        self.error = None
        self.loop = None
        self.stop_event = None
        self.thread = None


async def run_server(handle):
    loop = asyncio.get_running_loop()
    handle.stop_event = asyncio.Event()

    udp_transport, _ = await loop.create_datagram_endpoint(
        DiscoveryProtocol,
        local_addr=("0.0.0.0", DISCOVERY_PORT),
    )

    try:
        async with serve(handle_client, "0.0.0.0", WEBSOCKET_PORT):
            print("Server started")
            handle.ready.set()
            await handle.stop_event.wait()
    finally:
        udp_transport.close()


def server_thread(handle):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    handle.loop = loop

    try:
        loop.run_until_complete(run_server(handle))
    except Exception as e:
        print(f"Ошибка сервера: {e}")
        handle.error = e
    finally:
        handle.ready.set()
        loop.close()


def start_server():
    reset_server_state()
    handle = ServerHandle()
    handle.thread = threading.Thread(target=server_thread, args=(handle,), daemon=True)
    handle.thread.start()
    return handle


def stop_server(handle):
    handle.ready.wait(2)
    if handle.loop is not None and handle.stop_event is not None:
        try:
            handle.loop.call_soon_threadsafe(handle.stop_event.set)
        except RuntimeError:
            pass
    handle.thread.join(timeout=2)


ui_queue = queue.Queue()


class Session:
    def __init__(self, is_host):
        self.is_host = is_host
        self.server = None
        self.loop = None
        self.task = None
        self.ws = None
        self.cancelled = False


session = None


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


async def network_client(sess):
    connected = False
    try:
        if sess.is_host:

            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, sess.server.ready.wait, 5)
            if sess.server.error is not None:
                raise RuntimeError(f"Не удалось создать лобби: {sess.server.error}")
            server_url = f"ws://127.0.0.1:{WEBSOCKET_PORT}"
        else:
            server_url = await discover_server()

        print("Подключение к", server_url)

        async with connect(server_url) as websocket:
            sess.ws = websocket
            connected = True
            ui_queue.put((sess, "connected", None))

            async for color in websocket:
                ui_queue.put((sess, "color", color))

        ui_queue.put((sess, "error", "Соединение с сервером закрыто"))
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(e)
        text = "Соединение с сервером потеряно" if connected else str(e)
        ui_queue.put((sess, "error", text))
    finally:
        sess.ws = None


def network_thread(sess):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    sess.loop = loop
    sess.task = loop.create_task(network_client(sess))

    if sess.cancelled:
        sess.task.cancel()

    try:
        loop.run_until_complete(sess.task)
    except asyncio.CancelledError:
        pass
    finally:
        loop.close()


def start_session(is_host):
    global session

    sess = Session(is_host)
    session = sess

    if is_host:
        sess.server = start_server()

    threading.Thread(target=network_thread, args=(sess,), daemon=True).start()

    show_game_screen(
        "Лобби создано. Ждём соперника..." if is_host else "Поиск лобби в сети..."
    )


def stop_session():
    global session

    sess, session = session, None
    if sess is None:
        return

    sess.cancelled = True
    if sess.loop is not None and sess.task is not None:
        try:
            sess.loop.call_soon_threadsafe(sess.task.cancel)
        except RuntimeError:
            pass

    if sess.server is not None:
        stop_server(sess.server)


def change_color():
    sess = session
    if sess is None:
        return

    websocket = sess.ws
    loop = sess.loop

    if websocket is None or loop is None:
        return

    color = random.choice(COLORS)

    asyncio.run_coroutine_threadsafe(websocket.send(f"click:{color}"), loop)


last_scores = (0, 0)


def update_progress_bar(v1, v2):
    global last_scores
    last_scores = (v1, v2)

    total = v1 + v2
    canvas_width = canvas.winfo_width()
    canvas_height = canvas.winfo_height()

    if total == 0:
        split_x = canvas_width // 2
    else:
        split_x = int((v1 / total) * canvas_width)

    canvas.coords(rect_left, 0, 0, split_x, canvas_height)
    canvas.coords(rect_right, split_x, 0, canvas_width, canvas_height)


def set_bg(color):
    game_frame.configure(bg=color)
    lbl_status.configure(bg=color)
    lbl_yours.configure(bg=color)
    lbl_enemy.configure(bg=color)


def reset_ui_state():
    lbl_yours["text"] = "Твои клики: 0"
    lbl_enemy["text"] = "Клики очередняры: 0"
    set_bg("gray94")
    update_progress_bar(0, 0)


def show_msg_box(title, text, is_warning=False):
    if is_warning:
        messagebox.showwarning(title, text)
    else:
        messagebox.showinfo(title, text)


def show_menu_screen():
    game_frame.pack_forget()
    reset_ui_state()
    btn["state"] = "disabled"
    menu_frame.pack(fill=BOTH, expand=True)


def show_game_screen(status):
    menu_frame.pack_forget()
    reset_ui_state()
    lbl_status["text"] = status
    btn["state"] = "disabled"
    game_frame.pack(fill=BOTH, expand=True)
    root.update_idletasks()
    update_progress_bar(0, 0)


def leave_to_menu():
    stop_session()
    show_menu_screen()


def on_close():
    stop_session()
    root.destroy()


def __ui_queue__():
    while True:
        try:
            sess, message_type, value = ui_queue.get_nowait()
        except queue.Empty:
            break


        if sess is not session:
            continue

        if message_type == "connected":
            if not sess.is_host:
                lbl_status["text"] = "Подключено, ждём начала игры..."

        elif message_type == "error":
            leave_to_menu()
            root.after(10, lambda m=value: show_msg_box("Ошибка", m, is_warning=True))

        elif message_type == "color":
            if value.startswith("state:"):
                btn["state"] = "normal"
                lbl_status["text"] = ""
                _, p1, p2, my_role, bg_color = value.split(":")

                if my_role == "1":
                    lbl_yours["text"] = f"Твои клики: {p1}"
                    lbl_enemy["text"] = f"Клики очередняры: {p2}"
                    update_progress_bar(int(p1), int(p2))
                else:
                    lbl_yours["text"] = f"Твои клики: {p2}"
                    lbl_enemy["text"] = f"Клики очередняры: {p1}"
                    update_progress_bar(int(p2), int(p1))

                if bg_color != "none":
                    set_bg(bg_color)

            elif value.startswith("win:"):
                _, winner, role = value.split(":")
                btn["state"] = "disabled"

                reset_ui_state()
                if role == "win":
                    root.after(10, lambda: show_msg_box("Новая партия", "Вы победили!\nНачинается новая партия!"))
                else:
                    root.after(10, lambda: show_msg_box("Новая партия", "Победил очередняра!\nНачинается новая партия!"))

            elif value == "leave":
                btn["state"] = "disabled"
                reset_ui_state()
                lbl_status["text"] = "Ждём соперника..."
                root.after(10, lambda: show_msg_box("Игра окончена", "соперник ливнул с позором гг", is_warning=True))

    root.after(60, __ui_queue__)


def entered(event):
    btn["text"] = "Click Me!"


def left(event):
    btn["text"] = ""


root = Tk()
root.title("Clash of Clicks")
root.geometry("300x300+800+200")
root.configure(bg="gray94")
root.protocol("WM_DELETE_WINDOW", on_close)


menu_frame = Frame(root)

Label(menu_frame, text="Clash of Clicks", font=("Arial", 16, "bold")).pack(pady=(40, 30))

ttk.Button(
    menu_frame, text="Создать лобби", command=lambda: start_session(True)
).pack(ipadx=10, ipady=6, pady=5)

ttk.Button(
    menu_frame, text="Присоединиться к лобби", command=lambda: start_session(False)
).pack(ipadx=10, ipady=6, pady=5)


game_frame = Frame(root, bg="gray94")

lbl_status = Label(game_frame, text="", font=("Arial", 9, "italic"), bg="gray94")
lbl_status.pack(pady=(5, 0))

lbl_yours = Label(game_frame, text="Твои клики: 0", font=("Arial", 10), bg="gray94")
lbl_yours.pack(pady=(5, 0))

lbl_enemy = Label(game_frame, text="Клики очередняры: 0", font=("Arial", 10), bg="gray94")
lbl_enemy.pack(pady=(0, 5))

canvas = Canvas(game_frame, height=20, bg="grey", bd=0, highlightthickness=0)
canvas.pack(fill=X, padx=10, pady=10)

rect_left = canvas.create_rectangle(0, 0, 140, 20, fill="red", width=0)
rect_right = canvas.create_rectangle(140, 0, 280, 20, fill="blue", width=0)
canvas.bind("<Configure>", lambda e: update_progress_bar(*last_scores))

ttk.Button(game_frame, text="← В меню", command=leave_to_menu).pack(side=BOTTOM, pady=8)

btn = ttk.Button(game_frame, text="", command=change_color, state="disabled")
btn.pack(expand=True, ipadx=10, ipady=10)
btn.bind("<Enter>", entered)
btn.bind("<Leave>", left)

menu_frame.pack(fill=BOTH, expand=True)

root.after(50, __ui_queue__)
root.mainloop()