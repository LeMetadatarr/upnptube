"""
The screen side of the YouTube Lounge protocol. A screen holds one session per sender
app (YouTube, YouTube Music); a sender pairs with it over DIAL or with a TV code, then
controls it through YouTube's bind channel.
"""
import asyncio
import json
import logging
import random
import re
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import aiohttp

from .store import Store

LOG = logging.getLogger(__name__)

LOUNGE_URL = "https://www.youtube.com/api/lounge"
BIND_URL = f"{LOUNGE_URL}/bc/bind"
RPC_RETRIES = 3
RESTART_DELAYS = (5, 30, 120, 300)
TOKEN_REFRESH_SECONDS = 13 * 24 * 3600
DEVICE_INFO = {
    "year": 0,
    "os": "Linux",
    "osVersion": "",
    "chipset": "",
    "clientName": "TVHTML5",
    "dialAdditionalDataSupportLevel": "unsupported",
    "mdxDialServerType": "MDX_DIAL_SERVER_TYPE_UNKNOWN",
}


class LoungeError(Exception):
    pass


@dataclass(frozen=True)
class Client:
    key: str
    theme: str
    name: str


YOUTUBE = Client("YT", "cl", "YouTube")
YOUTUBE_MUSIC = Client("YTMUSIC", "m", "YouTube Music")
CLIENTS = (YOUTUBE, YOUTUBE_MUSIC)


@dataclass
class Message:
    """One bind-channel message. `aid` is the number of the sender's message it answers."""
    name: str
    payload: Any = None
    aid: int | None = None


class ChunkParser:
    """
    Decode the bind channel's framing: a character count on its own line, then a JSON
    array of [aid, [name, *args]] entries. Frames can arrive split across reads.
    """
    FRAME_START = re.compile(r"\s*\d+\s*\n")

    def __init__(self):
        self.buffer = ""
        self.decoder = json.JSONDecoder()

    def feed(self, text: str) -> list[Message]:
        self.buffer += text
        messages = []
        while self.buffer.strip():
            match = self.FRAME_START.match(self.buffer)
            if match:
                start = match.end()
            elif self.buffer.lstrip().startswith("["):
                start = len(self.buffer) - len(self.buffer.lstrip())
            else:
                break  # Only part of the character count has arrived.
            try:
                entries, end = self.decoder.raw_decode(self.buffer, start)
            except json.JSONDecodeError:
                break
            self.buffer = self.buffer[end:]
            for aid, (name, *args) in entries:
                messages.append(Message(name, args[0] if len(args) == 1 else args or None, int(aid)))
        return messages


def form_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def send_body(messages: list[Message], ofs: int) -> dict[str, str]:
    body = {"count": str(len(messages)), "ofs": str(ofs)}
    for index, message in enumerate(messages):
        body[f"req{index}__sc"] = message.name
        for key, value in (message.payload or {}).items():
            if value is not None:
                body[f"req{index}_{key}"] = form_value(value)
    return body


class Session:
    """
    The screen's session for one sender app. It stays bound while the service runs, so
    a sender can find the screen again without DIAL, and refreshes itself when YouTube
    drops it.
    """

    def __init__(self, http: aiohttp.ClientSession, client: Client, screen_name: str, screen_app: str,
                 brand: str, model: str, store: Store, store_key: str,
                 on_messages: Callable[[list[Message], "Session"], Awaitable[None]], log: logging.LoggerAdapter):
        self.http = http
        self.client = client
        self.screen_name = screen_name
        self.screen_app = screen_app
        self.device_info = json.dumps({**DEVICE_INFO, "brand": brand, "model": model})
        self.store = store
        self.store_key = f"{store_key}.{client.theme}"
        self.on_messages = on_messages
        self.log = log
        saved = store.get(self.store_key) or {}
        self.device_id: str = saved.get("deviceId") or str(uuid.uuid4())
        self.screen_id: str | None = saved.get("screenId")
        self.lounge_token: str | None = None
        self.sid: str | None = None
        self.gsessionid: str | None = None
        self.rid = 0
        self.aid = 3
        self.ofs = 0
        self.ready = asyncio.Event()
        self._send_lock = asyncio.Lock()
        self._restart_lock = asyncio.Lock()
        self._inbox: asyncio.Queue[list[Message]] = asyncio.Queue()
        self._tasks: set[asyncio.Task] = set()
        self._rpc: asyncio.Task | None = None
        self._closing = False

    # --- lifecycle

    async def begin(self):
        """Bind to the lounge, and keep the session bound until `end`."""
        self._closing = False
        try:
            await self._establish()
        except (aiohttp.ClientError, LoungeError, TimeoutError) as error:
            self.log.warning("%s: cannot bind yet (%s); retrying in the background", self.client.name, error)
            self._spawn(self._restart())
        self._spawn(self._dispatch())
        self._spawn(self._refresh_token_periodically())

    async def end(self):
        self._closing = True
        if self.ready.is_set():
            try:
                await self.send([Message("loungeScreenDisconnected")])
            except (aiohttp.ClientError, LoungeError, TimeoutError) as error:
                self.log.debug("%s: cannot say goodbye: %s", self.client.name, error)
        self.ready.clear()
        for task in [*self._tasks, self._rpc]:
            if task and task is not asyncio.current_task():
                task.cancel()
        self._tasks.clear()

    async def _establish(self):
        self.ready.clear()
        self.sid = self.gsessionid = None
        self.rid = random.randint(41000, 49999)
        self.aid = 3
        self.ofs = 0
        if not self.screen_id:
            self.screen_id = await self._generate_screen_id()
        try:
            self.lounge_token = await self._get_lounge_token()
        except (aiohttp.ClientError, LoungeError) as error:
            self.log.warning("%s: saved screen id rejected (%s); generating a new one", self.client.name, error)
            self.screen_id = await self._generate_screen_id()
            self.lounge_token = await self._get_lounge_token()
        initial = await self._init_session()
        if not (self.sid and self.gsessionid):
            raise LoungeError("the bind channel sent no session id")
        self.store.set(self.store_key, {"deviceId": self.device_id, "screenId": self.screen_id})
        if self._rpc:
            self._rpc.cancel()
        self._rpc = asyncio.create_task(self._listen())
        self.ready.set()
        self.log.debug("%s: session %s bound", self.client.name, self.sid)
        status = [message for message in initial if message.name == "loungeStatus"]
        if status:
            self._inbox.put_nowait(status)

    async def _restart(self):
        """Bind again after YouTube dropped the session; retry until it works or `end`."""
        if self._restart_lock.locked():
            await self.ready.wait()
            return
        async with self._restart_lock:
            self.ready.clear()
            attempt = 0
            while not self._closing:
                try:
                    await self._establish()
                    self.log.info("%s: session restored", self.client.name)
                    return
                except (aiohttp.ClientError, LoungeError, TimeoutError) as error:
                    delay = RESTART_DELAYS[min(attempt, len(RESTART_DELAYS) - 1)]
                    self.log.warning("%s: cannot bind (%s); retrying in %s s", self.client.name, error, delay)
                    attempt += 1
                    await asyncio.sleep(delay)

    async def _dispatch(self):
        # Handlers run here, not in the listener, because a handler's send can rebind,
        # and rebinding replaces the listener task.
        while True:
            messages = await self._inbox.get()
            try:
                await self.on_messages(messages, self)
            except Exception:
                self.log.exception("%s: cannot handle %s", self.client.name, [m.name for m in messages])

    async def _refresh_token_periodically(self):
        while True:
            await asyncio.sleep(TOKEN_REFRESH_SECONDS)
            self.log.info("%s: refreshing the lounge token", self.client.name)
            await self._restart()

    def _spawn(self, coro):
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    # --- pairing endpoints

    async def _generate_screen_id(self) -> str:
        async with self.http.get(f"{LOUNGE_URL}/pairing/generate_screen_id") as resp:
            resp.raise_for_status()
            return (await resp.text()).strip()

    async def _get_lounge_token(self) -> str:
        async with self.http.post(f"{LOUNGE_URL}/pairing/get_lounge_token_batch",
                                  data={"screen_ids": self.screen_id}) as resp:
            resp.raise_for_status()
            info = await resp.json(content_type=None)
        try:
            return info["screens"][0]["loungeToken"]
        except (KeyError, IndexError, TypeError):
            raise LoungeError(f"no lounge token for screen {self.screen_id}") from None

    async def register_pairing_code(self, code: str):
        """Accept the pairing code that a sender sends with a DIAL launch."""
        data = {"access_type": "permanent", "app": self.screen_app, "pairing_code": code,
                "screen_id": self.screen_id, "screen_name": self.screen_name, "device_id": self.device_id}
        async with self.http.post(f"{LOUNGE_URL}/pairing/register_pairing_code", data=data) as resp:
            if not resp.ok:
                raise LoungeError(f"cannot register pairing code: HTTP {resp.status}")

    async def get_pairing_code(self) -> str:
        """A TV code, which a sender types to link with this screen."""
        await asyncio.wait_for(self.ready.wait(), 30)
        data = {"access_type": "permanent", "app": self.screen_app, "lounge_token": self.lounge_token,
                "screen_id": self.screen_id, "screen_name": self.screen_name, "device_id": self.device_id}
        async with self.http.post(f"{LOUNGE_URL}/pairing/get_pairing_code?ctx=pair", data=data) as resp:
            resp.raise_for_status()
            return (await resp.text()).strip()

    # --- bind channel

    def _params(self, kind: str, aid: int | None = None) -> dict[str, str]:
        params = {
            "device": "LOUNGE_SCREEN", "id": self.device_id, "obfuscatedGaiaId": "", "name": self.screen_name,
            "app": self.screen_app, "theme": self.client.theme, "capabilities": "dsp,mic,dpa,ntb", "cst": "m",
            "mdxVersion": "2", "loungeIdToken": self.lounge_token or "", "VER": "8", "v": "2",
            "zx": uuid.uuid4().hex[:12], "t": "1",
        }
        if kind == "init":
            params.update(deviceInfo=self.device_info, RID=str(self.rid), CVER="1")
            self.rid += 1
        elif kind == "send":
            self.aid = max(aid, self.aid) if aid else self.aid
            params.update(deviceInfo=self.device_info, SID=self.sid, RID=str(self.rid), AID=str(self.aid),
                          gsessionid=self.gsessionid)
            if not aid:
                self.aid += 1
            self.rid += 1
        else:
            params.update(RID="rpc", SID=self.sid, CI="0", AID=str(self.aid), gsessionid=self.gsessionid,
                          TYPE="xmlhttp")
        return params

    def _note(self, messages: list[Message]):
        for message in messages:
            if message.name == "c":
                self.sid = message.payload[0]
            elif message.name == "S":
                self.gsessionid = message.payload
            if message.aid:
                self.aid = max(self.aid, message.aid)

    async def _init_session(self) -> list[Message]:
        async with self.http.post(BIND_URL, params=self._params("init"), data={"count": "0"}) as resp:
            resp.raise_for_status()
            messages = ChunkParser().feed(await resp.text())
        self._note(messages)
        return messages

    async def _listen(self):
        """Hold the long poll open and hand on what senders say; bind again when it fails."""
        failures = 0
        while not self._closing:
            try:
                async with self.http.get(BIND_URL, params=self._params("rpc"),
                                         timeout=aiohttp.ClientTimeout(total=None, sock_read=300)) as resp:
                    if resp.status != 200:
                        # 400 "Unknown SID" or 410: this session is gone; only a new one helps.
                        self.log.info("%s: bind channel refused (HTTP %s)", self.client.name, resp.status)
                        break
                    failures = 0
                    parser = ChunkParser()
                    async for chunk in resp.content.iter_any():
                        messages = parser.feed(chunk.decode("utf-8", "replace"))
                        self._note(messages)
                        if messages:
                            self._inbox.put_nowait(messages)
            except (aiohttp.ClientError, TimeoutError) as error:
                failures += 1
                self.log.warning("%s: bind channel error (%s), attempt %s", self.client.name, error, failures)
                if failures > RPC_RETRIES:
                    break
                await asyncio.sleep(1)
        if not self._closing:
            self._rpc = None
            await self._restart()

    async def send(self, messages: list[Message], aid: int | None = None):
        """Post messages to every sender in this session, in order."""
        await asyncio.wait_for(self.ready.wait(), 60)
        async with self._send_lock:
            body = send_body(messages, self.ofs)
            self.ofs += len(messages)
            self.log.debug("%s: send %s", self.client.name, body)
            async with self.http.post(BIND_URL, params=self._params("send", aid), data=body) as resp:
                if resp.ok:
                    return
                status = resp.status
        self.log.warning("%s: send refused (HTTP %s); binding again", self.client.name, status)
        await self._restart()
        async with self._send_lock:
            body = send_body(messages, self.ofs)
            self.ofs += len(messages)
            async with self.http.post(BIND_URL, params=self._params("send"), data=body) as resp:
                if not resp.ok:
                    raise LoungeError(f"send failed twice: HTTP {resp.status}")
