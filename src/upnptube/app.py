"""
The DIAL application named "YouTube": the screen that YouTube and YouTube Music
senders connect to, and the translation between their messages and the player.
"""
import json
import logging
import uuid
from dataclasses import dataclass
from enum import Enum
from urllib.parse import parse_qs

import aiohttp

from .lounge import CLIENTS, YOUTUBE, Client, LoungeError, Message, Session
from .player import Autoplay, NavInfo, Player, PlayerState, Status, Volume
from .store import Store

LOG = logging.getLogger(__name__)

SCREEN_APP = "upnptube"


class ResetPolicy(str, Enum):
    """When the last sender leaves, which kind of leaving clears the queue."""
    ALL_DISCONNECTED = "allDisconnected"
    ALL_EXPLICITLY_DISCONNECTED = "allExplicitlyDisconnected"


@dataclass
class Sender:
    id: str
    name: str
    client: Client | None
    capabilities: list[str]

    @classmethod
    def parse(cls, data: dict) -> "Sender | None":
        client = next((c for c in CLIENTS if c.theme == data.get("theme")), None)
        name = data.get("name") or data.get("clientName")
        if not data.get("id") or not name or client is None:
            return None
        return cls(data["id"], name, client, (data.get("capabilities") or "").split(","))

    @property
    def supports_autoplay(self) -> bool:
        return "atp" in self.capabilities


def _loaded(state: PlayerState) -> int:
    return state.duration if state.status in (Status.PLAYING, Status.PAUSED, Status.LOADING) else 0


def now_playing(state: PlayerState | None, aid: int | None = None) -> Message:
    payload = {}
    current = state.queue.current if state else None
    if state and current:
        payload = {
            "currentTime": state.position, "duration": state.duration, "cpn": state.cpn,
            "loadedTime": _loaded(state), "videoId": current.id, "state": int(state.status),
            "seekableStartTime": 0, "seekableEndTime": state.duration,
            "listId": current.list_id, "currentIndex": current.index, "params": current.params, "ctt": current.ctt,
        }
    return Message("nowPlaying", payload, aid)


def on_state_change(state: PlayerState, aid: int | None = None) -> Message:
    return Message("onStateChange", {
        "state": int(state.status), "currentTime": state.position, "duration": state.duration,
        "loadedTime": _loaded(state), "seekableStartTime": 0, "seekableEndTime": state.duration, "cpn": state.cpn,
    }, aid)


def on_volume_changed(volume: Volume, aid: int | None = None) -> Message:
    return Message("onVolumeChanged", {"volume": volume.level, "muted": volume.muted}, aid)


def on_autoplay_mode_changed(mode: Autoplay, aid: int | None = None) -> Message:
    return Message("onAutoplayModeChanged", {"autoplayMode": mode.value}, aid)


def on_has_previous_next_changed(nav: NavInfo | None, aid: int | None = None) -> Message:
    return Message("onHasPreviousNextChanged", {"hasPrevious": bool(nav and nav.has_previous),
                                                "hasNext": bool(nav and nav.has_next)}, aid)


def autoplay_up_next(video_id: str | None, aid: int | None = None) -> Message:
    return Message("autoplayUpNext", {"videoId": video_id} if video_id else {}, aid)


class YouTubeApp:
    allow_stop = False

    def __init__(self, http: aiohttp.ClientSession, player: Player, screen_name: str, brand: str, model: str,
                 store: Store, store_key: str, log: logging.LoggerAdapter,
                 reset_policy: ResetPolicy = ResetPolicy.ALL_EXPLICITLY_DISCONNECTED, autoplay: bool = True):
        self.player = player
        self.reset_policy = reset_policy
        self.autoplay_on_connect = Autoplay.ENABLED if autoplay else Autoplay.DISABLED
        self.log = log
        self.pid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"upnptube:{store_key}"))
        self.state = "stopped"
        self.sessions = {client.theme: Session(http, client, screen_name, SCREEN_APP, brand, model, store,
                                               store_key, self._handle_messages, log)
                         for client in CLIENTS}
        self.active: Session | None = None
        self.senders: list[Sender] = []
        self.implicitly_disconnected: set[str] = set()
        self._autoplay_before_unsupported: Autoplay | None = None
        player.on_state = self._on_player_state

    async def start(self):
        self.state = "starting"
        for session in self.sessions.values():
            await session.begin()
        self.state = "running"

    async def stop(self):
        self.state = "stopping"
        await self.player.reset()
        for session in self.sessions.values():
            await session.end()
        self.senders.clear()
        self.state = "stopped"

    async def launch(self, data: str) -> str:
        """A sender asked DIAL to launch the app; its pairing code links it to a session."""
        query = parse_qs(data or "")
        code = query.get("pairingCode", [None])[0]
        theme = query.get("theme", [YOUTUBE.theme])[0]
        session = self.sessions.get(theme)
        if not code:
            raise LoungeError("launch data has no pairing code")
        if session is None:
            raise LoungeError(f"launch data names an unknown theme {theme!r}")
        await self._switch_to(session)
        await session.register_pairing_code(code)
        return self.pid

    async def pairing_code(self) -> str:
        return await self.sessions[YOUTUBE.theme].get_pairing_code()

    async def _switch_to(self, session: Session):
        if self.active is session:
            return
        old, self.active = self.active, session
        if old is not None:
            await self.player.reset()
            self.senders.clear()
            self.implicitly_disconnected.clear()
            await self._send(old, [Message("loungeScreenDisconnected")])

    async def _send(self, session: Session, messages: list[Message], aid: int | None = None):
        try:
            await session.send(messages, aid)
        except (aiohttp.ClientError, LoungeError, TimeoutError) as error:
            self.log.warning("%s: cannot send %s: %s", session.client.name, [m.name for m in messages], error)

    # --- autoplay

    async def _set_autoplay_mode(self, mode: Autoplay, aid: int | None = None):
        if not self.active:
            return
        before = self.player.queue.autoplay
        await self.player.queue.set_autoplay_mode(mode)
        after = self.player.queue.autoplay
        messages = [on_autoplay_mode_changed(mode, aid)]
        if (before and before.id) != (after and after.id):
            messages.append(autoplay_up_next(after and after.id, aid))
        await self._send(self.active, messages)

    async def _set_autoplay_by_capabilities(self, aid: int | None = None):
        if not self.senders:
            return
        current = self.player.queue.autoplay_mode
        if not all(sender.supports_autoplay for sender in self.senders):
            if current != Autoplay.UNSUPPORTED:
                self._autoplay_before_unsupported = current
            mode = Autoplay.UNSUPPORTED
        else:
            mode = current if current != Autoplay.UNSUPPORTED else (
                self._autoplay_before_unsupported or self.autoplay_on_connect)
            self._autoplay_before_unsupported = None
        await self._set_autoplay_mode(mode, aid)

    # --- messages from senders

    async def _handle_messages(self, messages: list[Message], session: Session):
        for message in messages:
            async with self.player.lock:
                replies = await self._handle(message, session)
            if replies:
                await self._send(session, replies)

    async def _handle(self, message: Message, session: Session) -> list[Message]:
        name, data, aid = message.name, message.payload or {}, message.aid
        active = session is self.active
        player = self.player
        if name not in ("noop", "c", "S"):
            self.log.debug("%s: %s #%s %s", session.client.name, name, aid, data)

        if name == "getNowPlaying":
            return [now_playing(await player.get_state() if active else None, aid)]
        if name == "loungeStatus":
            return await self._lounge_status(data, session, aid)
        if name == "getVolume":
            return [on_volume_changed(await player.get_volume(), aid)]
        if not active:
            return []

        if name in ("setPlaylist", "updatePlaylist"):
            return await self._playlist(name, data, session, aid)
        if name == "next":
            await player.next(aid)
        elif name == "previous":
            await player.previous(aid)
        elif name == "pause":
            await player.pause(aid)
        elif name == "play":
            await player.resume(aid)
        elif name == "stopVideo":
            await player.stop(aid)
        elif name == "seekTo":
            await player.seek(int(float(data["newTime"])), aid)
        elif name == "setVolume":
            muted = str(data.get("muted", "false")).strip().lower() == "true"
            volume = Volume(int(float(data["volume"])), muted)
            if volume != await player.get_volume():
                await player.set_volume(volume, aid)
        elif name == "setAutoplayMode":
            await self._set_autoplay_mode(Autoplay(data["autoplayMode"]), aid)
        return []

    async def _lounge_status(self, data: dict, session: Session, aid: int | None) -> list[Message]:
        try:
            devices = json.loads(data.get("devices") or "[]")
        except ValueError:
            self.log.warning("Cannot parse the devices of loungeStatus: %s", data.get("devices"))
            devices = []
        senders = [s for s in (Sender.parse(d) for d in devices if d.get("type") == "REMOTE_CONTROL") if s]

        try:
            event = json.loads(data["connectionEventDetails"]) if data.get("connectionEventDetails") else {}
        except ValueError:
            event = {}
        device_id = event.get("deviceId")
        if not device_id and senders and not self.senders:
            # Senders that stayed connected while this service restarted or rebound.
            await self._switch_to(session)
            self.senders = senders
            await self._set_autoplay_by_capabilities(aid)
            return [on_has_previous_next_changed(self.player.nav_info(), aid), now_playing(None, aid)]
        if device_id:
            joining = next((s for s in senders if s.id == device_id), None)
            if joining:
                return await self._sender_connected(joining, session, aid)
            leaving = next((s for s in self.senders if s.id == device_id), None)
            if leaving:
                await self._sender_disconnected(leaving, session, event.get("ui") != "true")
                return []
        nav = self.player.nav_info() if session is self.active else None
        return [on_has_previous_next_changed(nav, aid),
                on_autoplay_mode_changed(nav.autoplay_mode if nav else Autoplay.UNSUPPORTED, aid)]

    async def _sender_connected(self, sender: Sender, session: Session, aid: int | None) -> list[Message]:
        await self._switch_to(session)
        if any(s.id == sender.id for s in self.senders):
            return []
        self.implicitly_disconnected.discard(sender.id)
        self.senders.append(sender)
        self.log.info("%s: %s connected", session.client.name, sender.name)
        await self._set_autoplay_by_capabilities(aid)
        state = await self.player.get_state()
        return [on_has_previous_next_changed(self.player.nav_info(), aid), now_playing(state, aid),
                on_state_change(state, aid)]

    async def _sender_disconnected(self, sender: Sender, session: Session, implicit: bool):
        if session is not self.active:
            return
        self.senders = [s for s in self.senders if s.id != sender.id]
        if implicit:
            self.implicitly_disconnected.add(sender.id)
        self.log.info("%s: %s disconnected%s", session.client.name, sender.name, " (implicit)" if implicit else "")
        if not self.senders:
            if self.reset_policy == ResetPolicy.ALL_DISCONNECTED or not self.implicitly_disconnected:
                await self.player.reset()
        elif self.player.queue.autoplay_mode == Autoplay.UNSUPPORTED:
            await self._set_autoplay_by_capabilities()

    async def _playlist(self, name: str, data: dict, session: Session, aid: int | None) -> list[Message]:
        player, queue = self.player, self.player.queue
        replies = []
        last_id = (data.get("videoIds") or "").split(",")[-1]
        dismissed = bool(queue.video_ids) and queue.video_ids[-1] != last_id
        if dismissed:
            await self._send(session, [autoplay_up_next(None, aid)])
        before = queue.state()
        before_autoplay = None if dismissed else before.autoplay
        nav_before = player.nav_info()
        await queue.update(name, data, session.client)
        after = queue.state()
        nav_after = player.nav_info()
        if (before_autoplay and before_autoplay.id) != (after.autoplay and after.autoplay.id):
            replies.append(autoplay_up_next(after.autoplay and after.autoplay.id, aid))

        def position(state):
            return state.current and (state.current.id, state.current.index)

        if name == "setPlaylist" and position(before) != position(after):
            await player.stop(aid)
            if after.current:
                await player.play(after.current, int(float(data.get("currentTime") or 0)), aid)
        elif name == "updatePlaylist" and not after.current:
            await player.stop(aid)
        else:
            replies.append(now_playing(await player.get_state(), aid))
            if (nav_before.has_next, nav_before.has_previous) != (nav_after.has_next, nav_after.has_previous):
                replies.append(on_has_previous_next_changed(nav_after, aid))
        return replies

    # --- player changes to senders

    async def _on_player_state(self, aid: int | None, current: PlayerState, previous: PlayerState | None):
        if not self.active or not self.senders:
            return
        if previous is None:
            status_changed = position_changed = volume_changed = playing_changed = autoplay_changed = True
        else:
            status_changed = previous.status != current.status
            position_changed = previous.position != current.position
            volume_changed = previous.volume != current.volume
            was, now = previous.queue.current, current.queue.current
            playing_changed = (was and (was.id, was.index)) != (now and (now.id, now.index)) or \
                previous.queue.list_id != current.queue.list_id
            autoplay_changed = (previous.queue.autoplay and previous.queue.autoplay.id) != \
                (current.queue.autoplay and current.queue.autoplay.id)
        messages = []
        if status_changed or position_changed:
            messages.append(on_state_change(current, aid))
        if playing_changed or (status_changed and (previous is None or previous.status != Status.PLAYING)):
            messages += [now_playing(current, aid), on_has_previous_next_changed(self.player.nav_info(), aid)]
        if volume_changed:
            messages.append(on_volume_changed(current.volume, aid))
        if autoplay_changed:
            messages.append(autoplay_up_next(current.queue.autoplay and current.queue.autoplay.id, aid))
        if messages:
            await self._send(self.active, messages)
