"""
Show each renderer of an upnptube service as an MPRIS player on the D-Bus session bus, so
desktop media controls and KDE Connect can control what a phone casts.

The bridge polls the control API of the service. It runs in the desktop session, apart from
the service, which can run in a container or on another host.
"""
import argparse
import asyncio
import contextlib
import logging
import re
import time
import unicodedata
from typing import Annotated

import aiohttp
from dbus_fast import BusType, DBusError, ErrorType, Variant
from dbus_fast.aio import MessageBus
from dbus_fast.annotations import (
    DBusBool,
    DBusDouble,
    DBusInt64,
    DBusObjectPath,
    DBusSignature,
    DBusStr,
    DBusVariant,
)
from dbus_fast.service import (
    PropertyAccess,
    ServiceInterface,
    dbus_method,
    dbus_property,
    dbus_signal,
)

LOG = logging.getLogger("upnptube.mpris")

DBusStrings = Annotated[list[str], DBusSignature("as")]
DBusMetadata = Annotated[dict[str, DBusVariant], DBusSignature("a{sv}")]

OBJECT_PATH = "/org/mpris/MediaPlayer2"
BUS_PREFIX = "org.mpris.MediaPlayer2.upnptube."
NO_TRACK = "/org/mpris/MediaPlayer2/TrackList/NoTrack"
SERVER = "http://127.0.0.1:8100"
POLL_SECONDS = 1.0
# A position this far from where playback should be means a seek, not drift between polls.
SEEK_THRESHOLD = 3
PLAYBACK_STATUS = {"PLAYING": "Playing", "LOADING": "Playing", "PAUSED": "Paused"}
ERRORS = (aiohttp.ClientError, TimeoutError, ValueError)
MICROSECONDS = 1_000_000


def bus_name(renderer_name: str) -> str:
    """A bus name element for the renderer: ASCII letters, digits and underscores, no leading digit."""
    ascii_name = unicodedata.normalize("NFKD", renderer_name).encode("ascii", "ignore").decode()
    element = re.sub(r"[^A-Za-z0-9_]+", "_", ascii_name).strip("_") or "renderer"
    return BUS_PREFIX + ("_" + element if element[0].isdigit() else element)


class Api:
    def __init__(self, http: aiohttp.ClientSession, server: str):
        self.http = http
        self.server = server.rstrip("/")

    async def renderers(self) -> list[dict]:
        async with self.http.get(f"{self.server}/api/renderers") as response:
            response.raise_for_status()
            return await response.json()

    async def status(self, udn: str) -> dict:
        async with self.http.get(f"{self.server}/api/status", params={"renderer": udn}) as response:
            response.raise_for_status()
            return await response.json()

    async def post(self, path: str, **body) -> bool:
        async with self.http.post(f"{self.server}/api/{path}", json=body) as response:
            if response.status != 200:
                LOG.debug("%s: HTTP %s %s", path, response.status, await response.text())
            return response.status == 200


class Root(ServiceInterface):
    """org.mpris.MediaPlayer2: the player has no window to raise and does not quit on request."""

    def __init__(self, identity: str):
        super().__init__("org.mpris.MediaPlayer2")
        self.identity = identity

    @dbus_method()
    def Raise(self):
        pass

    @dbus_method()
    def Quit(self):
        pass

    @dbus_property(access=PropertyAccess.READ)
    def CanQuit(self) -> DBusBool:
        return False

    @dbus_property(access=PropertyAccess.READ)
    def CanRaise(self) -> DBusBool:
        return False

    @dbus_property(access=PropertyAccess.READ)
    def HasTrackList(self) -> DBusBool:
        return False

    @dbus_property(access=PropertyAccess.READ)
    def Identity(self) -> DBusStr:
        return self.identity

    @dbus_property(access=PropertyAccess.READ)
    def SupportedUriSchemes(self) -> DBusStrings:
        return ["http", "https"]

    @dbus_property(access=PropertyAccess.READ)
    def SupportedMimeTypes(self) -> DBusStrings:
        return []


class Player(ServiceInterface):
    """org.mpris.MediaPlayer2.Player for one renderer, from the status the control API reports."""

    def __init__(self, api: Api, udn: str, wake: asyncio.Event):
        super().__init__("org.mpris.MediaPlayer2.Player")
        self.api = api
        self.udn = udn
        self.wake = wake
        self.status: dict = {}
        self.polled_at = time.monotonic()
        self.track_id = NO_TRACK
        self._track_key: tuple | None = None
        self._tracks = 0
        self._tasks: set[asyncio.Task] = set()

    # --- state

    @property
    def playback_status(self) -> str:
        return PLAYBACK_STATUS.get(self.status.get("state", ""), "Stopped")

    @property
    def has_track(self) -> bool:
        return self.playback_status != "Stopped" and bool(self.status.get("title"))

    def position(self) -> int:
        """The position in seconds now, moved on from the last poll while the track plays."""
        position = self.status.get("position", 0)
        if self.playback_status == "Playing":
            position += time.monotonic() - self.polled_at
        duration = self.status.get("duration", 0)
        return int(min(position, duration) if duration else position)

    def update(self, status: dict):
        """Take a new status, and signal what changed."""
        expected = self.position()
        before = self._properties()
        key = (status.get("video_id"), status.get("title")) if status.get("title") else None
        # A new track starts at its own position; only a jump within the same track is a seek.
        moved = key is not None and key == self._track_key and \
            abs(status.get("position", 0) - expected) >= SEEK_THRESHOLD
        if key != self._track_key:
            self._track_key = key
            self._tracks += 1
            self.track_id = f"/org/upnptube/track/{self._tracks}" if key else NO_TRACK
        self.status, self.polled_at = status, time.monotonic()
        after = self._properties()
        changed = {name: value for name, value in after.items() if value != before[name]}
        if changed:
            self.emit_properties_changed(changed)
        if moved:
            self.Seeked()

    def _properties(self) -> dict:
        has_track = self.has_track
        return {
            "PlaybackStatus": self.playback_status,
            "Metadata": self._metadata(),
            "Volume": self.status.get("volume", 0) / 100,
            "CanGoNext": bool(self.status.get("has_next")),
            "CanGoPrevious": bool(self.status.get("has_previous")),
            "CanPlay": has_track,
            "CanPause": has_track,
            "CanSeek": has_track and self.status.get("duration", 0) > 0,
        }

    def _metadata(self) -> dict[str, Variant]:
        if not self._track_key:
            return {"mpris:trackid": Variant("o", NO_TRACK)}
        metadata = {"mpris:trackid": Variant("o", self.track_id), "xesam:title": Variant("s", self.status["title"])}
        if self.status.get("artist"):
            metadata["xesam:artist"] = Variant("as", [self.status["artist"]])
        if self.status.get("duration"):
            metadata["mpris:length"] = Variant("x", self.status["duration"] * MICROSECONDS)
        if video_id := self.status.get("video_id"):
            metadata["mpris:artUrl"] = Variant("s", f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg")
            metadata["xesam:url"] = Variant("s", f"https://www.youtube.com/watch?v={video_id}")
        return metadata

    async def _send(self, path: str, **body):
        try:
            await self.api.post(path, renderer=self.udn, **body)
        except ERRORS as error:
            LOG.warning("%s failed: %s", path, error)
        # Show the result of the command now, not at the next poll.
        self.wake.set()

    def _spawn(self, coro):
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _seek_to(self, seconds: int):
        duration = self.status.get("duration", 0)
        if seconds >= duration:
            # The specification: a seek beyond the end of the track acts like a call to Next.
            if self.status.get("has_next"):
                await self._send("next")
            return
        await self._send("seek", position=max(seconds, 0))

    # --- methods

    @dbus_method()
    async def Next(self):
        if self.status.get("has_next"):
            await self._send("next")

    @dbus_method()
    async def Previous(self):
        if self.status.get("has_previous"):
            await self._send("previous")

    @dbus_method()
    async def Pause(self):
        if self.playback_status == "Playing":
            await self._send("pause")

    @dbus_method()
    async def Play(self):
        if self.playback_status == "Paused":
            await self._send("resume")

    @dbus_method()
    async def PlayPause(self):
        if not self.has_track:
            raise DBusError(ErrorType.FAILED, "Nothing plays")
        await self._send("pause" if self.playback_status == "Playing" else "resume")

    @dbus_method()
    async def Stop(self):
        await self._send("stop")

    @dbus_method()
    async def Seek(self, offset: DBusInt64):
        if self._properties()["CanSeek"]:
            await self._seek_to(self.position() + offset // MICROSECONDS)

    @dbus_method()
    async def SetPosition(self, track_id: DBusObjectPath, position: DBusInt64):
        length = self.status.get("duration", 0) * MICROSECONDS
        if self._properties()["CanSeek"] and track_id == self.track_id and 0 <= position <= length:
            await self._seek_to(position // MICROSECONDS)

    @dbus_method()
    def OpenUri(self, uri: DBusStr):
        if not uri.lower().startswith(("http://", "https://")):
            raise DBusError(ErrorType.NOT_SUPPORTED, f"Cannot open {uri}: only http and https URLs play")
        # Finding the audio takes seconds; the specification lets the call return before the URI opens.
        self._spawn(self._send("play", url=uri))

    # --- properties

    @dbus_property(access=PropertyAccess.READ)
    def PlaybackStatus(self) -> DBusStr:
        return self.playback_status

    @dbus_property()
    def Rate(self) -> DBusDouble:
        return 1.0

    @Rate.setter
    def Rate(self, value: DBusDouble):
        pass

    @dbus_property(access=PropertyAccess.READ)
    def MinimumRate(self) -> DBusDouble:
        return 1.0

    @dbus_property(access=PropertyAccess.READ)
    def MaximumRate(self) -> DBusDouble:
        return 1.0

    @dbus_property(access=PropertyAccess.READ)
    def Metadata(self) -> DBusMetadata:
        return self._metadata()

    @dbus_property()
    def Volume(self) -> DBusDouble:
        return self.status.get("volume", 0) / 100

    @Volume.setter
    def Volume(self, value: DBusDouble):
        self._spawn(self._send("volume", volume=round(min(max(value, 0.0), 1.0) * 100)))

    @dbus_property(access=PropertyAccess.READ)
    def Position(self) -> DBusInt64:
        return self.position() * MICROSECONDS

    @dbus_property(access=PropertyAccess.READ)
    def CanGoNext(self) -> DBusBool:
        return self._properties()["CanGoNext"]

    @dbus_property(access=PropertyAccess.READ)
    def CanGoPrevious(self) -> DBusBool:
        return self._properties()["CanGoPrevious"]

    @dbus_property(access=PropertyAccess.READ)
    def CanPlay(self) -> DBusBool:
        return self.has_track

    @dbus_property(access=PropertyAccess.READ)
    def CanPause(self) -> DBusBool:
        return self.has_track

    @dbus_property(access=PropertyAccess.READ)
    def CanSeek(self) -> DBusBool:
        return self._properties()["CanSeek"]

    @dbus_property(access=PropertyAccess.READ)
    def CanControl(self) -> DBusBool:
        return True

    @dbus_signal()
    def Seeked(self) -> DBusInt64:
        return self.position() * MICROSECONDS


class Bridge:
    def __init__(self, api: Api, bus_address: str | None = None, poll: float = POLL_SECONDS):
        self.api = api
        self.bus_address = bus_address
        self.poll = poll
        self.wake = asyncio.Event()
        # Each renderer has its own connection, because each player owns the same object path.
        self.players: dict[str, tuple[MessageBus, Player]] = {}

    async def run(self):
        reachable = True
        try:
            while True:
                try:
                    renderers = await self.api.renderers()
                    await self._sync(renderers)
                    for udn, (_, player) in self.players.items():
                        player.update(await self.api.status(udn))
                    reachable = True
                except ERRORS as error:
                    if reachable:
                        LOG.warning("Cannot reach upnptube at %s: %s", self.api.server, error)
                    reachable = False
                    await self._sync([])
                self.wake.clear()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self.wake.wait(), self.poll)
        finally:
            await self._sync([])

    async def _sync(self, renderers: list[dict]):
        names = {renderer["udn"]: renderer["name"] for renderer in renderers}
        for udn in set(self.players) - set(names):
            bus, _ = self.players.pop(udn)
            bus.disconnect()
            await bus.wait_for_disconnect()
            LOG.info("Removed the player of %s", udn)
        for udn in set(names) - set(self.players):
            bus = await MessageBus(bus_address=self.bus_address, bus_type=BusType.SESSION).connect()
            player = Player(self.api, udn, self.wake)
            bus.export(OBJECT_PATH, Root(names[udn]))
            bus.export(OBJECT_PATH, player)
            name = bus_name(names[udn])
            await bus.request_name(name)
            self.players[udn] = (bus, player)
            LOG.info("%s plays as %s", names[udn], name)


async def serve(args: argparse.Namespace):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as http:
        await Bridge(Api(http, args.server), poll=args.poll).run()


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0].strip())
    parser.add_argument("--server", default=SERVER, help="the upnptube control API (default: %(default)s)")
    parser.add_argument("--poll", type=float, default=POLL_SECONDS, help="seconds between status reads")
    parser.add_argument("-v", "--verbose", action="store_true", help="log each API call that fails")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(serve(args))


if __name__ == "__main__":
    main()
