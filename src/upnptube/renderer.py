"""One UPnP media renderer, offered to YouTube senders as a cast target."""
import asyncio
import logging
import time
import uuid
from urllib.parse import urlparse

import aiohttp

from . import media
from .app import ResetPolicy, YouTubeApp
from .dial import DialServer
from .media import MediaError, StreamServer, Track, Ytdlp
from .player import Player, Status, Video, Volume
from .ssdp import Ssdp, local_ip_for
from .store import Store
from .upnp import MediaRenderer, UpnpError

LOG = logging.getLogger(__name__)

ERRORS = (aiohttp.ClientError, UpnpError, TimeoutError)
POLL_SECONDS = 1
START_TIMEOUT = 45
# A paused HTTP stream goes stale; resume such a stream by loading it again.
STALE_PAUSE_SECONDS = 60
STREAM_RETRIES = 2


class Prefixed(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return f"[{self.extra['name']}] {msg}", kwargs


class UpnpPlayer(Player):
    def __init__(self, http: aiohttp.ClientSession, device: MediaRenderer, streams: StreamServer, ytdlp: Ytdlp,
                 slot: int, log: logging.LoggerAdapter):
        super().__init__(lambda video, exclude: media.find_up_next(http, video, exclude))
        self.device = device
        self.streams = streams
        self.ytdlp = ytdlp
        self.slot = slot
        self.log = log
        self.host = local_ip_for(urlparse(device.location).hostname)
        self.track: Track | None = None
        self.uri = ""
        self.offset = 0
        self.loaded_at = 0.0
        self.paused_at = 0.0
        self.started = False
        self.retries = 0
        self._tracks: dict[str, Track] = {}

    async def _track(self, video: Video) -> Track:
        track = self._tracks.get(video.id)
        if track is None:
            track = await self.ytdlp.resolve(media.WATCH_URL.format(video.id))
            self._tracks = {video.id: track}
        return track

    async def load(self, track: Track, position: int) -> bool:
        url = self.streams.publish(self.slot, track, position, self.host)
        try:
            await self.device.load(url, track.title, track.content_type, track.dlna_features, track.artist,
                                   track.duration)
            await self.device.play()
        except ERRORS as error:
            self.log.warning("Cannot play %s: %s", track.title, error)
            return False
        self.track, self.uri, self.offset = track, url, position
        self.loaded_at, self.started = time.monotonic(), False
        self.log.info("Playing %s from %s s", track.title, position)
        return True

    async def play_url(self, url: str) -> str:
        """Play any URL that yt-dlp supports, outside the YouTube queue, and return its title."""
        await self.reset()
        track = await self.ytdlp.resolve(url)
        if not await self.load(track, 0):
            raise MediaError(f"{self.device.friendly_name} refused {track.title}")
        await self.notify_external_change(Status.PLAYING)
        return track.title

    async def do_play(self, video: Video, position: int) -> bool:
        try:
            track = await self._track(video)
        except MediaError as error:
            self.log.warning("Cannot find the audio of %s: %s", video.id, error)
            return False
        self.retries = 0
        return await self.load(track, position)

    async def do_pause(self) -> bool:
        try:
            await self.device.pause()
        except ERRORS as error:
            self.log.warning("Pause failed: %s", error)
            return False
        self.paused_at = time.monotonic()
        return True

    async def do_resume(self) -> bool:
        if self.track and time.monotonic() - self.paused_at > STALE_PAUSE_SECONDS:
            return await self.load(self.track, await self.do_get_position())
        try:
            await self.device.play()
        except ERRORS as error:
            self.log.warning("Resume failed: %s", error)
            return False
        return True

    async def do_stop(self) -> bool:
        try:
            await self.device.stop()
        except ERRORS as error:
            self.log.warning("Stop failed: %s", error)
        self.streams.withdraw(self.slot)
        self.track, self.uri, self.offset = None, "", 0
        return True

    async def do_seek(self, position: int) -> bool:
        # The stream is not seekable, so a seek starts it again at the new position.
        return bool(self.track) and await self.load(self.track, position)

    async def do_set_volume(self, volume: Volume) -> bool:
        try:
            await self.device.set_volume(volume.level)
            await self.device.set_mute(volume.muted)
        except ERRORS as error:
            self.log.warning("Set volume failed: %s", error)
            return False
        return True

    async def do_get_volume(self) -> Volume:
        try:
            level = await self.device.get_volume()
        except ERRORS as error:
            self.log.warning("Get volume failed: %s", error)
            return Volume(0)
        try:
            muted = await self.device.get_mute()
        except ERRORS:
            muted = False
        return Volume(level, muted)

    async def do_get_position(self) -> int:
        if not self.track or self.status not in (Status.PLAYING, Status.PAUSED):
            return self.offset
        try:
            position = self.offset + await self.device.get_position()
        except ERRORS as error:
            self.log.warning("Get position failed: %s", error)
            return self.offset
        return min(position, self.track.duration) if self.track.duration else position

    async def do_get_duration(self) -> int:
        return self.track.duration if self.track else 0

    async def watch(self):
        """Follow the renderer: a track that ends plays the next, and a pause on the device shows."""
        while True:
            await asyncio.sleep(POLL_SECONDS)
            # Read the renderer under the lock: a state read before a command ran is stale after it.
            async with self.lock:
                if self.status not in (Status.PLAYING, Status.PAUSED) or not self.track:
                    continue
                try:
                    state = await self.device.get_transport_state()
                    await self._follow(state)
                except ERRORS as error:
                    self.log.debug("Cannot follow the renderer: %s", error)
                except Exception:
                    self.log.exception("Cannot follow the renderer")

    async def _follow(self, state: str):
        if self.status == Status.PLAYING and state == "PLAYING":
            self.started = True
        elif self.status == Status.PLAYING and state == "PAUSED_PLAYBACK":
            self.paused_at = time.monotonic()
            await self.notify_external_change(Status.PAUSED)
        elif self.status == Status.PAUSED and state == "PLAYING":
            await self.notify_external_change(Status.PLAYING)
        elif self.status == Status.PLAYING and state in ("STOPPED", "NO_MEDIA_PRESENT"):
            if await self.device.get_uri() != self.uri:
                self.log.info("Another controller took over the renderer")
                self.track, self.uri = None, ""
                await self.notify_external_change(Status.STOPPED)
                return
            if not self.started:
                if time.monotonic() - self.loaded_at > START_TIMEOUT:
                    self.log.warning("%s did not start within %s s", self.track.title, START_TIMEOUT)
                    await self.next()
                return
            position = self.offset + await self.device.get_position()
            if self.track.duration and position < self.track.duration - 5 and self.retries < STREAM_RETRIES:
                self.retries += 1
                self.log.warning("%s stopped at %s of %s s; loading it again", self.track.title, position,
                                 self.track.duration)
                await self.load(self.track, position)
                return
            self.log.info("%s ended", self.track.title)
            await self.next()


class Renderer:
    def __init__(self, http: aiohttp.ClientSession, ssdp: Ssdp, streams: StreamServer, ytdlp: Ytdlp,
                 store: Store, location: str, slot: int, dial_port: int, timeout: int, name_format: str):
        self.http = http
        self.ssdp = ssdp
        self.streams = streams
        self.ytdlp = ytdlp
        self.store = store
        self.location = location
        self.slot = slot
        self.dial_port = dial_port
        self.name_format = name_format
        self.device = MediaRenderer(location)
        self.player: UpnpPlayer | None = None
        self.app: YouTubeApp | None = None
        self.dial: DialServer | None = None
        self.error = False
        self.name = location
        self.log = Prefixed(LOG, {"name": location})
        self._watch: asyncio.Task | None = None
        self.refresh(timeout)

    async def start(self):
        try:
            await self.device.load_description()
        except ERRORS as error:
            # A stale renderer is removed and created again on the next discovery.
            self.log.warning("Cannot read the device description: %s", error)
            self.error = True
            return

        device = self.device
        self.name = device.friendly_name
        self.log = Prefixed(LOG, {"name": self.name})
        screen_name = self.name_format.format(name=device.friendly_name)
        key = device.udn or self.location
        self.player = UpnpPlayer(self.http, device, self.streams, self.ytdlp, self.slot, self.log)
        self.app = YouTubeApp(self.http, self.player, screen_name, device.manufacturer or "Generic",
                              device.model_name or "SmartTV", self.store, key, self.log,
                              ResetPolicy.ALL_EXPLICITLY_DISCONNECTED)
        self.dial = DialServer(self.app, self.dial_port, screen_name, device.manufacturer, device.model_name,
                               str(uuid.uuid5(uuid.NAMESPACE_URL, f"upnptube-dial:{key}")))
        try:
            await self.dial.start()
        except OSError as error:
            self.log.warning("Cannot start the DIAL server on port %s: %s", self.dial_port, error)
            self.error = True
            return
        await self.app.start()
        self._watch = asyncio.create_task(self.player.watch())
        self.ssdp.advertise(self.dial.device)
        self.log.info("Ready to cast as %r on port %s", screen_name, self.dial_port)

    async def close(self):
        if self._watch:
            self._watch.cancel()
        if self.dial:
            self.ssdp.withdraw(self.dial.device)
            await self.dial.stop()
        if self.app:
            await self.app.stop()
        self.streams.withdraw(self.slot)
        await self.device.close()

    @property
    def ready(self) -> bool:
        return self.app is not None and self.app.state == "running"

    def refresh(self, timeout: int):
        self.timeout = timeout
        self.last_seen = time.monotonic()

    def is_stale(self) -> bool:
        return self.error or time.monotonic() > self.last_seen + self.timeout
