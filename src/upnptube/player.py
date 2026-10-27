"""
The play queue and the player that a YouTube session drives. A device backend
implements the `do_*` methods; `Player` keeps the state that senders see.
"""
import asyncio
import logging
import uuid
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum, IntEnum

from .lounge import YOUTUBE, Client

LOG = logging.getLogger(__name__)


class Status(IntEnum):
    IDLE = -1
    PLAYING = 1
    PAUSED = 2
    LOADING = 3
    STOPPED = 4


class Autoplay(str, Enum):
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"
    UNSUPPORTED = "UNSUPPORTED"


@dataclass
class Video:
    id: str
    client: Client = YOUTUBE
    list_id: str | None = None
    index: int | None = None
    params: str | None = None
    ctt: str | None = None


@dataclass
class Volume:
    level: int
    muted: bool = False


@dataclass
class QueueState:
    list_id: str | None
    video_ids: list[str]
    current: Video | None
    autoplay: Video | None


@dataclass
class PlayerState:
    status: Status
    queue: QueueState
    position: int
    duration: int
    volume: Volume
    cpn: str


@dataclass
class NavInfo:
    has_previous: bool
    has_next: bool
    autoplay_mode: Autoplay


UpNextFinder = Callable[[Video, list[str]], Awaitable[Video | None]]


@dataclass
class Playlist:
    """
    The queue a sender set. Next and previous step through the sender's list; after its
    last video, the autoplay video follows when autoplay is on.
    """
    find_up_next: UpNextFinder
    list_id: str | None = None
    video_ids: list[str] = field(default_factory=list)
    current: Video | None = None
    autoplay_mode: Autoplay = Autoplay.UNSUPPORTED
    up_next: Video | None = None
    up_next_for: str | None = None

    @property
    def index(self) -> int:
        current = self.current
        if current is None or current.index is None or not 0 <= current.index < len(self.video_ids):
            return -1
        return current.index if self.video_ids[current.index] == current.id else -1

    @property
    def is_last(self) -> bool:
        return self.index < 0 or self.index == len(self.video_ids) - 1

    @property
    def has_previous(self) -> bool:
        return self.index > 0

    @property
    def has_next(self) -> bool:
        return not self.is_last

    @property
    def autoplay(self) -> Video | None:
        return self.up_next if self.is_last and self.autoplay_mode == Autoplay.ENABLED else None

    def state(self) -> QueueState:
        return QueueState(self.list_id, list(self.video_ids), self.current, self.autoplay)

    def _at(self, index: int) -> Video:
        current = self.current or Video("")
        return Video(self.video_ids[index], current.client, self.list_id, index, current.params, current.ctt)

    async def set_autoplay_mode(self, mode: Autoplay):
        if mode != self.autoplay_mode:
            self.autoplay_mode = mode
            await self.refresh_up_next()

    async def update(self, name: str, data: dict, client: Client):
        """Apply a sender's setPlaylist or updatePlaylist message."""
        if not data.get("listId"):
            self.clear()
            return
        self.list_id = data["listId"]
        self.video_ids = data["videoIds"].split(",") if data.get("videoIds") else []
        if name == "setPlaylist":
            if data.get("videoId") and data.get("currentIndex") not in (None, ""):
                self.current = Video(data["videoId"], client, self.list_id, int(data["currentIndex"]),
                                     data.get("params"), data.get("ctt"))
            else:
                self.current = None
        elif self.current:
            if self.current.id in self.video_ids:
                self.current.list_id = self.list_id
                self.current.index = self.video_ids.index(self.current.id)
                self.current.params = data.get("params")
            else:
                self.current = None
        await self.refresh_up_next()

    def set_local(self, video_ids: list[str], index: int = 0, list_id: str | None = None):
        """Queue videos from outside a cast session, such as the control API."""
        self.list_id = list_id or f"local-{uuid.uuid4().hex[:8]}"
        self.video_ids = list(video_ids)
        self.current = Video(self.video_ids[index], YOUTUBE, self.list_id, index)

    def clear(self):
        self.list_id = None
        self.video_ids = []
        self.current = None
        self.up_next = self.up_next_for = None

    def set_current(self, video: Video):
        self.current = video

    async def next(self) -> Video | None:
        if self.has_next:
            self.current = self._at(self.index + 1)
        elif self.autoplay:
            self.current = self.autoplay
        else:
            return None
        await self.refresh_up_next()
        return self.current

    async def previous(self) -> Video | None:
        if not self.has_previous:
            return None
        self.current = self._at(self.index - 1)
        await self.refresh_up_next()
        return self.current

    async def refresh_up_next(self):
        if not (self.current and self.is_last and self.autoplay_mode == Autoplay.ENABLED):
            self.up_next = self.up_next_for = None
            return
        if self.up_next_for == self.current.id:
            return
        try:
            self.up_next = await self.find_up_next(self.current, [*self.video_ids, self.current.id])
            self.up_next_for = self.current.id
        except Exception as error:
            LOG.warning("Cannot find a video to autoplay after %s: %s", self.current.id, error)
            self.up_next = None


StateListener = Callable[[int | None, PlayerState, PlayerState | None], Awaitable[None]]


class Player(ABC):
    """What a YouTube session drives. Each change of state goes to `on_state`."""

    def __init__(self, find_up_next: UpNextFinder):
        self.status = Status.IDLE
        self.queue = Playlist(find_up_next)
        self.cpn = uuid.uuid4().hex[:16]
        self.on_state: StateListener | None = None
        # Held by whoever drives the player (a sender, the device watcher, the control API).
        self.lock = asyncio.Lock()
        self._previous_state: PlayerState | None = None

    @abstractmethod
    async def do_play(self, video: Video, position: int) -> bool: ...

    @abstractmethod
    async def do_pause(self) -> bool: ...

    @abstractmethod
    async def do_resume(self) -> bool: ...

    @abstractmethod
    async def do_stop(self) -> bool: ...

    @abstractmethod
    async def do_seek(self, position: int) -> bool: ...

    @abstractmethod
    async def do_set_volume(self, volume: Volume) -> bool: ...

    @abstractmethod
    async def do_get_volume(self) -> Volume: ...

    @abstractmethod
    async def do_get_position(self) -> int: ...

    @abstractmethod
    async def do_get_duration(self) -> int: ...

    async def play(self, video: Video, position: int = 0, aid: int | None = None) -> bool:
        if self.status == Status.PLAYING:
            await self.stop()
        self.queue.set_current(video)
        await self._set_status(Status.LOADING, aid)
        ok = await self.do_play(video, position)
        await self._set_status(Status.PLAYING if ok else Status.STOPPED, aid)
        return ok

    async def pause(self, aid: int | None = None) -> bool:
        if self.status != Status.PLAYING or not await self.do_pause():
            return False
        await self._set_status(Status.PAUSED, aid)
        return True

    async def resume(self, aid: int | None = None) -> bool:
        if self.status == Status.PLAYING:
            await self._set_status(Status.PLAYING, aid)
            return False
        if self.status == Status.PAUSED:
            if not await self.do_resume():
                return False
            await self._set_status(Status.PLAYING, aid)
            return True
        if self.queue.current:
            return await self.play(self.queue.current, 0, aid)
        return False

    async def stop(self, aid: int | None = None) -> bool:
        if self.status in (Status.STOPPED, Status.IDLE):
            return True
        if not await self.do_stop():
            return False
        await self._set_status(Status.STOPPED, aid)
        return True

    async def seek(self, position: int, aid: int | None = None) -> bool:
        if self.status not in (Status.PLAYING, Status.PAUSED):
            return False
        was_paused = self.status == Status.PAUSED
        await self._set_status(Status.LOADING, aid)
        if not await self.do_seek(position):
            await self._set_status(Status.PAUSED if was_paused else Status.PLAYING, aid)
            return False
        if was_paused and await self.do_pause():
            await self._set_status(Status.PAUSED, aid)
        else:
            await self._set_status(Status.PLAYING, aid)
        return True

    async def next(self, aid: int | None = None) -> bool:
        video = await self.queue.next()
        if video is None:
            await self.stop(aid)
            return False
        return await self.play(video, 0, aid)

    async def previous(self, aid: int | None = None) -> bool:
        video = await self.queue.previous()
        if video is None:
            await self.stop(aid)
            return False
        return await self.play(video, 0, aid)

    async def set_volume(self, volume: Volume, aid: int | None = None) -> bool:
        if not await self.do_set_volume(volume):
            return False
        await self._set_status(None, aid)
        return True

    async def reset(self):
        self.queue.clear()
        await self.stop()
        self._previous_state = None
        await self._set_status(Status.IDLE)

    async def get_volume(self) -> Volume:
        volume = await self.do_get_volume()
        return Volume(min(max(volume.level, 0), 100), volume.muted)

    def nav_info(self) -> NavInfo:
        return NavInfo(self.queue.has_previous, self.queue.has_next, self.queue.autoplay_mode)

    async def get_state(self) -> PlayerState:
        return PlayerState(self.status, self.queue.state(), await self.do_get_position(),
                           await self.do_get_duration(), await self.get_volume(), self.cpn)

    async def notify_external_change(self, status: Status | None = None):
        """Report a change that the device made by itself, such as a pause on its remote."""
        await self._set_status(status)

    async def _set_status(self, status: Status | None, aid: int | None = None):
        if status is not None:
            self.status = status
        previous = self._previous_state
        current = self._previous_state = await self.get_state()
        if self.on_state:
            await self.on_state(aid, current, previous)
