"""Offer every UPnP media renderer on the network as a cast target for the YouTube apps."""
import argparse
import asyncio
import contextlib
import logging
import os
import signal
from pathlib import Path

import aiohttp
from aiohttp import web

from .control import ControlApi
from .media import StreamServer, Ytdlp
from .renderer import Renderer
from .ssdp import Ssdp
from .store import Store

LOG = logging.getLogger("upnptube")

MEDIA_RENDERER = "urn:schemas-upnp-org:device:MediaRenderer:1"
# Some renderers never announce themselves, so search for them on a fixed period too.
SEARCH_PERIOD = 600
SEARCH_WAIT = 5
PRUNE_PERIOD = 60
DIAL_PORT = 3100
HTTP_PORT = 8100
NAME_FORMAT = "🔊 {name}"
STATE_DIR = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local/state")) / "upnptube"


class UpnpTube:
    def __init__(self, session: aiohttp.ClientSession, store: Store, dial_base_port: int = DIAL_PORT,
                 http_port: int = HTTP_PORT, name_format: str = NAME_FORMAT, ytdlp_args: list[str] | None = None):
        self.session = session
        self.store = store
        self.dial_base_port = dial_base_port
        self.http_port = http_port
        self.name_format = name_format
        self.ytdlp = Ytdlp(ytdlp_args)
        self.streams = StreamServer(self.ytdlp, http_port)
        self.ssdp = Ssdp(MEDIA_RENDERER)
        self.ssdp.on_found = self.add_or_refresh
        self.ssdp.on_lost = self.remove
        # Renderers by the location of their description.
        self.renderers: dict[str, Renderer] = {}
        self._tasks: set[asyncio.Task] = set()
        self._runner: web.AppRunner | None = None

    async def start(self):
        """Serve the streams and the control API, then listen for renderers."""
        routes = web.Application()
        self.streams.add_routes(routes)
        ControlApi(lambda: list(self.renderers.values()), self.ytdlp).add_routes(routes)
        self._runner = web.AppRunner(routes)
        await self._runner.setup()
        await web.TCPSite(self._runner, port=self.http_port).start()
        await self.ssdp.start()

    async def stop(self):
        for task in self._tasks:
            task.cancel()
        self.ssdp.stop()
        await asyncio.gather(*(renderer.close() for renderer in self.renderers.values()))
        self.renderers.clear()
        if self._runner:
            await self._runner.cleanup()
        self.streams.close()

    async def run(self):
        await self.start()
        LOG.info("Streams and control API on port %s", self.http_port)
        self._spawn(self._search_periodically())
        try:
            while True:
                await asyncio.sleep(PRUNE_PERIOD)
                await self.prune()
        finally:
            await self.stop()

    async def _search_periodically(self):
        while True:
            LOG.info("Searching for media renderers")
            self.ssdp.search(SEARCH_WAIT)
            await asyncio.sleep(SEARCH_PERIOD)

    def add_or_refresh(self, location: str, age: int):
        """Called for each announcement or search answer of a renderer, valid for `age` seconds."""
        if renderer := self.renderers.get(location):
            renderer.refresh(age)
            return
        LOG.info("Found renderer %s", location)
        # Each renderer has its own DIAL port and stream slot; reuse the lowest free one.
        taken = {renderer.slot for renderer in self.renderers.values()}
        slot = min(set(range(len(taken) + 1)) - taken)
        renderer = Renderer(self.session, self.ssdp, self.streams, self.ytdlp, self.store, location, slot,
                            self.dial_base_port + slot, age, self.name_format)
        self.renderers[location] = renderer
        self._spawn(renderer.start())

    def remove(self, location: str):
        if renderer := self.renderers.pop(location, None):
            LOG.info("Renderer %s left", renderer.name)
            self._spawn(renderer.close())

    async def prune(self):
        for location, renderer in list(self.renderers.items()):
            if renderer.is_stale():
                LOG.info("Renderer %s is gone", renderer.name)
                del self.renderers[location]
                await renderer.close()

    def _spawn(self, coro):
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)


async def serve(args: argparse.Namespace):
    async with aiohttp.ClientSession() as session:
        service = UpnpTube(session, Store(args.state_dir / "state.json"), args.dial_port, args.http_port, args.name,
                           args.ytdlp_arg)
        task = asyncio.create_task(service.run())
        for sig in (signal.SIGINT, signal.SIGTERM):
            asyncio.get_running_loop().add_signal_handler(sig, task.cancel)
        with contextlib.suppress(asyncio.CancelledError):
            await task


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dial-port", type=int, default=DIAL_PORT,
                        help="DIAL port of the first renderer; each next renderer uses the next port")
    parser.add_argument("--http-port", type=int, default=HTTP_PORT, help="port of the streams and the control API")
    parser.add_argument("--name", default=NAME_FORMAT,
                        help="how senders see a renderer; {name} is its own name (default: %(default)s)")
    parser.add_argument("--state-dir", type=Path, default=STATE_DIR,
                        help="where screen ids are kept, so paired senders find the screens again")
    parser.add_argument("--ytdlp-arg", action="append", default=[], metavar="ARG",
                        help="an extra yt-dlp option, such as --ytdlp-arg=--cookies=/data/cookies.txt; repeatable")
    parser.add_argument("-v", "--verbose", action="store_true", help="log protocol details")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
    asyncio.run(serve(args))


if __name__ == "__main__":
    main()
