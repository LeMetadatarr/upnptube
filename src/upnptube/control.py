"""
An HTTP API to play URLs and control renderers, for clients such as castctl.

Every request names a renderer by part of its name, its UDN or its location URL.
Without a name, the request goes to the only renderer, if there is exactly one.
"""
import json
import logging
from collections.abc import Callable

from aiohttp import web

from . import media
from .media import MediaError
from .player import Volume
from .renderer import Renderer

LOG = logging.getLogger(__name__)


ERRORS = {400: web.HTTPBadRequest, 404: web.HTTPNotFound, 502: web.HTTPBadGateway}


def _error(status: int, message: str) -> web.HTTPException:
    return ERRORS[status](text=json.dumps({"error": message}), content_type="application/json")


class ControlApi:
    def __init__(self, renderers: Callable[[], list[Renderer]], ytdlp: media.Ytdlp):
        self.renderers = renderers
        self.ytdlp = ytdlp

    def add_routes(self, app: web.Application):
        app.router.add_get("/api/renderers", self._list)
        app.router.add_get("/api/status", self._status)
        app.router.add_get("/api/pairing-code", self._pairing_code)
        app.router.add_post("/api/play", self._play)
        app.router.add_post("/api/{command:pause|resume|stop|next|previous}", self._command)
        app.router.add_post("/api/volume", self._volume)

    def _find(self, query: str | None) -> Renderer:
        ready = [r for r in self.renderers() if r.ready]
        if query:
            ready = [r for r in ready if query.lower() in r.name.lower() or query in (r.location, r.device.udn)]
        if len(ready) == 1:
            return ready[0]
        names = ", ".join(r.name for r in self.renderers() if r.ready) or "none"
        message = "No renderer matches" if not ready else "Several renderers match; name one"
        raise _error(404 if not ready else 400, f"{message}. Renderers: {names}")

    async def _body(self, request: web.Request) -> dict:
        try:
            return await request.json() if request.can_read_body else {}
        except ValueError:
            raise _error(400, "The body is not JSON") from None

    async def _list(self, request: web.Request) -> web.Response:
        return web.json_response([{"name": r.name, "udn": r.device.udn, "location": r.location}
                                  for r in self.renderers() if r.ready])

    async def _status(self, request: web.Request) -> web.Response:
        renderer = self._find(request.query.get("renderer"))
        player = renderer.player
        state = await player.get_state()
        current = state.queue.current
        return web.json_response({
            "renderer": renderer.name, "state": state.status.name, "title": player.track.title if player.track else "",
            "video_id": current.id if current else None, "position": state.position, "duration": state.duration,
            "volume": state.volume.level, "muted": state.volume.muted,
            "senders": [sender.name for sender in renderer.app.senders],
        })

    async def _pairing_code(self, request: web.Request) -> web.Response:
        renderer = self._find(request.query.get("renderer"))
        try:
            code = await renderer.app.pairing_code()
        except Exception as error:
            raise _error(502, f"Cannot get a TV code: {error}") from None
        return web.json_response({"renderer": renderer.name, "code": code})

    async def _play(self, request: web.Request) -> web.Response:
        body = await self._body(request)
        url = body.get("url")
        if not url:
            raise _error(400, "Give a url to play")
        renderer = self._find(body.get("renderer"))
        player = renderer.player
        try:
            async with player.lock:
                link = media.youtube_link(url)
                if link:
                    await self._queue_youtube(renderer, url, *link)
                    if not await player.play(player.queue.current):
                        raise MediaError(f"{renderer.name} cannot play {url}")
                else:
                    await player.play_url(url)
        except MediaError as error:
            LOG.warning("Cannot play %s on %s: %s", url, renderer.name, error)
            raise _error(502, f"Cannot play {url}: {error}") from None
        return web.json_response({"renderer": renderer.name, "title": player.track.title if player.track else ""})

    async def _queue_youtube(self, renderer: Renderer, url: str, video_id: str | None, list_id: str | None):
        queue = renderer.player.queue
        ids = await self.ytdlp.playlist_ids(url) if list_id else []
        if video_id and video_id not in ids:
            ids.insert(0, video_id)
        if not ids:
            raise MediaError(f"no videos at {url}")
        queue.set_local(ids, ids.index(video_id) if video_id else 0, list_id)
        if not renderer.app.senders:
            await queue.set_autoplay_mode(renderer.app.autoplay_on_connect)
        await queue.refresh_up_next()

    async def _command(self, request: web.Request) -> web.Response:
        command = request.match_info["command"]
        renderer = self._find((await self._body(request)).get("renderer"))
        async with renderer.player.lock:
            done = await getattr(renderer.player, command)()
        if not done:
            raise _error(400, f"{renderer.name}: nothing to {command}")
        return web.json_response({"renderer": renderer.name, "done": command})

    async def _volume(self, request: web.Request) -> web.Response:
        body = await self._body(request)
        renderer = self._find(body.get("renderer"))
        try:
            level = int(body["volume"])
        except (KeyError, TypeError, ValueError):
            level = -1
        if not 0 <= level <= 100:
            raise _error(400, "Give a volume from 0 to 100")
        async with renderer.player.lock:
            if not await renderer.player.set_volume(Volume(level)):
                raise _error(502, f"{renderer.name}: set volume failed")
        return web.json_response({"renderer": renderer.name, "volume": level})
