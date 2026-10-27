"""
Find the audio behind a URL with yt-dlp, and serve it to renderers as a plain HTTP
stream. yt-dlp downloads the audio and ffmpeg passes it on, because YouTube refuses its
stream URLs to most other clients, and most renderers cannot fetch HTTPS anyway.
"""
import asyncio
import collections
import json
import logging
import os
import re
import secrets
import sys
from collections.abc import Iterator
from dataclasses import dataclass, field
from urllib.parse import parse_qs, urlparse

import aiohttp
from aiohttp import web

from .player import Video

LOG = logging.getLogger(__name__)

WATCH_URL = "https://www.youtube.com/watch?v={}"
# m4a first: its AAC passes to the renderer as ADTS without a re-encode.
AUDIO_FORMAT = "bestaudio[ext=m4a]/bestaudio/best"
YOUTUBE_HOSTS = {"youtube.com", "www.youtube.com", "m.youtube.com", "music.youtube.com", "youtu.be"}
VIDEO_ID = re.compile(r"^[A-Za-z0-9_-]{11}$")
NEXT_URL = "https://www.youtube.com/youtubei/v1/next?prettyPrint=false"
TV_CLIENT = {"clientName": "TVHTML5", "clientVersion": "7.20230405.08.01", "hl": "en", "gl": "US"}
RESOLVE_TIMEOUT = 120
CHUNK = 64 * 1024
ADTS_FEATURES = ("DLNA.ORG_PN=AAC_ADTS_320;DLNA.ORG_OP=00;DLNA.ORG_CI=1;"
                 "DLNA.ORG_FLAGS=01700000000000000000000000000000")
MP3_FEATURES = "DLNA.ORG_PN=MP3;DLNA.ORG_OP=00;DLNA.ORG_CI=1;DLNA.ORG_FLAGS=01700000000000000000000000000000"


class MediaError(Exception):
    pass


@dataclass
class Track:
    source: str
    title: str
    artist: str
    duration: int
    acodec: str
    info_json: bytes = field(repr=False)

    @property
    def passthrough(self) -> bool:
        """AAC goes to the renderer as it is; anything else is encoded to MP3."""
        return self.acodec.startswith("mp4a") or self.acodec == "aac"

    @property
    def content_type(self) -> str:
        return "audio/aac" if self.passthrough else "audio/mpeg"

    @property
    def ext(self) -> str:
        return "aac" if self.passthrough else "mp3"

    @property
    def dlna_features(self) -> str:
        return ADTS_FEATURES if self.passthrough else MP3_FEATURES


def youtube_link(url: str) -> tuple[str | None, str | None] | None:
    """The (video id, list id) of a YouTube URL, or None for any other URL."""
    parsed = urlparse(url)
    if (parsed.hostname or "").lower() not in YOUTUBE_HOSTS:
        return None
    query = parse_qs(parsed.query)
    video_id = query.get("v", [None])[0]
    parts = [part for part in parsed.path.split("/") if part]
    if parsed.hostname == "youtu.be" and parts:
        video_id = parts[0]
    elif len(parts) == 2 and parts[0] in ("shorts", "live", "embed"):
        video_id = parts[1]
    list_id = query.get("list", [None])[0]
    if video_id and not VIDEO_ID.match(video_id):
        video_id = None
    return (video_id, list_id) if video_id or list_id else None


class Ytdlp:
    """yt-dlp as a subprocess, so that its options and plugins apply to every call."""

    def __init__(self, extra_args: list[str] | None = None):
        self.command = [sys.executable, "-m", "yt_dlp", "--no-warnings", "--no-progress", *(extra_args or [])]

    async def _json(self, *args: str) -> dict:
        process = await asyncio.create_subprocess_exec(*self.command, *args, stdout=asyncio.subprocess.PIPE,
                                                       stderr=asyncio.subprocess.PIPE)
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), RESOLVE_TIMEOUT)
        except TimeoutError:
            process.kill()
            await process.wait()
            raise MediaError(f"yt-dlp took longer than {RESOLVE_TIMEOUT} s") from None
        if process.returncode:
            tail = " | ".join(stderr.decode("utf-8", "replace").strip().splitlines()[-2:])
            raise MediaError(f"yt-dlp failed: {tail}")
        return json.loads(stdout)

    async def resolve(self, url: str) -> Track:
        """The audio of `url`; for a playlist, of its first item."""
        info = await self._json("-J", "--no-playlist", "--playlist-items", "1", "-f", AUDIO_FORMAT, "--", url)
        if info.get("_type") == "playlist":
            entries = info.get("entries") or []
            if not entries:
                raise MediaError(f"nothing to play at {url}")
            info = entries[0]
        return Track(
            source=url,
            title=info.get("track") or info.get("title") or url,
            artist=info.get("artist") or info.get("channel") or info.get("uploader") or "",
            duration=int(info.get("duration") or 0),
            acodec=info.get("acodec") or "",
            info_json=json.dumps(info).encode(),
        )

    async def playlist_ids(self, url: str, limit: int = 200) -> list[str]:
        info = await self._json("-J", "--flat-playlist", "--playlist-end", str(limit), "--", url)
        return [entry["id"] for entry in info.get("entries") or [] if VIDEO_ID.match(entry.get("id") or "")]


def _watch_endpoints(node) -> Iterator[dict]:
    if isinstance(node, dict):
        if isinstance(node.get("watchEndpoint"), dict):
            yield node["watchEndpoint"]
        for value in node.values():
            yield from _watch_endpoints(value)
    elif isinstance(node, list):
        for value in node:
            yield from _watch_endpoints(value)


async def find_up_next(http: aiohttp.ClientSession, video: Video, exclude: list[str]) -> Video | None:
    """The video YouTube autoplays after `video`, or the first related one not in `exclude`."""
    body = {"context": {"client": TV_CLIENT}, "videoId": video.id, "enableMdxAutoplay": True, "isMdxPlayback": True}
    async with http.post(NEXT_URL, json=body, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        resp.raise_for_status()
        data = await resp.json()
    results = data.get("contents", {}).get("singleColumnWatchNextResults", {})
    autoplay = results.get("autoplay", {}).get("autoplay", {}).get("sets", [])
    for endpoint in [*_watch_endpoints(autoplay), *_watch_endpoints(results)]:
        video_id = endpoint.get("videoId")
        if video_id and VIDEO_ID.match(video_id) and video_id not in exclude:
            return Video(video_id, video.client)
    return None


@dataclass
class Stream:
    token: str
    track: Track
    start: int


class StreamServer:
    """Serve each renderer's current track at a URL that changes with every load."""

    def __init__(self, ytdlp: Ytdlp, port: int):
        self.ytdlp = ytdlp
        self.port = port
        self.streams: dict[int, Stream] = {}

    def add_routes(self, app: web.Application):
        app.router.add_route("GET", "/stream/{slot}/{name}", self._handle)
        app.router.add_route("HEAD", "/stream/{slot}/{name}", self._handle)

    def publish(self, slot: int, track: Track, start: int, host: str) -> str:
        """Serve `track` from `start` seconds for one renderer, and return its URL."""
        token = secrets.token_hex(8)
        self.streams[slot] = Stream(token, track, start)
        return f"http://{host}:{self.port}/stream/{slot}/{token}.{track.ext}"

    def withdraw(self, slot: int):
        self.streams.pop(slot, None)

    def close(self):
        self.streams.clear()

    async def _handle(self, request: web.Request) -> web.StreamResponse:
        stream = self.streams.get(int(request.match_info["slot"]))
        if stream is None or request.match_info["name"].split(".")[0] != stream.token:
            raise web.HTTPNotFound()
        track = stream.track
        response = web.StreamResponse(headers={
            "Content-Type": track.content_type,
            "transferMode.dlna.org": "Streaming",
            "contentFeatures.dlna.org": track.dlna_features,
            "Cache-Control": "no-cache",
        })
        if request.method == "HEAD":
            await response.prepare(request)
            return response

        LOG.debug("Stream %s from %s s to %s", track.title, stream.start, request.remote)
        read_end, write_end = os.pipe()
        try:
            download = await asyncio.create_subprocess_exec(
                *self.ytdlp.command, "--load-info-json", "-", "-f", AUDIO_FORMAT, "--no-part", "-o", "-",
                stdin=asyncio.subprocess.PIPE, stdout=write_end, stderr=asyncio.subprocess.PIPE)
            output = ["-c:a", "copy", "-f", "adts"] if track.passthrough else \
                ["-c:a", "libmp3lame", "-b:a", "256k", "-f", "mp3"]
            convert = await asyncio.create_subprocess_exec(
                "ffmpeg", "-hide_banner", "-loglevel", "error", *(["-ss", str(stream.start)] if stream.start else []),
                "-i", "pipe:0", "-vn", *output, "pipe:1",
                stdin=read_end, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        finally:
            os.close(read_end)
            os.close(write_end)

        errors = collections.deque(maxlen=4)

        async def collect(process, name):
            async for line in process.stderr:
                errors.append(f"{name}: {line.decode('utf-8', 'replace').strip()}")

        collectors = [asyncio.create_task(collect(download, "yt-dlp")), asyncio.create_task(collect(convert, "ffmpeg"))]
        sent = 0
        try:
            # yt-dlp reads the extraction from stdin, so it does not ask YouTube again.
            download.stdin.write(track.info_json)
            await download.stdin.drain()
            download.stdin.close()
            await response.prepare(request)
            while chunk := await convert.stdout.read(CHUNK):
                await response.write(chunk)
                sent += len(chunk)
        except (ConnectionResetError, BrokenPipeError, aiohttp.ClientConnectionResetError):
            LOG.debug("Renderer closed the stream of %s after %s bytes", track.title, sent)
        finally:
            for process in (download, convert):
                if process.returncode is None:
                    process.kill()
            await asyncio.gather(download.wait(), convert.wait())
            await asyncio.gather(*collectors, return_exceptions=True)
        if download.returncode not in (0, -9) or convert.returncode not in (0, -9):
            LOG.warning("Stream of %s ended after %s bytes: %s", track.title, sent, " | ".join(errors) or "no output")
        return response
