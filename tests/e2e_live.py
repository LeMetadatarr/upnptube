"""
Cast to a fake renderer from a fake phone, through YouTube's real Lounge servers.

The fake renderer answers AVTransport and RenderingControl SOAP calls and reads the
start of each stream it is told to play, as a speaker does. The fake phone pairs with a
TV code, binds to the lounge as a remote control and sends what the YouTube app sends.
Everything runs on 127.0.0.1, so no firewall rule is needed.

Usage: python tests/e2e_live.py [video_id]
"""
import asyncio
import json
import logging
import random
import re
import sys
import tempfile
import time
import uuid
from pathlib import Path

import aiohttp
from aiohttp import web

from upnptube.__main__ import UpnpTube
from upnptube.lounge import BIND_URL, LOUNGE_URL, ChunkParser
from upnptube.player import Status
from upnptube.ssdp import parse_message
from upnptube.store import Store

RENDERER_PORT = 49152
DIAL_PORT = 3150
HTTP_PORT = 8150
SSDP_PORT = 1900
SERVER = f"http://127.0.0.1:{HTTP_PORT}"
STREAM_URL = re.compile(rf"http://127\.0\.0\.1:{HTTP_PORT}/stream/0/[0-9a-f]{{16}}\.aac")
READ_BYTES = 65536

DESCRIPTION = """<?xml version="1.0"?>
<root xmlns="urn:schemas-upnp-org:device-1-0">
  <device>
    <deviceType>urn:schemas-upnp-org:device:MediaRenderer:1</deviceType>
    <friendlyName>Test Speaker</friendlyName>
    <manufacturer>Acme</manufacturer>
    <modelName>Fake 1</modelName>
    <UDN>uuid:00000000-0000-0000-0000-0000000000e2</UDN>
    <serviceList>
      <service>
        <serviceType>urn:schemas-upnp-org:service:AVTransport:1</serviceType>
        <controlURL>/ctl/avt</controlURL>
      </service>
      <service>
        <serviceType>urn:schemas-upnp-org:service:RenderingControl:1</serviceType>
        <controlURL>/ctl/rc</controlURL>
      </service>
    </serviceList>
  </device>
</root>"""


def is_adts(data: bytes) -> bool:
    """Every ADTS frame starts with a 12-bit sync word and says how long it is."""
    frames, position = 0, 0
    while position + 7 <= len(data):
        if data[position] != 0xFF or data[position + 1] & 0xF6 != 0xF0:
            return False
        length = ((data[position + 3] & 0x03) << 11) | (data[position + 4] << 3) | (data[position + 5] >> 5)
        if length < 7:
            return False
        position += length
        frames += 1
    return frames > 10


class FakeRenderer:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.actions: list[tuple[str, dict]] = []
        self.volume = 30
        self.muted = False
        self.uri = ""
        self.metadata = ""
        self.started_at: float | None = None
        self.position_offset = 0
        self.state = "NO_MEDIA_PRESENT"
        self.fetched: dict[str, bytes] = {}

    def position(self) -> int:
        if self.started_at is None:
            return self.position_offset
        return self.position_offset + int(time.monotonic() - self.started_at)

    async def serve(self):
        app = web.Application()
        app.router.add_get("/desc.xml", lambda r: web.Response(text=DESCRIPTION, content_type="text/xml"))
        app.router.add_post("/ctl/{service}", self.control)
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, "127.0.0.1", RENDERER_PORT).start()
        return runner

    async def fetch(self, uri: str):
        # A speaker reads the start of the stream; this one then hangs up.
        async with self.session.get(uri) as resp:
            self.fetched[uri] = await resp.content.readexactly(READ_BYTES) if resp.status == 200 else b""

    def finish(self, at: int):
        """The stream ran out at `at` seconds, as at the end of a track."""
        self.position_offset, self.started_at, self.state = at, None, "STOPPED"

    async def control(self, request: web.Request) -> web.Response:
        action = request.headers["SOAPACTION"].strip('"').split("#")[1]
        body = await request.text()
        args = dict(re.findall(r"<(\w+)>([^<]*)</\1>", body.split(f"u:{action}", 1)[1]))
        self.actions.append((action, args))
        if not action.startswith("Get"):
            logging.getLogger("fake-renderer").debug("%s %s", action, args.get("CurrentURI", ""))
        out = ""
        if action == "SetAVTransportURI":
            self.uri, self.metadata = args["CurrentURI"], args["CurrentURIMetaData"]
            self.position_offset, self.started_at, self.state = 0, None, "STOPPED"
            await self.fetch(self.uri)
        elif action == "Play":
            self.started_at, self.state = time.monotonic(), "PLAYING"
        elif action in ("Pause", "Stop"):
            self.position_offset, self.started_at = self.position(), None
            self.state = "PAUSED_PLAYBACK" if action == "Pause" else "STOPPED"
        elif action == "GetTransportInfo":
            out = f"<CurrentTransportState>{self.state}</CurrentTransportState>"
        elif action == "GetPositionInfo":
            p = self.position()
            out = f"<RelTime>{p // 3600}:{p // 60 % 60:02d}:{p % 60:02d}</RelTime>"
        elif action == "GetMediaInfo":
            out = f"<CurrentURI>{self.uri.replace('&', '&amp;')}</CurrentURI>"
        elif action == "GetVolume":
            out = f"<CurrentVolume>{self.volume}</CurrentVolume>"
        elif action == "SetVolume":
            self.volume = int(args["DesiredVolume"])
        elif action == "GetMute":
            out = f"<CurrentMute>{int(self.muted)}</CurrentMute>"
        elif action == "SetMute":
            self.muted = args["DesiredMute"] == "1"
        xml = (f'<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body>'
               f'<u:{action}Response xmlns:u="x">{out}</u:{action}Response></s:Body></s:Envelope>')
        return web.Response(text=xml, content_type="text/xml")


class FakePhone:
    """A Lounge remote control, as the YouTube app is after pairing."""

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.params = {}
        self.ofs = 0
        self.events: list[tuple[str, object]] = []
        self.listener: asyncio.Task | None = None

    async def pair(self, code: str) -> str:
        # A fresh code can take a few seconds to resolve; a person typing it never notices.
        for _ in range(15):
            async with self.session.post(f"{LOUNGE_URL}/pairing/get_screen", data={"pairing_code": code}) as resp:
                text = await resp.text()
            if resp.status == 200:
                break
            await asyncio.sleep(1)
        assert resp.status == 200, f"get_screen: HTTP {resp.status} {text[:200]!r}"
        screen = json.loads(text)["screen"]
        await self.connect(screen["screenId"], screen["loungeToken"])
        return screen["screenId"]

    async def connect(self, screen_id: str, token: str):
        self.params = {"device": "REMOTE_CONTROL", "id": str(uuid.uuid4()), "name": "e2e phone",
                       "app": "android-phone", "theme": "cl", "mdx-version": "3", "loungeIdToken": token,
                       "capabilities": "que,dsdtr,atp", "VER": "8", "v": "2", "CVER": "1", "RID": "1",
                       "zx": "xxxxxxxxxxxx", "t": "1"}
        async with self.session.post(BIND_URL, params=self.params, data={"count": "0"}) as resp:
            for message in ChunkParser().feed(await resp.text()):
                if message.name == "c":
                    self.params["SID"] = message.payload[0]
                elif message.name == "S":
                    self.params["gsessionid"] = message.payload
        self.listener = asyncio.create_task(self.listen())

    async def listen(self):
        params = {**self.params, "RID": "rpc", "CI": "0", "TYPE": "xmlhttp", "AID": "3"}
        while True:
            async with self.session.get(BIND_URL, params=params,
                                        timeout=aiohttp.ClientTimeout(total=None, sock_read=300)) as resp:
                parser = ChunkParser()
                async for chunk in resp.content.iter_any():
                    for message in parser.feed(chunk.decode()):
                        self.events.append((message.name, message.payload))

    async def send(self, sc: str, **data):
        self.ofs += 1
        body = {"count": "1", "ofs": str(self.ofs), "req0__sc": sc}
        body.update({f"req0_{key}": str(value) for key, value in data.items()})
        async with self.session.post(BIND_URL, params=self.params, data=body) as resp:
            assert resp.status == 200, f"{sc}: HTTP {resp.status}"

    async def wait_for(self, name: str, since: int, timeout: float = 60, **match):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            for event, data in self.events[since:]:
                if event == name and all(isinstance(data, dict) and data.get(k) == v for k, v in match.items()):
                    return data
            await asyncio.sleep(0.2)
        raise AssertionError(f"phone saw no {name} {match}; saw {self.events[since:]}")


async def wait_until(predicate, what: str, timeout: float = 60):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return
        await asyncio.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}")


def check(label: str, condition: bool, detail=""):
    print(f"{'PASS' if condition else 'FAIL'} {label} {detail}")
    if not condition:
        raise SystemExit(1)


async def api(session, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    async with session.request(method, f"{SERVER}{path}", json=body) as resp:
        return resp.status, await resp.json(content_type=None)


def loads(fake: FakeRenderer) -> list[str]:
    return [args["CurrentURI"] for action, args in fake.actions if action == "SetAVTransportURI"]


async def main(video_id: str):
    with tempfile.TemporaryDirectory(prefix="upnptube-e2e-") as state_dir:
        await run_checks(video_id, Path(state_dir))


async def run_checks(video_id: str, state_dir: Path):
    async with aiohttp.ClientSession() as session:
        fake = FakeRenderer(session)
        runner = await fake.serve()
        tube = UpnpTube(session, Store(state_dir / "state.json"), DIAL_PORT, HTTP_PORT)
        await tube.start()
        # What an SSDP response from the renderer would deliver.
        tube.add_or_refresh(f"http://127.0.0.1:{RENDERER_PORT}/desc.xml", 300)
        renderer = next(iter(tube.renderers.values()))
        await wait_until(lambda: renderer.ready, "the renderer to start", 30)
        check("renderer named from its description", renderer.dial.friendly_name == "🔊 Test Speaker",
              renderer.dial.friendly_name)
        screen_id = renderer.app.sessions["cl"].screen_id

        # The phone's DIAL search, sent straight to the SSDP listener.
        replies: list[bytes] = []

        class Probe(asyncio.DatagramProtocol):
            def datagram_received(self, data, addr):
                replies.append(data)

        probe = await asyncio.get_running_loop().create_datagram_endpoint(Probe, local_addr=("127.0.0.1", 0))
        probe[0].sendto(b'M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nMAN: "ssdp:discover"\r\n'
                        b"MX: 1\r\nST: urn:dial-multiscreen-org:service:dial:1\r\n\r\n", ("127.0.0.1", SSDP_PORT))
        await wait_until(lambda: replies, "SSDP reply", 5)
        _, headers = parse_message(replies[0])
        location = headers["LOCATION"]
        check("SSDP answers a DIAL search", location == f"http://127.0.0.1:{DIAL_PORT}/dial/device.xml",
              location)
        probe[0].close()

        async with session.get(location) as resp:
            app_url = resp.headers["Application-URL"]
            check("device description has the Application-URL", app_url == f"http://127.0.0.1:{DIAL_PORT}/dial/apps",
                  app_url)
        async with session.get(f"{app_url}/YouTube") as resp:
            text = await resp.text()
            check("the app runs before any launch, with its fixed pid",
                  "<state>running</state>" in text and renderer.app.pid in text)
        # A random code: a well-known one links the screen with strangers who use it too.
        code = "".join(random.choices("0123456789", k=12))
        async with session.post(f"{app_url}/YouTube", data=f"pairingCode={code}&theme=cl",
                                headers={"Content-Type": "text/plain"}) as resp:
            check("DIAL launch returns 201 and the pid", resp.status == 201
                  and resp.headers.get("LOCATION", "").endswith(renderer.app.pid), (resp.status, dict(resp.headers)))

        status, body = await api(session, "GET", "/api/pairing-code")
        check("the control API gives a 12-digit TV code", status == 200 and re.fullmatch(r"\d{12}", body["code"]), body)
        phone = FakePhone(session)
        paired = await phone.pair(body["code"])
        check("the TV code pairs the phone with this screen", paired == screen_id, (paired, screen_id))
        await wait_until(lambda: [s.name for s in renderer.app.senders] == ["e2e phone"], "the phone to connect", 30)
        check("only the phone is connected", [s.name for s in renderer.app.senders] == ["e2e phone"])

        mark = len(phone.events)
        # As castctl and the YouTube app send it for a single video.
        await phone.send("setPlaylist", videoId=video_id, currentTime=0, currentIndex=-1, listId="",
                         audioOnly="false", params="", playerParams="")
        await wait_until(lambda: fake.state == "PLAYING", "Play on the renderer", 90)
        first = fake.uri
        data = fake.fetched.get(first, b"")
        check("renderer was given a stream URL", bool(STREAM_URL.fullmatch(first)), first)
        check("the stream is ADTS audio", len(data) == READ_BYTES and is_adts(data), f"{len(data)} bytes {data[:4]!r}")
        check("DIDL metadata carries the title and an ADTS profile", "AAC_ADTS" in fake.metadata
              and "dc:title" in fake.metadata, fake.metadata[:300])
        playing = await phone.wait_for("nowPlaying", mark, videoId=video_id, state="1")
        check("phone sees nowPlaying with state 1 and a list", bool(playing.get("listId")), playing)

        mark, actions_mark = len(phone.events), len(fake.actions)
        await phone.send("pause")
        await phone.wait_for("onStateChange", mark, state="2")
        check("pause reaches the renderer and the phone", any(a == "Pause" for a, _ in fake.actions[actions_mark:]))

        mark, actions_mark = len(phone.events), len(fake.actions)
        await phone.send("play")
        await phone.wait_for("onStateChange", mark, state="1")
        check("resume reaches the renderer and the phone", any(a == "Play" for a, _ in fake.actions[actions_mark:]))

        mark = len(phone.events)
        await phone.send("setVolume", volume=55, muted="false")
        changed = await phone.wait_for("onVolumeChanged", mark, volume="55")
        check("volume reaches the renderer and the phone", fake.volume == 55 and not fake.muted, changed)

        mark = len(phone.events)
        await phone.send("seekTo", newTime=42)
        state = await phone.wait_for("onStateChange", mark, state="1")
        check("seek starts the stream again from 42 s", loads(fake)[-1] != first and fake.position_offset == 0
              and 42 <= int(float(state["currentTime"])) <= 45 and is_adts(fake.fetched[fake.uri]), state)

        await wait_until(lambda: renderer.player.queue.autoplay is not None, "an up-next video", 60)
        up_next = renderer.player.queue.autoplay.id
        await phone.wait_for("autoplayUpNext", 0, videoId=up_next)
        check("autoplay offers an up-next video to the phone", up_next != video_id, up_next)

        await wait_until(lambda: renderer.player.started, "the watcher to see playback", 10)
        mark, before = len(phone.events), fake.uri
        fake.finish(renderer.player.track.duration)
        await phone.wait_for("nowPlaying", mark, videoId=up_next, state="1", timeout=90)
        check("a track that ends plays the up-next video", fake.uri != before and is_adts(fake.fetched[fake.uri]),
              fake.uri)

        await wait_until(lambda: renderer.player.started, "the watcher to see playback", 10)
        mark, before = len(phone.events), fake.uri
        fake.finish(3)
        await wait_until(lambda: fake.uri != before and fake.state == "PLAYING", "the stream to load again", 60)
        check("a stream that breaks mid-track loads again at its position",
              renderer.player.queue.current.id == up_next and renderer.player.offset == 3, renderer.player.offset)

        mark = len(phone.events)
        fake.uri, fake.state = "http://elsewhere/other.mp3", "STOPPED"
        await phone.wait_for("onStateChange", mark, state="4", timeout=30)
        check("another controller taking over shows as stopped", renderer.player.status == Status.STOPPED)

        mark, actions_mark = len(phone.events), len(fake.actions)
        await phone.send("previous")
        await phone.wait_for("nowPlaying", mark, videoId=video_id, state="1", timeout=90)
        check("previous goes back to the first video", is_adts(fake.fetched[fake.uri]))

        mark, actions_mark = len(phone.events), len(fake.actions)
        await phone.send("stopVideo")
        await phone.wait_for("onStateChange", mark, state="4")
        check("stop reaches the renderer", any(a == "Stop" for a, _ in fake.actions[actions_mark:]))
        phone.listener.cancel()

        status, body = await api(session, "GET", "/api/renderers")
        check("API lists the renderer", status == 200 and [r["name"] for r in body] == ["Test Speaker"], body)
        status, body = await api(session, "POST", "/api/play", {"url": f"https://youtu.be/{video_id}"})
        check("API plays a YouTube link through the queue", status == 200 and fake.state == "PLAYING"
              and renderer.player.queue.current.id == video_id and is_adts(fake.fetched[fake.uri]), body)
        status, body = await api(session, "POST", "/api/pause", {"renderer": "speaker"})
        check("API pause, with a partial renderer name", status == 200 and fake.state == "PAUSED_PLAYBACK", body)
        status, body = await api(session, "POST", "/api/volume", {"volume": 20})
        check("API volume", status == 200 and fake.volume == 20, body)
        status, body = await api(session, "GET", "/api/status")
        check("API status reads the player", status == 200 and body["state"] == "PAUSED" and body["volume"] == 20
              and body["video_id"] == video_id and body["duration"] > 0 and body["title"], body)
        status, body = await api(session, "POST", "/api/next")
        check("API next plays the autoplay video", status == 200 and renderer.player.queue.current.id != video_id
              and fake.state == "PLAYING", body)
        status, body = await api(session, "POST", "/api/stop", {"renderer": "kitchen"})
        check("API refuses an unknown renderer", status == 404 and "No renderer matches" in body["error"], body)
        status, body = await api(session, "POST", "/api/volume", {"volume": 101})
        check("API refuses a volume above 100", status == 400 and fake.volume == 20, body)
        status, body = await api(session, "POST", "/api/play", {"url": "https://www.youtube.com/watch?v=xxxxxxxxxxx"})
        check("API reports a URL it cannot play", status == 502 and "Cannot play" in body["error"], body)
        status, body = await api(session, "POST", "/api/stop")
        check("API stop", status == 200 and fake.state == "STOPPED", (body, fake.state, fake.actions[-6:]))

        stream_url = fake.uri
        await tube.stop()
        saved = json.loads((state_dir / "state.json").read_text())
        check("the screen ids are saved", saved["uuid:00000000-0000-0000-0000-0000000000e2.cl"]["screenId"] == screen_id,
              saved)
        tube = UpnpTube(session, Store(state_dir / "state.json"), DIAL_PORT, HTTP_PORT)
        await tube.start()
        tube.add_or_refresh(f"http://127.0.0.1:{RENDERER_PORT}/desc.xml", 300)
        renderer = next(iter(tube.renderers.values()))
        await wait_until(lambda: renderer.ready, "the renderer to start again", 30)
        check("a restarted service keeps its screen id", renderer.app.sessions["cl"].screen_id == screen_id)
        async with session.get(stream_url) as resp:
            check("a restarted service no longer serves the old stream", resp.status == 404, resp.status)
        await tube.stop()
        await runner.cleanup()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
    if "-v" in sys.argv:
        logging.getLogger("upnptube").setLevel(logging.DEBUG)
        logging.getLogger("fake-renderer").setLevel(logging.DEBUG)
    asyncio.run(main(next((a for a in sys.argv[1:] if not a.startswith("-")), "69BP7sGFa74")))
