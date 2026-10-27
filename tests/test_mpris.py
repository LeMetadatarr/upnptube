import asyncio
import contextlib
import subprocess
import time

import aiohttp
import pytest
from aiohttp import web
from dbus_fast import DBusError, Message
from dbus_fast.aio import MessageBus

from upnptube.mpris import OBJECT_PATH, Api, Bridge, bus_name

NAME = "org.mpris.MediaPlayer2.upnptube.Test_Speaker"
UDN = "uuid:00000000-0000-0000-0000-0000000000e2"
PLAYING = {"renderer": "Test Speaker", "state": "PLAYING", "title": "Song", "artist": "Band",
           "video_id": "abcdefghijk", "position": 50, "duration": 200, "volume": 30, "muted": False,
           "has_previous": False, "has_next": True, "senders": ["Phone"]}


@pytest.fixture(scope="module")
def session_bus():
    """A private session bus, so the tests never touch the desktop's players."""
    daemon = subprocess.Popen(["dbus-daemon", "--session", "--print-address=1", "--nofork"],
                              stdout=subprocess.PIPE, text=True)
    yield daemon.stdout.readline().strip()
    daemon.terminate()
    daemon.wait()


class FakeService:
    """The control API of upnptube, with one renderer."""

    def __init__(self):
        self.renderers = [{"name": "Test Speaker", "udn": UDN, "location": "http://127.0.0.1:1/desc.xml"}]
        self.status = dict(PLAYING)
        self.calls: list[tuple[str, dict]] = []
        self.runner: web.AppRunner | None = None
        self.url = ""

    async def start(self):
        routes = web.Application()
        routes.router.add_get("/api/renderers", self._renderers)
        routes.router.add_get("/api/status", self._status)
        routes.router.add_post("/api/{path}", self._command)
        self.runner = web.AppRunner(routes)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        self.url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"

    async def _renderers(self, request):
        return web.json_response(self.renderers)

    async def _status(self, request):
        assert request.query["renderer"] == UDN
        return web.json_response(self.status)

    async def _command(self, request):
        self.calls.append((request.match_info["path"], await request.json()))
        return web.json_response({})


async def wait_until(condition, timeout=5.0):
    end = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < end, "timed out"
        await asyncio.sleep(0.02)


async def has_owner(bus: MessageBus, name: str) -> bool:
    reply = await bus.call(Message(destination="org.freedesktop.DBus", path="/org/freedesktop/DBus",
                                   interface="org.freedesktop.DBus", member="NameHasOwner", signature="s",
                                   body=[name]))
    return reply.body[0]


def with_bridge(address, scenario):
    async def main():
        service = FakeService()
        await service.start()
        async with aiohttp.ClientSession() as http:
            bridge = Bridge(Api(http, service.url), address, poll=0.05)
            task = asyncio.create_task(bridge.run())
            client = await MessageBus(bus_address=address).connect()
            try:
                await wait_until(lambda: any(player.status for _, player in bridge.players.values()))
                remote = client.get_proxy_object(NAME, OBJECT_PATH, await client.introspect(NAME, OBJECT_PATH))
                await scenario(service, client, remote)
            finally:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
                client.disconnect()
                await service.runner.cleanup()

    asyncio.run(main())


def test_bus_name_is_a_valid_element():
    assert bus_name("Living Room Stereo") == "org.mpris.MediaPlayer2.upnptube.Living_Room_Stereo"
    assert bus_name("1. Küche") == "org.mpris.MediaPlayer2.upnptube._1_Kuche"
    assert bus_name("🔊") == "org.mpris.MediaPlayer2.upnptube.renderer"


def test_the_status_shows_as_mpris_properties(session_bus):
    async def scenario(service, client, remote):
        player = remote.get_interface("org.mpris.MediaPlayer2.Player")
        root = remote.get_interface("org.mpris.MediaPlayer2")
        metadata = {key: variant.value for key, variant in (await player.get_metadata()).items()}
        assert metadata.pop("mpris:trackid").startswith("/org/upnptube/track/")
        assert metadata == {"xesam:title": "Song", "xesam:artist": ["Band"], "mpris:length": 200_000_000,
                            "mpris:artUrl": "https://i.ytimg.com/vi/abcdefghijk/hqdefault.jpg",
                            "xesam:url": "https://www.youtube.com/watch?v=abcdefghijk"}
        assert (await player.get_playback_status(), await player.get_volume(), await player.get_rate()) == \
            ("Playing", 0.3, 1.0)
        flags = [await player.get_can_go_next(), await player.get_can_go_previous(), await player.get_can_play(),
                 await player.get_can_pause(), await player.get_can_seek(), await player.get_can_control()]
        assert flags == [True, False, True, True, True, True]
        assert 50_000_000 <= await player.get_position() < 51_000_000
        assert (await root.get_identity(), await root.get_can_quit(), await root.get_has_track_list()) == \
            ("Test Speaker", False, False)

        service.status = {**PLAYING, "state": "STOPPED", "title": "", "video_id": None, "duration": 0}
        await wait_until(lambda: service.status["state"] == "STOPPED")
        await asyncio.sleep(0.2)
        assert await player.get_playback_status() == "Stopped"
        assert {k: v.value for k, v in (await player.get_metadata()).items()} == {
            "mpris:trackid": "/org/mpris/MediaPlayer2/TrackList/NoTrack"}
        assert [await player.get_can_play(), await player.get_can_pause(), await player.get_can_seek()] == \
            [False, False, False]

    with_bridge(session_bus, scenario)


def test_the_controls_call_the_api(session_bus):
    async def scenario(service, client, remote):
        player = remote.get_interface("org.mpris.MediaPlayer2.Player")
        renderer = {"renderer": UDN}

        async def calls_after(action, count):
            await action
            await wait_until(lambda: len(service.calls) >= count)
            await asyncio.sleep(0.1)
            return service.calls[count - 1:]

        assert await calls_after(player.call_play_pause(), 1) == [("pause", renderer)]
        assert await calls_after(player.call_next(), 2) == [("next", renderer)]
        # Previous is not possible here, so it does nothing; Pause is next in line.
        await player.call_previous()
        assert await calls_after(player.call_pause(), 3) == [("pause", renderer)]
        assert await calls_after(player.set_volume(0.55), 4) == [("volume", {**renderer, "volume": 55})]
        # The specification: a negative volume sets 0.0.
        assert await calls_after(player.set_volume(-0.5), 5) == [("volume", {**renderer, "volume": 0})]
        # 50 s plus 10 s.
        assert await calls_after(player.call_seek(10_000_000), 6) == [("seek", {**renderer, "position": 60})]
        # Back 80 s from 50 s stops at the start.
        assert await calls_after(player.call_seek(-80_000_000), 7) == [("seek", {**renderer, "position": 0})]
        # The specification: a seek beyond the end acts like Next.
        assert await calls_after(player.call_seek(500_000_000), 8) == [("next", renderer)]

        track_id = (await player.get_metadata())["mpris:trackid"].value
        await player.call_set_position("/org/upnptube/track/999", 10_000_000)
        await player.call_set_position(track_id, 300_000_000)
        await player.call_set_position(track_id, -1)
        assert await calls_after(player.call_set_position(track_id, 10_000_000), 9) == \
            [("seek", {**renderer, "position": 10})]

        assert await calls_after(player.call_open_uri("https://youtu.be/abcdefghijk"), 10) == \
            [("play", {**renderer, "url": "https://youtu.be/abcdefghijk"})]
        with pytest.raises(DBusError):
            await player.call_open_uri("file:///music/song.mp3")
        await asyncio.sleep(0.2)
        assert len(service.calls) == 10

    with_bridge(session_bus, scenario)


def test_changes_are_signalled_and_a_jump_is_a_seek(session_bus):
    async def scenario(service, client, remote):
        player = remote.get_interface("org.mpris.MediaPlayer2.Player")
        properties = remote.get_interface("org.freedesktop.DBus.Properties")
        changes: list[dict] = []
        seeks: list[int] = []
        properties.on_properties_changed(lambda interface, changed, invalid: changes.append(
            {key: variant.value for key, variant in changed.items()}))
        player.on_seeked(seeks.append)

        service.status = {**PLAYING, "state": "PAUSED"}
        await wait_until(lambda: changes)
        assert changes == [{"PlaybackStatus": "Paused"}] and seeks == []

        # The same track, 70 s further on: a seek.
        service.status = {**PLAYING, "position": 120}
        await wait_until(lambda: seeks)
        assert seeks == [120_000_000]
        assert changes[1:] == [{"PlaybackStatus": "Playing"}]

        # A new track starts at 0 s: new metadata, and no seek.
        service.status = {**PLAYING, "title": "Other", "video_id": "bbbbbbbbbbb", "position": 0}
        await wait_until(lambda: len(changes) > 2)
        await asyncio.sleep(0.2)
        assert seeks == [120_000_000]
        assert [sorted(change) for change in changes[2:]] == [["Metadata"]]
        assert changes[2]["Metadata"]["xesam:title"].value == "Other"

    with_bridge(session_bus, scenario)


def test_the_player_leaves_with_its_renderer_and_with_the_service(session_bus):
    async def scenario(service, client, remote):
        assert await has_owner(client, NAME)
        service.renderers = []
        await asyncio.sleep(0.3)
        assert not await has_owner(client, NAME)
        service.renderers = [{"name": "Test Speaker", "udn": UDN, "location": ""}]
        await asyncio.sleep(0.3)
        assert await has_owner(client, NAME)
        # The service stops: a player must not show a track that nobody can control.
        await service.runner.cleanup()
        await asyncio.sleep(0.3)
        assert not await has_owner(client, NAME)

    with_bridge(session_bus, scenario)
