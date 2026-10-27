import asyncio
import json
import logging

import pytest

from upnptube.app import ResetPolicy, YouTubeApp
from upnptube.dial import DEVICE_TYPE, SERVICE_TYPE
from upnptube.lounge import YOUTUBE, ChunkParser, Message, Session, send_body
from upnptube.media import _watch_endpoints, youtube_link
from upnptube.player import Autoplay, Player, Playlist, Status, Video, Volume
from upnptube.ssdp import RootDevice, Ssdp, max_age, parse_message
from upnptube.store import Store
from upnptube.upnp import MediaRenderer, UpnpError, format_duration, parse_duration

LOG = logging.LoggerAdapter(logging.getLogger("test"), {})

# A frame as the bind channel sends it: a character count, then the JSON array.
FRAME = '[[7,["c","SID1","",8]],[8,["S","gs1"]],[9,["setPlaylist",{"videoIds":"a,b,c","currentIndex":"1"}]]]'


def test_chunk_parser_reassembles_a_frame_split_at_every_byte():
    parser = ChunkParser()
    messages = []
    for char in f"{len(FRAME)}\n{FRAME}\n":
        messages += parser.feed(char)
    assert messages == [
        Message("c", ["SID1", "", 8], 7),
        Message("S", "gs1", 8),
        Message("setPlaylist", {"videoIds": "a,b,c", "currentIndex": "1"}, 9),
    ]
    assert parser.buffer.strip() == ""


def test_chunk_parser_reads_two_frames_in_one_read_and_holds_a_partial_third():
    parser = ChunkParser()
    text = '16\n[[1,["noop"]]\n]\n16\n[[2,["noop"]]\n]\n20\n[[3,["pau'
    assert [m.aid for m in parser.feed(text)] == [1, 2]
    assert parser.feed('se"]]]\n') == [Message("pause", None, 3)]


def test_send_body_numbers_each_message_and_drops_empty_fields():
    body = send_body([Message("onStateChange", {"state": 1, "currentTime": 7}),
                      Message("onVolumeChanged", {"volume": 30, "muted": False, "x": None}),
                      Message("autoplayUpNext")], ofs=5)
    assert body == {
        "count": "3", "ofs": "5",
        "req0__sc": "onStateChange", "req0_state": "1", "req0_currentTime": "7",
        "req1__sc": "onVolumeChanged", "req1_volume": "30", "req1_muted": "false",
        "req2__sc": "autoplayUpNext",
    }


def make_session(store=None, on_messages=None):
    async def ignore(messages, session):
        pass
    return Session(None, YOUTUBE, "Screen", "upnptube", "Acme", "Box", store or Store(None), "dev",
                   on_messages or ignore, LOG)


def test_session_counts_rid_and_aid_as_yt_cast_receiver_does():
    session = make_session()
    session.lounge_token, session.sid, session.gsessionid, session.rid = "tok", "SID", "gs", 100
    init = session._params("init")
    assert (init["RID"], init["CVER"], json.loads(init["deviceInfo"])["brand"]) == ("100", "1", "Acme")
    # An unsolicited message takes the current AID and moves it on.
    first = session._params("send")
    assert (first["RID"], first["AID"]) == ("101", "3")
    # A reply carries the AID of the sender's message when that is higher, and keeps it.
    reply = session._params("send", aid=12)
    assert (reply["RID"], reply["AID"]) == ("102", "12")
    later = session._params("send")
    assert later["AID"] == "12"
    rpc = session._params("rpc")
    assert (rpc["RID"], rpc["AID"], rpc["TYPE"], rpc["capabilities"], rpc["cst"]) == \
        ("rpc", "13", "xmlhttp", "dsp,mic,dpa,ntb", "m")


def test_session_takes_sid_and_aid_from_incoming_messages():
    session = make_session()
    session._note([Message("c", ["SID9", "", 8], 1), Message("S", "gs9", 2), Message("noop", None, 40)])
    assert (session.sid, session.gsessionid, session.aid) == ("SID9", "gs9", 40)


def test_session_reuses_the_saved_device_and_screen_ids(tmp_path):
    store = Store(tmp_path / "state.json")
    store.set("dev.cl", {"deviceId": "d-1", "screenId": "s-1"})
    session = make_session(Store(tmp_path / "state.json"))
    assert (session.device_id, session.screen_id) == ("d-1", "s-1")


def run(coro):
    return asyncio.run(coro)


async def no_up_next(video, exclude):
    return None


def test_playlist_steps_through_the_sender_list():
    queue = Playlist(no_up_next)
    run(queue.update("setPlaylist", {"listId": "RQ1", "videoIds": "a,b,c", "videoId": "b", "currentIndex": "1"},
                     YOUTUBE))
    assert (queue.current.id, queue.has_previous, queue.has_next) == ("b", True, True)
    assert run(queue.next()).id == "c"
    assert (queue.is_last, queue.has_next) == (True, False)
    assert run(queue.next()) is None
    assert [run(queue.previous()).id, run(queue.previous()).id] == ["b", "a"]
    assert run(queue.previous()) is None


def test_update_playlist_finds_the_current_video_at_its_new_index():
    queue = Playlist(no_up_next)
    run(queue.update("setPlaylist", {"listId": "RQ1", "videoIds": "a,b", "videoId": "b", "currentIndex": "1"},
                     YOUTUBE))
    run(queue.update("updatePlaylist", {"listId": "RQ1", "videoIds": "x,b,y"}, YOUTUBE))
    assert (queue.current.id, queue.index, queue.has_next) == ("b", 1, True)
    run(queue.update("updatePlaylist", {"listId": "RQ1", "videoIds": "x,y"}, YOUTUBE))
    assert queue.current is None


def test_autoplay_follows_the_last_video_only_when_enabled():
    asked = []

    async def finder(video, exclude):
        asked.append((video.id, sorted(exclude)))
        return Video("z")

    queue = Playlist(finder)
    run(queue.update("setPlaylist", {"listId": "RQ1", "videoIds": "a,b", "videoId": "b", "currentIndex": "1"},
                     YOUTUBE))
    assert queue.autoplay is None and asked == []
    run(queue.set_autoplay_mode(Autoplay.ENABLED))
    assert queue.autoplay.id == "z" and asked == [("b", ["a", "b", "b"])]
    run(queue.set_autoplay_mode(Autoplay.DISABLED))
    assert queue.autoplay is None


def test_youtube_link():
    assert youtube_link("https://www.youtube.com/watch?v=69BP7sGFa74&t=3") == ("69BP7sGFa74", None)
    assert youtube_link("https://youtu.be/69BP7sGFa74?si=x") == ("69BP7sGFa74", None)
    assert youtube_link("https://music.youtube.com/playlist?list=PL1") == (None, "PL1")
    assert youtube_link("https://m.youtube.com/shorts/abcdefghijk") == ("abcdefghijk", None)
    assert youtube_link("https://example.com/watch?v=69BP7sGFa74") is None
    assert youtube_link("https://www.youtube.com/") is None


def test_watch_endpoints_are_found_at_any_depth_in_order():
    data = {"a": {"x": {"watchEndpoint": {"videoId": "v1"}}}, "b": [{"y": {"watchEndpoint": {"videoId": "v2"}}}]}
    assert [e["videoId"] for e in _watch_endpoints(data)] == ["v1", "v2"]


class FakePlayer(Player):
    def __init__(self):
        super().__init__(no_up_next)
        self.calls = []

    async def do_play(self, video, position):
        self.calls.append(("play", video.id, position))
        return True

    async def do_pause(self):
        return True

    async def do_resume(self):
        return True

    async def do_stop(self):
        self.calls.append(("stop",))
        return True

    async def do_seek(self, position):
        return True

    async def do_set_volume(self, volume):
        return True

    async def do_get_volume(self):
        return Volume(30)

    async def do_get_position(self):
        return 0

    async def do_get_duration(self):
        return 200


class FakeSession:
    def __init__(self):
        self.client = YOUTUBE
        self.sent: list[list[str]] = []

    async def send(self, messages, aid=None):
        self.sent.append([m.name for m in messages])


def make_app(policy):
    player = FakePlayer()
    app = YouTubeApp(None, player, "Screen", "Acme", "Box", Store(None), "dev", LOG, policy)
    session = FakeSession()
    app.sessions = {"cl": session}
    app.state = "running"
    return app, player, session


def lounge_status(device_id, connected, ui):
    devices = [{"type": "REMOTE_CONTROL", "id": device_id, "name": "Phone", "theme": "cl",
                "capabilities": "atp,que"}] if connected else []
    return Message("loungeStatus", {"devices": json.dumps(devices),
                                    "connectionEventDetails": json.dumps({"deviceId": device_id, "ui": ui})}, 5)


@pytest.mark.parametrize("policy, ui, cleared", [
    (ResetPolicy.ALL_EXPLICITLY_DISCONNECTED, "false", False),
    (ResetPolicy.ALL_EXPLICITLY_DISCONNECTED, "true", True),
    (ResetPolicy.ALL_DISCONNECTED, "false", True),
])
def test_the_queue_survives_only_an_implicit_disconnect_under_the_explicit_policy(policy, ui, cleared):
    app, player, session = make_app(policy)

    async def scenario():
        await app._handle_messages([lounge_status("p1", True, "true")], session)
        assert [s.name for s in app.senders] == ["Phone"] and app.active is session
        assert player.queue.autoplay_mode == Autoplay.ENABLED
        await app._handle_messages([Message("setPlaylist", {
            "listId": "RQ1", "videoIds": "a,b", "videoId": "a", "currentIndex": "0", "currentTime": "12"}, 6)],
            session)
        assert player.calls == [("play", "a", 12)] and player.status == Status.PLAYING
        await app._handle_messages([lounge_status("p1", False, ui)], session)
        return app.senders, player.status, player.queue.current

    senders, status, current = run(scenario())
    assert senders == []
    if cleared:
        assert (status, current) == (Status.IDLE, None)
    else:
        assert (status, current.id) == (Status.PLAYING, "a")


def test_parse_message_upper_cases_keys_and_keeps_colons_in_values():
    start, headers = parse_message(
        b"HTTP/1.1 200 OK\r\nLocation: http://10.0.0.5:49152/desc.xml\r\nCache-Control: max-age=1800\r\nEXT:\r\n\r\n")
    assert start == "HTTP/1.1 200 OK"
    assert headers == {"LOCATION": "http://10.0.0.5:49152/desc.xml", "CACHE-CONTROL": "max-age=1800", "EXT": ""}


def test_durations_round_trip():
    assert parse_duration("1:02:03.500") == 3723
    assert parse_duration("0:00:09") == 9
    assert parse_duration("NOT_IMPLEMENTED") == 0
    assert format_duration(3723) == "1:02:03"


def test_max_age():
    assert max_age({"CACHE-CONTROL": "max-age=1800"}, 300) == 1800
    assert max_age({"CACHE-CONTROL": "no-cache, max-age=60"}, 300) == 60
    assert max_age({"CACHE-CONTROL": "max-age=soon"}, 300) == 300
    assert max_age({}, 300) == 300


class FakeDevice:
    location = "http://127.0.0.1:1/desc.xml"
    friendly_name = "Fake"

    def __init__(self):
        self.state = "PLAYING"

    async def get_transport_state(self):
        return self.state

    async def get_volume(self):
        return 30

    async def get_mute(self):
        return False

    async def get_position(self):
        return 5


def test_the_watcher_does_not_act_on_a_state_read_before_a_command(monkeypatch):
    from upnptube import renderer
    from upnptube.media import Track

    monkeypatch.setattr(renderer, "POLL_SECONDS", 0.01)
    device = FakeDevice()
    player = renderer.UpnpPlayer(None, device, None, None, 0, LOG)
    player.track = Track("src", "Song", "", 200, "mp4a.40.2", b"{}")
    player.status = Status.PAUSED
    reported = []

    async def record(aid, current, previous):
        reported.append(current.status)

    player.on_state = record

    async def scenario():
        device.state = "PAUSED_PLAYBACK"
        async with player.lock:
            watcher = asyncio.create_task(player.watch())
            await asyncio.sleep(0.05)
            # A resume command runs while the watcher waits.
            device.state = "PLAYING"
            player.status = Status.PLAYING
        await asyncio.sleep(0.05)
        watcher.cancel()

    run(scenario())
    # A stale read would report PAUSED for one poll, and a command in that window would misbehave.
    assert (player.status, reported) == (Status.PLAYING, [])


class SentDatagrams:
    def __init__(self):
        self.sent: list[tuple[bytes, tuple]] = []

    def sendto(self, data, addr):
        self.sent.append((data, addr))


def test_ssdp_answers_each_search_target_of_a_root_device():
    ssdp = Ssdp("urn:schemas-upnp-org:device:MediaRenderer:1")
    ssdp._unicast = SentDatagrams()
    ssdp.devices.append(RootDevice("1234", DEVICE_TYPE, 3100, "/dial/device.xml", [SERVICE_TYPE]))
    phone = ("127.0.0.1", 50000)

    def answers(target):
        ssdp._unicast.sent.clear()
        run(ssdp._answer(target, 0, phone))
        return [parse_message(data)[1] for data, addr in ssdp._unicast.sent if addr == phone]

    # A root device with one service and no embedded devices answers ssdp:all 3 + 2*0 + 1 times.
    assert sorted(h["ST"] for h in answers("ssdp:all")) == sorted(
        ["upnp:rootdevice", "uuid:1234", DEVICE_TYPE, SERVICE_TYPE])
    service = answers(SERVICE_TYPE)
    assert [(h["USN"], h["LOCATION"]) for h in service] == [
        (f"uuid:1234::{SERVICE_TYPE}", "http://127.0.0.1:3100/dial/device.xml")]
    assert answers("urn:schemas-upnp-org:device:MediaServer:1") == []


EMBEDDED = """<?xml version="1.0"?>
<root xmlns="urn:schemas-upnp-org:device-1-0">
  <URLBase>http://127.0.0.1:{port}/base/</URLBase>
  <device>
    <friendlyName>Hall &amp; Kitchen</friendlyName><UDN>uuid:root</UDN>
    <deviceList><device><serviceList><service>
      <serviceType>urn:schemas-upnp-org:service:AVTransport:2</serviceType><controlURL>avt</controlURL>
    </service></serviceList></device></deviceList>
  </device>
</root>"""

FAULT = """<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body><s:Fault>
<faultcode>s:Client</faultcode><faultstring>UPnPError</faultstring><detail>
<UPnPError xmlns="urn:schemas-upnp-org:control-1-0"><errorCode>701</errorCode>
<errorDescription>Transition not available</errorDescription></UPnPError></detail></s:Fault></s:Body></s:Envelope>"""


def test_media_renderer_finds_an_embedded_service_and_reports_a_soap_fault():
    from aiohttp import web

    calls = []

    async def control(request):
        calls.append((request.path, request.headers["SOAPACTION"]))
        return web.Response(status=500, text=FAULT, content_type="text/xml")

    async def describe(request):
        return web.Response(text=EMBEDDED.format(port=request.url.port))

    async def scenario():
        routes = web.Application()
        routes.router.add_get("/desc.xml", describe)
        routes.router.add_post("/base/avt", control)
        runner = web.AppRunner(routes)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        device = MediaRenderer(f"http://127.0.0.1:{port}/desc.xml")
        try:
            await device.load_description()
            with pytest.raises(UpnpError, match="701 Transition not available"):
                await device.pause()
            with pytest.raises(UpnpError, match="no RenderingControl"):
                await device.get_volume()
            return device.friendly_name, device.udn
        finally:
            await device.close()
            await runner.cleanup()

    assert run(scenario()) == ("Hall & Kitchen", "uuid:root")
    assert calls == [("/base/avt", '"urn:schemas-upnp-org:service:AVTransport:2#Pause"')]
