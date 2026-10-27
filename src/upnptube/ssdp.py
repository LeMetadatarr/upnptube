"""
SSDP, the discovery part of the UPnP Device Architecture: find media renderers on the
network, and announce the DIAL devices that YouTube senders look for.
"""
import asyncio
import email.utils
import logging
import platform
import random
import socket
import struct
from collections.abc import Callable
from dataclasses import dataclass, field

LOG = logging.getLogger(__name__)

GROUP = "239.255.255.250"
PORT = 1900
MAX_AGE = 1800
SERVER = f"{platform.system()}/{platform.release()} UPnP/1.1 upnptube/1.0"
BOOT_ID = "1"
CONFIG_ID = "1"


def parse_message(data: bytes) -> tuple[str, dict[str, str]]:
    """The request or status line of an SSDP datagram, and its headers with upper-case names."""
    text = data.decode("utf-8", "replace")
    first, _, rest = text.partition("\n")
    headers = {}
    for line in rest.splitlines():
        name, colon, value = line.partition(":")
        if colon and name.strip():
            headers[name.strip().upper()] = value.strip()
    return first.strip(), headers


def format_message(first_line: str, headers: dict[str, str]) -> bytes:
    return (first_line + "\r\n" + "".join(f"{name}: {value}\r\n" for name, value in headers.items())
            + "\r\n").encode()


def local_ip_for(remote: str) -> str:
    """The address of the interface that the route to `remote` leaves from."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.connect((remote, PORT))
        return probe.getsockname()[0]


def max_age(headers: dict[str, str], default: int) -> int:
    """The validity period of an announcement, from a header such as "CACHE-CONTROL: max-age=1800"."""
    for directive in headers.get("CACHE-CONTROL", "").split(","):
        name, _, value = directive.partition("=")
        if name.strip().lower() == "max-age" and value.strip().isdigit():
            return int(value)
    return default


@dataclass
class RootDevice:
    """A device of ours that answers searches: its description is at `path` on `port`."""
    uuid: str
    device_type: str
    port: int
    path: str
    service_types: list[str] = field(default_factory=list)

    def targets(self) -> dict[str, str]:
        """Each notification type of the device, with its unique service name."""
        udn = f"uuid:{self.uuid}"
        targets = {"upnp:rootdevice": f"{udn}::upnp:rootdevice", udn: udn}
        for kind in (self.device_type, *self.service_types):
            targets[kind] = f"{udn}::{kind}"
        return targets

    def location(self, host: str) -> str:
        return f"http://{host}:{self.port}{self.path}"


class _Datagrams(asyncio.DatagramProtocol):
    def __init__(self, handle: Callable[[bytes, tuple], None]):
        self.handle = handle

    def datagram_received(self, data, addr):
        try:
            self.handle(data, addr)
        except Exception:
            LOG.exception("Cannot handle an SSDP datagram from %s", addr[0])


class Ssdp:
    """
    The multicast socket on port 1900 hears announcements and searches. A second socket
    on a free port sends searches and announcements, and hears the answers to searches.
    """

    def __init__(self, watch_type: str):
        self.watch_type = watch_type
        self.on_found: Callable[[str, int], None] = lambda location, age: None
        self.on_lost: Callable[[str], None] = lambda location: None
        self.devices: list[RootDevice] = []
        self._group: asyncio.DatagramTransport | None = None
        self._unicast: asyncio.DatagramTransport | None = None
        self._tasks: set[asyncio.Task] = set()

    async def start(self):
        loop = asyncio.get_running_loop()
        group = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        # Other UPnP software on the host listens on port 1900 too.
        group.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            group.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        group.bind(("", PORT))
        group.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP,
                         struct.pack("4s4s", socket.inet_aton(GROUP), socket.inet_aton("0.0.0.0")))
        self._group, _ = await loop.create_datagram_endpoint(lambda: _Datagrams(self._on_group), sock=group)

        unicast = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        unicast.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
        unicast.bind(("", 0))
        self._unicast, _ = await loop.create_datagram_endpoint(lambda: _Datagrams(self._on_unicast), sock=unicast)
        self._spawn(self._announce_periodically())

    def stop(self):
        for device in list(self.devices):
            self.withdraw(device)
        for task in self._tasks:
            task.cancel()
        for transport in (self._group, self._unicast):
            if transport:
                transport.close()
        self._group = self._unicast = None

    def _spawn(self, coro):
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    # --- our devices

    def advertise(self, device: RootDevice):
        self.devices.append(device)
        self._announce(device, "ssdp:alive")

    def withdraw(self, device: RootDevice):
        if device in self.devices:
            self.devices.remove(device)
            self._announce(device, "ssdp:byebye")

    async def _announce_periodically(self):
        while True:
            await asyncio.sleep(MAX_AGE / 3)
            for device in self.devices:
                self._announce(device, "ssdp:alive")

    def _announce(self, device: RootDevice, kind: str):
        if not self._unicast:
            return
        host = local_ip_for(GROUP)
        for target, usn in device.targets().items():
            headers = {"HOST": f"{GROUP}:{PORT}", "NT": target, "NTS": kind, "USN": usn,
                       "BOOTID.UPNP.ORG": BOOT_ID, "CONFIGID.UPNP.ORG": CONFIG_ID}
            if kind == "ssdp:alive":
                headers |= {"CACHE-CONTROL": f"max-age={MAX_AGE}", "LOCATION": device.location(host),
                            "SERVER": SERVER}
            self._unicast.sendto(format_message("NOTIFY * HTTP/1.1", headers), (GROUP, PORT))

    async def _answer(self, search_target: str, delay: float, sender: tuple):
        await asyncio.sleep(delay)
        host = local_ip_for(sender[0])
        for device in self.devices:
            targets = device.targets()
            matches = targets.items() if search_target == "ssdp:all" else \
                [(search_target, targets[search_target])] if search_target in targets else []
            for target, usn in matches:
                headers = {"CACHE-CONTROL": f"max-age={MAX_AGE}", "DATE": email.utils.formatdate(usegmt=True),
                           "EXT": "", "LOCATION": device.location(host), "SERVER": SERVER, "ST": target,
                           "USN": usn, "BOOTID.UPNP.ORG": BOOT_ID, "CONFIGID.UPNP.ORG": CONFIG_ID}
                if self._unicast:
                    self._unicast.sendto(format_message("HTTP/1.1 200 OK", headers), sender)

    # --- renderers

    def search(self, wait: int = 3):
        """Ask every renderer to answer within `wait` seconds."""
        headers = {"HOST": f"{GROUP}:{PORT}", "MAN": '"ssdp:discover"', "MX": str(wait), "ST": self.watch_type}
        if self._unicast:
            self._unicast.sendto(format_message("M-SEARCH * HTTP/1.1", headers), (GROUP, PORT))

    def _on_unicast(self, data: bytes, sender: tuple):
        first, headers = parse_message(data)
        answered = first.startswith("HTTP/") and first.split()[1:2] == ["200"]
        if answered and headers.get("ST") == self.watch_type and headers.get("LOCATION"):
            self.on_found(headers["LOCATION"], max_age(headers, MAX_AGE))

    def _on_group(self, data: bytes, sender: tuple):
        first, headers = parse_message(data)
        method = first.split(" ", 1)[0].upper()
        if method == "M-SEARCH":
            if headers.get("MAN", "").strip('"') != "ssdp:discover":
                return
            try:
                wait = max(1, min(int(headers.get("MX", "1")), 5))
            except ValueError:
                wait = 1
            # Spread the answers over the wait the searcher allows, as the architecture requires.
            self._spawn(self._answer(headers.get("ST", ""), random.uniform(0, wait), sender))
        elif method == "NOTIFY" and headers.get("NT") == self.watch_type:
            kind = headers.get("NTS")
            if kind == "ssdp:alive" and headers.get("LOCATION"):
                self.on_found(headers["LOCATION"], max_age(headers, MAX_AGE))
            elif kind == "ssdp:byebye" and headers.get("LOCATION"):
                self.on_lost(headers["LOCATION"])
