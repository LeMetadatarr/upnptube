"""
Control of a UPnP MediaRenderer through the SOAP actions of its AVTransport:1 and
RenderingControl:1 services.
"""
import xml.etree.ElementTree as ET
from urllib.parse import urljoin
from xml.sax.saxutils import escape, quoteattr

import aiohttp

SOAP_ENV = "http://schemas.xmlsoap.org/soap/envelope/"
SOAP_ENCODING = "http://schemas.xmlsoap.org/soap/encoding/"
UPNP_DEVICE = "urn:schemas-upnp-org:device-1-0"
UPNP_CONTROL = "urn:schemas-upnp-org:control-1-0"
TRANSPORT = "AVTransport"
RENDERING = "RenderingControl"
TIMEOUT = aiohttp.ClientTimeout(total=10)


class UpnpError(Exception):
    pass


def parse_duration(value: str | None) -> int:
    """Whole seconds of an H+:MM:SS[.F] time; 0 for "NOT_IMPLEMENTED" and other non-times."""
    parts = (value or "").split(":")
    if len(parts) != 3:
        return 0
    try:
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(float(parts[2]))
    except ValueError:
        return 0


def format_duration(seconds: int) -> str:
    minutes, secs = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}:{minutes:02d}:{secs:02d}"


def didl_lite(url: str, title: str, artist: str, content_type: str, features: str, duration: int) -> str:
    """The metadata of one music track, as DIDL-Lite."""
    duration_attr = f" duration={quoteattr(format_duration(duration))}" if duration else ""
    protocol = quoteattr(f"http-get:*:{content_type}:{features}")
    return (
        '<DIDL-Lite xmlns="urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:upnp="urn:schemas-upnp-org:metadata-1-0/upnp/">'
        '<item id="1" parentID="0" restricted="1">'
        f"<dc:title>{escape(title)}</dc:title><upnp:artist>{escape(artist)}</upnp:artist>"
        "<upnp:class>object.item.audioItem.musicTrack</upnp:class>"
        f"<res protocolInfo={protocol}{duration_attr}>{escape(url)}</res>"
        "</item></DIDL-Lite>"
    )


def _xml(data: bytes, what: str) -> ET.Element:
    try:
        return ET.fromstring(data)
    except ET.ParseError as error:
        raise UpnpError(f"Invalid XML in {what}: {error}") from error


def _local(tag: str) -> str:
    return tag.rpartition("}")[2]


class MediaRenderer:
    def __init__(self, location: str):
        self.location = location
        self.friendly_name = ""
        self.manufacturer = ""
        self.model_name = ""
        self.udn = ""
        # Service name, such as "AVTransport", to its full type and its control URL.
        self.services: dict[str, tuple[str, str]] = {}
        # Many renderer stacks close the connection after each response, so open a new one for each request.
        self.http = aiohttp.ClientSession(connector=aiohttp.TCPConnector(force_close=True), timeout=TIMEOUT)

    async def close(self):
        await self.http.close()

    async def load_description(self):
        async with self.http.get(self.location) as response:
            response.raise_for_status()
            root = _xml(await response.read(), self.location)
        ns = {"d": UPNP_DEVICE}
        device = root.find("d:device", ns)
        if device is None:
            raise UpnpError(f"The description at {self.location} has no device")
        self.friendly_name = device.findtext("d:friendlyName", "", ns).strip()
        self.manufacturer = device.findtext("d:manufacturer", "", ns).strip()
        self.model_name = device.findtext("d:modelName", "", ns).strip()
        self.udn = device.findtext("d:UDN", "", ns).strip()
        base = root.findtext("d:URLBase", "", ns).strip() or self.location
        # A service can belong to an embedded device, so look at every service in the tree.
        for service in root.iterfind(".//d:service", ns):
            full_type = service.findtext("d:serviceType", "", ns).strip()
            name = full_type.split(":")[-2] if full_type.count(":") >= 4 else ""
            if name in (TRANSPORT, RENDERING) and name not in self.services:
                self.services[name] = (full_type, urljoin(base, service.findtext("d:controlURL", "", ns).strip()))
        if TRANSPORT not in self.services:
            raise UpnpError(f"{self.location} offers no AVTransport service")

    async def _action(self, service: str, action: str, **arguments) -> dict[str, str]:
        """Invoke one action with instance 0 and return its output arguments."""
        if service not in self.services:
            raise UpnpError(f"{self.friendly_name or self.location} offers no {service} service")
        full_type, url = self.services[service]
        body = "".join(f"<{name}>{escape(str(value))}</{name}>"
                       for name, value in {"InstanceID": 0, **arguments}.items())
        envelope = (f'<?xml version="1.0" encoding="utf-8"?>'
                    f'<s:Envelope xmlns:s="{SOAP_ENV}" s:encodingStyle="{SOAP_ENCODING}"><s:Body>'
                    f'<u:{action} xmlns:u="{full_type}">{body}</u:{action}></s:Body></s:Envelope>')
        headers = {"CONTENT-TYPE": 'text/xml; charset="utf-8"', "SOAPACTION": f'"{full_type}#{action}"'}
        async with self.http.post(url, data=envelope.encode(), headers=headers) as response:
            root = _xml(await response.read(), f"the {action} response")
        body_element = root.find(f"{{{SOAP_ENV}}}Body")
        if body_element is None or len(body_element) == 0:
            raise UpnpError(f"{action}: the response has no SOAP body")
        result = body_element[0]
        if _local(result.tag) == "Fault":
            code = result.findtext(f".//{{{UPNP_CONTROL}}}errorCode")
            text = result.findtext(f".//{{{UPNP_CONTROL}}}errorDescription")
            raise UpnpError(f"{action}: UPnP error {code} {text or ''}".rstrip())
        return {_local(child.tag): child.text or "" for child in result}

    async def load(self, url: str, title: str, content_type: str, features: str = "*", artist: str = "",
                   duration: int = 0):
        await self._action(TRANSPORT, "SetAVTransportURI", CurrentURI=url,
                           CurrentURIMetaData=didl_lite(url, title, artist, content_type, features, duration))

    async def play(self):
        await self._action(TRANSPORT, "Play", Speed="1")

    async def pause(self):
        await self._action(TRANSPORT, "Pause")

    async def stop(self):
        await self._action(TRANSPORT, "Stop")

    async def get_transport_state(self) -> str:
        return (await self._action(TRANSPORT, "GetTransportInfo")).get("CurrentTransportState", "")

    async def get_position(self) -> int:
        return parse_duration((await self._action(TRANSPORT, "GetPositionInfo")).get("RelTime"))

    async def get_uri(self) -> str:
        return (await self._action(TRANSPORT, "GetMediaInfo")).get("CurrentURI", "")

    async def get_volume(self) -> int:
        level = (await self._action(RENDERING, "GetVolume", Channel="Master")).get("CurrentVolume", "")
        return int(level) if level.isdigit() else 0

    async def set_volume(self, level: int):
        await self._action(RENDERING, "SetVolume", Channel="Master", DesiredVolume=level)

    async def get_mute(self) -> bool:
        return (await self._action(RENDERING, "GetMute", Channel="Master")).get("CurrentMute") in ("1", "true")

    async def set_mute(self, muted: bool):
        await self._action(RENDERING, "SetMute", Channel="Master", DesiredMute=int(muted))
