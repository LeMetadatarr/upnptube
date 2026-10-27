"""
The DIAL server of one cast target. It describes the device for SSDP, and its REST service
has one application, "YouTube", which phones launch with a pairing code.
"""
import logging
from xml.sax.saxutils import escape, quoteattr

from aiohttp import web

from .app import YouTubeApp
from .ssdp import RootDevice

LOG = logging.getLogger(__name__)

APP_NAME = "YouTube"
DEVICE_TYPE = "urn:dial-multiscreen-org:device:dial:1"
SERVICE_TYPE = "urn:dial-multiscreen-org:service:dial:1"
DESCRIPTION_PATH = "/dial/device.xml"
APPS_PATH = "/dial/apps"
# The DIAL specification lets a server refuse a launch body larger than this.
MAX_BODY = 4096


class DialServer:
    def __init__(self, app: YouTubeApp, port: int, friendly_name: str, manufacturer: str, model_name: str,
                 device_uuid: str):
        self.app = app
        self.port = port
        self.friendly_name = friendly_name
        self.manufacturer = manufacturer
        self.model_name = model_name
        self.device = RootDevice(device_uuid, DEVICE_TYPE, port, DESCRIPTION_PATH, [SERVICE_TYPE])
        self._runner: web.AppRunner | None = None

    async def start(self):
        routes = web.Application(client_max_size=MAX_BODY)
        routes.router.add_get(DESCRIPTION_PATH, self._describe_device)
        routes.router.add_get(f"{APPS_PATH}/{{name}}", self._describe_app)
        routes.router.add_post(f"{APPS_PATH}/{{name}}", self._launch)
        routes.router.add_delete(f"{APPS_PATH}/{{name}}/{{instance}}", self._stop)
        self._runner = web.AppRunner(routes)
        await self._runner.setup()
        await web.TCPSite(self._runner, port=self.port).start()

    async def stop(self):
        if self._runner:
            await self._runner.cleanup()

    def _apps_url(self, request: web.Request) -> str:
        # The address the request came in on is the one the phone can reach.
        host = request.transport.get_extra_info("sockname")[0] if request.transport else request.host
        return f"http://{host}:{self.port}{APPS_PATH}"

    def _app(self, request: web.Request):
        if request.match_info["name"] != APP_NAME:
            raise web.HTTPNotFound()

    async def _describe_device(self, request: web.Request) -> web.Response:
        xml = ('<?xml version="1.0"?>'
               '<root xmlns="urn:schemas-upnp-org:device-1-0">'
               "<specVersion><major>1</major><minor>0</minor></specVersion>"
               f"<device><deviceType>{DEVICE_TYPE}</deviceType>"
               f"<friendlyName>{escape(self.friendly_name)}</friendlyName>"
               f"<manufacturer>{escape(self.manufacturer)}</manufacturer>"
               f"<modelName>{escape(self.model_name)}</modelName>"
               f"<UDN>uuid:{self.device.uuid}</UDN></device></root>")
        return web.Response(text=xml, content_type="text/xml",
                            headers={"Application-URL": self._apps_url(request)})

    async def _describe_app(self, request: web.Request) -> web.Response:
        self._app(request)
        link = f"<link rel=\"run\" href={quoteattr(self.app.pid)}/>" if self.app.state == "running" else ""
        xml = ('<?xml version="1.0" encoding="UTF-8"?>'
               '<service xmlns="urn:dial-multiscreen-org:schemas:dial" dialVer="2.2">'
               f"<name>{APP_NAME}</name>"
               f'<options allowStop="{str(self.app.allow_stop).lower()}"/>'
               f"<state>{self.app.state}</state>{link}</service>")
        return web.Response(text=xml, content_type="text/xml")

    async def _launch(self, request: web.Request) -> web.Response:
        self._app(request)
        body = await request.text()
        LOG.debug("DIAL launch from %s: %s", request.remote, body)
        try:
            instance = await self.app.launch(body)
        except Exception:
            LOG.exception("Cannot launch %s", APP_NAME)
            return web.Response(status=503)
        location = f"{self._apps_url(request)}/{APP_NAME}/{instance}"
        return web.Response(status=201, headers={"LOCATION": location})

    async def _stop(self, request: web.Request) -> web.Response:
        self._app(request)
        # The screen stays bound to YouTube while the service runs (allowStop is false), so it refuses a stop.
        return web.Response(status=405)
