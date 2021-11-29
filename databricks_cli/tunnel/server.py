import logging
import asyncio

from urllib.parse import urljoin

import aiohttp
import socketio

from aiohttp import BasicAuth
from socketio.exceptions import BadNamespaceError
from tornado.iostream import IOStream, StreamClosedError
from tornado.tcpserver import TCPServer


DBX_TUNNEL_SERVER_PORT = 6006


logger = logging.getLogger(__name__)


class TunnelConfig:
    def __init__(self, databricks_config, org_id, cluster_id):
        self.host = databricks_config.host
        self.token = databricks_config.token
        self.org_id = org_id
        self.cluster_id = cluster_id
        self.remote_port = DBX_TUNNEL_SERVER_PORT
        # TODO(tunneling-cli): this assumes a multitenant workspace, we will need to confirm if
        #  this is still the case for st workspace
        self.endpoint_path = \
            f"driver-proxy-api/o/{self.org_id}/{self.cluster_id}/{self.remote_port}/"

    @property
    def endpoint_url(self):
        return urljoin(self.host, self.endpoint_path)

    @property
    def healthcheck_url(self):
        return f"{self.endpoint_url}health"

    @property
    def socket_path(self):
        return f"{self.endpoint_path}socket.io"


class TunnelClient:
    def __init__(self, stream: IOStream, session: aiohttp.ClientSession,
                 tunnel_config: TunnelConfig, debug=False):
        self.stream = stream
        self.tunnel_config = tunnel_config
        self.io = socketio.AsyncClient(http_session=session, logger=debug, engineio_logger=debug)
        self.connection_established = False

        @self.io.event
        def connect():  # pylint: disable=unused-variable
            self.connection_established = True
            logger.info("Connection to socket.io server established with id %s", self.io.sid)

        @self.io.on("response")
        def response(msg):  # pylint: disable=unused-variable
            data = msg.get("data")
            # receive responses from tunnel server then write to tcp's stream
            if isinstance(data, bytes):
                self.stream.write(data)
            else:
                logger.warning("Received string message from byte-based stream: %s", data)

    async def connect(self):
        while not self.io.connected:
            logger.info("initializing socket.io connection")
            try:
                headers = {
                    'Content-Type': 'application/json'
                }
                await self.io.connect(self.tunnel_config.endpoint_url,
                                      auth=None,
                                      transports=["websocket"],
                                      socketio_path=self.tunnel_config.socket_path,
                                      headers=headers)
            except socketio.exceptions.ConnectionError as e:
                logger.error("Connection exception:")
                logger.error(e)
                raise e

        while not self.connection_established:
            logger.info("Connection to the socket.io server is not yet established")
            await asyncio.sleep(1)

    def is_connected(self):
        return self.io.connected

    async def send_data(self, data):
        await self.io.emit("data_received", {"data": data})

    async def disconnect(self):
        await self.io.disconnect()


class StreamingServer(TCPServer):
    def __init__(self, tunnel_config: TunnelConfig, debug=False):
        super(StreamingServer, self).__init__()
        self.tunnel_config = tunnel_config
        self.socket_id = None
        self.client = None
        self.debug = debug
        if self.debug:
            logger.setLevel(logging.DEBUG)
            logging.getLogger('tornado.application').setLevel(logging.DEBUG)
            logging.getLogger('tornado.general').setLevel(logging.DEBUG)

    # pylint: disable=invalid-overridden-method
    async def handle_stream(self, stream: IOStream, address: tuple):
        logger.info("Handle stream from: %s", address)
        session = aiohttp.ClientSession(auth=BasicAuth("token", self.tunnel_config.token))
        self.socket_id = address[1]
        self.client = TunnelClient(stream, session, self.tunnel_config, debug=self.debug)
        await self.main_routine(stream)

    async def main_routine(self, stream):
        logger.info("Launching main routine for socket with id: %s", self.socket_id)
        await self.client.connect()
        while True:
            try:
                # read input stream from the client and emit this to socketio server
                data = await stream.read_bytes(1024 * 50, partial=True)
                await asyncio.sleep(0.05)
                await self.client.send_data(data)
            except StreamClosedError:
                logger.info("Stream is gracefully closed")
                await self.client.disconnect()
                break
            except KeyboardInterrupt:
                await self.client.disconnect()
                logger.info("Stopping server gracefully")
                break
            except BadNamespaceError:
                if not self.client.is_connected():
                    await self.client.connect()
            except socketio.exceptions.ConnectionError as e:
                logger.error("Connection error:")
                logger.error(e)
                break
