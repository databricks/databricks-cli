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
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logging.getLogger('tornado.application').setLevel(logging.DEBUG)
logging.getLogger('tornado.general').setLevel(logging.DEBUG)


# TODO(ML-17779): Refactor for testing and modularity. Refactor loggings.
class TunnelConfig:
    def __init__(self, databricks_config, org_id, cluster_id):
        self.host = databricks_config.host
        self.token = databricks_config.token
        self.org_id = org_id
        self.cluster_id = cluster_id
        self.remote_port = DBX_TUNNEL_SERVER_PORT
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
                 tunnel_config: TunnelConfig):
        logging.getLogger('socketio').setLevel(logging.DEBUG)
        logging.getLogger('engineio').setLevel(logging.DEBUG)
        self.stream = stream
        self.tunnel_config = tunnel_config
        self.io = socketio.AsyncClient(http_session=session, logger=True, engineio_logger=True)
        self.connection_established = False

        @self.io.event
        def connect():  # pylint: disable=unused-variable
            self.connection_established = True
            logging.info("Connection to socket.io server established with id %s", self.io.sid)

        @self.io.on("response")
        def response(msg):  # pylint: disable=unused-variable
            data = msg.get("data")
            # receive responses from tunnel server then write to tcp's stream
            if isinstance(data, bytes):
                self.stream.write(data)
            else:
                logging.warning("Received string message from byte-based stream: %s", data)

    async def connect(self):
        while not self.io.connected:
            logging.info("initializing socket.io connection")
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
                print(e)
                logging.error("Connection exception:")
                logging.error(e)
                raise e

        while not self.connection_established:
            logging.info("Connection to the socket.io server is not yet established")
            await asyncio.sleep(1)

    def is_connected(self):
        return self.io.connected

    async def send_data(self, data):
        await self.io.emit("data_received", {"data": data})

    async def disconnect(self):
        await self.io.disconnect()


class StreamingServer(TCPServer):
    def __init__(self, tunnel_config: TunnelConfig):
        super(StreamingServer, self).__init__()
        self.tunnel_config = tunnel_config
        self.socket_id = None
        self.client = None

    # pylint: disable=invalid-overridden-method
    async def handle_stream(self, stream: IOStream, address: tuple):
        logger.info("handle stream: %s", address)
        session = aiohttp.ClientSession(auth=BasicAuth("token", self.tunnel_config.token))
        self.socket_id = address[1]
        self.client = TunnelClient(stream, session, self.tunnel_config)
        await self.main_routine(stream)

    async def main_routine(self, stream):
        logging.info("Launching main routine for socket with id %s", self.socket_id)
        await self.client.connect()
        while True:
            try:
                # read input stream from the client and emit this to socketio server
                data = await stream.read_bytes(1024 * 50, partial=True)
                await asyncio.sleep(0.05)
                await self.client.send_data(data)
            except StreamClosedError:
                logger.info("Stream gracefully closed")
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

# TCP Server
# receive input data from users: response handling --> emit events to the tunneling server from
# the input streaming
# Tunneling client sends data to server
# receive responses from tunneling server --> write to output stream

# Algorithm
# Setup regular tcp server
# Setup socketio session
# Setup stream handler:
# indefinitely handle stream until cancelled / terminated
# has a socketio client that writes to the socketio server
