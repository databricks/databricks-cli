import asyncio
import aiohttp
import logging
import socketio

from urllib.parse import urljoin

from aiohttp import BasicAuth
from socketio.exceptions import BadNamespaceError
from tornado.iostream import IOStream, StreamClosedError
from tornado.tcpserver import TCPServer


DBX_TUNNEL_SERVER_PORT = 6006


# TODO(ML-17779): Refactor for testing and modularity. Refactor loggings.
class TunnelConfig:
    def __init__(self, databricks_config, org_id, cluster_id, remote_port=DBX_TUNNEL_SERVER_PORT):
        self._config = databricks_config
        self.org_id = org_id
        self.cluster_id = cluster_id
        self.remote_port = remote_port

    @property
    def host(self):
        return self._config.host

    @property
    def token(self):
        return self._config.token

    @property
    def endpoint_url(self):
        return urljoin(self._config.host, self.endpoint_path)

    @property
    def endpoint_path(self):
        return f"driver-proxy-api/o/{self.org_id}/{self.cluster_id}/{self.remote_port}/"

    @property
    def healthcheck_url(self):
        return f"{self.endpoint_url}health"

    @property
    def socket_path(self):
        return f"{self.endpoint_path}socket.io"


class AdvancedClient:
    def __init__(self, stream: IOStream, session: aiohttp.ClientSession, tunnel_config: TunnelConfig):
        logging.getLogger('socketio').setLevel(logging.DEBUG)
        logging.getLogger('engineio').setLevel(logging.DEBUG)
        self.stream = stream
        self.tunnel_config = tunnel_config
        self.io = socketio.AsyncClient(http_session=session, logger=True, engineio_logger=True)
        self.connection_established = False

        @self.io.event
        def connect():
            print('THIS CONNECT')
            self.connection_established = True
            logging.info(f"Connection to socket.io server established with id {self.io.sid}")

        @self.io.on("response")
        def response(msg):
            print('RESPONSE')
            data = msg.get("data")
            if isinstance(data, bytes):
                self.stream.write(data)
            else:
                logging.warning(f"Received string message from byte-based stream: {data}")

    async def connect(self):
        while not self.io.connected:
            logging.info("initializing socket.io connection")
            try:
                print('BEFORE CONNECT')
                headers = {
                  'Content-Type': 'application/json'
                }
                await self.io.connect(self.tunnel_config.endpoint_url,
                                      auth=None,
                                      transports=["websocket"],
                                      socketio_path=self.tunnel_config.socket_path,
                                      headers=headers)

                print('AFTER CONNECT')
            except socketio.exceptions.ConnectionError as e:
                print('THE EXCEPTION')
                print(e)
                logging.error("Connection exception:")
                logging.error(e)
                raise e

        while not self.connection_established:
            logging.info("Connection to the socket.io server is not yet established")
            await asyncio.sleep(1)


class StreamHandler:
    def __init__(self, stream: IOStream, address: tuple, session: aiohttp.ClientSession, tunnel_config: TunnelConfig):
        self.client = AdvancedClient(stream, session, tunnel_config)
        self.stream = stream
        self.socket_id = address[1]

    async def main_routine(self):
        logging.info(f"Launching main routine for socket with id {self.socket_id}")
        await self.client.connect()
        while True:
            try:
                data = await self.stream.read_bytes(1024 * 50, partial=True)
                await asyncio.sleep(0.05)
                await self.client.io.emit("data_received", {"data": data})
            except StreamClosedError:
                logging.info("Stream gracefully closed")
                await self.client.io.disconnect()
                break
            except KeyboardInterrupt:
                await self.client.io.disconnect()
                logging.info("Stopping server gracefully")
                break
            except BadNamespaceError:
                if not self.client.io.connected:
                    await self.client.connect()
            except socketio.exceptions.ConnectionError as e:
                logging.error("Connection error:")
                logging.error(e)
                break


class LocalServer(TCPServer):
    def __init__(self, tunnel_config: TunnelConfig):
        super(LocalServer, self).__init__()
        self.tunnel_config = tunnel_config
        self.session = aiohttp.ClientSession(auth=BasicAuth("token", self.tunnel_config.token))

    async def handle_stream(self, stream: IOStream, address: tuple):
        handler = StreamHandler(stream, address, self.session, self.tunnel_config)
        await handler.main_routine()
