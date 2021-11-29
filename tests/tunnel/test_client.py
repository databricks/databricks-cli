import aiohttp
import mock
import pytest
import socketio

from databricks_cli.tunnel.server import TunnelClient
from tests.tunnel.conftest import SOCKET_PATH


@pytest.fixture(name="tunnel_client")
async def tunnel_client_fixture(tunnel_config):
    with mock.patch('tornado.iostream.IOStream') as io_stream:
        session = aiohttp.ClientSession()
        tunnel_client = TunnelClient(io_stream, session, tunnel_config)
        yield tunnel_client
        await tunnel_client.disconnect()
        await session.close()


@pytest.fixture
def tunnel_client_mock():
    with mock.patch('databricks_cli.tunnel.server.TunnelClient') as TunnelClientMock:
        _tunnel_client_mock = mock.MagicMock()
        TunnelClientMock.return_value = _tunnel_client_mock
        yield _tunnel_client_mock


class AsyncMock(mock.MagicMock):
    # pylint: disable=useless-super-delegation, invalid-overridden-method
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)


class TestTunnelClient:
    @pytest.mark.asyncio
    async def test_event_handlers(self, tunnel_client):
        event_handlers = tunnel_client.io.handlers
        assert "connect" in event_handlers["/"]
        assert "response" in event_handlers["/"]

    @pytest.mark.asyncio
    async def test_connect_success(self, endpoint_url, tunnel_client):
        # pylint: disable=unused-argument
        def mock_successful_conn(*args, **kwargs):
            tunnel_client.io.connected = True
            # mock successful connect from server
            connect_handler = tunnel_client.io.handlers["/"]["connect"]
            connect_handler()

        tunnel_client.io.connect = AsyncMock(spec=tunnel_client.io.connect,
                                             side_effect=mock_successful_conn)
        await tunnel_client.connect()

        assert tunnel_client.io.connect.call_count == 1
        tunnel_client.io.connect.assert_called_with(endpoint_url,
                                                    auth=None,
                                                    headers={'Content-Type': 'application/json'},
                                                    socketio_path=SOCKET_PATH,
                                                    transports=['websocket'])
        assert tunnel_client.connection_established

    @pytest.mark.asyncio
    async def test_connect_fail(self, endpoint_url, tunnel_client):
        # pylint: disable=unused-argument
        def mock_successful_conn(*args, **kwargs):
            raise socketio.exceptions.ConnectionError("throw an error")

        with pytest.raises(socketio.exceptions.ConnectionError):
            tunnel_client.io.connect = AsyncMock(spec=tunnel_client.io.connect,
                                                 side_effect=mock_successful_conn)
            await tunnel_client.connect()

        assert not tunnel_client.connection_established
