import aiohttp
import mock
import pytest

from databricks_cli.tunnel.server import TunnelClient
from tests.tunnel.conftest import SOCKET_PATH


@pytest.fixture(name="tunnel_client")
def tunnel_client_fixture(tunnel_config):
    with mock.patch('tornado.iostream.IOStream') as io_stream:
        yield TunnelClient(io_stream, aiohttp.ClientSession(), tunnel_config)


@pytest.fixture
def tunnel_client_mock():
    with mock.patch('databricks_cli.tunnel.server.TunnelClient') as TunnelClientMock:
        _tunnel_client_mock = mock.MagicMock()
        TunnelClientMock.return_value = _tunnel_client_mock
        yield _tunnel_client_mock


# connect functionality
# succeeded:
# when io.connected is False,
# assert io.connect is called: ignore results
# io.connected should be modified to True

# trigger an event
# assert connection_established is True
# failure
# pending? ask marco


class AsyncMock(mock.MagicMock):
    # pylint: disable=useless-super-delegation, invalid-overridden-method
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)


@pytest.mark.asyncio
async def test_connect_success(endpoint_url, tunnel_client):
    # pylint: disable=unused-argument
    def set_io_conn(*args, **kwargs):
        tunnel_client.io.connected = True
        # TODO(ML-17999): mock connect event
        tunnel_client.connection_established = True

    tunnel_client.io.connect = AsyncMock(spec=tunnel_client.io.connect, side_effect=set_io_conn)
    await tunnel_client.connect()

    assert tunnel_client.io.connect.call_count == 1
    tunnel_client.io.connect.assert_called_with(endpoint_url,
                                                auth=None,
                                                headers={'Content-Type': 'application/json'},
                                                socketio_path=SOCKET_PATH,
                                                transports=['websocket'])
