# Databricks CLI
# Copyright 2021 Databricks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"), except
# that the use of services to which certain application programming
# interfaces (each, an "API") connect requires that the user first obtain
# a license for the use of the APIs from Databricks, Inc. ("Databricks"),
# by creating an account at www.databricks.com and agreeing to either (a)
# the Community Edition Terms of Service, (b) the Databricks Terms of
# Service, or (c) another written agreement between Licensee and Databricks
# for the use of the APIs.
#
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import mock
import pytest
import socketio

from tests.tunnel.conftest import SOCKET_PATH


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
        assert tunnel_client.is_connected()
        assert tunnel_client.connection_established

    @pytest.mark.asyncio
    async def test_connect_fail(self, tunnel_client):
        with pytest.raises(socketio.exceptions.ConnectionError):
            await tunnel_client.connect()

        assert not tunnel_client.is_connected()
        assert not tunnel_client.connection_established

    async def test_send_data(self, tunnel_client):
        tunnel_client.io.emit = AsyncMock(spec=tunnel_client.io.emit)
        data = "data"
        await tunnel_client.send_data(data)
        tunnel_client.io.emit.assert_called_with('data_received', {'data': data})

    async def test_disconnect(self, tunnel_client):
        tunnel_client.io.disconnect = AsyncMock(spec=tunnel_client.io.disconnect)
        await tunnel_client.disconnect()
        tunnel_client.io.disconnect.assert_called()
