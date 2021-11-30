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
import aiohttp
import mock
import pytest

from databricks_cli.tunnel.server import TunnelConfig, TunnelClient
from tests.clusters.test_cli import CLUSTER_ID

SOCKET_PATH = "/socket.io"
ORG_ID = "123456"


@pytest.fixture(name="host")
def host_fixture(unused_tcp_port):
    return f"http://0.0.0.0:{unused_tcp_port}"


@pytest.fixture(name="endpoint_url")
def endpoint_url_fixture(host):
    return f"{host}/"


@pytest.fixture(name='tunnel_config')
def tunnel_config_fixture(host):
    with mock.patch('databricks_cli.sdk.ApiClient') as api_client:
        tg = TunnelConfig(host=host, token=api_client.token, org_id=ORG_ID, cluster_id=CLUSTER_ID)
        tg.endpoint_path = "/"
        yield tg


def test_tunnel_config(tunnel_config, endpoint_url):
    assert tunnel_config.endpoint_url == endpoint_url
    assert tunnel_config.socket_path == SOCKET_PATH


@pytest.fixture(name="tunnel_client")
async def tunnel_client_fixture(tunnel_config):
    with mock.patch('tornado.iostream.IOStream') as io_stream:
        session = aiohttp.ClientSession()
        tunnel_client = TunnelClient(io_stream, session, tunnel_config)
        yield tunnel_client
        await tunnel_client.disconnect()
        await session.close()
