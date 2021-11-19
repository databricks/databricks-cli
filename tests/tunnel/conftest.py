import mock
import pytest

from databricks_cli.tunnel.server import TunnelConfig


SOCKET_PATH = "/socket.io"
ORG_ID = "123456"


@pytest.fixture(name="host")
def host_fixture(unused_tcp_port):
    return f"http://localhost:{unused_tcp_port}"


@pytest.fixture(name="endpoint_url")
def endpoint_url_fixture(host):
    return f"{host}/"


@pytest.fixture(name='tunnel_config')
def tunnel_config_fixture(host):
    with mock.patch('databricks_cli.sdk.ApiClient') as api_client:
        api_client.config.host = host
        api_client.config.org_id = ORG_ID
        tg = TunnelConfig(api_client.config, "123456", "abcd")
        tg.endpoint_path = "/"
        yield tg


def test_tunnel_config(tunnel_config, endpoint_url):
    assert tunnel_config.endpoint_url == endpoint_url
    assert tunnel_config.socket_path == SOCKET_PATH
