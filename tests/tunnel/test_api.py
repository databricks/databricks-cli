import json
import os
import shutil

from pathlib import Path
from unittest import mock

import pytest
import requests

from databricks_cli.tunnel.api import TunnelApi, generate_key_pair
from databricks_cli.tunnel.server import StreamingServer
from tests.clusters.test_cli import CLUSTER_ID
from tests.tunnel.conftest import ORG_ID


@pytest.fixture(name="tunnel_api")
def tunnel_api_fixture():
    with mock.patch('databricks_cli.tunnel.api.ClusterService') as ClusterServiceMock:
        ClusterServiceMock.return_value = mock.MagicMock()
        with mock.patch('databricks_cli.tunnel.api.CommandExecutionService') as CmdExecServiceMock:
            CmdExecServiceMock.return_value = mock.MagicMock()
            _tunnel_api = TunnelApi(mock.MagicMock())
            _tunnel_api.cluster_client = ClusterServiceMock
            _tunnel_api.command_client = CmdExecServiceMock
            _tunnel_api.cluster_id = CLUSTER_ID
            _tunnel_api.org_id = ORG_ID
            yield _tunnel_api


def test_is_cluster_running(tunnel_api):
    tunnel_api.cluster_client.get_cluster.return_value = {"state": "TERMINATED"}
    with pytest.raises(RuntimeError):
        tunnel_api.is_cluster_running()

    tunnel_api.cluster_client.get_cluster.return_value = {"state": "RUNNING"}
    assert tunnel_api.is_cluster_running()


class MockResponse(requests.Response):
    def __init__(self, status_code, headers=None, content=None):
        super(MockResponse, self).__init__()
        self.status_code = status_code
        if headers:
            self.headers.update(headers)
        if content:
            if isinstance(content, str):
                content = content.encode()
            self._content = content


def test_get_org_id(tunnel_api):
    resp = MockResponse(status_code=200, headers={"x-databricks-org-id": ORG_ID})
    tunnel_api.cluster_client.client.perform_query.return_value = resp
    assert tunnel_api.get_org_id() == ORG_ID


def test_send_public_key_to_driver(tunnel_api):
    _, public_key = generate_key_pair()
    tunnel_api.command_client.get_command_status = mock.Mock(
        side_effect=[{"status": "Running"}, {"status": "Finished"}]
    )
    tunnel_api.send_public_key_to_driver(public_key)
    assert tunnel_api.command_client.execute_command_until_terminated.call_count == 1


def test_setup_local_keys(tunnel_api):
    private_key, public_key = generate_key_pair()
    default_ssh_dir = os.getcwd() + "/.ssh"
    tunnel_api.default_ssh_dir = default_ssh_dir

    def cleanup_sshdir():
        dirpath = Path(default_ssh_dir)
        if dirpath.exists() and dirpath.is_dir():
            shutil.rmtree(dirpath)

    try:
        cleanup_sshdir()
        assert not Path(default_ssh_dir).exists()
        tunnel_api.setup_local_keys(private_key, public_key)
        assert Path(tunnel_api.local_private_key_path).exists()
        assert (Path(tunnel_api.local_private_key_path).stat().st_mode & 0o777) == 0o600
        assert Path(tunnel_api.local_public_key_path).exists()
        assert (Path(tunnel_api.local_public_key_path).stat().st_mode & 0o777) == 0o600
    finally:
        cleanup_sshdir()


@mock.patch('requests.get', mock.Mock(side_effect=[
    MockResponse(status_code=502),
    MockResponse(status_code=200, content=json.dumps({"status": "not working"})),
    MockResponse(status_code=200, content=json.dumps({"status": "working"}))]))
def test_ping_remote_tunnel(tunnel_api):
    # status code is not 200
    with pytest.raises(RuntimeError, match=r".*502.*"):
        tunnel_api.tunnel_config = mock.MagicMock()
        tunnel_api.is_remote_tunnel_alive()

    # status isn't working
    with pytest.raises(RuntimeError, match=r".*not working.*"):
        tunnel_api.is_remote_tunnel_alive()

    # status code == 200 and status is working
    assert tunnel_api.is_remote_tunnel_alive()


def test_start_local_server(tunnel_api, unused_tcp_port):
    with mock.patch.object(StreamingServer, 'listen') as listen_mock:
        with mock.patch("databricks_cli.tunnel.api.IOLoop.current") as il_current:
            tunnel_api.start_local_server(unused_tcp_port)
            assert listen_mock.call_count == 1
            il_current.return_value.start.assert_called()
            listen_mock.assert_called_with(unused_tcp_port)
