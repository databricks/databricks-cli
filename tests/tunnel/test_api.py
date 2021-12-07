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
import json
import os
import shutil
import socket

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
    with mock.patch('databricks_cli.tunnel.api.ClusterApi') as ClusterServiceMock:
        ClusterServiceMock.return_value = mock.MagicMock()
        with mock.patch('databricks_cli.tunnel.api.CommandApi', autospec=True) as CmdApiMock:
            CmdApiMock.return_value = mock.MagicMock()
            with mock.patch('databricks_cli.tunnel.api.ExecutionContextApi', autospec=True) as \
                    ExecCtxApiMock:
                ExecCtxApiMock.return_value = mock.MagicMock()
                _tunnel_api = TunnelApi(mock.MagicMock(), cluster_id=CLUSTER_ID)
                _tunnel_api.cluster_client = ClusterServiceMock
                _tunnel_api.command_client = CmdApiMock
                _tunnel_api.exec_ctx_client = ExecCtxApiMock
                _tunnel_api.cluster_id = CLUSTER_ID
                _tunnel_api.org_id = ORG_ID
                yield _tunnel_api


@pytest.fixture(name="default_ssh_dir")
def mock_ssh_dir():
    return os.getcwd() + "/.ssh"


def cleanup_sshdir(default_ssh_dir):
    dirpath = Path(default_ssh_dir)
    if dirpath.exists() and dirpath.is_dir():
        shutil.rmtree(dirpath)


def test_is_cluster_running(tunnel_api):
    tunnel_api.cluster_client.get_cluster.return_value = {"state": "TERMINATED"}
    with pytest.raises(RuntimeError):
        tunnel_api.check_cluster()

    tunnel_api.cluster_client.get_cluster.return_value = {"state": "RUNNING"}
    tunnel_api.check_cluster()


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
    tunnel_api.cluster_client.client.client.perform_query.return_value = resp
    assert tunnel_api.get_org_id() == ORG_ID


def test_generate_key_pairs():
    private_key, public_key = generate_key_pair()
    from cryptography.hazmat.primitives import serialization
    rsa_private_key = serialization.load_ssh_private_key(private_key, password=None)
    assert rsa_private_key.key_size == 4096
    rsa_public_key = serialization.load_ssh_public_key(public_key)
    assert rsa_public_key.key_size == 4096


class TestSendPublicKeyToDriver:

    def test_script(self, tunnel_api, default_ssh_dir):
        _, public_key = generate_key_pair()
        public_key_path = Path(default_ssh_dir + "/authorized_keys")

        def mock_cmd_until_terminated(**kwargs):
            cmd = kwargs.get("command")
            try:
                cleanup_sshdir(default_ssh_dir)
                assert not Path(default_ssh_dir).exists()
                exec(cmd)  # pylint:disable=exec-used
                assert public_key_path.exists()
                with open(public_key_path, "r") as fp:
                    content = fp.read()
                assert content.strip().encode("utf-8") == public_key
            finally:
                cleanup_sshdir(default_ssh_dir)
            return {"status": "Finished", "results": {"resultType": ""}}

        tunnel_api.default_remote_ssh_dir = default_ssh_dir
        tunnel_api.command_client.execute_command_until_terminated = \
            mock.MagicMock(side_effect=mock_cmd_until_terminated)
        tunnel_api.send_public_key_to_driver(public_key)

    def test_execute_command(self, tunnel_api):
        _, public_key = generate_key_pair()

        err_msg = "some err"
        tunnel_api.command_client.execute_command_until_terminated = mock.MagicMock(
            side_effect=[
                {"status": "Finished", "results": {"resultType": ""}},
                {"status": "Error", "results": {"resultType": "", "data": err_msg}},
                {"status": "Finished", "results": {"resultType": "error", "cause": err_msg}}
            ])
        # success
        tunnel_api.send_public_key_to_driver(public_key)

        # fail: error
        with pytest.raises(RuntimeError, match=".*some err.*"):
            tunnel_api.send_public_key_to_driver(public_key)

        # fail: finished with error
        with pytest.raises(RuntimeError, match=".*some err.*"):
            tunnel_api.send_public_key_to_driver(public_key)


def test_setup_local_keys(tunnel_api, default_ssh_dir):
    private_key, public_key = generate_key_pair()
    tunnel_api.default_local_ssh_dir = default_ssh_dir

    try:
        cleanup_sshdir(default_ssh_dir)
        assert not Path(default_ssh_dir).exists()
        tunnel_api.setup_local_keys(private_key, public_key)
        assert Path(tunnel_api.local_private_key_path).exists()
        assert (Path(tunnel_api.local_private_key_path).stat().st_mode & 0o777) == 0o600
        assert Path(tunnel_api.local_public_key_path).exists()
        assert (Path(tunnel_api.local_public_key_path).stat().st_mode & 0o777) == 0o600
    finally:
        cleanup_sshdir(default_ssh_dir)


@mock.patch('requests.get', mock.Mock(side_effect=[
    MockResponse(status_code=502),
    MockResponse(status_code=200, content=json.dumps({"status": "not working"})),
    MockResponse(status_code=200, content=json.dumps({"status": "working"}))]))
def test_ping_remote_tunnel(tunnel_api):
    # status code is not 200
    with pytest.raises(RuntimeError, match=r".*502.*"):
        tunnel_api.tunnel_config = mock.MagicMock()
        tunnel_api.check_remote_tunnel()

    # status isn't working
    with pytest.raises(RuntimeError, match=r".*not working.*"):
        tunnel_api.check_remote_tunnel()

    # status code == 200 and status is working
    tunnel_api.check_remote_tunnel()


class TestStartLocalServer:
    def test_start_local_server_with_port(self, tunnel_api, unused_tcp_port):
        with mock.patch.object(StreamingServer, 'listen') as listen_mock:
            with mock.patch("databricks_cli.tunnel.api.IOLoop.current") as il_mock:
                tunnel_api.start_local_server(unused_tcp_port)
                assert listen_mock.call_count == 1
                il_mock.return_value.start.assert_called()
                listen_mock.assert_called_with(unused_tcp_port)

    def test_start_local_server_without_port_success(self, tunnel_api):
        with mock.patch.object(StreamingServer, 'add_sockets') as as_mock:
            with mock.patch('tornado.netutil.bind_sockets') as bs_mock:
                bs_mock.return_value = [socket.socket()]
                with mock.patch("databricks_cli.tunnel.api.IOLoop.current") as il_mock:
                    tunnel_api.start_local_server()
                    bs_mock.assert_called_with(0, '')
                    as_mock.assert_called()
                    il_mock.return_value.start.assert_called()

    def test_start_local_server_without_port_fail(self, tunnel_api):
        with mock.patch.object(StreamingServer, 'add_sockets') as as_mock:
            with mock.patch('tornado.netutil.bind_sockets') as bs_mock:
                bs_mock.return_value = []
                with mock.patch("databricks_cli.tunnel.api.IOLoop.current") as il_mock:
                    with pytest.raises(RuntimeError):
                        tunnel_api.start_local_server()
                    bs_mock.assert_called_with(0, '')
                    as_mock.assert_not_called()
                    il_mock.return_value.start.assert_not_called()
