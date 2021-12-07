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
import os
from pathlib import Path

import click
import requests
import tornado
from cryptography.hazmat.primitives.asymmetric import rsa
from tornado.ioloop import IOLoop

from databricks_cli.clusters.api import ClusterApi
from databricks_cli.commands.api import CommandApi, ExecutionContextApi
from databricks_cli.tunnel.server import TunnelConfig, StreamingServer


def generate_key_pair():
    """
    Generate the private and public key pairs and serialize them into bytes using OpenSSH formats.

    :return: (private_key, public_key) tuples.
    """
    # TODO(tunneling-cli): support customer-supplied key / passphrase encryption
    from cryptography.hazmat.primitives import serialization

    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=4096
    )

    private_key = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.OpenSSH,
        encryption_algorithm=serialization.NoEncryption()
    )

    public_key = key.public_key().public_bytes(
        encoding=serialization.Encoding.OpenSSH,
        format=serialization.PublicFormat.OpenSSH
    )

    return private_key, public_key


class TunnelApi(object):
    def __init__(self, api_client, cluster_id, debug=False):
        self.cluster_client = ClusterApi(api_client)
        self.command_client = CommandApi(api_client)
        self.exec_ctx_client = ExecutionContextApi(api_client)
        self.cluster_id = cluster_id
        self._api_client = api_client
        self._default_local_ssh_dir = "~/.ssh"
        self._default_remote_ssh_dir = "~/.ssh"
        self.context_id = None
        self.org_id = None
        self.tunnel_config = None
        self.debug = debug

    def check_cluster(self):
        # check if cluster exist and running
        cluster_info = self.cluster_client.get_cluster(self.cluster_id)
        if cluster_info["state"] not in ["RUNNING", "RESIZING"]:
            raise RuntimeError(f'Cluster with ID: {self.cluster_id} is not RUNNING.')

    def get_org_id(self):
        resp = self.cluster_client.client.client.perform_query("GET", "/clusters/get",
                                                               data={"cluster_id": self.cluster_id},
                                                               return_raw_response=True)
        # TODO(tunneling-cli): this assumes a multitenant workspace, we will need to confirm if
        #  this exists for st workspace
        return resp.headers["x-databricks-org-id"]

    def create_context(self):
        response = self.exec_ctx_client.create_context(language="python",
                                                       cluster_id=self.cluster_id)
        return response["id"]

    def destroy_context(self):
        if self.context_id and self.cluster_id:
            self.exec_ctx_client.destroy_context(cluster_id=self.cluster_id,
                                                 context_id=self.context_id)

    def send_public_key_to_driver(self, public_key):
        click.echo("Sending public key to the driver...")
        public_key_str = public_key.decode("utf-8")
        install_keys_on_driver_cmd = f"""
from pathlib import Path

Path("{self.default_remote_ssh_dir}").expanduser().mkdir(exist_ok=True)
content = "{public_key_str}"
with Path("{self.default_remote_ssh_dir}/authorized_keys").expanduser().open("a+") as fp:
    for line in fp:
        if content in line:
            break
    else:
        fp.write(content)
        fp.write("\\n")""".strip()
        resp = self.command_client. \
            execute_command_until_terminated(language="python",
                                             cluster_id=self.cluster_id,
                                             context_id=self.context_id,
                                             command=install_keys_on_driver_cmd)
        if resp["status"] != "Finished":
            err_data = resp.get("results", {}).get("data", "")
            raise RuntimeError(f"Sending public key to driver was not successful: {err_data}")
        if resp["results"]["resultType"] == "error":
            err_data = resp.get("results", {}).get("cause", "")
            raise RuntimeError(f"Sending public key to driver was not successful: {err_data}")

    @property
    def default_remote_ssh_dir(self):
        return self._default_remote_ssh_dir

    @default_remote_ssh_dir.setter
    def default_remote_ssh_dir(self, value):
        self._default_remote_ssh_dir = value

    @property
    def default_local_ssh_dir(self):
        return self._default_local_ssh_dir

    @default_local_ssh_dir.setter
    def default_local_ssh_dir(self, value):
        self._default_local_ssh_dir = value

    @property
    def local_private_key_path(self):
        return f"{self.default_local_ssh_dir}/{self.cluster_id}"

    @property
    def local_public_key_path(self):
        return f"{self.local_private_key_path}.pub"

    def setup_ssh_keys(self):
        click.echo("Setting up SSH keys...")

        private_key, public_key = generate_key_pair()
        self.send_public_key_to_driver(public_key)
        self.setup_local_keys(private_key, public_key)

    def setup_local_keys(self, private_key, public_key):
        private_key_path = Path(self.local_private_key_path).expanduser()
        private_key_path.parent.mkdir(parents=True, exist_ok=True)

        if private_key_path.exists():
            private_key_path.unlink()
        private_key_path.write_bytes(private_key)
        os.chmod(private_key_path, 0o600)

        public_key_path = Path(self.local_public_key_path).expanduser()
        if public_key_path.exists():
            public_key_path.unlink()
        public_key_path.write_bytes(public_key)
        os.chmod(public_key_path, 0o600)

    def check_remote_tunnel(self):
        click.echo("Ping the remote tunnel server...")
        resp = requests.get(f"{self.tunnel_config.healthcheck_url}",
                            headers=self._api_client.default_headers)
        if resp.status_code != 200:
            raise RuntimeError(f"Unable to connect to the remote tunnel server: {resp.status_code}")
        status = resp.json()["status"]
        if status != "working":
            raise RuntimeError(f"Unexpected remote tunnel server status: {status}")

    def cleanup(self):
        self.destroy_context()

    def start_tunneling(self, local_port=None):
        try:
            # check prerequisites:
            # - running cluster
            # - tunnel server is alive
            self.check_cluster()
            self.org_id = self.get_org_id()
            self.tunnel_config = TunnelConfig(host=self._api_client.host,
                                              token=self._api_client.token,
                                              org_id=self.org_id,
                                              cluster_id=self.cluster_id)
            self.check_remote_tunnel()
            # setup prerequisites:
            # - exec context
            # - setup ssh keys
            if self.context_id is not None:
                self.destroy_context()
            self.context_id = self.create_context()
            self.setup_ssh_keys()

            self.start_local_server(local_port=local_port)
        finally:
            self.cleanup()

    def start_local_server(self, local_port=None):
        server = StreamingServer(self.tunnel_config, debug=self.debug)
        if local_port:
            server.listen(local_port)
        else:
            # find a random free port
            sockets = tornado.netutil.bind_sockets(0, '')
            if sockets:
                server.add_sockets(sockets)
                for s in sockets:
                    local_port = s.getsockname()[1]
                    if local_port is not None:
                        break
            if local_port is None:
                raise RuntimeError("Could not find a free port")
        click.echo(f"Starting local tunneling on localhost:{local_port}")
        click.echo("Please use the following command for SSH connection:")
        click.echo(f"ssh root@localhost -p {local_port} -i {self.local_private_key_path}")
        IOLoop.current().start()
