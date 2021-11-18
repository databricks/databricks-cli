import os
from pathlib import Path
from typing import Tuple

import requests
from cryptography.hazmat.primitives.asymmetric import rsa
from tornado.ioloop import IOLoop

from databricks_cli.configure.config import get_api_config
from databricks_cli.sdk import ClusterService, CommandApiService
from databricks_cli.tunnel.server import TunnelConfig, LocalServer


def generate_key_pair() -> Tuple[bytes, bytes]:
    from cryptography.hazmat.primitives import serialization

    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048
    )

    private_key = key.private_bytes(
        serialization.Encoding.PEM,  # noqa
        serialization.PrivateFormat.TraditionalOpenSSL,  # noqa
        serialization.NoEncryption()
    )

    public_key = key.public_key().public_bytes(
        serialization.Encoding.OpenSSH,  # noqa
        serialization.PublicFormat.OpenSSH  # noqa
    )

    return private_key, public_key


class TunnelApi(object):
    def __init__(self, api_client):
        self.cluster_client = ClusterService(api_client)
        self.command_client = CommandApiService(api_client)
        self._config = get_api_config()
        self.default_language = "python"
        self._init_params()

    def _init_params(self):
        self.cluster_id = None
        self.context_id = None
        self.org_id = None
        self.tunnel_config = None

    def _is_cluster_running(self):
        # check if cluster exist and running
        cluster_info = self.cluster_client.get_cluster(self.cluster_id)
        if cluster_info["state"] not in ["RUNNING", "RESIZING"]:
            raise RuntimeError('Cluster is not RUNNING.')

    def _get_org_id(self):
        resp = self.cluster_client.client.perform_query("GET", "/clusters/get",
                                                        data={"cluster_id": self.cluster_id},
                                                        return_raw_response=True)
        # TODO(ML-17779): this assumes a multitenant workspace, will need to confirm if this is true for st workspace
        return resp.headers["x-databricks-org-id"]

    def execute_request(self):
        pass

    def _create_context(self):
        response = self.command_client.create_context(language=self.default_language,
                                                      cluster_id=self.cluster_id)
        print(f"create context response: {response}")
        return response["id"]

    def _destroy_context(self):
        response = self.command_client.destroy_context(cluster_id=self.cluster_id,
                                                       context_id=self.context_id)
        print(f"destroy context response: {response}")

    def _execute_cmd(self, cmd):
        self.command_client.execute_command(language=self.default_language,
                                            cluster_id=self.cluster_id,
                                            context_id=self.context_id,
                                            command=cmd)

    def _send_public_key_to_driver(self, public_key):
        print("send public key to the driver...")
        public_key_str = public_key.decode('utf-8')
        install_keys_on_driver_cmd = f"""
from pathlib import Path

Path("~/.ssh").expanduser().mkdir(exist_ok=True)
Path("~/.ssh/id_rsa.pub").expanduser().write_text("{public_key_str}")
Path("~/.ssh/authorized_keys").expanduser().write_text("{public_key_str}")
"""
        return self.command_client.execute_command_until_terminated(language=self.default_language,
                                                                    cluster_id=self.cluster_id,
                                                                    context_id=self.context_id,
                                                                    command=install_keys_on_driver_cmd)

    @property
    def local_private_key_path(self):
        return f"~/.ssh/{self.cluster_id}"

    @property
    def local_public_key_path(self):
        return f"{self.local_private_key_path}.pub"

    def _setup_local_keys(self, private_key, public_key):
        private_key_path = Path(f"{self.local_private_key_path}").expanduser()
        if private_key_path.exists():
            private_key_path.unlink()
        private_key_path.write_bytes(private_key)
        os.chmod(private_key_path, 0o600)

        public_key_path = Path(f"{self.local_public_key_path}").expanduser()
        if public_key_path.exists():
            public_key_path.unlink()
        public_key_path.write_bytes(public_key)
        os.chmod(public_key_path, 0o600)

    def remote_healthcheck(self):
        print("Perform remote healthcheck...")
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self._config.token}'
        }
        resp = requests.get(f"{self.tunnel_config.healthcheck_url}", headers=headers)
        assert resp.status_code == 200, f"Unable to connect to the remote tunnel server: {resp.status_code}"
        status = resp.json()["status"]
        assert status == "working", f"Unexpected remote tunnel server status: {status}"

    def start_tunneling(self, cluster_id, local_port):
        self.cluster_id = cluster_id

        # 1) check if cluster exist and running
        self._is_cluster_running()
        self.org_id = self._get_org_id()

        # 2) generate context id
        if self.context_id is not None:
            self._destroy_context()

        self.context_id = self._create_context()

        if self.context_id is None:
            raise RuntimeError("Unable to setup the tunnel: empty context")

        # 3) setup keys
        self._setup_ssh_keys()

        # 4) create tunnel config
        self.tunnel_config = TunnelConfig(self._config, self.org_id, self.cluster_id)

        # 5) health check
        self.remote_healthcheck()

        # 6) Run the local tunneling server
        print(f"Starting local SSH server on port localhost:{local_port}")
        print("Please use the following command for ssh connection:")
        print(f"ssh root@localhost -p {local_port} -i {self.local_private_key_path}")

        server = LocalServer(self.tunnel_config)
        server.listen(local_port)
        IOLoop.current().start()

        # cleanup
        self._destroy_context()
        self._init_params()

    def _setup_ssh_keys(self):
        print("Setting up SSH keys...")

        # 3.1) generate key pairs
        private_key, public_key = generate_key_pair()
        # 3.2) share ssh key to the cluster
        resp = self._send_public_key_to_driver(public_key)
        # only proceed if resp is finished
        assert resp["status"] == "Finished", f"Sending public key to driver is not successful: {resp.text}"
        # 3.3) set ssh key on local machine
        self._setup_local_keys(private_key, public_key)

