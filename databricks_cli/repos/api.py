# Databricks CLI
# Copyright 2017 Databricks, Inc.
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
import os.path
import requests
from databricks_cli.sdk import ReposService, WorkspaceService


class ReposApi(object):
    def __init__(self, api_client):
        self.client = ReposService(api_client)
        self.ws_client = WorkspaceService(api_client)

    def update(self, repo_id, branch):
        return self.client.update(repo_id, branch)

    def get_repos_id(self, path):
        if not path.startswith("/Repos/"):
            raise ValueError("Path must start with /Repos/ !")

        p = path
        while p != "/Repos":
            try:
                status = self.ws_client.get_status(p)
                if status['object_type'] == 'REPO':
                    return status['object_id']
            except requests.exceptions.HTTPError:
                pass

            p = os.path.dirname(p)

        raise RuntimeError("Can't find Repos ID for {path}".format(path=path))