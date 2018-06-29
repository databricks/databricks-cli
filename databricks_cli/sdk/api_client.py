#!/usr/bin/env python

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


"""
A common class to be used by client of different APIs
"""

import base64
import json
import warnings
import requests
import ssl

from . import version

from requests.adapters import HTTPAdapter

try:
    from requests.packages.urllib3.poolmanager import PoolManager
    from requests.packages.urllib3 import exceptions
except ImportError:
    from urllib3.poolmanager import PoolManager
    from urllib3 import exceptions

from databricks_cli.version import version as databricks_cli_version

class TlsV1HttpAdapter(HTTPAdapter):
    """
    A HTTP adapter implementation that specifies the ssl version to be TLS1.
    This avoids problems with openssl versions that
    use SSL3 as a default (which is not supported by the server side).
    """

    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = PoolManager(num_pools=connections, maxsize=maxsize, block=block, ssl_version=ssl.PROTOCOL_TLSv1_2)

class ApiClient(object):
    """
    A partial Python implementation of dbc rest api
    to be used by different versions of the client.
    """
    def __init__(self, user = None, password = None, host = None, token = None,
                 apiVersion = version.API_VERSION, default_headers = {}, verify = True):
        if host[-1] == "/":
            host = host[:-1]

        self.session = requests.Session()
        self.session.mount('https://', TlsV1HttpAdapter())

        self.url = "%s/api/%s" % (host, apiVersion)
        if user is not None and password is not None:
            encoded_auth = (user + ":" + password).encode()
            user_header_data = "Basic " + base64.standard_b64encode(encoded_auth).decode()
            auth = {'Authorization': user_header_data, 'Content-Type': 'text/json'}
        elif token is not None:
            auth = {'Authorization': 'Bearer {}'.format(token), 'Content-Type': 'text/json'}
        else:
            auth = {}
        user_agent = {'user-agent': 'databricks-cli-{v}'.format(v=databricks_cli_version)}
        self.default_headers = {}
        self.default_headers.update(auth)
        self.default_headers.update(default_headers)
        self.default_headers.update(user_agent)
        self.verify = verify

    def close(self):
        """Close the client"""
        pass

    # helper functions starting here

    def perform_query(self, method, path, data = {}, headers = None):
        """set up connection and perform query"""
        if headers is None:
            headers = self.default_headers

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", exceptions.InsecureRequestWarning)
            resp = self.session.request(method, self.url + path, data = json.dumps(data),
                verify = self.verify, headers = headers)

        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise e
        return resp.json()
