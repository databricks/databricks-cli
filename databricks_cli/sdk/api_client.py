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

import version

from requests.adapters import HTTPAdapter

try:
    from requests.packages.urllib3.poolmanager import PoolManager
    from requests.packages.urllib3 import exceptions
    requests.packages.urllib3.disable_warnings()
except ImportError:
    from urllib3.poolmanager import PoolManager
    from urllib3 import exceptions

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
    def __init__(self, user = None, password = None, host = None, configUrl = None, 
            apiVersion = version.API_VERSION, default_headers = {}):
        if configUrl:
            self.url = configUrl
            params = self.performQuery("/", headers = {})[1]
            params = credential.json
            user = str(params["user"])
            password = str(params["password"])
            host = str(params["apiUrl"].split("/api")[0])

        if host[-1] == "/":
            host = host[:-1]

        self.session = requests.Session()
        self.session.mount('https://', TlsV1HttpAdapter())

        self.url = "%s/api/%s" % (host, apiVersion)
        if user is not None and password is not None:
            userHeaderData = "Basic " + base64.standard_b64encode(user + ":" + password)
            auth = {'Authorization': userHeaderData, 'Content-Type': 'text/json'}
        else:
            auth = {}
        self.default_headers = dict(auth.items() + default_headers.items())

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
                verify = False, headers = headers)

        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError, e:
            raise e
        return resp.json()

