# Databricks CLI
# Copyright 2020 Databricks, Inc.
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

import sys
import time


def spinning_cursor():
    while True:
        for cursor in '|/-\\':
            yield cursor


class SqlApi(object):
    def __init__(self, api_client):
        self.client = api_client

    # Wrapper around api_client.perform_auery
    def perform_query(self, method, path, data=None):
        return self.client.perform_query(method, path, data=data, use_api_prefix=False)

    def do_query(self, data_source_id, query):
        _data = {
            "apply_auto_limit": True,
            "data_source_id": data_source_id,
            "max_age": 0,
            "parameters": {},
            "query": query,
        }
        job_info = self.perform_query('POST', '/sql/api/query_results', data=_data)

        if 'job' not in job_info or 'id' not in job_info['job']:
            raise RuntimeError("Did not find job id in response from query_results: %s" % (job_info))

        status_uri = '/sql/api/query_executions/' + job_info['job']['id']
        # Use input timeout and/or max_attempts?
        max_attempts = 20
        timeout = 30  # sec
        deadline = time.time() + timeout
        spinner = spinning_cursor()
        job_status = None
        for i in range(max_attempts):
            job_status = self.perform_query('GET', status_uri)
            if job_status['job']['status'] == 3:
                # Query is ready!
                break
            if job_status['job']['error'] != "":
                raise RuntimeError("Error from job status: %s" % job_status['job']['error'])
            if time.time() > deadline or i == max_attempts-1:
                raise RuntimeError("Query timed out.")

            sys.stdout.write(next(spinner))
            sys.stdout.flush()
            time.sleep(0.3)
            sys.stdout.write('\b')

        query_result_uri = '/sql/api/query_results/' + job_status['job']['query_result_id']
        return self.perform_query('GET', query_result_uri)

    def list_data_sources(self):
        return self.perform_query('GET', '/sql/api/data_sources')

