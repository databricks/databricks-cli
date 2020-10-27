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

from copy import deepcopy

from databricks_cli.clusters.api import ClusterApi
from databricks_cli.sdk import JobsService


class JobsApi(object):
    def __init__(self, api_client):
        self.api_client = api_client
        self.client = JobsService(api_client)

    def create_job(self, json, headers=None):
        json = self._convert_cluster_name_to_id(json)
        return self.client.client.perform_query('POST', '/jobs/create', data=json, headers=headers)

    def list_jobs(self, headers=None):
        resp = self.client.list_jobs(headers=headers)
        if 'jobs' not in resp:
            resp['jobs'] = []
        return resp

    def delete_job(self, job_id, headers=None):
        return self.client.delete_job(job_id, headers=headers)

    def get_job(self, job_id, headers=None):
        return self.client.get_job(job_id, headers=headers)

    def reset_job(self, json, headers=None):
        # reset should support cluster_name:
        json = self._convert_cluster_name_to_id(json)
        return self.client.client.perform_query('POST', '/jobs/reset', data=json, headers=headers)

    def run_now(self, job_id, jar_params, notebook_params, python_params, spark_submit_params,
                headers=None):
        return self.client.run_now(job_id, jar_params, notebook_params, python_params,
                                   spark_submit_params, headers=headers)

    def _list_jobs_by_name(self, name, headers=None):
        jobs = self.list_jobs(headers=headers)['jobs']
        result = list(filter(lambda job: job['settings']['name'] == name, jobs))
        return result

    def clone_job(self, job_id, job_name, headers=None):
        job_info = self.get_job(job_id, headers=headers)
        if 'settings' not in job_info:
            # failure
            return job_info

        upload_json = deepcopy(job_info['settings'])
        upload_json['name'] = job_name

        return self.create_job(json=upload_json, headers=headers)

    def _convert_cluster_name_to_id(self, json):
        """
        If json contains cluster_name instead of existing_cluster_id, convert it to a cluster_id
        :return: json
        """

        cluster_data = json
        if 'new_settings' in json:
            cluster_data = json['new_settings']

        # early out the easy things
        if not json or 'existing_cluster_id' in cluster_data:
            return json

        if 'cluster_name' in cluster_data:
            cluster_id = self._get_cluster_id(cluster_data['cluster_name'])
            cluster_data['existing_cluster_id'] = cluster_id
            del cluster_data['cluster_name']
        return json

    def _get_cluster_id(self, cluster_name):
        # at this point we might have cluster_name
        # lookup the cluster.
        clusters_api = ClusterApi(self.api_client)
        return clusters_api.get_cluster_id_for_name(cluster_name)
