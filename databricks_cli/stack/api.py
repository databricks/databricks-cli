# Databricks CLI
# Copyright 2018 Databricks, Inc.
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
import json
from datetime import datetime
import time
import six

import click

from requests.exceptions import HTTPError
from databricks_cli.jobs.api import JobsApi
from databricks_cli.version import version as CLI_VERSION
from databricks_cli.configure.config import get_profile_from_context, get_config_for_profile
from databricks_cli.stack.exceptions import ConfigError

DEBUG_MODE = False
_home = os.path.expanduser('~')
MS_SEC = 1000
STACK_STATUS_INSERT = 'deployed'

# Stack Deployment Status Folder- WIP
# STACK_DIR = os.path.join(_home, 'databricks', 'stacks', 'beta')

# Resource Types
JOBS_TYPE = 'job'

# Config Outer Fields
STACK_NAME = 'name'
STACK_RESOURCES = 'resources'
STACK_DEPLOYED = 'deployed'

# Resource Fields
RESOURCE_ID = 'id'
RESOURCE_TYPE = 'type'
RESOURCE_PROPERTIES = 'properties'

# Deployed Resource Fields
RESOURCE_PHYSICAL_ID = 'physical_id'
RESOURCE_DEPLOY_OUTPUT = 'deploy_output'
RESOURCE_DEPLOY_TIMESTAMP = 'timestamp'


class StackApi(object):
    def __init__(self, api_client):
        self.jobs_client = JobsApi(api_client)
        self.host = "host/"
        if click.get_current_context(silent=True):
            profile = get_profile_from_context()
            config = get_config_for_profile(profile)
            self.host = config.host
        self.deployed_resources = {}
        self.deployed_resource_config = {}

    def _parse_config_file(self, filename):
        """Parse the json config"""
        parsed_conf = {}
        with open(filename, 'r') as f:
            parsed_conf = json.load(f)

        return parsed_conf

    def _generate_stack_status_path(self, stack_path):
        stack_path_split = stack_path.split('.')
        stack_path_split.insert(-1, STACK_STATUS_INSERT)
        return '.'.join(stack_path_split)

    def _load_deploy_metadata(self, stack_path, save_path=None):
        parsed_conf = {}
        default_status_path = self._generate_stack_status_path(stack_path)
        try:
            if os.path.exists(default_status_path):
                with open(default_status_path, 'r') as f:
                    parsed_conf = json.load(f)
                click.echo("Using deployment status file at %s" % default_status_path)
            elif save_path and os.path.exists(save_path):
                with open(save_path, 'r') as f:
                    parsed_conf = json.load(f)
                click.echo("Using deployment status file at %s" % stack_path)
        except ValueError:
            pass

        if STACK_RESOURCES in parsed_conf:
            self.deployed_resource_config = parsed_conf[STACK_RESOURCES]
        if STACK_DEPLOYED in parsed_conf:
            self.deployed_resources = {resource[RESOURCE_ID]: resource for resource in
                                       parsed_conf[STACK_DEPLOYED]}

        return parsed_conf

    def _get_deployed_resource(self, resource_id, resource_type):
        """
        Returns the databricks physical ID of a resource with RESOURCE_ID and RESOURCE_TYPE

        :param resource_id: Internal stack identifier of resource
        :param resource_type: Resource type of stack resource
        :return: JSON object of Physical ID of resource on databricks
        """
        if not self.deployed_resources:
            return None
        if resource_id in self.deployed_resources:
            deployed_resource = self.deployed_resources[resource_id]
            deployed_resource_type = deployed_resource[RESOURCE_TYPE]
            deployed_physical_id = deployed_resource[RESOURCE_PHYSICAL_ID]
            if resource_type != deployed_resource_type:
                click.echo("Resource %s is not of type %s", (resource_id, resource_type))
                return None
            return deployed_physical_id
        return None

    def _store_deploy_metadata(self, stack_path, data, custom_path=None):
        stack_status_filepath = self._generate_stack_status_path(stack_path)
        stack_file_folder = os.path.dirname(stack_status_filepath)
        if not os.path.exists(stack_file_folder):
            os.makedirs(stack_file_folder)
        with open(stack_status_filepath, 'w+') as f:
            click.echo('Storing deploy status metadata to %s' % stack_status_filepath)
            json.dump(data, f, indent=2)

        if custom_path:
            custom_path_folder = os.path.dirname(custom_path)
            if not os.path.exists(custom_path_folder):
                os.makedirs(custom_path_folder)
            with open(custom_path, 'w+') as f:
                click.echo('Storing deploy status metadata to %s' % os.path.abspath(custom_path))
                json.dump(data, f, indent=2)

    def deploy_job(self, resource_id, job_settings, physical_id=None):
        job_id = None
        print("Deploying job %s with settings: \n%s \n" % (resource_id, json.dumps(
            job_settings, indent=2, separators=(',', ': '))))

        if physical_id:  # job exists
            job_id = physical_id
        elif 'name' in job_settings:
            jobs_same_name = self.jobs_client.get_jobs_by_name(job_settings['name'])
            if jobs_same_name:
                creator_name = jobs_same_name[0]['creator_user_name']
                timestamp = jobs_same_name[0]['created_time'] / MS_SEC
                date_created = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                click.echo('Warning: Job exists with same name created by %s on %s. Job will '
                           'be overwritten' % (creator_name, date_created))
                job_id = jobs_same_name[0]['job_id']

        if job_id:
            try:
                # Check if persisted job still exists, otherwise create new job.
                self.jobs_client.get_job(job_id)
            except HTTPError:
                job_id = None

        if job_id:
            click.echo("Updating Job: %s" % resource_id)
            self.jobs_client.reset_job({'job_id': job_id, 'new_settings': job_settings})
            click.echo("Link: %s#job/%s" % (self.host, str(job_id)))
        else:
            click.echo("Creating Job: %s" % resource_id)
            job_id = self.jobs_client.create_job(job_settings)['job_id']
            click.echo("%s Created with ID %s. Link: %s#job/%s" % (
                resource_id, str(job_id), self.host, str(job_id)))

        deploy_output = self.jobs_client.get_job(job_id)

        return job_id, deploy_output

    def deploy_resource(self, resource):  # overwrite to be added
        try:
            resource_id = resource[RESOURCE_ID]
            resource_type = resource[RESOURCE_TYPE]
            resource_properties = resource[RESOURCE_PROPERTIES]
        except KeyError as e:
            raise ConfigError("%s doesn't exist in resource config" % str(e))

        # Deployment
        physical_id = self._get_deployed_resource(resource_id, resource_type)

        if resource_type == JOBS_TYPE:
            job_id = physical_id['job_id'] if physical_id and 'job_id' in physical_id else None
            job_id, deploy_output = self.deploy_job(resource_id, resource_properties,
                                                    job_id)
            physical_id = {'job_id': job_id}
        else:
            raise ConfigError('Resource type %s not found')

        resource_deploy_info = {RESOURCE_ID: resource_id, RESOURCE_TYPE: resource_type}
        if six.PY3:
            resource_deploy_info[RESOURCE_DEPLOY_TIMESTAMP] = datetime.now().timestamp()
        elif six.PY2:
            resource_deploy_info[RESOURCE_DEPLOY_TIMESTAMP] = \
                time.mktime(datetime.now().timetuple())
        resource_deploy_info[RESOURCE_PHYSICAL_ID] = physical_id
        resource_deploy_info[RESOURCE_DEPLOY_OUTPUT] = deploy_output
        return resource_deploy_info

    def deploy(self, filename, save_status_path=None):  # overwrite to be added
        config_filepath = os.path.abspath(filename)
        config_dir = os.path.dirname(config_filepath)
        cli_cwd = os.getcwd()
        if save_status_path:
            save_status_path = os.path.abspath(save_status_path)
        os.chdir(config_dir)  # Switch current working directory to where json is stored
        try:
            parsed_conf = self._parse_config_file(filename)
            stack_name = parsed_conf['name'] if 'name' in parsed_conf else None

            self._load_deploy_metadata(config_filepath, save_status_path)

            deploy_metadata = {STACK_NAME: stack_name, 'cli_version': CLI_VERSION}
            click.echo('Deploying stack %s' % stack_name)
            deploy_metadata[STACK_RESOURCES] = parsed_conf[STACK_RESOURCES]
            deployed_resources = []
            if STACK_RESOURCES not in parsed_conf:
                raise ConfigError("'%s' not in configuration" % STACK_RESOURCES)
            for resource in parsed_conf[STACK_RESOURCES]:
                click.echo()
                click.echo("Deploying resource")
                deploy_status = self.deploy_resource(resource)  # overwrite to be added
                if deploy_status:
                    deployed_resources.append(deploy_status)
            deploy_metadata[STACK_DEPLOYED] = deployed_resources
            self._store_deploy_metadata(config_filepath, deploy_metadata, save_status_path)
            os.chdir(cli_cwd)
        except Exception:
            os.chdir(cli_cwd)
            raise
