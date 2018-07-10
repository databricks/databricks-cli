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
from databricks_cli.stack.exceptions import StackError

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
        """
        Parse the json stack configuration template to a readable dict format.

        :param filename: File path of the JSON stack configuration template.
        :return: dict of parsed JSON stack config template.
        """
        parsed_conf = {}
        with open(filename, 'r') as f:
            parsed_conf = json.load(f)

        return parsed_conf

    def _generate_stack_status_path(self, stack_path):
        """
        Given a path to the stack configuration template JSON file, generates a path to where the
        deployment status JSON will be stored after successful deployment of the stack.

        :param stack_path: Path to the stack config template JSON file
        :return: The path to the stack status file.

        >>> self._generate_stack_status_path('./stack.json')
        './stack.deployed.json'
        """
        stack_path_split = stack_path.split('.')
        stack_path_split.insert(-1, STACK_STATUS_INSERT)
        return '.'.join(stack_path_split)

    def _load_deploy_metadata(self, stack_path):
        """
        Loads the deployment status metadata for a stack given a path to the stack configuration
        template JSON file. Looks for the default local stack status path generated from
        _generate_stack_status_path.

        When loaded, the stack resource configurations from the past deployment will be loaded into
        self.deployed_resource_config, using the RESOURCE_ID field of each resource as a key in the
        dictionary.
        The output from the databricks server of the deployment of each resource will also be
        loaded in self.deployed_resources in the same way.

        :param stack_path: path to JSON stack configuration template.
        :return: The dict of parsed JSON of the stack deployment status.
        If path doesn't exist, will return an empty dict.
        """
        parsed_conf = {}
        default_status_path = self._generate_stack_status_path(stack_path)
        try:
            if os.path.exists(default_status_path):
                with open(default_status_path, 'r') as f:
                    parsed_conf = json.load(f)
                click.echo("Using deployment status file at %s" % default_status_path)
        except ValueError:
            # Handles a bad JSON read. Will just pass and parsed_conf will be empty dict.
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

    def _store_deploy_metadata(self, stack_path, data):
        """
        Stores status data related to stack deployment given the path to the stack configuration
        template. The status JSON file is stored to the default path generated from
        self._generate_stack_status_path.

        :param stack_path: Path to the JSON configuration template of the stack.
        :param data: Given status metadata to store.
        :return: None
        """
        stack_status_filepath = self._generate_stack_status_path(stack_path)
        stack_file_folder = os.path.dirname(stack_status_filepath)
        if not os.path.exists(stack_file_folder):
            os.makedirs(stack_file_folder)
        with open(stack_status_filepath, 'w+') as f:
            json.dump(data, f, indent=2, sort_keys=True)
            click.echo('Storing deploy status metadata to %s' % stack_status_filepath)

    def create_job(self, job_settings):
        """
        Given settings of the job in job_settings, create a new job. For purposes of idempotency
        and to reduce leaked resources in alpha versions of stack deployment, if a job exists
        with the same name, that job will be updated. If multiple jobs are found with the same name,
        this is dangerous and the deployment will stop.

        :param job_settings:
        :return: job_id, Physical ID of job on Databricks server.
        """
        if 'name' in job_settings:
            jobs_same_name = self.jobs_client.get_jobs_by_name(job_settings['name'])
            if len(jobs_same_name) == 1:
                first_job = jobs_same_name[0]
                creator_name = first_job['creator_user_name']
                timestamp = first_job['created_time'] / MS_SEC
                date_created = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                click.echo('Warning: Job exists with same name created by %s on %s. Job will '
                           'be overwritten' % (creator_name, date_created))
                return self.update_job(job_settings, first_job['job_id'])
            elif len(jobs_same_name) > 1:
                # Dangerous, raise error
                raise StackError('Multiple jobs with the same name already exist, aborting job'
                                 ' resource deployment')
            else:
                click.echo("Creating new job")
        else:
            click.echo('Warning: Creating untitled job.')
        job_id = self.jobs_client.create_job(job_settings)['job_id']
        return job_id

    def update_job(self, job_settings, job_id):
        """
        Given job settings
        :param job_settings: job settings to update the job with.
        :param job_id: physical id of job in databricks server.
        :return: job_id-
        """

        # Check if persisted job still exists, otherwise create new job.
        try:
            self.jobs_client.get_job(job_id)
        except HTTPError:
            return self.create_job(job_settings)

        click.echo("Updating Job")
        self.jobs_client.reset_job({'job_id': job_id, 'new_settings': job_settings})
        return job_id

    def deploy_job(self, resource_id, job_settings, physical_id=None):
        """
        Deploys a job resource by either creating a job if the job isn't kept track of through
        the physical_id of the job or updating an existing job. The job is created or updated using
        the the settings specified in the inputted job_settings.

        :param resource_id: The stack-internal resource ID of the job.
        :param job_settings: A dict of the Databricks JobSettings data structure
        :param physical_id: A dict object containing 'job_id' field of job identifier in Databricks
        server

        :return: tuple of (physical_id, deploy_output), where physical_id contains
        """
        click.echo("Deploying job '%s' with settings: \n%s \n" % (resource_id, json.dumps(
            job_settings, indent=2, separators=(',', ': '))), nl=False)

        if physical_id and 'job_id' in physical_id:
            job_id = self.update_job(job_settings, physical_id['job_id'])
        else:
            job_id = self.create_job(job_settings)

        job_link = "%s#job/%s" % (self.host, str(job_id))
        click.echo("Job Link: %s" % job_link)
        physical_id = {'job_id': job_id, "link": job_link}
        deploy_output = self.jobs_client.get_job(job_id)
        return physical_id, deploy_output

    def deploy_resource(self, resource):  # overwrite to be added
        """
        Deploys a resource given a resource information extracted from the stack JSON configuration
        template.

        :param resource: A dict of the resource with fields of RESOURCE_ID, RESOURCE_TYPE and
        RESOURCE_PROPERTIES
        :return: dict resource_deploy_info- A dictionary of deployment
        information of the resource to be stored at deploy time. It includes the resource id of
        the resource along with the physical id and deploy output of the resource.
        """
        try:
            resource_id = resource[RESOURCE_ID]
            resource_type = resource[RESOURCE_TYPE]
            resource_properties = resource[RESOURCE_PROPERTIES]
        except KeyError as e:
            raise StackError("%s doesn't exist in resource config" % str(e))

        # Deployment
        physical_id = self._get_deployed_resource(resource_id, resource_type)

        if resource_type == JOBS_TYPE:
            physical_id, deploy_output = self.deploy_job(resource_id, resource_properties,
                                                         physical_id)
        else:
            raise StackError("Resource type '%s' not found" % resource_type)

        resource_deploy_info = {RESOURCE_ID: resource_id, RESOURCE_TYPE: resource_type}
        if six.PY3:
            resource_deploy_info[RESOURCE_DEPLOY_TIMESTAMP] = datetime.now().timestamp()
        elif six.PY2:
            resource_deploy_info[RESOURCE_DEPLOY_TIMESTAMP] = \
                time.mktime(datetime.now().timetuple())
        resource_deploy_info[RESOURCE_PHYSICAL_ID] = physical_id
        resource_deploy_info[RESOURCE_DEPLOY_OUTPUT] = deploy_output
        return resource_deploy_info

    def deploy(self, filename):  # overwrite to be added
        """
        Deploys a stack given stack JSON configuration template at path filename.

        Loads the JSON template as well as status JSON if stack has been deployed before.
        After going through each of the resources and deploying them, stores status JSON
        of deployment with deploy status of each resource deployment.
        
        :param filename: Path to stack JSON configuration template
        :return: None.
        """
        config_filepath = os.path.abspath(filename)
        config_dir = os.path.dirname(config_filepath)
        cli_cwd = os.getcwd()
        os.chdir(config_dir)  # Switch current working directory to where json is stored
        try:
            parsed_conf = self._parse_config_file(filename)
            stack_name = parsed_conf['name'] if 'name' in parsed_conf else None

            self._load_deploy_metadata(config_filepath)

            deploy_metadata = {STACK_NAME: stack_name, 'cli_version': CLI_VERSION}
            click.echo('Deploying stack %s' % stack_name)
            deploy_metadata[STACK_RESOURCES] = parsed_conf[STACK_RESOURCES]
            deployed_resources = []
            if STACK_RESOURCES not in parsed_conf:
                raise StackError("'%s' not in configuration" % STACK_RESOURCES)
            for resource in parsed_conf[STACK_RESOURCES]:
                click.echo()
                click.echo("Deploying resource")
                deploy_status = self.deploy_resource(resource)  # overwrite to be added
                if deploy_status:
                    deployed_resources.append(deploy_status)
            deploy_metadata[STACK_DEPLOYED] = deployed_resources
            self._store_deploy_metadata(config_filepath, deploy_metadata)
            os.chdir(cli_cwd)
        except Exception:
            # For any exception during deployment, set cwd back to what it was.
            os.chdir(cli_cwd)
            raise
