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

# Resource Services
JOBS_SERVICE = 'jobs'

# Config Outer Fields
STACK_NAME = 'name'
STACK_RESOURCES = 'resources'
STACK_DEPLOYED = 'deployed'

# Resource Fields
RESOURCE_ID = 'id'
RESOURCE_SERVICE = 'service'
RESOURCE_PROPERTIES = 'properties'

# Deployed Resource Fields
RESOURCE_PHYSICAL_ID = 'physical_id'
RESOURCE_DEPLOY_OUTPUT = 'deploy_output'
RESOURCE_DEPLOY_TIMESTAMP = 'timestamp'
CLI_VERSION_KEY = 'cli_version'


class StackApi(object):
    def __init__(self, api_client):
        self.jobs_client = JobsApi(api_client)
        self.host = "/"  # default host if cannot get host.
        if click.get_current_context(silent=True):
            profile = get_profile_from_context()
            config = get_config_for_profile(profile)
            self.host = config.host
        self.previous_deploy_resource_config_map = {}
        self.previous_deploy_resource_status_map = {}

    def _parse_config_file(self, filename):
        """
        Parse the json stack configuration template to a readable dict format.

        :param filename: File path of the JSON stack configuration template.
        :return: dict of parsed JSON stack config template.
        """
        stack_conf = {}
        with open(filename, 'r') as f:
            stack_conf = json.load(f)

        return stack_conf

    def _json_type_handler(self, obj):
        """
        Helper function to convert certain objects into a compatible JSON format.

        Right now, converts a datetime object to an integer timestamp.

        :param obj: Object that may be a datetime object.
        :return: Timestamp integer if object is a datetime object.
        """
        if isinstance(obj, datetime):
            # Get timestamp of datetime object- works with python2 and 3
            return int(time.mktime(obj.timetuple()))
        raise TypeError("Object of type '%s' is not JSON serializable" % type(obj))

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

    def _load_stack_status(self, status_path):
        """
        Loads the deployment status metadata for a stack given a path to the stack configuration
        template JSON file. Looks for the default local stack status path generated from
        _generate_stack_status_path.

        When loaded, the stack resource configurations from the past deployment will be loaded into
        self.deployed_resource_config, using the RESOURCE_ID field of each resource as a key in the
        dictionary.
        The output from the databricks server of the deployment of each resource will also be
        loaded in self.deployed_resources in the same way.

        :param status_path: path to JSON stack configuration template.
        :return: The dict of parsed JSON of the stack deployment status.
        If path doesn't exist, will return an empty dict.
        """
        stack_status = {}
        try:
            if os.path.exists(status_path):
                with open(status_path, 'r') as f:
                    stack_status = json.load(f)
                click.echo("Using deployment status file at %s" % status_path)

                # Store map from resource_id to past deployment configuration of resources.
                self.previous_deploy_resource_config_map = \
                    {resource[RESOURCE_ID]: resource for resource in stack_status[STACK_RESOURCES]}
                # Storing map from resource_id to past deployment resource status.
                self.previous_deploy_resource_status_map = \
                    {resource[RESOURCE_ID]: resource for resource in stack_status[STACK_DEPLOYED]}
        except ValueError:
            # Handles a bad JSON read. Will just pass and parsed_conf will be empty dict.
            pass
        except KeyError as e:
            # This error should only be raised if there's an implementation error with stack status
            raise StackError("Error with resource status schema from last deployment- "
                             "Missing %s. aborting." % str(e))

        return stack_status

    def _get_deployed_resource_physical_id(self, resource_id, resource_service):
        """
        Returns the databricks physical ID of a resource with RESOURCE_ID and RESOURCE_SERVICE

        This uses information from the loaded stack status (specifically, self.deployed_resources)
        to get needed information.

        :param resource_id: Internal stack identifier of resource
        :param resource_service: Resource service of stack resource
        :return: JSON object of Physical ID of resource on databricks
        """
        if not self.previous_deploy_resource_status_map:
            return None
        if resource_id in self.previous_deploy_resource_status_map:
            deployed_resource = self.previous_deploy_resource_status_map[resource_id]
            try:
                deployed_resource_service = deployed_resource[RESOURCE_SERVICE]
                deployed_physical_id = deployed_resource[RESOURCE_PHYSICAL_ID]
            except KeyError as e:
                # Should only be here if there's an implementation error with stack status
                raise StackError("Error with resource status schema from last deployment- "
                                 "Missing %s. aborting." % str(e))
            if resource_service != deployed_resource_service:
                raise StackError("Past deployment had same 'resource_id' '%s' with different "
                                 "service '%s'. Please change 'resource id' value."
                                 % (resource_id, resource_service))
            return deployed_physical_id
        return None

    def _save_stack_status(self, status_path, status_data):
        """
        Stores status data related to stack deployment given a path to the status data file.

        :param status_path: Path to the JSON configuration template of the stack.
        :param status_data: Given status metadata to store.
        :return: None
        """
        with open(status_path, 'w+') as f:
            json.dump(status_data, f, indent=2, sort_keys=True, default=self._json_type_handler)
            click.echo('Storing deployed stack status metadata to %s' % status_path)

    def put_job(self, job_settings):
        """
        Given settings of the job in job_settings, create a new job. For purposes of idempotency
        and to reduce leaked resources in alpha versions of stack deployment, if a job exists
        with the same name, that job will be updated. If multiple jobs are found with the same name,
        the deployment will abort.

        :param job_settings:
        :return: job_id, Physical ID of job on Databricks server.
        """
        if 'name' not in job_settings:
            raise StackError("Please supply 'name' in job resource 'resource_properties'")
        job_name = job_settings['name']
        jobs_same_name = self.jobs_client._list_jobs_by_name(job_name)
        if len(jobs_same_name) > 1:
            raise StackError("Multiple jobs with the same name '%s' already exist, aborting"
                             " stack deployment" % job_name)
        elif len(jobs_same_name) == 1:
            existing_job = jobs_same_name[0]
            creator_name = existing_job['creator_user_name']
            timestamp = existing_job['created_time'] / MS_SEC  # Convert to readable date.
            date_created = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            click.echo("Warning: Job exists with same name '%s' created by %s on %s. Job will "
                       "be overwritten" % (job_name, creator_name, date_created))
            self.update_job(job_settings, existing_job['job_id'])
            return existing_job['job_id']
        else:
            click.echo("Creating new job")
            job_id = self.jobs_client.create_job(job_settings)['job_id']
            return job_id

    def update_job(self, job_settings, job_id):
        """
        Given job settings and an existing job_id of a job, update the job settings on databricks.

        :param job_settings: job settings to update the job with.
        :param job_id: physical job_id of job in databricks server.
        """
        click.echo("Updating Job")
        self.jobs_client.reset_job({'job_id': job_id, 'new_settings': job_settings})

    def deploy_job(self, resource_id, resource_properties, physical_id=None):
        """
        Deploys a job resource by either creating a job if the job isn't kept track of through
        the physical_id of the job or updating an existing job. The job is created or updated using
        the the settings specified in the inputted job_settings.

        :param resource_id: The stack-internal resource ID of the job.
        :param resource_properties: A dict of the Databricks JobSettings data structure
        :param physical_id: A dict object containing 'job_id' field of job identifier in Databricks
        server

        :return: tuple of (physical_id, deploy_output), where physical_id contains a 'job_id' field
        of the physical job_id of the job on databricks. deploy_output is the output of the job
        from databricks when a GET request is called for it.
        """
        job_settings = resource_properties  # resource_properties of jobs are solely job settings.
        click.echo("Deploying job '%s' with settings: \n%s \n" % (resource_id, json.dumps(
            job_settings, indent=2, separators=(',', ': '))), nl=False)

        if physical_id and 'job_id' in physical_id:
            job_id = physical_id['job_id']
            try:
                self.update_job(job_settings, physical_id['job_id'])
            except HTTPError:
                # If updating a job fails, create the job with put_job
                job_id = self.put_job(job_settings)
        else:
            job_id = self.put_job(job_settings)

        job_url = "%s#job/%s" % (self.host, str(job_id))
        click.echo("Job URL: %s" % job_url)
        physical_id = {'job_id': job_id, "url": job_url}
        deploy_output = self.jobs_client.get_job(job_id)
        return physical_id, deploy_output

    def deploy_resource(self, resource):  # overwrite to be added
        """
        Deploys a resource given a resource information extracted from the stack JSON configuration
        template.

        :param resource: A dict of the resource with fields of RESOURCE_ID, RESOURCE_SERVICE and
        RESOURCE_PROPERTIES.
        ex. {'id': 'example-resource', 'service': 'jobs', 'properties': {...}}
        :return: dict resource_deploy_info- A dictionary of deployment information of the
        resource to be stored at deploy time. It includes the resource id of the resource along
        with the physical id and deploy output of the resource.
        ex. {'id': 'example-resource', 'service': 'jobs', 'physical_id': {'job_id': 123},
        'timestamp': 123456789, 'deploy_output': {..}}
        """
        try:
            resource_id = resource[RESOURCE_ID]
            resource_service = resource[RESOURCE_SERVICE]
            resource_properties = resource[RESOURCE_PROPERTIES]
        except KeyError as e:
            raise StackError("%s doesn't exist in resource config" % str(e))

        # Deployment
        physical_id = self._get_deployed_resource_physical_id(resource_id, resource_service)

        if resource_service == JOBS_SERVICE:
            physical_id, deploy_output = self.deploy_job(resource_id, resource_properties,
                                                         physical_id)
        else:
            raise StackError("Resource service '%s' not supported" % resource_service)

        resource_deploy_info = {RESOURCE_ID: resource_id, RESOURCE_SERVICE: resource_service,
                                RESOURCE_DEPLOY_TIMESTAMP: datetime.now(),
                                RESOURCE_PHYSICAL_ID: physical_id,
                                RESOURCE_DEPLOY_OUTPUT: deploy_output}
        return resource_deploy_info

    def deploy(self, config_path):  # overwrite to be added
        """
        Deploys a stack given stack JSON configuration template at path config_path.

        Loads the JSON template as well as status JSON if stack has been deployed before.
        After going through each of the resources and deploying them, stores status JSON
        of deployment with deploy status of each resource deployment.

        :param config_path: Path to stack JSON configuration template. Must have the fields of
        'name', the name of the stack and 'resources', a list of stack resources.
        :return: None.
        """
        config_dir = os.path.dirname(os.path.abspath(config_path))
        cli_cwd = os.getcwd()
        os.chdir(config_dir)  # Switch current working directory to where json config is stored
        try:
            parsed_conf = self._parse_config_file(config_path)
            if STACK_NAME not in parsed_conf:
                raise StackError("'%s' not in configuration" % STACK_NAME)
            stack_name = parsed_conf[STACK_NAME]
            status_path = self._generate_stack_status_path(config_path)
            self._load_stack_status(status_path)

            click.echo('Deploying stack %s' % stack_name)
            deployed_resources = []
            if STACK_RESOURCES not in parsed_conf:
                raise StackError("'%s' not in configuration" % STACK_RESOURCES)
            for resource in parsed_conf[STACK_RESOURCES]:
                click.echo()
                click.echo("Deploying resource")
                resource_status = self.deploy_resource(resource)  # overwrite to be added
                deployed_resources.append(resource_status)

            # stack deploy status is original config with deployed resource statuses added
            new_stack_status = parsed_conf
            new_stack_status.update({STACK_DEPLOYED: deployed_resources})
            new_stack_status.update({CLI_VERSION_KEY: CLI_VERSION})

            self._save_stack_status(status_path, new_stack_status)
            os.chdir(cli_cwd)
        except Exception:
            # For any exception during deployment, set cwd back to what it was.
            os.chdir(cli_cwd)
            raise
