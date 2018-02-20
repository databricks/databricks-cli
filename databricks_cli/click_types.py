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

from click import ParamType, Option, MissingParameter, UsageError


class OutputClickType(ParamType):
    name = 'FORMAT'
    help = 'can be "JSON" or "TABLE". Set to TABLE by default.'

    def convert(self, value, param, ctx):
        if value is None:
            return value
        if value.lower() != 'json' and value.lower() != 'table':
            raise RuntimeError('output must be "json" or "table"')
        return value

    @classmethod
    def is_json(cls, value):
        return value is not None and value.lower() == 'json'

    @classmethod
    def is_table(cls, value):
        return value is not None and value.lower() == 'table'


class JsonClickType(ParamType):
    name = 'JSON'

    @classmethod
    def help(cls, endpoint):
        return 'JSON string to POST to {}.'.format(endpoint)


class JobIdClickType(ParamType):
    name = 'JOB_ID'
    help = 'Can be found in the URL at https://*.cloud.databricks.com/#job/$JOB_ID.'


class RunIdClickType(ParamType):
    name = 'RUN_ID'


class ClusterIdClickType(ParamType):
    name = 'CLUSTER_ID'
    help = ('Can be found in the URL at '
            'https://*.cloud.databricks.com/#/setting/clusters/$CLUSTER_ID/configuration.')


class OneOfOption(Option):
    def __init__(self, *args, **kwargs):
        self.one_of = kwargs.pop('one_of')
        super(OneOfOption, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        cleaned_opts = set([o.replace('_', '-') for o in opts.keys()])
        if len(cleaned_opts.intersection(set(self.one_of))) == 0:
            raise MissingParameter('One of {} must be provided.'.format(self.one_of))
        if len(cleaned_opts.intersection(set(self.one_of))) > 1:
            raise UsageError('Only one of {} should be provided.'.format(self.one_of))
        return super(OneOfOption, self).handle_parse_result(ctx, opts, args)
