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

# pylint:disable=import-error
# pylint:disable=bare-except

import warnings


def issue_deprecation_warning():
    # Don't print deprecation warning when running the CLI itself.
    import sys
    import os
    if sys.argv and os.path.basename(sys.argv[0]) == 'databricks':
        return
    warnings.warn("the databricks_cli module is deprecated in favor of databricks-sdk-py. Python "
                  "3.12 will be the last version of Python supported by databricks-cli. Please "
                  "migrate to databricks-sdk-py as documented in the migration guide: "
                  "https://docs.databricks.com/en/dev-tools/cli/migrate.html",
                  DeprecationWarning, stacklevel=3)

issue_deprecation_warning()

def initialize_cli_for_databricks_notebooks():
    import IPython
    from databricksCli import init_databricks_cli_config_provider
    init_databricks_cli_config_provider(IPython.get_ipython().user_ns.entry_point)


try:
    # Initialize custom config provider which is available in Databricks notebooks.
    initialize_cli_for_databricks_notebooks()
except: # noqa
    pass
