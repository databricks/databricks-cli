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

import imp
import os
from setuptools import setup, find_packages

version = imp.load_source(
    'databricks_cli.version', os.path.join('databricks_cli', 'version.py')).version

setup(
    name='databricks-cli',
    version=version,
    packages=find_packages(exclude=['tests', 'tests.*']),
    install_requires=[
        'click>=6.7',
        'requests>=2.17.3',
        'tabulate>=0.7.7',
    ],
    entry_points='''
        [console_scripts]
        databricks=databricks_cli.cli:cli
        dbfs=databricks_cli.dbfs.cli:dbfs_group
    ''',
    zip_safe=False,
    author='Andrew Chen',
    author_email='andrewchen@databricks.com',
    description='A command line interface for Databricks',
    long_description=open('README.rst').read(),
    license='Apache License 2.0',
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Programming Language :: Python :: 2.7',
    ],
    keywords='databricks cli',
    url='https://github.com/databricks/databricks-cli'
)
