databricks-cli
==============
.. image:: https://travis-ci.org/databricks/databricks-cli.svg?branch=master
   :target: https://travis-ci.org/databricks/databricks-cli
   :alt: Build Status

The Databricks Command Line Interface (CLI) is an open source tool which provides an easy to use interface to
the Databricks platform. The CLI is built on top of the Databricks Rest APIs. Currently,
the CLI fully implements the DBFS API and the Workspace API.

**PLEASE NOTE**, this CLI is under active development and is released as
an experimental client. This means that interfaces are still subject to change.

If you're interested in contributing to the project please reach out.
In addition, please leave bug reports as issues on our `Github project <https://github.com/databricks/databricks-cli>`_.

Requirements
------------

-  Python Version > 2.7.9 or > 3.6

Installation
---------------

To install simply run
``pip install --upgrade databricks-cli``

Then set up authentication using username/password or `authentication token <https://docs.databricks.com/api/latest/authentication.html#token-management>`_. Credentials are stored at ``~/.databrickscfg``.

- ``databricks configure`` (enter hostname/username/password at prompt)
- ``databricks configure --token`` (enter hostname/auth-token at prompt)

Multiple connection profiles are also supported with ``databricks configure --profile <profile> [--token]``.
The connection profile can be used as such: ``databricks workspace ls --profile <profile>``.

Then you're all set to go! To test that your authentication information is working, try a quick test like
``databricks workspace ls``.

Known Issues
---------------
``AttributeError: 'module' object has no attribute 'PROTOCOL_TLSv1_2'``

The Databricks web service requires clients speak TLSV1.2. The built in
version of Python for MacOS does not have this version of TLS built in.

To use databricks-cli you should install a version of Python which has ``ssl.PROTOCOL_TLSv1_2``.
For MacOS, the easiest way may be to install Python with `Homebrew <https://brew.sh/>`_.

Workspace CLI Examples
-----------------------
The implemented commands for the Workspace CLI can be listed by running ``databricks workspace -h``.
Commands are run by appending them to ``databricks workspace``. To make it easier to use the workspace
CLI, feel free to alias ``databricks workspace`` to something shorter. For more information
reference `Aliasing Command Groups section <#aliasing-command-groups>`_.

.. code::

    $ databricks workspace -h
    Usage: databricks workspace [OPTIONS] COMMAND [ARGS]...

      Utility to interact with the Databricks Workspace. Workspace paths must be
      absolute and be prefixed with `/`.

    Options:
      -v, --version
      -h, --help     Show this message and exit.

    Commands:
      delete      Deletes objects from the Databricks...
      export      Exports a file from the Databricks workspace...
      export_dir  Recursively exports a directory from the...
      import      Imports a file from local to the Databricks...
      import_dir  Recursively imports a directory from local to...
      list        List objects in the Databricks Workspace
      ls          List objects in the Databricks Workspace
      mkdirs      Make directories in the Databricks Workspace.
      rm          Deletes objects from the Databricks...

Listing Workspace Files
^^^^^^^^^^^^^^^^^^^^^^^^
.. code::

    $ databricks workspace ls /Users/example@databricks.com
    Usage Logs ETL
    Common Utilities
    guava-21.0

Importing a local directory of notebooks
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The ``databricks workspace import_dir`` command will recursively import a directory
from the local filesystem to the Databricks workspace. Only directories and
files with the extensions of ``.scala``, ``.py``, ``.sql``, ``.r``, ``.R`` are imported.
When imported, these extensions will be stripped off the name of the notebook.

To overwrite existing notebooks at the target path, the flag ``-o`` must be added.

.. code::

    $ tree
    .
    ├── a.py
    ├── b.scala
    ├── c.sql
    ├── d.R
    └── e

.. code::

    $ databricks workspace import_dir . /Users/example@databricks.com/example
    ./a.py -> /Users/example@databricks.com/example/a
    ./b.scala -> /Users/example@databricks.com/example/b
    ./c.sql -> /Users/example@databricks.com/example/c
    ./d.R -> /Users/example@databricks.com/example/d

.. code::

    $ databricks workspace ls /Users/example@databricks.com/example -l
    NOTEBOOK   a  PYTHON
    NOTEBOOK   b  SCALA
    NOTEBOOK   c  SQL
    NOTEBOOK   d  R
    DIRECTORY  e

Exporting a workspace directory to the local filesystem
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Similarly, it is possible to export a directory of notebooks from the Databricks workspace
to the local filesystem. To do this, the command is simply

.. code::

    $ databricks workspace export_dir /Users/example@databricks.com/example .

DBFS CLI Examples
-----------------------
The implemented commands for the DBFS CLI can be listed by running ``databricks fs -h``.
Commands are run by appending them to ``databricks fs`` and all dbfs paths should be prefixed with
``dbfs:/``. To make the command less verbose, we've
gone ahead and aliased ``dbfs`` to ``databricks fs``.

.. code::

    $ databricks fs -h
    Usage: databricks fs [OPTIONS] COMMAND [ARGS]...

      Utility to interact with DBFS. DBFS paths are all prefixed
      with dbfs:/. Local paths can be absolute or local.

    Options:
      -v, --version
      -h, --help     Show this message and exit.

    Commands:
      configure
      cp         Copy files to and from DBFS.
      ls         List files in DBFS.
      mkdirs     Make directories in DBFS.
      mv         Moves a file between two DBFS paths.
      rm         Remove files from dbfs.

Copying a file to DBFS
^^^^^^^^^^^^^^^^^^^^^^^^
.. code::

    dbfs cp test.txt dbfs:/test.txt
    # Or recursively
    dbfs cp -r test-dir dbfs:/test-dir

Copying a file from DBFS
^^^^^^^^^^^^^^^^^^^^^^^^
.. code::

    dbfs cp dbfs:/test.txt ./test.txt
    # Or recursively
    dbfs cp -r dbfs:/test-dir ./test-dir

Jobs CLI Examples
--------------------
The implemented commands for the jobs CLI can be listed by running ``databricks jobs -h``.
Job run commands are handled by ``databricks runs -h``.

.. code::

    $ databricks jobs -h
    Usage: databricks jobs [OPTIONS] COMMAND [ARGS]...

      Utility to interact with jobs.

      This is a wrapper around the jobs API
      (https://docs.databricks.com/api/latest/jobs.html). Job runs are handled
      by ``databricks runs``.

    Options:
      -v, --version  [VERSION]
      -h, --help     Show this message and exit.

    Commands:
      create   Creates a job.
      delete   Deletes the specified job.
      get      Describes the metadata for a job.
      list     Lists the jobs in the Databricks Job Service.
      reset    Resets (edits) the definition of a job.
      run-now  Runs a job with optional per-run parameters.

.. code::

    $ databricks runs -h
    Usage: databricks runs [OPTIONS] COMMAND [ARGS]...

      Utility to interact with job runs.

    Options:
      -v, --version  [VERSION]
      -h, --help     Show this message and exit.

    Commands:
      cancel  Cancels the run specified.
      get     Gets the metadata about a run in json form.
      list    Lists job runs.
      submit  Submits a one-time run.

Listing and finding jobs
^^^^^^^^^^^^^^^^^^^^^^^^^
The ``databricks jobs list`` command has two output formats, ``JSON`` and ``TABLE``.
The ``TABLE`` format is outputted by default and returns a two column table (job ID, job name).

To find a job by name

.. code::

    databricks jobs list | grep "JOB_NAME"

Copying a job
^^^^^^^^^^^^^^^^^^^^^^^^
This example requires the program ``jq``.
See `jq section <#jq>`_ for more details.

.. code::

    SETTINGS_JSON=$(databricks jobs get --job-id 284907 | jq .settings)
    # JQ Explanation:
    #   - peek into top level `settings` field.
    databricks jobs create --json "$SETTINGS_JSON"

Deleting "Untitled" Jobs
^^^^^^^^^^^^^^^^^^^^^^^^
.. code::

    databricks jobs list --output json | jq '.jobs[] | select(.settings.name == "Untitled") | .job_id' | xargs -n 1 databricks jobs delete --job-id
    # Explanation:
    #   - List jobs in JSON.
    #   - Peek into top level `jobs` field.
    #   - Select only jobs with name equal to "Untitled"
    #   - Print those job ID's out.
    #   - Invoke `databricks jobs delete --job-id` once per row with the $job_id appended as an argument to the end of the command.

Clusters CLI Examples
-----------------------
The implemented commands for the clusters CLI can be listed by running ``databricks clusters -h``.

.. code::

    $ databricks clusters -h
    Usage: databricks clusters [OPTIONS] COMMAND [ARGS]...

      Utility to interact with Databricks clusters.

    Options:
      -v, --version  [VERSION]
      -h, --help     Show this message and exit.

    Commands:
      create           Creates a Databricks cluster.
      delete           Removes a Databricks cluster given its ID.
      get              Retrieves metadata about a cluster.
      list             Lists active and recently terminated clusters.
      list-node-types  Lists possible node types for a cluster.
      list-zones       Lists zones where clusters can be created.
      restart          Restarts a Databricks cluster given its ID.
      spark-versions   Lists possible Databricks Runtime versions...
      start            Starts a terminated Databricks cluster given its ID.

Listing runtime versions
^^^^^^^^^^^^^^^^^^^^^^^^^
.. code::

    databricks clusters spark-versions

Listing node types
^^^^^^^^^^^^^^^^^^^
.. code::

    databricks clusters list-node-types

Libraries CLI
--------------

You run library subcommands by appending them to ``databricks libraries``.

.. code::

  $ databricks libraries -h
  Usage: databricks libraries [OPTIONS] COMMAND [ARGS]...

    Utility to interact with libraries.

    This is a wrapper around the libraries API
    (https://docs.databricks.com/api/latest/libraries.html).

  Options:
    -v, --version  [VERSION]
    -h, --help     Show this message and exit.

  Commands:
    all-cluster-statuses  Get the status of all libraries.
    cluster-status        Get the status of all libraries for a specified
                          cluster.
    install               Install a library on a cluster.
    list                  Shortcut to `all-cluster-statuses` or `cluster-
                          status`.
    uninstall             Uninstall a library on a cluster.

Install a JAR from DBFS
^^^^^^^^^^^^^^^^^^^^^^^^

.. code::

    databricks libraries install --cluster-id $CLUSTER_ID --jar dbfs:/test-dir/test.jar

List library statuses for a cluster
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code::

    databricks libraries list --cluster-id $CLUSTER_ID


Aliasing Command Groups
--------------------------
Sometimes it can be inconvenient to prefix each CLI invocation with the name of a command group. Writing
``databricks workspace ls`` can be quite verbose! To make the CLI easier to use, you can alias different
command groups to shorter commands. For example to shorten ``databricks workspace ls`` to ``dw ls`` in the
Bourne again shell, you can add ``alias dw="databricks workspace"`` to the appropriate bash profile. Typically,
this file is located at ``~/.bash_profile``.

jq
---
Some Databricks CLI commands will output the JSON response from the API endpoint. Sometimes it can be
useful to parse out parts of the JSON to pipe into other commands. For example, to copy a job
definition, we must take the ``settings`` field of ``/api/2.0/jobs/get`` use that as an argument
to the ``databricks jobs create`` command.

In these cases, we recommend you to use the utility ``jq``. MacOS users can install ``jq`` through
Homebrew with ``brew install jq``.

For more information on ``jq`` reference its `documentation <https://stedolan.github.io/jq/>`_.

Using Docker
------------
.. code::

    # build image
    docker build -t databricks-cli .

    # run container
    docker run -it databricks-cli

    # run command in docker
    docker run -it databricks-cli fs --help
