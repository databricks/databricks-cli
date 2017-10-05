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

-  Python Version > 2.7.9
-  Python 3 is not supported

Installation
---------------

To install simply run
``pip install databricks-cli``

In order to upgrade your databricks-cli installation please run
``pip install --upgrade databricks-cli``

Known Issues
---------------
``AttributeError: 'module' object has no attribute 'PROTOCOL_TLSv1_2'``

For compliance reasons, our webapp requires the client to speak TLSV1.2. The built in
version of Python for MacOS does not have this version of TLS built in.

To use databricks-cli you should install a version of Python which has ``ssl.PROTOCOL_TLSv1_2``.
For MacOS, the easiest way may be to install Python with `Homebrew <https://brew.sh/>`_.

Setting Up Authentication
--------------------------
There are two ways to authenticate to Databricks. The first way is to use your username and password pair.
To do this run ``databricks configure`` and follow the prompts. The second and recommended way is to use
an access token generated from Databricks. To configure the CLI to use the access token run
``databricks configure --token``. After following the prompts, your access credentials will be stored
in the file ``~/.databrickscfg``.

Read `Token Management <https://docs.databricks.com/api/latest/authentication.html#token-management>`_ for more information about Databricks Access Tokens.

Workspace CLI Examples
-----------------------
The implemented commands for the Workspace CLI can be listed by running ``databricks workspace -h``.
Commands are run by appending them to ``databricks workspace``. To make it easier to use the workspace
CLI, feel free to alias ``databricks workspace`` to something shorter. For more information
reference `Aliasing Command Groups <https://github.com/andrewmchen/databricks-cli/tree/docs#aliasing-command-groups>`_.

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


.. _alias_databricks_cli:

Aliasing Command Groups
--------------------------
Sometimes it can be inconvenient to prefix each CLI invocation with the name of a command group. Writing
``databricks workspace ls`` can be quite verbose! To make the CLI easier to use, you can alias different
command groups to shorter commands. For example to shorten ``databricks workspace ls`` to ``dw ls`` in the
Bourne again shell, you can add ``alias dw="databricks workspace"`` to the appropriate bash profile. Typically,
this file is located at ``~/.bash_profile``.
