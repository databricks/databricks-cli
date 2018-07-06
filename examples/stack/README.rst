Stack Configuration Template JSON Schema
========================================

Example Config Template
-----------------------

.. code::

    {
      "name": "test_stack",
      "resources": [
        {
          "id": "job1",
          "type": "workspace",
          "properties": {
            "source_path": "dev/job1.py",
            "workspace_path": "/Users/example@example.com/dev/job1",
            "language": "PYTHON",
            "format": "SOURCE",
            "object_type": "NOTEBOOK"
          }
        },
        {
          "id": "example directory",
          "type": "workspace",
          "properties": {
            "source_path": "prod",
            "workspace_path": "/Users/example@example.com/example_dir",
            "object_type": "DIRECTORY"
          }
        },
        {
          "id": "job1 in dbfs",
          "type": "dbfs",
          "properties": {
            "source_path": "dev/job1.py",
            "dbfs_path": "dbfs:/example_dbfs_dir/job1.py"
          }
        },
        {
          "id": "client job test 1",
          "type": "job",
          "properties": {
            "name": "Client job test 1",
            "new_cluster": {
              "spark_version": "4.0.x-scala2.11",
              "node_type_id": "r3.xlarge",
              "aws_attributes": {
                "availability": "SPOT"
              },
              "num_workers": 3
            },
            "timeout_seconds": 7200,
            "max_retries": 1,
            "schedule": {
              "quartz_cron_expression": "0 15 22 ? * *",
              "timezone_id": "America/Los_Angeles"
            },
            "notebook_task": {
              "notebook_path": "/Users/example@example.com/job1"
            }
          }
        },
        {
          "id": "client job test 2",
          "type": "job",
          "properties": {
            "name": "client job test 2",
            "new_cluster": {
              "spark_version": "4.0.x-scala2.11",
              "node_type_id": "r3.xlarge",
              "aws_attributes": {
                "availability": "SPOT"
              },
              "num_workers": 1
            },
            "timeout_seconds": 1200,
            "max_retries": 2,
            "notebook_task": {
              "notebook_path": "/Users/example@example.com/example_dir/prod/common/prodJob"
            }
          }
        }
      ]
    }

Outer Fields
------------
``"name"``: REQUIRED- The name of the stack. When the stack deployment status is persisted, it will take the
name of <name>.json

``"resources"``: REQUIRED-  A list of stack resources. The specification of the resource fields is in the next section.

Resource Fields
---------------
``"id"``: REQUIRED- This is a unique stack identifier of the resource that the stack will use

``"type"``: ``"job"|"workspace"``- REQUIRED- The type of databricks resource that this resource is.

``"properties"``: REQUIRED- This is a JSON object of properties related to the resource and is different
depending on the type of resource

Job Resource Properties
^^^^^^^^^^^^^^^^^^^^^^^
JSON object of the Databricks `JobSettings <https://docs.databricks.com/api/latest/jobs.html#jobsettings>`_ REST API data structure.


Workspace Resource Properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
``"source_path"``: REQUIRED- Local source path of Workspace notebooks or directories.

``"workspace_path"``: REQUIRED- Matching remote Workspace paths of notebooks or directories.

``"object_type"``: ``"NOTEBOOK"|"DIRECTORY"`` REQUIRED- This specifies the whether a notebook or directory
is being managed by the stack. This corresponds with the `ObjectType <https://docs.databricks.com/api/latest/workspace.html#objecttype>`_
REST API data structure.

``"language"``: ``"SCALA"|"PYTHON"|"SQL"|"R"`` OPTIONAL- This is the language of the notebook and should
only be specified if ``"object_type=="NOTEBOOK"``. This corresponds with the Databricks `Language <https://docs.databricks.com/api/latest/workspace.html#language>`_
REST API data structure. If not provided, the language will be inferred from the file extension.

``"format"``: ``"SOURCE"|"DBC"|"HTML"|"IPYNB"`` OPTIONAL- This is the export format of the notebook.
This corresponds with the Databricks `ExportFormat <https://docs.databricks.com/api/latest/workspace.html#exportformat>`_ REST API data structure.
If not provided, will default to ``"SOURCE"``.

DBFS Resource Properties
^^^^^^^^^^^^^^^^^^^^^^^^
``"source_path"``: REQUIRED- Local source path of DBFS files or directories.

``"dbfs_path"``: REQUIRED- Matching remote DBFS path. MUST start with ``dbfs:/`` (ex. ``dbfs:/this/is/a/sample/path``)
