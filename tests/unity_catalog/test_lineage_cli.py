# pylint:disable=redefined-outer-name

import mock
import pytest
from click.testing import CliRunner
from databricks_cli.unity_catalog.utils import mc_pretty_format

from databricks_cli.unity_catalog import lineage_cli
from tests.utils import provide_conf


@pytest.fixture()
def lineage_api_mock():
    with mock.patch('databricks_cli.unity_catalog.lineage_cli.UnityCatalogApi') as uc_api_mock:
        _lineage_api_mock = mock.MagicMock()
        uc_api_mock.return_value = _lineage_api_mock
        yield _lineage_api_mock


RUN_PAGE_URL = '/lineage-tracking/column-lineage/get'
TABLE_NAME = 'main.lineage.source'
COLUMN_NAME = 'price'

EXPECTED_OUTPUT = '''digraph "lineage graph of main.lineage.source" {
\t"main.lineage.source" -> "main.lineage.target1","main.lineage.target2","main.lineage.target3";
\t"main.lineage.target1" -> "main.lineage.target3";
\t"main.lineage.target3" -> "main.lineage.target4";
}'''


@provide_conf
def test_get_table_lineage(lineage_api_mock):
    """
      +--------------------------+
      |                          |
      |                          v
      |    +--->target1------->target3------>target4
      |    |
    source-|
           |
           +--->target2
    """
    with mock.patch('databricks_cli.unity_catalog.lineage_cli.click.echo') as echo_mock:
        # lineage_api_mock.list_lineages_by_table_recursive.return_value = {
        #     'source': ['target1', 'target2', 'target3'],
        #     'target1': ['target3'],
        #     'target3': ['target4']
        # }
        lineage_api_mock.list_lineages_by_table.side_effect = [
            {
                'downstream_tables':
                    [
                        {'catalog_name': 'main', 'schema_name': 'lineage', 'name': 'target1'},
                        {'catalog_name': 'main', 'schema_name': 'lineage', 'name': 'target2'},
                        {'catalog_name': 'main', 'schema_name': 'lineage', 'name': 'target3'}
                    ],
                'upstream_tables': []
            },  # source downstream

            {
                'downstream_tables':
                    [
                        {'catalog_name': 'main', 'schema_name': 'lineage', 'name': 'target3'}
                    ],
                'upstream_tables': []
            },  # target1 downstream

            {},  # target2 downstream

            {
                'downstream_tables':
                    [
                        {'catalog_name': 'main', 'schema_name': 'lineage', 'name': 'target4'}
                    ],
                'upstream_tables': []
            },  # target3 downstream

            {},  # target4 downstream

            {
                'downstream_tables':
                    [
                        {'catalog_name': 'main', 'schema_name': 'lineage', 'name': 'target1'},
                        {'catalog_name': 'main', 'schema_name': 'lineage', 'name': 'target2'},
                        {'catalog_name': 'main', 'schema_name': 'lineage', 'name': 'target3'}
                    ],
                'upstream_tables': []
            },  # 1st time downstream
        ]
        runner = CliRunner()
        runner.invoke(
            cli=lineage_cli.list_table_lineages_cli,
            args=['--table-name', TABLE_NAME, '--level', 3]
        )
        assert echo_mock.call_args[0][0] == EXPECTED_OUTPUT


EMPTY_LINEAGE_OUTPUT = '''digraph "lineage graph of main.lineage.source" {
\t
}'''


@provide_conf
def test_get_table_lineage_with_empty_result(lineage_api_mock):
    with mock.patch('databricks_cli.unity_catalog.lineage_cli.click.echo') as echo_mock:
        lineage_api_mock.list_lineages_by_table_recursive.return_value = {}
        runner = CliRunner()
        runner.invoke(cli=lineage_cli.list_table_lineages_cli, args=['--table-name', TABLE_NAME])
        assert echo_mock.call_args[0][0] == EMPTY_LINEAGE_OUTPUT


COLUMN_LINEAGE_OUTPUT = '''{
      "downstream_cols": [
        {
          "workspace_id": 6051921418418893,
          "table_type": "TABLE",
          "catalog_name": "main",
          "table_name": "dinner_price",
          "schema_name": "lineage",
          "name": "full_menu"
        }
      ],
      "upstream_cols": [
        {
          "workspace_id": 6051921418418893,
          "table_type": "TABLE",
          "catalog_name": "main",
          "table_name": "menu",
          "schema_name": "lineage",
          "name": "app"
        },
        {
          "workspace_id": 6051921418418893,
          "table_type": "TABLE",
          "catalog_name": "main",
          "table_name": "menu",
          "schema_name": "lineage",
          "name": "desert"
        }
      ]
    }'''


@provide_conf
def test_get_column_lineage(lineage_api_mock):
    with mock.patch('databricks_cli.unity_catalog.lineage_cli.click.echo') as echo_mock:
        lineage_api_mock.list_lineages_by_column.return_value = COLUMN_LINEAGE_OUTPUT
        runner = CliRunner()
        runner.invoke(
            cli=lineage_cli.list_column_lineages_cli,
            args=['--table-name', TABLE_NAME, '--column-name', COLUMN_NAME]
        )
        assert sorted(echo_mock.call_args[0][0]) == sorted(mc_pretty_format(COLUMN_LINEAGE_OUTPUT))
