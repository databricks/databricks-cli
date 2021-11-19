import mock
import pytest

from mock import ANY

from databricks_cli.sdk import CommandExecutionService
from tests.utils import provide_conf


@pytest.fixture()
def cmd_exec_service():
    with mock.patch('databricks_cli.sdk.ApiClient') as api_client:
        yield CommandExecutionService(api_client)


@provide_conf
def test_get_context_status(cmd_exec_service):
    cmd_exec_service.get_context_status(cluster_id=ANY,
                                        context_id=ANY)
    cmd_exec_service.v1_client.perform_query \
        .assert_called_with(method='GET', path='/contexts/status',
                            data={'clusterId': ANY, 'contextId': ANY})


@provide_conf
def test_create_context(cmd_exec_service):
    cmd_exec_service.create_context(language=ANY, cluster_id=ANY)
    cmd_exec_service.v1_client.perform_query \
        .assert_called_with(method="POST", path="/contexts/create",
                            data={'language': ANY, 'clusterId': ANY})


@provide_conf
def test_destroy_context(cmd_exec_service):
    cmd_exec_service.destroy_context(cluster_id=ANY, context_id=ANY)
    cmd_exec_service.v1_client.perform_query \
        .assert_called_with(method="POST", path="/contexts/destroy",
                            data={'contextId': ANY, 'clusterId': ANY})


@provide_conf
def test_get_command_status(cmd_exec_service):
    cmd_exec_service.get_command_status(cluster_id=ANY, context_id=ANY, command_id=ANY)
    cmd_exec_service.v1_client.perform_query \
        .assert_called_with(method="GET", path="/commands/status",
                            data={'contextId': ANY, 'clusterId': ANY, 'commandId': ANY})


@provide_conf
def test_execute_command(cmd_exec_service):
    cmd_exec_service.execute_command(cluster_id=ANY, context_id=ANY, command=ANY, language=ANY)
    cmd_exec_service.v1_client.perform_query \
        .assert_called_with(method="POST", path="/commands/execute",
                            data={
                                'contextId': ANY, 'clusterId': ANY, 'command': ANY, 'language': ANY
                            })


@provide_conf
def test_execute_command_until_terminated(cmd_exec_service):
    cmd_exec_service.get_command_status = mock.Mock(
        side_effect=[{"status": "Running"}, {"status": "Finished"}]
    )
    cmd_exec_service.execute_command_until_terminated(
        cluster_id=ANY, context_id=ANY, command=ANY, language=ANY)
    cmd_exec_service.v1_client.perform_query \
        .assert_called_with(method="POST", path="/commands/execute",
                            data={
                                'contextId': ANY, 'clusterId': ANY, 'command': ANY, 'language': ANY
                            })
    assert cmd_exec_service.get_command_status.call_count == 2
