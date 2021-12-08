import mock
import pytest

from mock import ANY

from databricks_cli.sdk.v1_service import CommandService, ExecutionContextService


@pytest.fixture()
def cmd_service():
    with mock.patch('databricks_cli.sdk.ApiClient') as api_client:
        yield CommandService(api_client)


@pytest.fixture()
def exec_ctx_service():
    with mock.patch('databricks_cli.sdk.ApiClient') as api_client:
        yield ExecutionContextService(api_client)


class TestExecutionContextService:
    def test_get_context_status(self, exec_ctx_service):
        exec_ctx_service.get_context_status(cluster_id=ANY,
                                            context_id=ANY)
        exec_ctx_service.v1_client.perform_query \
            .assert_called_with(method='GET', path='/contexts/status',
                                data={'clusterId': ANY, 'contextId': ANY})

    def test_create_context(self, exec_ctx_service):
        exec_ctx_service.create_context(language=ANY, cluster_id=ANY)
        exec_ctx_service.v1_client.perform_query \
            .assert_called_with(method="POST", path="/contexts/create",
                                data={'language': ANY, 'clusterId': ANY})

    def test_destroy_context(self, exec_ctx_service):
        exec_ctx_service.destroy_context(cluster_id=ANY, context_id=ANY)
        exec_ctx_service.v1_client.perform_query \
            .assert_called_with(method="POST", path="/contexts/destroy",
                                data={'contextId': ANY, 'clusterId': ANY})


class TestCommandService:
    def test_get_command_status(self, cmd_service):
        cmd_service.get_command_status(cluster_id=ANY, context_id=ANY, command_id=ANY)
        cmd_service.v1_client.perform_query \
            .assert_called_with(method="GET", path="/commands/status",
                                data={'contextId': ANY, 'clusterId': ANY, 'commandId': ANY})

    def test_execute_command(self, cmd_service):
        cmd_service.execute_command(cluster_id=ANY, context_id=ANY, command=ANY, language=ANY)
        cmd_service.v1_client.perform_query \
            .assert_called_with(method="POST", path="/commands/execute",
                                data={
                                    'contextId': ANY, 'clusterId': ANY, 'command': ANY,
                                    'language': ANY
                                })
