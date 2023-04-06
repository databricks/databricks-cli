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

# pylint:disable=redefined-outer-name

import mock
import pytest
from click.testing import CliRunner
from databricks_cli.unity_catalog.utils import mc_pretty_format

from databricks_cli.unity_catalog import delta_sharing_cli
from tests.utils import provide_conf

SHARE_NAME = 'test_share'
SHARES = {
    'shares': [
        {
            'name': SHARE_NAME
        }
    ]
}
SHARE = {
    'name': SHARE_NAME
}
SHARE_PERMISSIONS = {
    'privilege_assignments': [
        {
            'principal': 'test_recipient',
            'privileges': [
                'SELECT'
            ]
        }
    ]
}

RECIPIENT_NAME = 'test_recipient'
RECIPIENTS = {
    'recipients': [
        {
            'name': RECIPIENT_NAME
        }
    ]
}
RECIPIENT = {
    'name': RECIPIENT_NAME
}
RECIPIENT_PERMISSIONS = {
    'share_name': SHARE_NAME,
    'privilege_assignments': [
        {
            'principal': 'test_recipient',
            'privileges': [
                'SELECT'
            ]
        }
    ]
}

PROVIDER_NAME = 'test_provider'
PROVIDERS = {
    'providers': [
        {
            'name': PROVIDER_NAME
        }
    ]
}
PROVIDER = {
    'name': PROVIDER_NAME
}
PROVIDER_SHARES = {
    'shares': [
        {
            'name': SHARE_NAME
        }
    ]
}


@pytest.fixture()
def api_mock():
    with mock.patch(
            'databricks_cli.unity_catalog.delta_sharing_cli.UnityCatalogApi') as uc_api_mock:
        _delta_sharing_api_mock = mock.MagicMock()
        uc_api_mock.return_value = _delta_sharing_api_mock
        yield _delta_sharing_api_mock


@pytest.fixture()
def echo_mock():
    with mock.patch('databricks_cli.unity_catalog.delta_sharing_cli.click.echo') as echo_mock:
        yield echo_mock


############################################################
#                                                          #
#                          SHARES                          #
#                                                          #
############################################################


@provide_conf
def test_create_share_cli(api_mock, echo_mock):
    api_mock.create_share.return_value = SHARE
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.create_share_cli,
        args=['--name', SHARE_NAME])
    api_mock.create_share.assert_called_once_with(SHARE_NAME)
    echo_mock.assert_called_once_with(mc_pretty_format(SHARE))


@provide_conf
def test_list_shares_cli(api_mock, echo_mock):
    api_mock.list_shares.return_value = SHARES
    runner = CliRunner()
    runner.invoke(delta_sharing_cli.list_shares_cli)
    api_mock.list_shares.assert_called_once()
    echo_mock.assert_called_once_with(mc_pretty_format(SHARES))


@provide_conf
def test_get_share_cli(api_mock, echo_mock):
    api_mock.get_share.return_value = SHARE
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.get_share_cli,
        args=['--name', SHARE_NAME])
    api_mock.get_share.assert_called_once_with(SHARE_NAME, True)
    echo_mock.assert_called_once_with(mc_pretty_format(SHARE))


@provide_conf
def test_get_share_cli_exclude_shared_data(api_mock, echo_mock):
    api_mock.get_share.return_value = SHARE
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.get_share_cli,
        args=['--name', SHARE_NAME, '--include-shared-data', False])
    api_mock.get_share.assert_called_once_with(SHARE_NAME, False)
    echo_mock.assert_called_once_with(mc_pretty_format(SHARE))


@provide_conf
def test_update_share_cli(api_mock, echo_mock):
    api_mock.update_share.return_value = SHARE
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.update_share_cli,
        args=[
            '--name', SHARE_NAME,
            '--new-name', 'new_share_name',
            '--comment', 'comment',
            '--owner', 'owner',
            '--add-table', 'add.table.one',
            '--add-table', 'add.table.two',
            '--remove-table', 'remove.table.one',
            '--remove-table', 'remove.table.two'
        ])
    expected_data = {
        'name': 'new_share_name',
        'comment': 'comment',
        'owner': 'owner',
        'updates': [
            {
                'action': 'ADD',
                'data_object': {
                    'data_object_type': 'TABLE',
                    'name': 'add.table.one'
                }
            },
            {
                'action': 'ADD',
                'data_object': {
                    'data_object_type': 'TABLE',
                    'name': 'add.table.two'
                }
            },
            {
                'action': 'REMOVE',
                'data_object': {
                    'data_object_type': 'TABLE',
                    'name': 'remove.table.one'
                }
            },
            {
                'action': 'REMOVE',
                'data_object': {
                    'data_object_type': 'TABLE',
                    'name': 'remove.table.two'
                }
            }
        ]
    }
    api_mock.update_share.assert_called_once_with(SHARE_NAME, expected_data)
    echo_mock.assert_called_once_with(mc_pretty_format(SHARE))

@provide_conf
def test_add_share_schema_with_comment_cli(api_mock, echo_mock):
    api_mock.update_share.return_value = SHARE
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.add_share_schema_cli,
        args=[
            '--share', SHARE_NAME,
            '--schema', 'catalog.schema',
            '--comment', 'add.comment',
        ])
    expected_data = {
        'updates': [
            {
                'action': 'ADD',
                'data_object': {
                    'data_object_type': 'SCHEMA',
                    'name': 'catalog.schema',
                    'comment': 'add.comment',
                }
            }
        ]
    }
    api_mock.update_share.assert_called_once_with(SHARE_NAME, expected_data)
    echo_mock.assert_called_once_with(mc_pretty_format(SHARE))


@provide_conf
def test_add_share_schema_cli(api_mock, echo_mock):
    api_mock.update_share.return_value = SHARE
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.add_share_schema_cli,
        args=[
            '--share', SHARE_NAME,
            '--schema', 'catalog.schema',
        ])
    expected_data = {
        'updates': [
            {
                'action': 'ADD',
                'data_object': {
                    'data_object_type': 'SCHEMA',
                    'name': 'catalog.schema',
                }
            }
        ]
    }
    api_mock.update_share.assert_called_once_with(SHARE_NAME, expected_data)
    echo_mock.assert_called_once_with(mc_pretty_format(SHARE))


@provide_conf
def test_update_share_schema_cli(api_mock, echo_mock):
    api_mock.update_share.return_value = SHARE
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.update_share_schema_cli,
        args=[
            '--share', SHARE_NAME,
            '--schema', 'catalog.schema',
            '--comment', 'update.comment',
        ])
    expected_data = {
        'updates': [
            {
                'action': 'UPDATE',
                'data_object': {
                    'data_object_type': 'SCHEMA',
                    'name': 'catalog.schema',
                    'comment': 'update.comment',
                }
            }
        ]
    }
    api_mock.update_share.assert_called_once_with(SHARE_NAME, expected_data)
    echo_mock.assert_called_once_with(mc_pretty_format(SHARE))


@provide_conf
def test_remove_share_schema_cli(api_mock, echo_mock):
    api_mock.update_share.return_value = SHARE
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.remove_share_schema_cli,
        args=[
            '--share', SHARE_NAME,
            '--schema', 'catalog.schema',
        ])
    expected_data = {
        'updates': [
            {
                'action': 'REMOVE',
                'data_object': {
                    'data_object_type': 'SCHEMA',
                    'name': 'catalog.schema',
                }
            }
        ]
    }
    api_mock.update_share.assert_called_once_with(SHARE_NAME, expected_data)
    echo_mock.assert_called_once_with(mc_pretty_format(SHARE))


@provide_conf
def test_add_share_table_cli(api_mock, echo_mock):
    api_mock.update_share.return_value = SHARE
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.add_share_table_cli,
        args=[
            '--share', SHARE_NAME,
            '--table', 'add.table',
            '--shared-as', 'catalog.schema.table',
            '--comment', 'add.comment',
            '--partitions', '[{"values": [{"name": "a", "op": "EQUAL", "value": "1"}]}]',
            '--cdf',
            '--start-version', '1'
        ])
    expected_data = {
        'updates': [
            {
                'action': 'ADD',
                'data_object': {
                    'data_object_type': 'TABLE',
                    'name': 'add.table',
                    'comment': 'add.comment',
                    'shared_as': 'catalog.schema.table',
                    'cdf_enabled': True,
                    'partitions': [
                        {
                            'values': [
                                {
                                    'name': 'a',
                                    'op': 'EQUAL',
                                    'value': '1',
                                },
                            ]
                        },
                    ],
                    'start_version': 1
                }
            }
        ]
    }
    api_mock.update_share.assert_called_once_with(SHARE_NAME, expected_data)
    echo_mock.assert_called_once_with(mc_pretty_format(SHARE))


@provide_conf
def test_update_share_table_cli(api_mock, echo_mock):
    api_mock.update_share.return_value = SHARE
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.update_share_table_cli,
        args=[
            '--share', SHARE_NAME,
            '--table', 'update.table',
            '--shared-as', 'catalog.schema.table',
            '--comment', 'update.comment',
            '--partitions', '[{"values": [{"name": "a", "op": "EQUAL", "value": "1"}]}]',
            '--cdf',
            '--start-version', '1'
        ])
    expected_data = {
        'updates': [
            {
                'action': 'UPDATE',
                'data_object': {
                    'data_object_type': 'TABLE',
                    'name': 'update.table',
                    'comment': 'update.comment',
                    'shared_as': 'catalog.schema.table',
                    'cdf_enabled': True,
                    'partitions': [
                        {
                            'values': [
                                {
                                    'name': 'a',
                                    'op': 'EQUAL',
                                    'value': '1',
                                },
                            ]
                        },
                    ],
                    'start_version': 1
                }
            }
        ]
    }
    api_mock.update_share.assert_called_once_with(SHARE_NAME, expected_data)
    echo_mock.assert_called_once_with(mc_pretty_format(SHARE))


@provide_conf
def test_remove_share_table_cli_by_table(api_mock, echo_mock):
    api_mock.update_share.return_value = SHARE
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.remove_share_table_cli,
        args=[
            '--share', SHARE_NAME,
            '--table', 'remove.table',
        ])
    expected_data = {
        'updates': [
            {
                'action': 'REMOVE',
                'data_object': {
                    'data_object_type': 'TABLE',
                    'name': 'remove.table',
                }
            }
        ]
    }
    api_mock.update_share.assert_called_once_with(SHARE_NAME, expected_data)
    echo_mock.assert_called_once_with(mc_pretty_format(SHARE))


@provide_conf
def test_remove_share_table_cli_by_shared_as(api_mock, echo_mock):
    api_mock.update_share.return_value = SHARE
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.remove_share_table_cli,
        args=[
            '--share', SHARE_NAME,
            '--shared-as', 'catalog.schema.table',
        ])
    expected_data = {
        'updates': [
            {
                'action': 'REMOVE',
                'data_object': {
                    'data_object_type': 'TABLE',
                    'shared_as': 'catalog.schema.table',
                }
            }
        ]
    }
    api_mock.update_share.assert_called_once_with(SHARE_NAME, expected_data)
    echo_mock.assert_called_once_with(mc_pretty_format(SHARE))


@provide_conf
def test_remove_share_table_cli_asserts_error_if_both_specified(api_mock):
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.remove_share_table_cli,
        args=[
            '--share', SHARE_NAME,
            '--table', 'remove.table',
            '--shared-as', 'catalog.schema.table'
        ]
    )
    assert not api_mock.update_share.called

@provide_conf
def test_update_share_cli_with_json(api_mock, echo_mock):
    api_mock.update_share.return_value = SHARE
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.update_share_cli,
        args=[
            '--name', SHARE_NAME,
            '--json', '{ "name": "new_share_name" }'
        ])
    api_mock.update_share.assert_called_once_with(
        SHARE_NAME,
        {
            'name': 'new_share_name'
        })
    echo_mock.assert_called_once_with(mc_pretty_format(SHARE))


@provide_conf
def test_update_share_cli_fails_with_params_and_json():
    runner = CliRunner()
    result = runner.invoke(
        delta_sharing_cli.update_share_cli,
        args=[
            '--name', SHARE_NAME,
            '--new-name', 'new_share_name',
            '--json', '{ "name": "new_share_name" }'
        ])
    assert result.exit_code == 1


@provide_conf
def test_delete_share_cli(api_mock):
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.delete_share_cli,
        args=[
            '--name', SHARE_NAME
        ])
    api_mock.delete_share.assert_called_once_with(SHARE_NAME)
    

@provide_conf
def test_list_share_permissions_cli(api_mock, echo_mock):
    api_mock.list_share_permissions.return_value = SHARE_PERMISSIONS
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.list_share_permissions_cli,
        args=[
            '--name', SHARE_NAME
        ])
    api_mock.list_share_permissions.assert_called_once_with(SHARE_NAME)
    echo_mock.assert_called_once_with(mc_pretty_format(SHARE_PERMISSIONS))


@provide_conf
def test_update_share_permissions_cli(api_mock, echo_mock):
    api_mock.update_share_permissions.return_value = SHARE_PERMISSIONS
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.update_share_permissions_cli,
        args=[
            '--name', SHARE_NAME,
            '--json', '{ "changes": [ { "principal": "test_recipient", "add": [ "SELECT" ] } ] }'
        ])
    expected_data = {
        'changes': [
            {
                'principal': 'test_recipient',
                'add': ['SELECT']
            }
        ]
    }
    api_mock.update_share_permissions.assert_called_once_with(SHARE_NAME, expected_data)
    echo_mock.assert_called_once_with(mc_pretty_format(SHARE_PERMISSIONS))


############################################################
#                                                          #
#                        RECIPIENTS                        #
#                                                          #
############################################################


@provide_conf
def test_create_recipient_cli(api_mock, echo_mock):
    api_mock.create_recipient.return_value = RECIPIENT
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.create_recipient_cli,
        args=[
            '--name', RECIPIENT_NAME,
            '--comment', 'comment',
            '--allowed-ip-address', '8.8.8.8',
            '--allowed-ip-address', '8.8.4.4',
            '--property', 'k1=v1',
            '--property', 'k2=v2'
        ])
    api_mock.create_recipient.assert_called_once_with(
        RECIPIENT_NAME,
        'comment',
        None,
        ('8.8.8.8', '8.8.4.4'),
        [{"key": "k1", "value": "v1"}, {"key": "k2", "value": "v2"}]
    )
    echo_mock.assert_called_once_with(mc_pretty_format(RECIPIENT))
    

@provide_conf
def test_create_recipient_cli_invalid_custom_property(api_mock):
    api_mock.create_recipient.return_value = RECIPIENT
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.create_recipient_cli,
        args=[
            '--name', RECIPIENT_NAME,
            '--property', 'k1=v1=v2'
        ])

    assert not api_mock.create_recipient.called


@provide_conf
def test_create_recipient_cli_with_sharing_id(api_mock, echo_mock):
    api_mock.create_recipient.return_value = RECIPIENT
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.create_recipient_cli,
        args=[
            '--name', RECIPIENT_NAME,
            '--sharing-id', '123e4567-e89b-12d3-a456-426614174000'
        ])
    api_mock.create_recipient.assert_called_once_with(
        RECIPIENT_NAME, None, '123e4567-e89b-12d3-a456-426614174000', (), [])
    echo_mock.assert_called_once_with(mc_pretty_format(RECIPIENT))


@provide_conf
def test_list_recipients_cli(api_mock, echo_mock):
    api_mock.list_recipients.return_value = RECIPIENTS
    runner = CliRunner()
    runner.invoke(delta_sharing_cli.list_recipients_cli)
    api_mock.list_recipients.assert_called_once()
    echo_mock.assert_called_once_with(mc_pretty_format(RECIPIENTS))


@provide_conf
def test_get_recipient_cli(api_mock, echo_mock):
    api_mock.get_recipient.return_value = RECIPIENT
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.get_recipient_cli,
        args=['--name', RECIPIENT_NAME])
    api_mock.get_recipient.assert_called_once_with(RECIPIENT_NAME)
    echo_mock.assert_called_once_with(mc_pretty_format(RECIPIENT))


@provide_conf
def test_update_recipient_cli(api_mock, echo_mock):
    api_mock.update_recipient.return_value = RECIPIENT
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.update_recipient_cli,
        args=[
            '--name', RECIPIENT_NAME,
            '--new-name', 'new_recipient_name',
            '--comment', 'comment',
            '--owner', 'owner',
            '--allowed-ip-address', '8.8.8.8',
            '--allowed-ip-address', '8.8.4.4',
            '--property', 'k1=v1',
            '--property', 'k2=v2'
        ])
    expected_data = {
        'name': 'new_recipient_name',
        'comment': 'comment',
        'owner': 'owner',
        'ip_access_list': {
            'allowed_ip_addresses': (
                '8.8.8.8',
                '8.8.4.4'
            )
        },
        'properties_kvpairs': {
            'properties': [
                {"key": "k1", "value": "v1"}, 
                {"key": "k2", "value": "v2"}
            ]
        }
    }
    api_mock.update_recipient.assert_called_once_with(RECIPIENT_NAME, expected_data)
    echo_mock.assert_called_once_with(mc_pretty_format(RECIPIENT))


@provide_conf
def test_update_recipient_cli_with_json(api_mock, echo_mock):
    api_mock.update_recipient.return_value = RECIPIENT
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.update_recipient_cli,
        args=[
            '--name', RECIPIENT_NAME,
            '--json', '{ "name": "new_recipient_name" }'
        ])
    api_mock.update_recipient.assert_called_once_with(
        RECIPIENT_NAME,
        {
            'name': 'new_recipient_name'
        })
    echo_mock.assert_called_once_with(mc_pretty_format(RECIPIENT))


@provide_conf
def test_update_recipient_cli_fails_with_params_and_json():
    runner = CliRunner()
    result = runner.invoke(
        delta_sharing_cli.update_recipient_cli,
        args=[
            '--name', RECIPIENT_NAME,
            '--new-name', 'new_recipient_name',
            '--json', '{ "name": "new_recipient_name" }'
        ])
    assert result.exit_code == 1


@provide_conf
def test_rotate_recipient_token_cli(api_mock, echo_mock):
    api_mock.rotate_recipient_token.return_value = RECIPIENT
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.rotate_recipient_token_cli,
        args=[
            '--name', RECIPIENT_NAME,
            '--existing-token-expire-in-seconds', 3600
        ])
    api_mock.rotate_recipient_token.assert_called_once_with(
        RECIPIENT_NAME,
        3600)
    echo_mock.assert_called_once_with(mc_pretty_format(RECIPIENT))


@provide_conf
def test_list_recipient_permissions_cli(api_mock, echo_mock):
    api_mock.get_recipient_share_permissions.return_value = RECIPIENT_PERMISSIONS
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.list_recipient_permissions_cli,
        args=[
            '--name', RECIPIENT_NAME
        ])
    api_mock.get_recipient_share_permissions.assert_called_once_with(RECIPIENT_NAME)
    echo_mock.assert_called_once_with(mc_pretty_format(RECIPIENT_PERMISSIONS))


@provide_conf
def test_delete_recipient_cli(api_mock):
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.delete_recipient_cli,
        args=[
            '--name', RECIPIENT_NAME
        ])
    api_mock.delete_recipient.assert_called_once_with(RECIPIENT_NAME)


############################################################
#                                                          #
#                        PROVIDERS                         #
#                                                          #
############################################################


@provide_conf
def test_create_provider_cli(api_mock, echo_mock):
    api_mock.create_provider.return_value = PROVIDER
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.create_provider_cli,
        args=[
            '--name', PROVIDER_NAME,
            '--comment', 'comment',
            '--recipient-profile-json',
            '{ "shareCredentialsVersion": 1, "bearerToken": "TOKEN", '
            '"endpoint": "https://endpoint.com" }'
        ])
    expected_profile = {
        'shareCredentialsVersion': 1,
        'bearerToken': 'TOKEN',
        'endpoint': 'https://endpoint.com'
    }
    api_mock.create_provider.assert_called_once_with(
        PROVIDER_NAME, 'comment', expected_profile)
    echo_mock.assert_called_once_with(mc_pretty_format(PROVIDER))
    

@provide_conf
def test_create_provider_cli_fails_without_recipient_profile(api_mock):
    api_mock.create_provider.return_value = PROVIDER
    runner = CliRunner()
    result = runner.invoke(
        delta_sharing_cli.create_provider_cli,
        args=[
            '--name', PROVIDER_NAME,
        ])
    assert result.exit_code == 1


@provide_conf
def test_list_providers_cli(api_mock, echo_mock):
    api_mock.list_providers.return_value = PROVIDERS
    runner = CliRunner()
    runner.invoke(delta_sharing_cli.list_providers_cli)
    api_mock.list_providers.assert_called_once()
    echo_mock.assert_called_once_with(mc_pretty_format(PROVIDERS))


@provide_conf
def test_get_provider_cli(api_mock, echo_mock):
    api_mock.get_provider.return_value = PROVIDER
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.get_provider_cli,
        args=['--name', PROVIDER_NAME])
    api_mock.get_provider.assert_called_once_with(PROVIDER_NAME)
    echo_mock.assert_called_once_with(mc_pretty_format(PROVIDER))


@provide_conf
def test_update_provider_cli(api_mock, echo_mock):
    api_mock.update_provider.return_value = PROVIDER
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.update_provider_cli,
        args=[
            '--name', PROVIDER_NAME,
            '--new-name', 'new_provider_name',
            '--comment', 'comment',
            '--owner', 'owner'
        ])
    expected_data = {
        'name': 'new_provider_name',
        'comment': 'comment',
        'owner': 'owner'
    }
    api_mock.update_provider.assert_called_once_with(PROVIDER_NAME, provider_spec=expected_data)
    echo_mock.assert_called_once_with(mc_pretty_format(PROVIDER))


@provide_conf
def test_update_provider_cli_with_recipient_profile(api_mock, echo_mock):
    api_mock.update_provider.return_value = PROVIDER
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.update_provider_cli,
        args=[
            '--name', PROVIDER_NAME,
            '--recipient-profile-json',
            '{ "shareCredentialsVersion": 1, "bearerToken": "TOKEN", '
            '"endpoint": "https://endpoint.com" }'
        ])
    expected_profile = {
        'shareCredentialsVersion': 1,
        'bearerToken': 'TOKEN',
        'endpoint': 'https://endpoint.com'
    }
    expected_data = {
        'name': None,
        'comment': None,
        'owner': None,
        'recipient_profile_str': mc_pretty_format(expected_profile)
    }
    api_mock.update_provider.assert_called_once_with(PROVIDER_NAME, provider_spec=expected_data)
    echo_mock.assert_called_once_with(mc_pretty_format(PROVIDER))


@provide_conf
def test_update_provider_cli_with_json(api_mock, echo_mock):
    api_mock.update_provider.return_value = PROVIDER
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.update_provider_cli,
        args=[
            '--name', PROVIDER_NAME,
            '--json', '{ "name": "new_provider_name" }'
        ])
    api_mock.update_provider.assert_called_once_with(
        PROVIDER_NAME,
        provider_spec={
            'name': 'new_provider_name'
        })
    echo_mock.assert_called_once_with(mc_pretty_format(PROVIDER))


@provide_conf
def test_update_provider_cli_fails_with_params_and_json():
    runner = CliRunner()
    result = runner.invoke(
        delta_sharing_cli.update_provider_cli,
        args=[
            '--name', PROVIDER_NAME,
            '--new-name', 'new_provider_name',
            '--json', '{ "name": "new_provider_name" }'
        ])
    assert result.exit_code == 1


@provide_conf
def test_list_provider_shares_cli(api_mock, echo_mock):
    api_mock.list_provider_shares.return_value = PROVIDER_SHARES
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.list_provider_shares_cli,
        args=[
            '--name', PROVIDER_NAME
        ])
    api_mock.list_provider_shares.assert_called_once_with(PROVIDER_NAME)
    echo_mock.assert_called_once_with(mc_pretty_format(PROVIDER_SHARES))


@provide_conf
def test_delete_provider_cli(api_mock):
    runner = CliRunner()
    runner.invoke(
        delta_sharing_cli.delete_provider_cli,
        args=[
            '--name', PROVIDER_NAME
        ])
    api_mock.delete_provider.assert_called_once_with(PROVIDER_NAME)
