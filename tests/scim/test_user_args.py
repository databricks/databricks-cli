import pytest

from databricks_cli.scim.cli import validate_user_params


@pytest.mark.parametrize('json_file, json, user_name, groups, entitlements, roles, should_fail', [
    # json_file, json, user_name, groups, entitlements, roles, should_fail
    [None, None, None, None, None, None, True],
    [None, None, 'ac', None, None, None, False],
    [None, '{}', None, None, None, None, False],
    [None, '{}', 'ac', None, None, None, True],
    ['so', None, None, None, None, None, False],
    ['so', None, 'ac', None, None, None, True],
    ['so', '{}', None, None, None, None, True],
    ['so', '{}', 'ac', None, None, None, True],

])
def test_user_args(json_file, json, user_name, groups, entitlements, roles, should_fail):
    if should_fail:
        with pytest.raises(RuntimeError):
            validate_user_params(json_file, json, user_name, groups, entitlements, roles)
    else:
        validate_user_params(json_file, json, user_name, groups, entitlements, roles)
