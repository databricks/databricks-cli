from typing import Optional

from databricks_cli.sdk.preview_service import PreviewService


class PermissionsService(PreviewService):
    def __init__(self, client):
        super(PermissionsService, self).__init__('permissions')
        self.client = client

    def create_url(self, object_type, object_id, suffix=''):
        # type: (str, str, str) -> str
        return '{base}/{object_type}/{object_id}{suffix}'.format(base=self.url_base,
                                                                 object_type=object_type,
                                                                 object_id=object_id, suffix=suffix)

    def get_permissions(self, object_type, object_id, headers=None):
        # type: (str, str, Optional[dict]) -> dict
        """
        Get the permissions for an object type and id

        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/get-tokens-permissions
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/get-passwords-permissions
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/get-cluster-permissions
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/get-instance-pool-permissions
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/get-job-permissions
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/get-notebook-permissions
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/get-directory-permissions
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/get-registered-model-permissions
        """

        return self.client.perform_query('GET', self.create_url(object_type=object_type,
                                                                object_id=object_id),
                                         headers=headers)

    def get_possible_permissions(self, object_type, object_id, headers=None):
        # type: (str, str, Optional[dict]) -> dict
        """
        Get the permission levels for an object type.

        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/get-tokens-permission-levels
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/get-password-permission-levels
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/get-clusters-permission-levels
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/get-instance-pools-permission-levels
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/get-jobs-permission-levels
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/get-notebooks-permission-levels
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/get-directories-permission-levels
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/get-registered-models-permission-levels
        """

        return self.client.perform_query('GET', self.create_url(object_type=object_type,
                                                                object_id=object_id,
                                                                suffix='/permissionLevels'),
                                         headers=headers)

    def add_permissions(self, object_type, object_id, data, headers=None):
        # type: (str, str, dict, Optional[dict]) -> dict
        """
        Add permissions, this does not REMOVE.
        A remove requires an update_permissions call to complete replacement.

        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/set-tokens-permissions
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/set-password-permissions
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/set-cluster-permissions
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/set-instance-pool-permissions
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/set-job-permissions
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/set-notebook-permissions
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/set-directory-permissions
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/set-registered-model-permissions
        """

        return self.client.perform_query('PATCH', self.create_url(object_type, object_id),
                                         data=data,
                                         headers=headers)

    def update_permissions(self, object_type, object_id, data, headers=None):
        # type: (str, str, dict, Optional[dict]) -> dict
        """
        Update/Overwrite all permissions
        This overwrites all of the permissions for an object.
        This is how you remove permissions, you call update with a complete set of permissions.

        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/update-tokens-permissions
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/update-all-password-permissions
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/update-all-cluster-permissions
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/update-all-instance-pool-permissions
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/update-all-job-permissions
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/update-all-notebook-permissions
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/update-all-directory-permissions
        https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/update-registered-model-permissions
        """

        return self.client.perform_query('PUT', self.create_url(object_type, object_id),
                                         data=data,
                                         headers=headers)
