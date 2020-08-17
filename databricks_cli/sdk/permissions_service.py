from typing import Optional

from databricks_cli.sdk.preview_service import PreviewService


class PermissionsService(PreviewService):
    def __init__(self, client):
        super(PermissionsService, self).__init__('permissions')
        self.client = client

    def create_url(self, object_type, object_id, suffix=''):
        # type: (str, str, str) -> str
        return '{}/{}/{}{}'.format(self.url_base, object_type, object_id, suffix)

    def get_permissions(self, object_type, object_id, headers=None):
        # type: (str, str, Optional[dict]) -> dict
        return self.client.perform_query('GET', self.create_url(object_type, object_id),
                                         headers=headers)

    def get_possible_permissions(self, object_type, object_id, headers=None):
        # type: (str, str, Optional[dict]) -> dict
        return self.client.perform_query('GET', self.create_url(object_type, object_id,
                                                                '/permissionLevels'),
                                         headers=headers)

    def add_permissions(self, object_type, object_id, data, headers=None):
        # type: (str, str, dict, Optional[dict]) -> dict
        return self.client.perform_query('PATCH', self.create_url(object_type, object_id),
                                         data=data,
                                         headers=headers)

    def update_permissions(self, object_type, object_id, data, headers=None):
        # type: (str, str, dict, Optional[dict]) -> dict
        return self.client.perform_query('PUT', self.create_url(object_type, object_id),
                                         data=data,
                                         headers=headers)
