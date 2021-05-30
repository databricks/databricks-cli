class PreviewService(object):
    """
    This class makes it easier to create preview endpoints.
    """
    PREVIEW_BASE = '/preview/'

    def __init__(self, base_url):
        self.url_base = self.create_preview_url(base_url)

    def create_preview_url(self, base_url):
        return self.PREVIEW_BASE + base_url
