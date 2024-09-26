from feedparser import FeedParserDict

from rssbox.modules.download import Download
from rssbox.modules.sonicbit import SonicBit


class Hook:
    """Base class for hooks"""

    def __init__(self):
        pass

    def on_new_entry(self, entry: FeedParserDict) -> FeedParserDict | bool:
        """Called when a new entry is added to the database, return `True` to continue processing, `False` to stop or make changes in entry and return `FeedParserDict`"""
        return entry

    def on_sonicbit_download_not_found(
        self, sonicbit: SonicBit, download: Download
    ) -> bool:
        """Called when a download is not found in sonicbit, return `True` to continue processing, `False` to stop"""

        return True

    def on_download_timeout(self, download: Download):
        """Called when a download times out"""

    def on_before_upload_error(
        self, sonicbit: SonicBit, download: Download, error: Exception
    ) -> bool:
        """Called when an upload fails, return `True` for soft failure or `False` to mark the download as failed"""
        return False

    def on_after_upload_error(
        self, sonicbit: SonicBit, download: Download, error: Exception
    ):
        """Called after an upload fails"""

    def on_upload_complete(
        self, sonicbit: SonicBit, download_dict: dict, files_uploaded: int
    ):
        """Called when an upload is complete"""

    def on_add_download_error(
        self, sonicbit: SonicBit, download: Download, error: Exception
    ) -> bool:
        """Called when an add download fails"""
        return True
