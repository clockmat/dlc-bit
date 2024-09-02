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

    def on_sonicbit_download_not_found(self, sonicbit: SonicBit, download: Download):
        """Called when a download is not found in sonicbit, return `True` to continue processing, `False` to stop"""

        return True
