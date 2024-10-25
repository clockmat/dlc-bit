import logging

from feedparser import FeedParserDict

from rssbox.config import Config
from rssbox.enum import DownloadStatus
from rssbox.modules.download import Download
from rssbox.modules.errors import TooLargeTorrentError, TorrentHashCalculationError
from rssbox.modules.sonicbit import SonicBit

logger = logging.getLogger(__name__)


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
        if isinstance(error, TooLargeTorrentError):
            logger.info(f"Stopping large torrent: {download.name}")
            download.mark_as_too_large()
            sonicbit.mark_as_idle()
            return False

        if isinstance(error, TorrentHashCalculationError):
            logger.info(f"Stopping invalid torrent: {download.name}")
            download._stop_with_status(
                DownloadStatus.INVALID_TORRENT, Config.DOWNLOAD_ERROR_RECORD_EXPIRY
            )
            sonicbit.mark_as_idle()
            return False

        return True
