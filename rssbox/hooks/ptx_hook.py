import logging

from rssbox.enum import DownloadStatus
from rssbox.hooks.hook import Hook
from rssbox.modules.download import Download
from rssbox.modules.sonicbit import SonicBit

logger = logging.getLogger(__name__)


class PTXHook(Hook):
    def on_sonicbit_download_not_found(
        self, sonicbit: SonicBit, download: Download
    ) -> bool:
        return True

    def on_download_timeout(self, download: Download):
        logger.warning(f"Removing timed out download {download.name}")
        download.delete()

    def on_after_upload_error(
        self, sonicbit: SonicBit, download: Download, error: Exception
    ):
        """Called after an upload fails"""
        if download.status == DownloadStatus.ERROR:
            logger.warning(f"Removing failed download {download.name}")
            download.delete()
