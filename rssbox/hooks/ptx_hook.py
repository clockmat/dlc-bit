import logging
from datetime import timedelta

from feedparser import FeedParserDict
from translators import translate_text

from rssbox.enum import DownloadStatus
from rssbox.hooks.hook import Hook
from rssbox.modules.download import Download
from rssbox.modules.sonicbit import SonicBit

logger = logging.getLogger(__name__)


class PTXHook(Hook):
    def on_new_entry(self, entry: FeedParserDict) -> FeedParserDict | bool:
        if entry.link.startswith("https://sukebei.nyaa.si/"):
            entry["title"] = translate_text(entry.title)
        return entry

    def on_sonicbit_download_not_found(
        self, sonicbit: SonicBit, download: Download
    ) -> bool:
        if sonicbit.time_taken < timedelta(hours=3):
            logger.warning(
                f"Removing large download {download.name} from sonicbit {sonicbit.id} after {sonicbit.time_taken_str}"
            )
            download.delete()
        else:
            download.mark_as_failed()

        sonicbit.mark_as_idle()
        return False

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
