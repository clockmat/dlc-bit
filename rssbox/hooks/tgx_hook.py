import logging
import re
from datetime import timedelta

from feedparser import FeedParserDict

from rssbox.config import Config
from rssbox.hooks.hook import Hook
from rssbox.modules.download import Download
from rssbox.modules.errors import TorrentHashCalculationException
from rssbox.modules.sonicbit import SonicBit

logger = logging.getLogger(__name__)


class TGXHook(Hook):
    def on_new_entry(self, entry: FeedParserDict) -> FeedParserDict | bool:
        if re.search(
            r"((TV|Movies)\s*:\s*(Episodes\s*HD|Packs|HD|CAM\/TS|4K\s*UHD|Bollywood))",
            entry.category,
            re.IGNORECASE,
        ):
            return super().on_new_entry(entry)

        return False

    def on_sonicbit_download_not_found(
        self, sonicbit: SonicBit, download: Download
    ) -> bool:
        if sonicbit.time_taken < timedelta(minutes=5):
            logger.warning(
                f"Stopping large download {download.name} from sonicbit {sonicbit.id} after {sonicbit.time_taken_str}"
            )
            download.mark_as_too_large()
            sonicbit.mark_as_idle()
            return False

        return super().on_sonicbit_download_not_found(sonicbit, download)

    def on_add_download_error(
        self, sonicbit: SonicBit, download: Download, error: Exception
    ) -> bool:
        if isinstance(error, TorrentHashCalculationException):
            download._stop_with_status(
                Download.INVALID_TORRENT, Config.DOWNLOAD_ERROR_RECORD_EXPIRY
            )
            sonicbit.mark_as_idle()
            return False

        return super().on_add_download_error(sonicbit, download, error)
