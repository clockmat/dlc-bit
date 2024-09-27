import re

from feedparser import FeedParserDict

from rssbox.config import Config
from rssbox.hooks.hook import Hook
from rssbox.modules.download import Download
from rssbox.modules.errors import TorrentHashCalculationException
from rssbox.modules.sonicbit import SonicBit


class TGXHook(Hook):
    def on_new_entry(self, entry: FeedParserDict) -> FeedParserDict | bool:
        if re.search(
            r"((TV|Movies)\s*:\s*(Episodes\s*HD|Packs|HD|CAM\/TS|4K\s*UHD|Bollywood))",
            entry.category,
            re.IGNORECASE,
        ):
            return super().on_new_entry(entry)

        return False

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
