import logging

from feedparser import FeedParserDict
from translators import translate_text

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
        download.mark_as_failed()
        sonicbit.mark_as_idle()
        return False
