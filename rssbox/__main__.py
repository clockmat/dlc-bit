import logging
from typing import List

from feedparser import FeedParserDict

from rssbox import accounts, downloads, scheduler, watchrss_database, workers
from rssbox.config import Config
from rssbox.handlers.file_handler import FileHandler
from rssbox.hooks.hook import Hook
from rssbox.utils import clean_empty_dirs

from .modules.download import Download
from .modules.watchrss import WatchRSS
from .sonicbit_client import SonicBitClient
from .handlers.ptx_file_handler import PTXFileHandler

logger = logging.getLogger(__name__)
hook = Hook()


def on_new_entries(entries: List[FeedParserDict]):
    logger.info(f"{len(entries)} new entries")
    for entry in entries:
        if entry_result := hook.on_new_entry(entry):
            if isinstance(entry_result, FeedParserDict):
                entry = entry_result

            try:
                Download.from_entry(client=downloads, entry=entry).create()
            except Exception as e:
                logging.error(f"Error while adding download to database: {e}")

    return True


clean_empty_dirs(Config.DOWNLOAD_PATH)
watchrss = WatchRSS(
    url=Config.RSS_URL,
    db=watchrss_database,
    callback=on_new_entries,
    check_confirmation=True,
)
watchrss.check()
scheduler.add_job(watchrss.check, "interval", minutes=1, id="watchrss_check")

file_handler = PTXFileHandler()

sonicbit_client = SonicBitClient(
    accounts, downloads, workers, scheduler, file_handler, hook
)
sonicbit_client.start()
scheduler.shutdown(wait=True)
