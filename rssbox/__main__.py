import logging

from rssbox import (
    accounts,
    deta,
    downloads,
    files,
    scheduler,
    watchrss_database,
    workers,
)
from rssbox.config import Config
from rssbox.handlers.deta_file_handler import DetaFileHandler
from rssbox.utils import clean_empty_dirs

from .modules.download import Download
from .modules.watchrss import WatchRSS
from .sonicbit_client import SonicBitClient

logger = logging.getLogger(__name__)


def on_new_entries(entries):
    logger.info(f"{len(entries)} new entries")
    for entry in entries:
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

file_handler = DetaFileHandler(deta, files)

sonicbit_client = SonicBitClient(accounts, downloads, workers, scheduler, file_handler)
sonicbit_client.start()
scheduler.shutdown(wait=True)
