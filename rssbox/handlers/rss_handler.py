import logging
import random
from datetime import datetime, timedelta
from threading import Thread
from typing import List

from apscheduler.schedulers.base import BaseScheduler
from feedparser import FeedParserDict
from pymongo.collection import Collection

from rssbox.hooks.hook import Hook
from rssbox.modules.download import Download
from rssbox.modules.watchrss import WatchRSS
from rssbox.utils import md5hash

logger = logging.getLogger(__name__)


class RSSHandler:
    rss_url: str
    scheduler: BaseScheduler
    db: Collection
    downloads_db: Collection
    hook: Hook
    watch_rss: WatchRSS

    def __init__(
        self,
        rss_url: str,
        scheduler: BaseScheduler,
        db: Collection,
        downloads_db: Collection,
        hook: Hook,
    ):
        self.rss_url = rss_url
        self.scheduler = scheduler
        self.db = db
        self.downloads_db = downloads_db
        self.hook = hook
        self.watch_rss = WatchRSS(
            self.rss_url,
            self.db,
            self.on_new_entries,
            check_confirmation=True,
        )

    @property
    def id(self):
        return md5hash(self.rss_url)

    def start_rss(self):
        logger.debug(f"Starting RSS: {self.rss_url}")
        random_start_time = datetime.now() + timedelta(seconds=random.randint(0, 60))
        self.scheduler.add_job(
            self.watch_rss.check,
            "interval",
            minutes=1,
            id=f"watchrss={self.id}",
            next_run_time=random_start_time,
        )
        t = Thread(target=self.watch_rss.check)
        t.start()

    def stop_rss(self):
        self.scheduler.remove_job(f"watchrss={self.id}")

    def on_new_entries(self, entries: List[FeedParserDict]):
        logger.info(f"{len(entries)} new entries")
        for entry in entries:
            if entry_result := self.hook.on_new_entry(entry):
                if isinstance(entry_result, FeedParserDict):
                    entry = entry_result

                try:
                    Download.create(
                        client=self.downloads_db, name=entry.title, url=entry.link
                    )
                except Exception as error:
                    logging.exception(
                        f"Error while adding download to database: {error}"
                    )

        return True
