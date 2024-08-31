import logging
from datetime import datetime, timedelta, timezone
from ssl import SSLEOFError
from time import sleep

import nanoid
from apscheduler.schedulers.background import BackgroundScheduler
from pymongo import ReturnDocument
from pymongo.collection import Collection

from rssbox.enum import DownloadStatus, SonicBitStatus
from rssbox.handlers.file_handler import FileHandler
from rssbox.handlers.worker_handler import WorkerHandler
from rssbox.modules.download import Download
from rssbox.modules.heartbeat import Heartbeat
from rssbox.modules.sonicbit import SonicBit

logger = logging.getLogger(__name__)


class SonicBitClient:
    id: str
    accounts: Collection
    downloads: Collection
    workers: Collection
    scheduler: BackgroundScheduler
    file_handler: FileHandler

    def __init__(
        self,
        accounts: Collection,
        downloads: Collection,
        workers: Collection,
        scheduler: BackgroundScheduler,
        file_handler: FileHandler,
    ):
        self.id = nanoid.generate(alphabet="1234567890abcdef")
        self.accounts = accounts
        self.downloads = downloads
        self.workers = workers
        self.scheduler = scheduler
        self.file_handler = file_handler

        logger.info(f"Initializing SonicBitClient with ID: {self.id}")

        self.heartbeat = Heartbeat(self.id, self.workers, self.scheduler)
        self.worker_handler = WorkerHandler(
            self.workers, self.accounts, self.downloads, self.scheduler
        )

        self.worker_handler.clean_stale_sonicbit_and_workers()

    def start(self):
        with self.heartbeat:
            self.begin_download()  # First download
            self.scheduler.add_job(
                self.begin_download, "interval", seconds=30, id="begin_download"
            )
            self.check_downloads()
            self.begin_download()

    def get_sonicbit(self, account: dict) -> SonicBit:
        return SonicBit(client=self.accounts, account=account)

    def get_free_sonicbit(self) -> SonicBit:
        result = self.accounts.find_one_and_update(
            {
                "$or": [
                    {"status": SonicBitStatus.IDLE.value},
                    {"status": {"$exists": False}},
                    {"status": ""},
                ]
            },
            {"$set": {"status": SonicBitStatus.PROCESSING.value, "locked_by": self.id}},
            sort=[("priority", -1)],
            return_document=ReturnDocument.AFTER,
        )
        if not result:
            return None

        return self.get_sonicbit(result)

    def get_pending_download(self) -> Download | None:
        raw_download = self.downloads.find_one_and_update(
            {
                "status": DownloadStatus.PENDING.value,
                "$or": [
                    {"locked_by": {"$exists": False}},  # Not locked by any instance
                    {"locked_by": None},  # Explicitly not locked
                    {"locked_by": ""},  # Explicitly not locked
                ],
            },
            {"$set": {"locked_by": self.id}},
            return_document=ReturnDocument.AFTER,
        )

        if not raw_download:
            return None

        return Download(self.downloads, raw_download)

    def get_download_to_check(self) -> SonicBit | None:
        locked_account = self.accounts.find_one_and_update(
            {
                "status": SonicBitStatus.DOWNLOADING.value,
                "$or": [
                    {"locked_by": {"$exists": False}},  # Not locked by any instance
                    {"locked_by": None},  # Explicitly not locked
                    {"locked_by": ""},  # Explicitly not locked
                ],
            },  # Ensure it's still unlocked
            {
                "$set": {
                    "status": SonicBitStatus.LOCKED.value,
                    "locked_by": self.id,
                    "last_checked_at": datetime.now(tz=timezone.utc),
                }
            },
            sort=[("last_checked_at", 1)],
            return_document=ReturnDocument.AFTER,
        )

        if locked_account:
            return self.get_sonicbit(locked_account)
        else:
            return None

    def check_downloads(self):
        limit = 3
        timeout_in_seconds = 8 * 60  # 8 minutes
        now = datetime.now(tz=timezone.utc)

        while True:
            if limit <= 0 or datetime.now(tz=timezone.utc) - now > timedelta(
                seconds=timeout_in_seconds
            ):
                break

            sonicbit = self.get_download_to_check()
            if not sonicbit:
                break

            download = sonicbit.download

            if not download:
                logger.info(
                    f"SonicBit downloaded but no download found for {sonicbit.download_id} ({sonicbit.id})"
                )
                sonicbit.mark_as_idle()
                continue
            if not download.hash:
                logger.info(
                    f"SonicBit downloaded but no download name found for {sonicbit.download_id} ({sonicbit.id})"
                )
                sonicbit.reset()
                continue

            torrent_list = sonicbit.list_torrents()
            torrent = torrent_list.torrents.get(download.hash)
            if not torrent:
                logger.info(f"Torrent not found for {download.name} by {sonicbit.id}")
                sonicbit.reset()
                continue

            if torrent.progress == 100:
                logger.info(f"Downloaded {download.name} by {sonicbit.id}")
                try:
                    sonicbit.mark_as_uploading(self.id)
                    files_uploaded = self.file_handler.upload(download, torrent)
                    if files_uploaded:
                        sonicbit.mark_as_completed()
                        limit -= 1
                    else:
                        logger.info(
                            f"No files uploaded for {download.name} by {sonicbit.id}"
                        )
                        sonicbit.update_status(SonicBitStatus.DOWNLOADING)
                        sleep(5)
                except SSLEOFError as error:
                    logger.error(
                        f"Failed to upload {download.name} to {sonicbit.id}: {error}"
                    )
                    sonicbit.mark_as_failed(soft=True)
                except Exception as error:
                    logger.error(
                        f"Failed to upload {download.name} to {sonicbit.id}: {error}"
                    )
                    sonicbit.mark_as_failed()
            else:
                if sonicbit.download_timeout():
                    logger.info(
                        f"Download timed out for {download.name} by {sonicbit.id}"
                    )
                else:
                    logger.info(
                        f"Download in progress for {download.name} by {sonicbit.id} ({torrent.progress}%) ({sonicbit.time_taken})"
                    )
                    sonicbit.update_status(SonicBitStatus.DOWNLOADING)
                    sleep(5)

    def begin_download(self):
        while True:
            download = self.get_pending_download()
            if not download:
                break

            sonicbit = self.get_free_sonicbit()
            if not sonicbit:
                download.unlock()
                logger.debug("No sonicbit accounts available for downloading")
                break

            try:
                sonicbit.add_download(download=download)
                logger.info(f"Torrent {download.name} added to {sonicbit.id}")
            except Exception as error:
                download.unlock()
                logger.error(f"Failed to add {download.name} to {sonicbit.id}: {error}")
