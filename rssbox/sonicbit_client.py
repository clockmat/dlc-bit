import logging
from datetime import datetime, timedelta, timezone
from time import sleep

import nanoid
from apscheduler.schedulers.background import BackgroundScheduler
from pymongo import ReturnDocument
from pymongo.collection import Collection

from rssbox.config import Config
from rssbox.enum import DownloadStatus, SonicBitStatus
from rssbox.handlers.file_handler import FileHandler
from rssbox.handlers.worker_handler import WorkerHandler
from rssbox.hooks.hook import Hook
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
    hook: Hook

    def __init__(
        self,
        accounts: Collection,
        downloads: Collection,
        workers: Collection,
        scheduler: BackgroundScheduler,
        file_handler: FileHandler,
        hook: Hook,
        id: str = None,
    ):
        self.id = id or nanoid.generate(alphabet="1234567890abcdef")
        self.accounts = accounts
        self.downloads = downloads
        self.workers = workers
        self.scheduler = scheduler
        self.file_handler = file_handler
        self.hook = hook
        self.HEARTBEAT_INTERVAL = 30

        logger.info(f"Initializing {type(self).__name__} with ID: {self.id}")

        self.heartbeat = Heartbeat(
            self.id, self.workers, self.scheduler, self.HEARTBEAT_INTERVAL
        )
        self.worker_handler = WorkerHandler(
            self.workers,
            self.accounts,
            self.downloads,
            self.scheduler,
            self.HEARTBEAT_INTERVAL,
        )

        self.worker_handler.clean_stale_sonicbit_and_workers()

    def start(
        self,
        download_only: bool = True,
        upload_only: bool = True,
        process_only: bool = True,
    ):
        with self.heartbeat:
            if download_only or process_only:
                logger.debug("Starting download checks and scheduler")
                self.start_downloads()  # First download
                if not download_only:
                    self.scheduler.add_job(
                        self.start_downloads,
                        "interval",
                        minutes=3,
                        id="start_downloads",
                        max_instances=5,
                    )

            if upload_only or process_only:
                logger.debug("Starting upload checks")
                self.check_downloads()

            if download_only or process_only:
                self.start_downloads()

    def get_sonicbit(self, account: dict) -> SonicBit:
        return SonicBit(client=self.accounts, account=account)

    def get_free_sonicbit(self) -> SonicBit:
        result = self.accounts.find_one_and_update(
            {
                "$or": [
                    {"status": SonicBitStatus.IDLE.value},
                    {"status": {"$exists": False}},
                    {"status": ""},
                ],
            },
            {
                "$set": {
                    "status": SonicBitStatus.PROCESSING.value,
                    "locked_by": self.id,
                    "last_used_at": datetime.now(tz=timezone.utc),
                }
            },
            sort=[("priority", -1), ("last_used_at", 1)],
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
        now = datetime.now(tz=timezone.utc)

        while True:
            if datetime.now(tz=timezone.utc) - now > timedelta(
                seconds=Config.DOWNLOAD_CHECK_TIMEOUT
            ):
                break

            sonicbit = self.get_download_to_check()
            if not sonicbit:
                break

            download = sonicbit.download

            if not download:
                logger.warning(
                    f"SonicBit downloading but no download found for {sonicbit.download_id} ({sonicbit.id})"
                )
                sonicbit.mark_as_idle()
                continue
            if not download.hash:
                logger.warning(
                    f"SonicBit downloading but no download's hash found for {sonicbit.download_id} ({sonicbit.id})"
                )
                sonicbit.reset()
                continue

            torrent_list = sonicbit.list_torrents()
            torrent = torrent_list.torrents.get(download.hash)
            if not torrent:
                logger.warning(
                    f"Torrent not found for {download.name} by {sonicbit.id} after {sonicbit.time_taken_str}"
                )
                if self.hook.on_sonicbit_download_not_found(sonicbit, download):
                    sonicbit.reset()
                continue

            if torrent.progress == 100:
                logger.info(f"Downloaded {download.name} by {sonicbit.id}")
                try:
                    sonicbit.mark_as_uploading(self.id)
                    files_uploaded = self.file_handler.upload(download, torrent)
                    if files_uploaded:
                        sonicbit.mark_as_completed()
                        self.hook.on_upload_complete(
                            sonicbit, download.dict, files_uploaded
                        )
                    else:
                        logger.warning(
                            f"No files uploaded for {download.name} by {sonicbit.id}"
                        )
                        sonicbit.unlock(SonicBitStatus.DOWNLOADING)
                        sleep(5)
                except Exception as error:
                    logger.exception(
                        f"Failed to upload {download.name} to {sonicbit.id}: {error}"
                    )
                    soft = self.hook.on_before_upload_error(sonicbit, download, error)
                    sonicbit.mark_as_failed(soft=soft)
                    self.hook.on_after_upload_error(sonicbit, download, error)
            else:
                if sonicbit.download_timeout():
                    logger.warning(
                        f"Download timed out for {download.name} by {sonicbit.id}"
                    )
                    self.hook.on_download_timeout(download)
                else:
                    logger.debug(
                        f"Download in progress for {download.name} by {sonicbit.id} ({torrent.progress}%) ({sonicbit.time_taken_str})"
                    )
                    sonicbit.unlock(SonicBitStatus.DOWNLOADING)
                    sleep(5)

    def start_downloads(self):
        now = datetime.now(tz=timezone.utc)

        while True:
            if datetime.now(tz=timezone.utc) - now > timedelta(
                seconds=Config.DOWNLOAD_START_TIMEOUT
            ):
                break

            download = self.get_pending_download()
            if not download:
                break

            sonicbit = self.get_free_sonicbit()
            if not sonicbit:
                download.unlock()
                logger.debug("No sonicbit accounts available for downloading")
                break

            try:
                sonicbit.add_download_with_retries(download=download)
                logger.info(f"Torrent {download.name} added to {sonicbit.id}")
            except Exception as error:
                logger.error(f"Failed to add {download.name} to {sonicbit.id}: {error}")
                if self.hook.on_add_download_error(sonicbit, download, error):
                    download.unlock()
                    sonicbit.mark_as_idle()
