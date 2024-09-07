import logging
from datetime import datetime, timedelta, timezone
from time import sleep

from pymongo.collection import Collection
from sonicbit import SonicBit as SonicBitClient

from rssbox import downloads, mongo_client
from rssbox.config import Config
from rssbox.enum import SonicBitStatus
from rssbox.modules.download import Download
from rssbox.modules.token_handler import TokenHandler
from rssbox.utils import calulate_torrent_hash

logger = logging.getLogger(__name__)


class SonicBit(SonicBitClient):
    client: Collection
    id: str
    status: SonicBitStatus
    added_at: datetime | None
    download_id: str | None
    locked_by: str | None
    last_checked_at: datetime | None
    last_used_at: datetime | None

    def __init__(self, client: Collection, account: dict):
        self.client = client
        self.id = account["_id"]
        self.status = SonicBitStatus(account.get("status", SonicBitStatus.IDLE.value))
        self.added_at = account.get("added_at", None)
        self.download_id = account.get("download_id", None)
        self.locked_by = account.get("locked_by", None)
        self.priority = account.get("priority", 0)
        self.last_checked_at = account.get("last_checked_at", None)
        self.last_used_at = account.get("last_used_at", None)
        self.__download = None

        super().__init__(
            email=self.id,
            password=account["password"],
            token=account.get("token", None),
            token_handler=TokenHandler(self.client),
        )

    def get_download_link(self, file: dict | str):
        if isinstance(file, dict):
            file = file["folder_file_id"]

        response = self.fetchFile(file)
        return response["url"]

    def purge(self):
        torrent_list = self.list_torrents()
        for torrent in torrent_list.torrents.values():
            torrent.delete(with_file=True)
        # clear storage
        self.clear_storage()

    def add_download(self, download: Download):
        self.purge()

        [download_url] = self.add_torrent(uri=download.url)

        if download_url == download.url:
            hash = self.get_torrent_hash(download.url)
            self.verify_download(hash)
            self.mark_as_downloading(download, hash=hash)
        else:
            raise Exception("Download URL does not match")

    def add_download_with_retries(self, download: Download, retries: int = 3):
        try:
            self.add_download(download)
        except Exception as error:
            if retries > 0:
                logger.info(
                    f"Retry adding download {download.name} after error: {error}"
                )
                self.add_download_with_retries(download, retries - 1)
            else:
                raise error

    def save(self):
        self.client.update_one(
            {"_id": self.id},
            {
                "$set": {
                    "status": self.status.value,
                    "added_at": self.added_at,
                    "download_id": self.download_id,
                    "locked_by": self.locked_by,
                    "priority": self.priority,
                    "last_checked_at": self.last_checked_at,
                }
            },
        )

    def unlock(self, status: SonicBitStatus = SonicBitStatus.IDLE):
        self.status = status
        self.locked_by = None
        self.save()

    def mark_as_downloading(self, download: Download, hash: str):
        self.download_id = download.id
        self.added_at = datetime.now(tz=timezone.utc)
        self.status = SonicBitStatus.DOWNLOADING
        self.locked_by = None

        with mongo_client.start_session() as session:
            with session.start_transaction():
                self.download.mark_as_processing(hash=hash)
                self.save()

    def mark_as_idle(self):
        self.status = SonicBitStatus.IDLE
        self.added_at = None
        self.download_id = None
        self.locked_by = None
        self.save()

    def mark_as_uploading(self, locked_by: str):
        self.locked_by = locked_by
        self.status = SonicBitStatus.UPLOADING
        self.save()

    def mark_as_failed(self, soft=False):
        with mongo_client.start_session() as session:
            with session.start_transaction():
                self.mark_as_idle()
                self.download.mark_as_failed(soft=soft)

    def mark_as_completed(self):
        with mongo_client.start_session() as session:
            with session.start_transaction():
                self.mark_as_idle()
                self.download.delete()

    def mark_as_timeout(self):
        with mongo_client.start_session() as session:
            with session.start_transaction():
                self.mark_as_idle()
                self.download.mark_as_timeout()

    def checked(self):
        self.last_checked_at = datetime.now(tz=timezone.utc)
        self.save()

    def reset(self):
        with mongo_client.start_session() as session:
            with session.start_transaction():
                self.mark_as_idle()
                self.download.mark_as_pending()

    def download_timeout(self, timeout: int = Config.DOWNLOAD_TIMEOUT) -> bool:
        if self.added_at and self.added_at + timedelta(seconds=timeout) < datetime.now(
            tz=timezone.utc
        ):
            self.mark_as_timeout()
            return True

        return False

    def get_download(self) -> Download | None:
        if self.download_id:
            if raw_download := downloads.find_one({"_id": self.download_id}):
                self.__download = Download(downloads, raw_download)
                return self.__download
        return None

    def verify_download(
        self, hash: str, timeout: int = Config.DOWNLOAD_ADD_VERIFY_TIMEOUT
    ) -> bool:
        logger.debug(f"Verifying download {hash}")

        now = datetime.now(tz=timezone.utc)
        while True:
            if datetime.now(tz=timezone.utc) - now > timedelta(seconds=timeout):
                raise Exception(f"Verify download timed out for download hash: {hash}")

            torrents = self.list_torrents()
            for download_hash in torrents.torrents.keys():
                if hash == download_hash:
                    return True
            sleep(1)

    @property
    def download(self) -> Download | None:
        return self.__download or self.get_download()

    @property
    def time_taken(self) -> timedelta:
        if self.added_at:
            return datetime.now(tz=timezone.utc) - self.added_at

        self.added_at = datetime.now(tz=timezone.utc)
        self.save()
        return self.time_taken

    @property
    def time_taken_str(self) -> str:
        return str(self.time_taken).split(".", 2)[0]

    def get_torrent_hash(self, uri: str) -> str | None:
        return calulate_torrent_hash(uri).upper()
