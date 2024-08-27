import logging
from datetime import datetime, timedelta, timezone
import re

from pymongo.collection import Collection
from sonicbit import SonicBit as SonicBitClient
from rssbox import downloads, mongo_client
from rssbox.enum import SonicBitStatus
from rssbox.modules.download import Download
from rssbox.modules.token_handler import TokenHandler

logger = logging.getLogger(__name__)

class SonicBit(SonicBitClient):
    client: Collection
    id: str
    status: SonicBitStatus
    added_at: datetime | None
    download_id: str | None
    locked_by: str | None
    last_checked_at: datetime | None

    def __init__(self, client: Collection, account: dict):
        self.client = client
        self.id = account["_id"]
        self.status = SonicBitStatus(account.get("status", SonicBitStatus.IDLE.value))
        self.added_at = account.get("added_at", None)
        self.download_id = account.get("download_id", None)
        self.locked_by = account.get("locked_by", None)
        self.priority = account.get("priority", 0)
        self.last_checked_at = account.get("last_checked_at", None)
        self.__download = None

        super().__init__(email=self.id, password=account["password"], token=account.get("token", None), token_handler=TokenHandler(self.client))


    def get_download_link(self, file: dict | str):
        if isinstance(file, dict):
            file = file["folder_file_id"]

        response = self.fetchFile(file)
        return response["url"]

    def purge(self):
        torrent_list = self.list_torrents()
        for torrent in torrent_list.torrents.values():
            torrent.delete(with_file=True)


    def add_download(self, download: Download):
        self.purge()

        try:
            [download_url] = self.add_torrent(uri=download.url)
        except Exception as error:
            self.mark_as_idle()
            raise error
        
        if download_url == download.url:
            hash = self.get_torrent_hash(download.url)
            self.mark_as_downloading(download, hash=hash)
        else:
            self.mark_as_idle()
            raise Exception("Download URL does not match")

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

    def update_status(self, status: SonicBitStatus):
        self.status = status
        if status in [SonicBitStatus.DOWNLOADING, SonicBitStatus.IDLE]:
            self.locked_by = None
        self.save()

    def mark_as_downloading(
        self, download: Download, hash: str
    ):
        self.download_id = download.id
        self.added_at = datetime.now(tz=timezone.utc)
        self.status = SonicBitStatus.DOWNLOADING
        self.locked_by = None

        with mongo_client.start_session() as session:
            with session.start_transaction():
                self.save()
                self.download.mark_as_processing(hash=hash)

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

    def checked(self):
        self.last_checked_at = datetime.now(tz=timezone.utc)
        self.save()

    def reset(self):
        with mongo_client.start_session() as session:
            with session.start_transaction():
                self.mark_as_idle()
                self.download.mark_as_pending()

    def download_timeout(self, timeout: int = 60 * 60 * 2.5) -> bool:
        if self.added_at and self.added_at + timedelta(seconds=timeout) < datetime.now(
            tz=timezone.utc
        ):
            self.reset()
            return True

        return False

    def get_download(self) -> Download | None:
        if self.download_id:
            raw_download = downloads.find_one({"_id": self.download_id})
            if raw_download:
                return Download(downloads, raw_download)
        return None

    @property
    def download(self) -> Download | None:
        if not self.__download:
            self.__download = self.get_download()
        return self.__download
    
    @property
    def time_taken(self):
        if self.added_at:
            return str(datetime.now(tz=timezone.utc) - self.added_at).split('.', 2)[0]
        
        self.added_at = datetime.now(tz=timezone.utc)
        self.save()
        return self.time_taken
    
    def get_torrent_hash(self, uri: str) -> str | None:
        if uri.startswith("magnet:"):
            return re.search(r"xt=urn:btih:([a-zA-Z0-9]+)", uri).group(1).upper()
        else:
            raise NotImplementedError("Only magnet links are supported")