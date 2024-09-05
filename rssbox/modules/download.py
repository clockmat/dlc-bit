import logging

from bson.objectid import ObjectId
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

from rssbox.config import Config
from rssbox.enum import DownloadStatus

logger = logging.getLogger(__name__)


class Download:
    url: str
    name: str
    id: str
    status: DownloadStatus
    hash: str | None
    locked_by: str | None
    retries: int

    def __init__(self, client: Collection, dict: dict):
        self.client = client
        self.url = dict["url"]
        self.name = dict["name"]
        self.id = dict["_id"]
        self.status = DownloadStatus(dict["status"])

        self.hash = dict.get("hash")
        self.locked_by = dict.get("locked_by")
        self.retries = dict.get("retries", 0)

    @property
    def dict(self):
        return {
            "url": self.url,
            "name": self.name,
            "status": self.status.value,
            "hash": self.hash,
            "locked_by": self.locked_by,
            "retries": self.retries,
        }

    def save(self):
        self.client.update_one({"_id": self.id}, {"$set": self.dict}, upsert=True)

    def mark_as_processing(self, hash: str):
        self.status = DownloadStatus.PROCESSING
        self.hash = hash
        self.locked_by = None
        self.save()

    def mark_as_pending(self):
        self.status = DownloadStatus.PENDING
        self.hash = None
        self.locked_by = None
        self.save()

    def mark_as_failed(self, soft=False):
        if not soft:
            self.retries += 1

        if self.retries >= Config.DOWNLOAD_RETRIES:
            logger.warning(f"Retry limit reached for {self.name}")
            self.status = DownloadStatus.ERROR
            self.hash = None
            self.locked_by = None
            self.save()
        else:
            self.mark_as_pending()

    def mark_as_timeout(self):
        self.status = DownloadStatus.TIMEOUT
        self.hash = None
        self.locked_by = None
        self.save()

    def unlock(self):
        self.locked_by = None
        self.save()

    def delete(self):
        self.client.delete_one({"_id": self.id})

    @staticmethod
    def create(
        client: Collection,
        name: str,
        url: str,
        status: DownloadStatus = DownloadStatus.PENDING,
    ) -> ObjectId:
        document_id = ObjectId()
        document = {
            "url": url,
            "name": name,
            "status": status.value,
            "_id": document_id,
        }

        try:
            client.insert_one(document)
            return document_id
        except DuplicateKeyError:
            logger.warning(f"Duplicate key for download: {name}")
            result = client.find_one({"url": url})
            return result["_id"]
