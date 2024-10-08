import logging
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.background import BackgroundScheduler
from pymongo.collection import Collection

from rssbox.enum import DownloadStatus, SonicBitStatus

logger = logging.getLogger(__name__)


class WorkerHandler:
    def __init__(
        self,
        workers: Collection,
        accounts: Collection,
        downloads: Collection,
        scheduler: BackgroundScheduler,
        heartbeat_interval: int,
    ):
        self.workers = workers
        self.accounts = accounts
        self.downloads = downloads
        self.scheduler = scheduler
        self.HEARTBEAT_INTERVAL = heartbeat_interval

    def start(self):
        self.scheduler.add_job(
            self.clean_stale_sonicbit_and_workers, "interval", seconds=40
        )

    def clean_stale_sonicbit_and_workers(self):
        logger.debug(
            "Unlocking idle or stale workers, sonicbit accounts, and downloads"
        )

        timeout_period = timedelta(seconds=self.HEARTBEAT_INTERVAL * 2)
        current_time = datetime.now(tz=timezone.utc)
        timeout_threshold = current_time - timeout_period

        # Find and delete stale workers, then capture their IDs
        stale_workers = self.workers.find(
            {"last_heartbeat": {"$lt": timeout_threshold}}, {"_id": 1}
        )

        stale_worker_ids = [worker["_id"] for worker in stale_workers]

        if stale_worker_ids:
            result = self.workers.delete_many({"_id": {"$in": stale_worker_ids}})
            logger.info(f"Removed {result.deleted_count} stale workers")
        else:
            logger.debug("No stale workers to remove")

        # Process the accounts table
        self.process_stale_sonicbit(stale_worker_ids, timeout_threshold)

        # Process the downloads table
        self.process_stale_downloads(stale_worker_ids, timeout_threshold)

    def process_stale_sonicbit(self, stale_worker_ids, timeout_threshold):
        logger.debug("Checking for stale or orphaned SonicBit accounts")

        # Find accounts that are in PROCESSING, UPLOADING, or LOCKED status and are orphaned or idle
        pipeline = [
            {
                "$match": {
                    "status": {
                        "$in": [
                            SonicBitStatus.PROCESSING.value,
                            SonicBitStatus.UPLOADING.value,
                            SonicBitStatus.LOCKED.value,
                        ]
                    }
                }
            },
            {
                "$lookup": {
                    "from": "workers",
                    "localField": "locked_by",
                    "foreignField": "_id",
                    "as": "worker",
                }
            },
            {"$unwind": {"path": "$worker", "preserveNullAndEmptyArrays": True}},
            {
                "$match": {
                    "$or": [
                        {
                            "worker": {"$exists": False}
                        },  # Worker doesn't exist (orphaned)
                        {
                            "worker.last_heartbeat": {"$lt": timeout_threshold}
                        },  # Worker is stale
                        {
                            "locked_by": {"$in": stale_worker_ids}
                        },  # Locked by a stale worker
                    ]
                }
            },
            {"$project": {"_id": 1, "status": 1}},
        ]

        orphaned_or_idle_accounts = list(self.accounts.aggregate(pipeline))

        if orphaned_or_idle_accounts:
            for account in orphaned_or_idle_accounts:
                new_status = (
                    SonicBitStatus.DOWNLOADING.value
                    if account["status"]
                    in [SonicBitStatus.LOCKED.value, SonicBitStatus.UPLOADING.value]
                    else SonicBitStatus.IDLE.value
                )

                # Update each account individually based on the condition
                self.accounts.update_one(
                    {"_id": account["_id"]},
                    {
                        "$set": {
                            "status": new_status,
                            "locked_by": None,
                        }
                    },
                )

            logger.info(
                f"Updated {len(orphaned_or_idle_accounts)} orphaned or idle SonicBit accounts"
            )
        else:
            logger.debug("No orphaned or idle SonicBit accounts to update")

    def process_stale_downloads(self, stale_worker_ids, timeout_threshold):
        logger.debug("Checking for stale or orphaned downloads")

        # Find downloads that are locked by a non-existing or stale worker
        pipeline = [
            {
                "$match": {
                    "status": {
                        "$in": [
                            DownloadStatus.PENDING.value,
                            DownloadStatus.PROCESSING.value,
                        ]
                    },
                    "locked_by": {"$ne": None},
                }
            },
            {
                "$lookup": {
                    "from": "workers",
                    "localField": "locked_by",
                    "foreignField": "_id",
                    "as": "worker",
                }
            },
            {"$unwind": {"path": "$worker", "preserveNullAndEmptyArrays": True}},
            {
                "$match": {
                    "$or": [
                        {
                            "worker": {"$exists": False}
                        },  # Worker doesn't exist (orphaned)
                        {
                            "worker.last_heartbeat": {"$lt": timeout_threshold}
                        },  # Worker is stale
                        {
                            "locked_by": {"$in": stale_worker_ids}
                        },  # Locked by a stale worker
                    ]
                }
            },
            {"$project": {"_id": 1}},
        ]

        orphaned_or_idle_download_ids = [
            download["_id"] for download in self.downloads.aggregate(pipeline)
        ]

        if orphaned_or_idle_download_ids:
            self.downloads.update_many(
                {"_id": {"$in": orphaned_or_idle_download_ids}},
                {
                    "$set": {
                        "status": DownloadStatus.PENDING.value,  # Revert to pending for reprocessing
                        "locked_by": None,
                    }
                },
            )

            logger.info(
                f"Updated {len(orphaned_or_idle_download_ids)} orphaned or idle downloads"
            )
        else:
            logger.debug("No orphaned or idle downloads to update")

        # Find downloads in PROCESSING that don't have a corresponding entry in the accounts table
        processing_downloads_without_account = self.downloads.aggregate(
            [
                {"$match": {"status": DownloadStatus.PROCESSING.value}},
                {
                    "$lookup": {
                        "from": "accounts",
                        "localField": "_id",
                        "foreignField": "download_id",
                        "as": "account",
                    }
                },
                {"$match": {"account": {"$size": 0}}},  # No corresponding account found
                {"$project": {"_id": 1}},
            ]
        )

        processing_download_ids_without_account = [
            download["_id"] for download in processing_downloads_without_account
        ]

        if processing_download_ids_without_account:
            self.downloads.update_many(
                {"_id": {"$in": processing_download_ids_without_account}},
                {
                    "$set": {
                        "status": DownloadStatus.PENDING.value,  # Revert to pending for reprocessing
                        "locked_by": None,
                    }
                },
            )
            logger.info(
                f"Updated {len(processing_download_ids_without_account)} processing downloads without account references to pending"
            )
        else:
            logger.debug("No processing downloads without account references found")
