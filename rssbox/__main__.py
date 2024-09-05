import logging
import os

import click
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler

from rssbox import accounts, downloads, watchrss_database, workers
from rssbox.config import Config
from rssbox.handlers.file_handler import FileHandler
from rssbox.handlers.rss_handler import RSSHandler
from rssbox.hooks.hook import Hook
from rssbox.sonicbit_client import SonicBitClient
from rssbox.utils import clean_empty_dirs

logger = logging.getLogger(__name__)
rss_handlers = {}


def main(
    rss_only: bool,
    download_only: bool,
    upload_only: bool,
    process_only: bool,
    client_id: str = None,
):
    clean_empty_dirs(Config.DOWNLOAD_PATH)
    hook = Hook()
    scheduler_class = BlockingScheduler if rss_only else BackgroundScheduler
    scheduler = scheduler_class(timezone="UTC")

    if not download_only or not upload_only or not process_only:
        for rss_url in Config.RSS_URLS:
            rss_handler = RSSHandler(
                rss_url=rss_url,
                scheduler=scheduler,
                db=watchrss_database,
                downloads_db=downloads,
                hook=hook,
            )
            rss_handler.start_rss()
            rss_handlers[rss_url] = rss_handler

        if rss_only:
            logger.info(f"RSS only mode, listening for {len(rss_handlers)} RSS feeds")

    scheduler.start()

    if not rss_only:
        if not download_only and not upload_only and not process_only:
            process_only = True

        file_handler = FileHandler()
        sonicbit_client = SonicBitClient(
            accounts, downloads, workers, scheduler, file_handler, hook, client_id
        )
        sonicbit_client.start(download_only, upload_only, process_only)

    scheduler.shutdown(wait=True)


@click.command()
@click.option("--debug", "-d", is_flag=True, help="Enable debug mode")
@click.option(
    "--rss-only",
    "-r",
    is_flag=True,
    help="Only start RSS handlers, no processing of files",
)
@click.option(
    "--download-only",
    "-d",
    is_flag=True,
    help="Only download files, no rss or upload checks",
)
@click.option(
    "--upload-only",
    "-u",
    is_flag=True,
    help="Only upload files, no rss or download checks",
)
@click.option(
    "--process-only", "-p", is_flag=True, help="Only process files, no rss checks"
)
@click.option("--id", "-i", help="ID to use for the client")
def cli(
    debug: bool,
    rss_only: bool,
    download_only: bool,
    upload_only: bool,
    process_only: bool,
    id: str,
):
    if debug or os.environ.get("LOG_LEVEL", "INFO").upper() == "DEBUG":
        logging.getLogger().setLevel(logging.DEBUG)

    main(rss_only, download_only, upload_only, process_only, client_id=id)


if __name__ == "__main__":
    cli()
