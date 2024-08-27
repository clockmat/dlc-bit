import logging
import os
from datetime import datetime, timezone

import humanize
import requests
from deta import Deta, _Base

from rssbox.config import Config
from sonicbit.types import Torrent, TorrentFile
from rssbox.utils import delete_file, md5hash
from rssbox.modules.download import Download

logger = logging.getLogger(__name__)


class FileHandler:
    def __init__(self, deta: Deta, files: _Base):
        self.deta = deta
        self.files = files

    def upload(self, download: Download, torrent: Torrent) -> int:
        files: list[TorrentFile] = []
        for torrent_file in torrent.files:
            if self.check_extension(torrent_file.extension):
                files.append(torrent_file)
        
        if len(files) == 0:
            return 0
        
        if len(files) == 1:
            file = files[0]
            filename = self.reformat_name(download.name, file.extension)
            return self.process_file(download, file, filename)
        
        count = 0
        for file in files:
            filename = self.reformat_name(download.name, file.extension, subname=file.name)
            count += self.process_file(download, file, filename)

        return count


    def process_file(self, download: Download, file: TorrentFile, filename: str):
        filepath = self.get_filepath(filename)

        self.download_file(file, filepath, filename)
        self.upload_file(file, filepath, filename)
        return 1

    def check_extension(self, ext: str):
        if ext.lower() in Config.FILTER_EXTENSIONS:
            return True
        return False

    def download_file(self, file: TorrentFile, filepath: str, filename: str) -> str:
        if os.path.exists(filepath) and os.path.getsize(filepath) == file.size:
            return filepath

        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        response = requests.get(file.download_url, stream=True)

        if response.status_code == 200:
            logger.info(
                f"Downloading {filename} to {filepath} ({humanize.naturalsize(file.size)})"
            )
            with open(filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)

            logger.info(
                f"Downloaded {filename} to {filepath} ({humanize.naturalsize(file.size)})"
            )
            return filepath

        return None

    def upload_file(self, file: TorrentFile, filepath: str, filename: str):
        drive_name = md5hash(filename)
        drive = self.deta.Drive(drive_name)
        logger.info(
            f"Uploading {filename} ({humanize.naturalsize(file.size)}) to {drive_name}"
        )
        result = drive.put(name=filename, path=filepath)

        self.files.insert(
            {
                "name": filename,
                "size": file.size,
                "hash": drive_name,
                "created_at": datetime.now(tz=timezone.utc).isoformat(),
                "downloads_count": 0,
            }
        )
        logger.info(
            f"Uploaded {filename} ({humanize.naturalsize(file.size)}) to {drive_name}"
        )

        self.remove_file(filename)
        return result

    def get_filepath(self, filename: str) -> str:
        return os.path.join(self.get_filedir(filename), filename)

    def get_filedir(self, filename: str) -> str:
        return os.path.join(Config.DOWNLOAD_PATH, md5hash(filename))

    def remove_file(self, filename: str):
        filedir = self.get_filedir(filename)
        delete_file(filedir)

    def reformat_name(self, name: str, ext: str, subname: str = None) -> str:
        new_name =  self.sanitize_name(name)
        if subname:
            subname = self.sanitize_name(subname)
            new_name = f"{new_name}.{subname}"
        return f"{new_name}.{ext.lower()}"
    
    def sanitize_name(self, name: str) -> str:
        return ".".join(name.replace("[XC]", "").replace("-", " ").split())