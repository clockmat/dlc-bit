import json
import logging
import os
from base64 import b64decode
from typing import List

import PTN
import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from sonicbit.types import Torrent, TorrentFile

from rssbox.config import Config
from rssbox.handlers.file_handler import FileHandler
from rssbox.modules.download import Download
from rssbox.utils import md5hash

logger = logging.getLogger(__name__)


class GDriveHandler(FileHandler):
    def __init__(self):
        service_account = json.loads(
            b64decode(os.environ["GDRIVE_SERVICE_ACCOUNT"]).decode()
        )
        creds = Credentials.from_service_account_info(service_account)
        self.client = build("drive", "v3", credentials=creds)
        self.folder_id = os.environ["GDRIVE_FOLDER_ID"]
        self.__G_DRIVE_DIR_MIME_TYPE = "application/vnd.google-apps.folder"
        self.__MIN_FILE_SIZE = 50 * 1024 * 1024  # 50MB

    def upload(self, download: Download, torrent: Torrent) -> int:
        upload_count = 0

        files_to_upload: List[TorrentFile] = []

        for file in torrent.files:
            if (
                self.check_extension(file.extension)
                and file.size > self.__MIN_FILE_SIZE
            ):
                files_to_upload.append(file)
            else:
                filename, ext = os.path.splitext(file.name)
                if self.check_extension(ext[1:]) and file.size > self.__MIN_FILE_SIZE:
                    file.extension = ext[1:]
                    file.name = filename
                    files_to_upload.append(file)

        if len(files_to_upload) > 1:
            title = self.reformat_title(download.name)
            folder_id = self.create_folder(title)

            for file in files_to_upload:
                title = self.reformat_title(file.name, file.extension)
                upload_count += self.upload_file(title, file.download_url, folder_id)
        else:
            file = files_to_upload[0]
            folder_id = self.folder_id

            parsed_file_name = PTN.parse(file.name, standardise=False)
            if parsed_file_name.get("episode"):
                folder_title = self.reformat_title(file.name, without_episode=True)
                folder_id = self.find_or_create_folder(folder_title)

            title = self.reformat_title(file.name, file.extension)
            upload_count += self.upload_file(title, file.download_url, folder_id)

        return upload_count

    def upload_file(
        self, filename: str, download_url: str, folder_id: str = None
    ) -> int:
        if not folder_id:
            folder_id = self.folder_id

        filepath = self.download_file_from_url(download_url, filename)
        file_metadata = {
            "name": filename,
            "parents": [folder_id],
        }
        media = MediaFileUpload(filepath, chunksize=2 * 1024 * 1024, resumable=True)

        logger.info(f"Uploading {filename}")
        file = (
            self.client.files()
            .create(
                body=file_metadata,
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            )
            .execute()
        )

        logger.info(f"Uploaded {filename} to {file.get('id')}")
        return 1

    def download_file_from_url(self, url: str, filename: str):
        filedir = os.path.join(Config.DOWNLOAD_PATH, md5hash(filename))
        os.makedirs(filedir, exist_ok=True)

        filepath = os.path.join(filedir, filename)

        logger.info(f"Downloading {filename}")
        response = requests.get(url, stream=True)
        with open(filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=2 * 1024 * 1024):
                if chunk:
                    f.write(chunk)
                    f.flush()

        return filepath

    def create_folder(self, folder_name: str, folder_id: str = None):
        metadata = {
            "name": folder_name,
            "mimeType": self.__G_DRIVE_DIR_MIME_TYPE,
            "parents": [folder_id] if folder_id else [self.folder_id],
        }

        folder = (
            self.client.files().create(supportsAllDrives=True, body=metadata).execute()
        )
        file_id = folder.get("id")

        logger.info(f"Created folder {folder_name} with id {file_id}")
        return file_id

    def find_or_create_folder(self, folder_name: str, folder_id: str = None):
        if not folder_id:
            folder_id = self.folder_id

        folder_name = self.format_search_keyword(folder_name)
        response = (
            self.client.files()
            .list(
                q=f"name = '{folder_name}' and '{folder_id}' in parents and mimeType = '{self.__G_DRIVE_DIR_MIME_TYPE}'",
                spaces="drive",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                fields="files(id, name)",
            )
            .execute()
        )

        if response.get("files"):
            existing_folder = response.get("files")[0]
            if existing_folder.get("name") == folder_name:
                logger.info(
                    f"Found folder {folder_name} with id {existing_folder.get('id')}"
                )
                return existing_folder.get("id")

        return self.create_folder(folder_name, folder_id)

    def reformat_title(
        self,
        parsed: dict | str,
        ext: str = "",
        seperator: str = " ",
        without_episode: bool = False,
    ):
        if isinstance(parsed, str):
            parsed = PTN.parse(parsed, standardise=False)

        if ext:
            ext = f".{ext}".lower() if not ext.startswith(".") else ext.lower()

        title = parsed.get("title", "")
        title = seperator.join(title.split())

        resolution = parsed.get("resolution", "")
        if resolution:
            resolution = f"{seperator}{resolution}"

        year = parsed.get("year", "")
        if year:
            year = f"{seperator}({year})"

        quality = parsed.get("quality", "")
        if quality:
            quality = f"{seperator}{quality}"

        network = parsed.get("network", "")
        if network:
            network = f"{seperator}{network}"

        codec = parsed.get("codec", "")
        if codec:
            codec = f"{seperator}{codec}"

        audio = parsed.get("audio", "")
        if audio:
            audio = f"{seperator}{audio}"

        tv_show = ""
        if parsed.get("season") or parsed.get("episode"):
            season = parsed.get("season", "")
            if season:
                if isinstance(season, list):
                    starting_season = season[0]
                    ending_season = season[-1]
                    season = f"{seperator}S{starting_season:02d}-S{ending_season:02d}"
                else:
                    season = f"{seperator}S{season:02d}"

            episode_text = ""
            if not without_episode:
                episode = parsed.get("episode", "")
                if episode:
                    prefix = "" if season else seperator
                    if isinstance(episode, list):
                        starting_episode = episode[0]
                        ending_episode = episode[-1]
                        episode = (
                            f"{prefix}E{starting_episode:02d}-E{ending_episode:02d}"
                        )
                    else:
                        episode = f"{prefix}E{episode:02d}"

                episode_name = parsed.get("episodeName", "")
                if episode_name:
                    episode_name = f"{seperator}{seperator.join(episode_name.split())}"

                episode_text = f"{episode}{episode_name}"

            tv_show = f"{season}{episode_text}"
        else:
            tv_show = parsed.get("episodeName", "")
            if tv_show:
                tv_show = f"{seperator}-{seperator}{seperator.join(tv_show.split())}"

        return (
            f"{title}{tv_show}{year}{resolution}{quality}{network}{codec}{audio}{ext}"
        )

    def format_search_keyword(self, keyword):
        # escape "\"
        # the \ should be escaped first
        keyword = keyword.replace("\\", "\\\\")
        # escape '''
        keyword = keyword.replace("'", "\\'")
        return keyword
