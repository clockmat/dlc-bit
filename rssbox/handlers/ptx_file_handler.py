import logging
import math
import os
import re
from time import sleep
from typing import Generator

from nanoid import generate
from requests import Session
from sonicbit.types import Torrent

from rssbox.config import Config
from rssbox.handlers.file_handler import FileHandler
from rssbox.modules.download import Download
from rssbox.utils import delete_file

logger = logging.getLogger(__name__)


class PTXFileHandler(FileHandler):
    session: Session

    def __init__(self):
        super().__init__()
        self.session = Session()
        self.__base_url = os.environ["PTX_BASE_URL"]
        self.__description = os.environ["PTX_DESCRIPTION"]

        cookies = os.environ["PTX_COOKIES"]
        self.session.headers.update(
            {
                "Cookie": cookies,
                "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Mobile Safari/537.36",
                "Origin": self.__base_url,
                "Referer": f"{self.__base_url}/upload-video/",
                "X-Requested-With": "XMLHttpRequest",
                "Cache-Control": "no-cache",
            }
        )
        self.REGEX_TO_REMOVE = [r"(MP4-[a-zA-Z0-9]+\s\[XC\])"]
        self.MANUAL_UPLOAD_CHUNK_SIZE = 150 * 1024 * 1024

        if os.path.exists(Config.DOWNLOAD_PATH):
            delete_file(Config.DOWNLOAD_PATH)
            os.makedirs(Config.DOWNLOAD_PATH)

    def url(self, path: str) -> str:
        return f"{self.__base_url}{path}"

    def upload(self, download: Download, torrent: Torrent) -> int:
        # always return 1 to skip images
        count = 1
        for torrent_file in torrent.files:
            if self.check_extension(torrent_file.extension):
                logger.debug(
                    f"Uploading {torrent_file.name} ({torrent_file.extension}) ({torrent_file.download_url})"
                )
                count += self.upload_file(torrent_file.download_url, download.name)

        return count

    def upload_file(self, download_url: str, filename: str) -> int:
        logger.info(f"Uploading {filename}")

        try:
            filecode = self.start_upload(download_url)
            filecode = self.wait_for_upload(filecode, download_url)
        except Exception as error:
            if (
                str(error)
                == "The specified URL is not working or does not return video file"
            ):
                logger.info(f"Starting manual upload for {filename}")
                filecode = self.start_manual_upload(download_url)
            else:
                raise error

        self.publish(filecode, filename)
        logger.info(f"Uploaded {filename}")
        return 1

    def publish(self, filecode: str, filename: str) -> int:
        filename = self.sanitize_filename(filename)

        data = [
            (
                "title",
                filename,
            ),
            (
                "description",
                f"{filename} | {self.__description}",
            ),
            ("filter", ""),
            ("category_ids[]", "76"),
            ("tags", "vph"),
            ("filter", ""),
            ("screenshot", ""),
            ("function", "get_block"),
            ("block_id", "video_edit_video_edit"),
            ("action", "add_new_complete"),
            ("file", f"{filecode}.mp4"),
            ("file_hash", filecode),
            ("format", "json"),
            ("mode", "async"),
        ]

        response = self.session.post(self.url(f"/upload-video/{filecode}/"), data=data)
        if "Video has been created successfully." in response.text:
            return filecode
        else:
            raise Exception(response.text)

    def wait_for_upload(self, filecode: str, download_url: str) -> str:
        while True:
            json = self.upload_request(filecode, download_url)
            if json["status"] == "success" and json["data"].get("state") == "uploading":
                percent = json["data"]["percent"]
                logger.debug(f"Uploading {filecode} ({percent})")
                sleep(3)
            elif json["status"] == "success" and json["data"].get("filename"):
                return json["data"]["filename"]
            elif json["status"] == "failure":
                error_message = json["errors"][0]["message"]
                if error_message == "duplicate":
                    return filecode
                else:
                    raise Exception(error_message)
            else:
                raise Exception(
                    f"Unable to get upload status for filecode: {filecode}, download_url: {download_url}"
                )

    def start_upload(self, download_url: str) -> str:
        filecode = self.generate_filecode()

        json = self.upload_request(filecode, download_url)

        if json["status"] == "success":
            return filecode
        else:
            raise Exception(json["errors"][0]["message"])

    def upload_request(self, filecode: str, download_url: str) -> dict:
        data = {
            "upload_option": "url",
            "filename": filecode,
            "upload_v2": "true",
            "url": download_url,
        }

        params = {
            "mode": "async",
            "format": "json",
            "action": "upload_file",
        }

        return self.session.post(
            self.url("/upload-video/"), data=data, params=params
        ).json()

    def read_file_in_chunks(
        self, file: str, chunk_size: int
    ) -> Generator[bytes, None, None]:
        with open(file, "rb") as f:
            while content := f.read(chunk_size):
                yield content

    def start_manual_upload(self, download_url: str) -> str:
        filecode = self.generate_filecode()
        filepath = self.download_file(filecode, download_url)
        filesize = os.path.getsize(filepath)

        params = {
            "mode": "async",
            "format": "json",
            "action": "upload_file",
        }

        chunks = math.ceil(filesize / self.MANUAL_UPLOAD_CHUNK_SIZE)
        fields = {
            "filename": (None, filecode),
            "realname": (None, f"{filecode}.mp4"),
            "upload_option": (None, "file"),
            "chunks": (None, str(chunks)),
            "index": (None, "1"),
            "size": (None, str(filesize)),
        }

        for i, chunk in enumerate(
            self.read_file_in_chunks(filepath, self.MANUAL_UPLOAD_CHUNK_SIZE)
        ):
            chunk_fields = {
                **fields,
                "index": (None, str(i + 1)),
                "content": ("blob", chunk, "application/octet-stream"),
            }

            response = self.session.post(
                self.url("/upload-video/"), params=params, files=chunk_fields
            ).json()
            if not response["status"] == "success":
                self.delete_filecode(filecode)
                raise Exception(response["errors"][0]["message"])

        fields["index"] = (None, "0")
        response = self.session.post(
            self.url("/upload-video/"), params=params, files=fields
        ).json()

        if response["status"] == "success":
            if filename := response["data"].get("filename"):
                self.delete_filecode(filecode)
                return filename

        self.delete_filecode(filecode)
        raise Exception(response["errors"][0]["message"])

    def download_file(self, filecode: str, download_url: str) -> str:
        filedir = os.path.join(Config.DOWNLOAD_PATH, filecode)
        if not os.path.exists(filedir):
            os.makedirs(filedir)

        filepath = os.path.join(filedir, f"{filecode}.mp4")

        with self.session.get(download_url, stream=True) as response:
            if (
                os.path.exists(filepath)
                and str(os.path.getsize(filepath)) == response.headers["Content-Length"]
            ):
                return filepath

            with open(filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)

        return filepath

    def generate_filecode(self) -> str:
        return "6" + generate(alphabet="1234567890", size=31)

    def sanitize_filename(self, filename: str) -> str:
        for regex in self.REGEX_TO_REMOVE:
            filename = re.sub(regex, "", filename)
        return filename.strip()

    def delete_filecode(self, filecode: str):
        filedir = os.path.join(Config.DOWNLOAD_PATH, filecode)
        delete_file(filedir)
