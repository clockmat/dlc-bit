from time import sleep
import logging
import os
from rssbox.handlers.file_handler import FileHandler
from rssbox.modules.download import Download
from sonicbit.types import Torrent
from requests import Session
from nanoid import generate

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

    def url(self, path: str) -> str:
        return f"{self.__base_url}{path}"

    def upload(self, download: Download, torrent: Torrent) -> int:
        count = 0
        for torrent_file in torrent.files:
            if self.check_extension(torrent_file.extension):
               logger.info(f"Uploading {torrent_file.name} {torrent_file.extension} {torrent_file.download_url}")
               count += self.upload_file(torrent_file.download_url, download.name)
        
        return count

    def upload_file(self, download_url: str, filename: str) -> int:
        logger.info(f"Uploading {filename}")
        filecode = self.start_upload(download_url)
        filecode = self.wait_for_upload(filecode, download_url)
        self.publish(filecode, filename)
        logger.info(f"Uploaded {filename}")
        return 1

    def publish(self, filecode: str, filename: str) -> int:
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
                error = json["errors"][0]["message"]
                if error['code'] == 'duplicate':
                    return filecode
                else:
                    raise Exception(error)
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
            "url": download_url
        }

        params = {
            "mode": "async",
            "format": "json",
            "action": "upload_file",
        }

        return self.session.post(self.url("/upload-video/"), data=data, params=params).json()

    def generate_filecode(self) -> str:
        return "6" + generate(alphabet="1234567890", size=31)


