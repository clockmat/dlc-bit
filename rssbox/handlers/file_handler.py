from sonicbit.types import Torrent

from rssbox.config import Config
from rssbox.modules.download import Download


class FileHandler:
    """Base class for handling files"""

    def __init__(self):
        pass

    def upload(self, download: Download, torrent: Torrent) -> int:
        return 0

    def check_extension(self, ext: str):
        if ext.lower() in Config.FILTER_EXTENSIONS:
            return True
        return False
