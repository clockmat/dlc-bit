from sonicbit.types import Torrent

from rssbox.modules.download import Download


class FileHandler:
    """Base class for handling files"""

    def __init__(self):
        pass

    def upload(self, download: Download, torrent: Torrent) -> int:
        return 0
