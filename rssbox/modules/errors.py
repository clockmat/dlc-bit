class SeedboxDownError(Exception):
    """Raised when the seedbox is down"""


class TorrentHashCalculationError(Exception):
    """Raised when the torrent hash cannot be calculated"""


class TooLargeTorrentError(Exception):
    """Raised when the torrent is too large"""
