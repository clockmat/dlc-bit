import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    RSS_URL_RAW = os.environ["RSS_URL"]
    RSS_URLS = list(map(lambda x: x.strip(), RSS_URL_RAW.split("|")))

    MONGO_URL = os.environ["MONGO_URL"]
    MONGO_DATABASE = os.environ.get("MONGO_DATABASE")

    DEFAULT_FILTER_EXTENSIONS = "mkv,mp4,avi,mpg,mpeg,webm,flv,wmv,mov,m4v,3gp,ogv,mkv,avi,mpg,mpeg,webm,flv,wmv,mov,m4v,3gp,ogv"

    FILTER_EXTENSIONS = os.environ.get("FILTER_EXTENSIONS", DEFAULT_FILTER_EXTENSIONS)
    FILTER_EXTENSIONS = list(
        set(
            map(
                lambda x: x.strip().lower().replace(".", ""),
                FILTER_EXTENSIONS.split(","),
            )
        )
    )

    DOWNLOAD_PATH = os.environ.get("DOWNLOAD_PATH", "downloads")
    DOWNLOAD_PATH = os.path.abspath(DOWNLOAD_PATH)

    LOG_FILE = os.environ.get("LOG_FILE", "rssbox.log")

    # SonicBit download timeout
    DOWNLOAD_TIMEOUT = int(
        os.environ.get("DOWNLOAD_TIMEOUT", 60 * 60 * 2.5)
    )  # 2 hours and 30 minutes
    # SonicBit download retries
    DOWNLOAD_RETRIES = int(os.environ.get("DOWNLOAD_RETRIES", 5))  # 5 retries
    # SonicBit download add verify timeout
    DOWNLOAD_ADD_VERIFY_TIMEOUT = int(
        os.environ.get("DOWNLOAD_ADD_VERIFY_TIMEOUT", 15)
    )  # 15 seconds
    # SonicBit download check function timeout
    DOWNLOAD_CHECK_TIMEOUT = int(
        os.environ.get("DOWNLOAD_CHECK_TIMEOUT", 8 * 60)
    )  # 8 minutes
    # SonicBit download start function timeout
    DOWNLOAD_START_TIMEOUT = int(
        os.environ.get("DOWNLOAD_START_TIMEOUT", 2 * 60)
    )  # 2 minutes
    DOWNLOAD_ERROR_RECORD_EXPIRY = int(
        os.environ.get("DOWNLOAD_ERROR_EXPIRE_RECORD", 60 * 60 * 24 * 7)
    )  # 7 days
    DOWNLOAD_TIMEOUT_RECORD_EXPIRY = int(
        os.environ.get("DOWNLOAD_TIMEOUT_EXPIRE_RECORD", 60 * 60 * 24 * 7)
    )  # 7 days
